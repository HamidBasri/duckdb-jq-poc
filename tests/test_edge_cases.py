"""Hand-built dataset pinning down the hard cases; identical expectations for both engines."""

from typing import Any

import pytest

from app.domain.models import DatasetManifest, DatasetStatus, Engine, Entity, OutputName, SourceFormat
from tests.conftest import EngineRunner, outputs_of

CSV_FILES = {
    Entity.COUNTERPARTIES: """counterparty_id,legal_name,lei,country,sector,credit_rating,parent_counterparty_id
CPA,Alpha Holdings,LEI-A,us,Banking,aa,
CPB,Beta Bank,LEI-B,gb,Banking,a,CPA
CPC,Gamma Securities,LEI-C,de,Corporate,bbb,CPB
CPX,Xeno One,LEI-X,fr,Corporate,bb,CPY
CPY,Xeno Two,LEI-Y,fr,Corporate,bb,CPX
CPZ,Below Cycle,LEI-Z,fr,Corporate,b,CPX
CPO,Orphan Child,LEI-O,jp,Insurance,a,CP-MISSING
""",
    Entity.INSTRUMENTS: """instrument_id,isin,instrument_type,currency,issuer_counterparty_id,underlying_instrument_id,maturity_date
IN1,US0000000001,equity,usd,CPA,,
IN2,DE0000000002,OPTION,EUR,,IN1,2027-06-30
""",
    Entity.BOOKS: "book_id,desk,trader,region\nBK1,Equities,A. Rahimi,emea\n",
    Entity.FX_RATES: "currency,rate_to_usd\nUSD,1\nEUR,1.1\n",
    # TR02 amends TR01 but arrives first (forward reference); TR04 is duplicated exactly.
    Entity.TRADES: """trade_id,previous_trade_id,event_type,trade_date,settlement_date,instrument_id,counterparty_id,book_id,side,quantity,price,currency
TR02,TR01,amend,2025-01-10,2025-01-12,IN1,CPC,BK1,BUY,200,10.5,USD
TR01,,NEW,10/01/2025,2025-01-12,IN1,CPC,BK1, b ,100,10.5,usd
TR03,TR02,CANCEL,2025-01-10,2025-01-12,IN1,CPC,BK1,BUY,200,10.5,USD
TR04,,NEW,2025-02-01,2025-02-03,IN1,CPZ,BK1,SELL,10,20,USD
TR04,,NEW,2025-02-01,2025-02-03,IN1,CPZ,BK1,SELL,10,20,USD
TR05,,NEW,2025-02-01,2025-02-03,IN1,CPO,BK1,S,10,20,USD
TR07,,NEW,2025-03-01,2025-03-03,IN2,CPA,BK1,BUY,5,4,
TR08,,NEW,2025-03-01,N/A,IN1,CPA,BK1,BUY,5,4,USD
TR09,,NEW,2025-03-01,2025-03-03,IN1,CPB,BK1,buy,"1,500",2,USD
TR10,TR-MISSING,AMEND,2025-03-01,2025-03-03,IN1,CPA,BK1,BUY,5,4,USD
TR11,,NEW,2025-02-30,2025-03-03,IN1,CPA,BK1,BUY,5,4,USD
TR12,TR12,AMEND,2025-03-01,2025-03-03,IN1,CPA,BK1,BUY,5,4,USD
TR13,,NEW,2025-03-01,2025-02-27,IN1,CPA,BK1,BUY,5,4,USD
""",
}


@pytest.fixture(scope="module", params=list(Engine))
def outputs(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("edge")
    dataset_dir = root / "dataset"
    dataset_dir.mkdir()
    for entity, content in CSV_FILES.items():
        (dataset_dir / f"{entity}.csv").write_text(content)
    manifest = DatasetManifest(
        dataset_id="edge",
        origin="uploaded",
        status=DatasetStatus.READY,
        files={entity: {SourceFormat.CSV: f"{entity}.csv"} for entity in CSV_FILES},
    )
    outcome = EngineRunner(manifest, dataset_dir, root / "runs")(request.param, SourceFormat.CSV)
    return {
        "quality": outcome.quality,
        "trades": {doc["trade_id"]: doc for doc in outputs_of(outcome, OutputName.TRADES)},
        "rejects": {doc["trade_id"]: doc for doc in outputs_of(outcome, OutputName.REJECTS)},
        "exposures": {doc["ultimate_parent_id"]: doc for doc in outputs_of(outcome, OutputName.EXPOSURES)},
    }


def test_duplicates_removed(outputs: dict[str, Any]) -> None:
    assert outputs["quality"]["raw_rows"] == 13
    assert outputs["quality"]["duplicates_removed"] == 1


def test_forward_and_backward_references_resolve_amendment_chain(outputs: dict[str, Any]) -> None:
    trades = outputs["trades"]
    assert trades["TR03"]["lineage"] == {
        "root_trade_id": "TR01",
        "version": 3,
        "chain": ["TR01", "TR02", "TR03"],
        "previous_trade_id": "TR02",
        "superseded_by": None,
        "is_latest": True,
    }
    assert trades["TR01"]["lineage"]["superseded_by"] == "TR02"
    assert trades["TR01"]["lineage"]["is_latest"] is False
    assert trades["TR01"]["trade_date"] == "2025-01-10"  # DD/MM/YYYY parsed
    assert trades["TR01"]["side"] == "BUY"  # " b " normalised


def test_deep_counterparty_nesting(outputs: dict[str, Any]) -> None:
    counterparty = outputs["trades"]["TR01"]["counterparty"]
    assert counterparty["hierarchy_path"] == ["CPA", "CPB", "CPC"]
    assert counterparty["hierarchy_depth"] == 2
    assert counterparty["ultimate_parent"] == {"counterparty_id": "CPA", "legal_name": "Alpha Holdings"}
    assert counterparty["parent"] == {
        "counterparty_id": "CPB",
        "legal_name": "Beta Bank",
        "country": "GB",
        "parent": {"counterparty_id": "CPA", "legal_name": "Alpha Holdings", "country": "US", "parent": None},
    }


def test_hierarchy_cycle_is_flagged_not_looped(outputs: dict[str, Any]) -> None:
    trade = outputs["trades"]["TR04"]
    assert trade["counterparty"]["ultimate_parent"] is None
    assert trade["counterparty"]["hierarchy_path"] is None
    assert "W_COUNTERPARTY_HIERARCHY_CYCLE" in trade["data_quality"]["warnings"]


def test_orphan_parent_becomes_root(outputs: dict[str, Any]) -> None:
    trade = outputs["trades"]["TR05"]
    assert trade["counterparty"]["hierarchy_depth"] == 0
    assert trade["data_quality"]["warnings"] == ["W_COUNTERPARTY_PARENT_UNRESOLVED"]


def test_imputation_and_cross_reference_enrichment(outputs: dict[str, Any]) -> None:
    trades = outputs["trades"]
    option = trades["TR07"]
    assert option["economics"]["currency"] == "EUR"  # imputed from the instrument
    assert option["economics"]["notional_usd"] == pytest.approx(22.0)  # 5 * 4 * 1.1
    assert option["instrument"]["underlying"] == {
        "instrument_id": "IN1",
        "isin": "US0000000001",
        "instrument_type": "EQUITY",
    }
    assert option["data_quality"]["warnings"] == ["W_CURRENCY_IMPUTED"]
    assert trades["TR08"]["settlement_date"] == "2025-03-03"
    assert trades["TR08"]["data_quality"]["warnings"] == ["W_SETTLEMENT_IMPUTED"]
    assert trades["TR09"]["economics"]["quantity"] == 1500
    assert trades["TR01"]["instrument"]["issuer"]["counterparty_id"] == "CPA"


def test_self_reference_is_a_lineage_cycle(outputs: dict[str, Any]) -> None:
    trade = outputs["trades"]["TR12"]
    assert trade["lineage"]["version"] is None
    assert trade["lineage"]["superseded_by"] == "TR12"  # it references itself, so it is never "latest"
    assert "W_LINEAGE_CYCLE" in trade["data_quality"]["warnings"]


def test_rejections(outputs: dict[str, Any]) -> None:
    rejects = outputs["rejects"]
    assert set(rejects) == {"TR10", "TR11", "TR13"}
    assert rejects["TR10"]["errors"] == ["E_PREVIOUS_TRADE_UNRESOLVED"]
    assert rejects["TR11"]["errors"] == ["E_TRADE_DATE_INVALID"]  # 30 Feb is not rolled over
    assert rejects["TR13"]["errors"] == ["E_SETTLEMENT_BEFORE_TRADE"]


def test_exposures_exclude_superseded_and_cancelled(outputs: dict[str, Any]) -> None:
    group = outputs["exposures"]["CPA"]
    # TR01/TR02 are superseded and TR03 is a CANCEL, so the CPC chain contributes nothing;
    # TR12 is superseded by itself. Live trades: TR07, TR08 (CPA) and TR09 (CPB).
    assert group["trade_count"] == 3
    assert group["by_counterparty"] == [
        {"counterparty_id": "CPA", "trade_count": 2, "gross_notional_usd": pytest.approx(42.0)},
        {"counterparty_id": "CPB", "trade_count": 1, "gross_notional_usd": pytest.approx(3000.0)},
    ]
    assert group["by_currency"][0]["currency"] == "EUR"
