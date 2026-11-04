"""Browsing raw and output data, editing rows as a new version, and versioned rules applied to new runs."""

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.domain.models import Entity
from tests.test_api import wait_for
from tests.test_edge_cases import CSV_FILES
from tests.test_parity import RULE_BREAKERS


@pytest.fixture(scope="module")
def client(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TestClient]:
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("POC_DATA_DIR", str(tmp_path_factory.mktemp("rules-data")))
        get_settings.cache_clear()
        from app.main import create_app

        with TestClient(create_app()) as test_client:
            yield test_client
    get_settings.cache_clear()


@pytest.fixture(scope="module")
def dataset_id(client: TestClient) -> str:
    """The edge-case files plus one trade breaking each switchable rule, uploaded as CSV."""
    files = {
        str(entity): (f"{entity}.csv", content + (RULE_BREAKERS if entity is Entity.TRADES else ""))
        for entity, content in CSV_FILES.items()
    }
    upload = client.post("/api/datasets/upload", files=files).json()
    assert settled(client, upload["dataset_id"])["status"] == "ready"
    return str(upload["dataset_id"])


def settled(client: TestClient, dataset_id: str) -> dict[str, Any]:
    return wait_for(
        lambda: client.get(f"/api/datasets/{dataset_id}").json(), lambda d: d["status"] != "pending"
    )


def run_both(client: TestClient, dataset_id: str) -> dict[str, Any]:
    comparison_id = client.post("/api/comparisons", json={"dataset_id": dataset_id}).json()["comparison"][
        "comparison_id"
    ]
    return wait_for(
        lambda: client.get(f"/api/comparisons/{comparison_id}").json(),
        lambda r: r["comparison"]["status"] == "completed",
    )


def test_browse_a_raw_table(client: TestClient, dataset_id: str) -> None:
    page = client.get(f"/api/datasets/{dataset_id}/tables/trades", params={"limit": 3}).json()
    assert page["total"] == 20 and page["row_numbers"] == [1, 2, 3]
    assert page["columns"][0] == "trade_id" and page["rows"][1][0] == "TR01"
    found = client.get(f"/api/datasets/{dataset_id}/tables/trades", params={"q": "HOLD"}).json()
    assert found["total"] == 1 and found["rows"][0][0] == "TR93"
    by_row = client.get(f"/api/datasets/{dataset_id}/tables/trades", params={"q": "5"}).json()
    assert 5 in by_row["row_numbers"]
    assert client.get(f"/api/datasets/{dataset_id}/tables/nope").status_code == 422


def test_edits_are_saved_as_a_new_version(client: TestClient, dataset_id: str) -> None:
    bad_column = client.post(
        f"/api/datasets/{dataset_id}/edits",
        json={"edits": [{"entity": "trades", "row": 1, "column": "nope", "value": "x"}]},
    )
    assert bad_column.status_code == 422 and "nope" in bad_column.text
    bad_row = client.post(
        f"/api/datasets/{dataset_id}/edits", json={"deleted_rows": [{"entity": "trades", "row": 999}]}
    )
    assert bad_row.status_code == 422
    assert client.post(f"/api/datasets/{dataset_id}/edits", json={}).status_code == 422

    version = client.post(
        f"/api/datasets/{dataset_id}/edits",
        json={
            "edits": [{"entity": "trades", "row": 17, "column": "side", "value": "BUY"}],  # TR93: HOLD
            "deleted_rows": [{"entity": "trades", "row": 1}],  # TR02
        },
    ).json()
    assert version["parent_id"] == dataset_id and version["version"] == 2
    ready = settled(client, version["dataset_id"])
    assert ready["status"] == "ready" and ready["row_counts"]["trades"] == 19
    edited = client.get(f"/api/datasets/{version['dataset_id']}/tables/trades", params={"q": "TR93"}).json()
    assert edited["rows"][0][edited["columns"].index("side")] == "BUY"
    original = client.get(f"/api/datasets/{dataset_id}/tables/trades", params={"q": "TR93"}).json()
    assert original["rows"][0][original["columns"].index("side")] == "HOLD"


def test_rules_are_versioned_and_checked(client: TestClient) -> None:
    overview = client.get("/api/rules").json()
    assert overview["current"]["version"] == 0
    locked = {code["code"] for code in overview["codes"] if code["locked"]}
    assert "E_TRADE_ID_MISSING" in locked and "E_SIDE_INVALID" not in locked
    assert overview["columns"]["trades"][0] == "trade_id"

    refused = client.put("/api/rules", json={"disabled_rules": ["E_TRADE_ID_MISSING"]})
    assert refused.status_code == 422 and "can't be switched off" in refused.text
    unknown = client.put(
        "/api/rules",
        json={"corrections": [{"entity": "trades", "column": "nope", "match": "a", "replace": "b"}]},
    )
    assert unknown.status_code == 422
    rule = {"entity": "trades", "column": "side", "match": "HOLD", "replace": "BUY"}
    assert client.put("/api/rules", json={"corrections": [rule, rule]}).status_code == 422


def test_new_runs_follow_the_rules_and_record_them(client: TestClient, dataset_id: str) -> None:
    before = run_both(client, dataset_id)
    assert before["comparison"]["rules_version"] == 0
    rejected_before = before["runs"][0]["quality"]["error_counts"]
    assert rejected_before["E_SIDE_INVALID"] >= 1 and rejected_before["E_SETTLEMENT_BEFORE_TRADE"] >= 1

    saved = client.put(
        "/api/rules",
        json={
            "corrections": [
                {
                    "entity": "trades",
                    "column": "side",
                    "match": "HOLD",
                    "replace": "BUY",
                    "note": "desk confirmed",
                }
            ],
            "disabled_rules": ["E_SETTLEMENT_BEFORE_TRADE"],
            "note": "fix HOLD trades",
        },
    ).json()
    assert saved["version"] == 1
    again = client.put(
        "/api/rules", json={"corrections": saved["corrections"], "disabled_rules": saved["disabled_rules"]}
    )
    assert again.json()["version"] == 1  # nothing changed, no new version
    assert [r["version"] for r in client.get("/api/rules/history").json()] == [1]

    after = run_both(client, dataset_id)
    assert after["equivalent"] is True, after
    assert after["comparison"]["rules_version"] == 1
    counts = after["runs"][0]["quality"]["error_counts"]
    assert (
        "E_SETTLEMENT_BEFORE_TRADE" not in counts
        and counts.get("E_SIDE_INVALID", 0) < rejected_before["E_SIDE_INVALID"]
    )
    run_id = after["runs"][0]["run_id"]
    assert client.get(f"/api/runs/{run_id}").json()["rules"]["version"] == 1
    for trade_id in ("TR93", "TR13"):  # HOLD corrected to BUY; settles before trade but the rule is off
        page = client.get(f"/api/runs/{run_id}/outputs/trades/page", params={"q": trade_id}).json()
        assert page["total"] == 1 and page["documents"][0]["trade_id"] == trade_id
    old_run = before["runs"][0]["run_id"]
    assert client.get(f"/api/runs/{old_run}").json()["rules"]["version"] == 0  # old runs keep their rules


def test_browse_outputs_page_by_page(client: TestClient, dataset_id: str) -> None:
    report = run_both(client, dataset_id)
    run = report["runs"][0]
    first = client.get(f"/api/runs/{run['run_id']}/outputs/trades/page", params={"limit": 4}).json()
    assert first["total"] == run["quality"]["output_rows"] and len(first["documents"]) == 4
    rest = client.get(
        f"/api/runs/{run['run_id']}/outputs/trades/page", params={"offset": 4, "limit": 500}
    ).json()
    ids = [d["trade_id"] for d in first["documents"] + rest["documents"]]
    assert len(ids) == len(set(ids)) == first["total"]
    rejects = client.get(f"/api/runs/{run['run_id']}/outputs/rejects/page").json()
    assert rejects["total"] == run["quality"]["rejected_rows"] and rejects["documents"][0]["errors"]
    assert client.get(f"/api/runs/{run['run_id']}/outputs/quality_report/page").status_code == 422
