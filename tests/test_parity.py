"""Both engines must produce semantically identical outputs for the same input and params."""

import re
from datetime import date
from pathlib import Path

import pytest

from app.application.verification import first_difference, verify_outputs
from app.config import EngineSettings
from app.domain.contract import LOCKED_RULES, ErrorCode, WarningCode
from app.domain.models import (
    DatasetManifest,
    DatasetStatus,
    Engine,
    Entity,
    OutputName,
    RuleSet,
    SourceFormat,
    TransformParams,
)
from tests.conftest import EngineRunner, RunEngine, outputs_of
from tests.test_edge_cases import CSV_FILES

ENGINES_DIR = Path(__file__).parents[1] / "app" / "engines"


def assert_equivalent(run: RunEngine, source_format: SourceFormat, params: TransformParams, tag: str) -> int:
    duck = run(Engine.DUCKDB, source_format, params, tag)
    jq = run(Engine.JQ, source_format, params, tag)
    result = verify_outputs(Engine.DUCKDB, duck.outputs, Engine.JQ, jq.outputs, 1_000_000, 1_000_000)
    assert result.equivalent, result.model_dump()
    return len(outputs_of(duck, OutputName.TRADES))


@pytest.mark.parametrize("source_format", list(SourceFormat))
def test_engines_equivalent_on_generated_data(generated: RunEngine, source_format: SourceFormat) -> None:
    assert assert_equivalent(generated, source_format, TransformParams(), "all") > 2_500


def test_engines_equivalent_with_filters(generated: RunEngine) -> None:
    params = TransformParams(
        trade_date_from=date(2025, 3, 1),
        trade_date_to=date(2025, 9, 30),
        instrument_types=["equity", "option"],
        min_notional_usd=10_000,
        latest_only=True,
    )
    filtered = assert_equivalent(generated, SourceFormat.PARQUET, params, "filtered")
    assert 0 < filtered < 1_500


def test_csv_and_parquet_sources_agree(generated: RunEngine) -> None:
    from_parquet = generated(Engine.DUCKDB, SourceFormat.PARQUET, tag="fmt")
    from_csv = generated(Engine.DUCKDB, SourceFormat.CSV, tag="fmt")
    assert first_difference(from_parquet.quality, from_csv.quality) is None


def test_rule_codes_match_contract() -> None:
    """The SQL scripts and jq programs must emit exactly the codes the contract declares."""
    for engine_dir, pattern in (("duckdb_engine/sql", "*.sql"), ("jq_engine/programs", "*.jq")):
        source = " ".join(path.read_text() for path in (ENGINES_DIR / engine_dir).glob(pattern))
        assert set(re.findall(r"\bE_[A-Z_]+\b", source)) == {code.value for code in ErrorCode}, engine_dir
        assert set(re.findall(r"\bW_[A-Z_]+\b", source)) == {code.value for code in WarningCode}, engine_dir


@pytest.mark.parametrize(
    "settings",
    [
        EngineSettings(duckdb_lean=True),
        EngineSettings(duckdb_storage="disk", duckdb_memory_limit="256MB"),
        EngineSettings(duckdb_preserve_insertion_order=False, duckdb_threads=2),
    ],
    ids=["lean", "disk-256MB", "unordered-2-threads"],
)
def test_duckdb_memory_profiles_do_not_change_results(generated: RunEngine, settings: EngineSettings) -> None:
    default = generated(Engine.DUCKDB, tag="profile-default")
    tuned = generated(
        Engine.DUCKDB, tag=f"profile-{settings.duckdb_storage}-{settings.duckdb_lean}", settings=settings
    )
    result = verify_outputs(
        Engine.DUCKDB, default.outputs, Engine.DUCKDB, tuned.outputs, 1_000_000, 1_000_000
    )
    assert result.equivalent, result.model_dump()


def test_failed_run_removes_partial_outputs(generated: RunEngine) -> None:
    outcome = generated(
        Engine.DUCKDB, tag="oom", settings=EngineSettings(duckdb_memory_limit="1MB"), check=False
    )
    assert not outcome.succeeded and "Out of Memory" in (outcome.error or "")
    assert outcome.outputs == {}
    assert not (generated.run_dir(Engine.DUCKDB, SourceFormat.PARQUET, "oom") / "output").exists()


# One trade per rule that a user may switch off, on top of the edge-case files.
RULE_BREAKERS = """TR90,,SWAP,2025-03-01,2025-03-03,IN1,CPA,BK1,BUY,5,4,USD
TR91,TR01,NEW,2025-03-01,2025-03-03,IN1,CPA,BK1,BUY,5,4,USD
TR92,,AMEND,2025-03-01,2025-03-03,IN1,CPA,BK1,BUY,5,4,USD
TR93,,NEW,2025-03-01,2025-03-03,IN1,CPA,BK1,HOLD,5,4,USD
TR94,,NEW,2025-03-01,2025-03-03,IN1,CPA,BK1,BUY,5,-4,USD
TR95,,NEW,2025-03-01,2025-03-03,IN1,CPA,BK1,BUY,5,N/A,USD
TR96,,NEW,,2025-03-03,IN1,CPA,BK1,BUY,5,4,USD
"""
SWITCHABLE = [code for code in ErrorCode if code not in LOCKED_RULES]


@pytest.fixture(scope="module")
def breakers(tmp_path_factory: pytest.TempPathFactory) -> EngineRunner:
    root = tmp_path_factory.mktemp("breakers")
    dataset_dir = root / "dataset"
    dataset_dir.mkdir()
    for entity, content in CSV_FILES.items():
        (dataset_dir / f"{entity}.csv").write_text(
            content + (RULE_BREAKERS if entity is Entity.TRADES else "")
        )
    manifest = DatasetManifest(
        dataset_id="breakers",
        origin="uploaded",
        status=DatasetStatus.READY,
        files={entity: {SourceFormat.CSV: f"{entity}.csv"} for entity in CSV_FILES},
    )
    return EngineRunner(manifest, dataset_dir, root / "runs")


@pytest.mark.parametrize("code", SWITCHABLE)
def test_engines_agree_with_a_rule_switched_off(breakers: EngineRunner, code: ErrorCode) -> None:
    """Switching a reject rule off must not crash either engine or make them disagree."""
    baseline = breakers(Engine.DUCKDB, SourceFormat.CSV, tag=f"on-{code.lower()}")
    assert baseline.quality is not None and baseline.quality["error_counts"].get(code, 0) > 0, (
        "rule never fired"
    )
    rules = RuleSet(version=1, disabled_rules=[code])
    duck = breakers(Engine.DUCKDB, SourceFormat.CSV, tag=f"off-{code.lower()}", rules=rules)
    jq = breakers(Engine.JQ, SourceFormat.CSV, tag=f"off-{code.lower()}", rules=rules)
    result = verify_outputs(Engine.DUCKDB, duck.outputs, Engine.JQ, jq.outputs, 1_000_000, 1_000_000)
    assert result.equivalent, result.model_dump()
    assert duck.quality is not None and code not in duck.quality["error_counts"]
    assert duck.quality["valid_rows"] > baseline.quality["valid_rows"]
