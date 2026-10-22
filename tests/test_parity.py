"""Both engines must produce semantically identical outputs for the same input and params."""

import re
from datetime import date
from pathlib import Path

import pytest

from app.application.verification import first_difference, verify_outputs
from app.config import EngineSettings
from app.domain.contract import ErrorCode, WarningCode
from app.domain.models import Engine, OutputName, SourceFormat, TransformParams
from tests.conftest import RunEngine, outputs_of

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
