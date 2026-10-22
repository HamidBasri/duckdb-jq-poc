"""Per-row status tracking: every source row gets the right state, for both engines."""

from pathlib import Path

import duckdb
import pytest

from app.application.tracking import build_timeline
from app.domain.models import (
    DatasetManifest,
    DatasetStatus,
    Engine,
    OutputName,
    PipelineRun,
    RowState,
    RunOutcome,
    SourceFormat,
    TransformParams,
)
from app.infrastructure.row_tracker import DuckDbRowStatusReader
from tests.conftest import EngineRunner, RunEngine
from tests.test_edge_cases import CSV_FILES

READER = DuckDbRowStatusReader()


@pytest.fixture(scope="module")
def edge_runner(tmp_path_factory: pytest.TempPathFactory) -> EngineRunner:
    root = tmp_path_factory.mktemp("tracking")
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
    return EngineRunner(manifest, dataset_dir, root / "runs")


def rows_by_number(outcome: RunOutcome) -> dict[int, dict[str, object]]:
    _, rows = READER.find(Path(outcome.outputs[OutputName.ROW_STATUS]), None, None, 1_000, 0)
    return {row.source_row: row.model_dump() for row in rows}


@pytest.mark.parametrize("engine", list(Engine))
def test_every_source_row_gets_its_state(edge_runner: EngineRunner, engine: Engine) -> None:
    outcome = edge_runner(engine, SourceFormat.CSV, tag="track")
    rows = rows_by_number(outcome)
    assert outcome.tracking is not None and outcome.tracking.rows == 13
    assert rows[5]["state"] == RowState.DUPLICATE and rows[5]["duplicate_of_row"] == 4
    assert rows[5]["stopped_at"] == "clean"
    for number, code in (
        (10, "E_PREVIOUS_TRADE_UNRESOLVED"),
        (11, "E_TRADE_DATE_INVALID"),
        (13, "E_SETTLEMENT_BEFORE_TRADE"),
    ):
        assert rows[number]["state"] == RowState.REJECTED and rows[number]["errors"] == [code]
    assert (
        rows[7]["state"] == RowState.DELIVERED
        and rows[7]["in_exposure"]
        and rows[7]["exposure_group"] == "CPA"
    )
    assert rows[7]["warnings"] == ["W_CURRENCY_IMPUTED"]
    assert rows[2]["state"] == RowState.DELIVERED and not rows[2]["in_exposure"]  # TR01: superseded by TR02
    assert rows[3]["event_type"] == "CANCEL" and not rows[3]["in_exposure"]
    assert rows[4]["exposure_group"] == "CPZ"  # parent chain loops, so the trade groups under itself


def test_filters_mark_rows_filtered_out(edge_runner: EngineRunner) -> None:
    outcome = edge_runner(Engine.JQ, SourceFormat.CSV, TransformParams(latest_only=True), tag="latest")
    rows = rows_by_number(outcome)
    assert {n for n, r in rows.items() if r["state"] == RowState.FILTERED_OUT} == {
        1,
        2,
        12,
    }  # TR02, TR01, TR12
    assert rows[1]["stopped_at"] == "transform"


def test_engines_produce_the_same_row_table(generated: RunEngine) -> None:
    duck = generated(Engine.DUCKDB, tag="rows-duck")
    jq = generated(Engine.JQ, tag="rows-jq")
    query = "SELECT * EXCLUDE (output_offset) FROM read_parquet('{}')"
    with duckdb.connect() as con:
        differences = con.execute(
            f"SELECT count(*) FROM (({query.format(duck.outputs[OutputName.ROW_STATUS])}) "
            f"EXCEPT ({query.format(jq.outputs[OutputName.ROW_STATUS])}))"
        ).fetchone()
    assert differences == (0,)
    assert duck.tracking is not None and jq.tracking is not None
    assert duck.tracking.states == jq.tracking.states


def test_output_offsets_point_at_the_right_document(generated: RunEngine) -> None:
    outcome = generated(Engine.JQ, tag="offsets")
    _, rows = READER.find(Path(outcome.outputs[OutputName.ROW_STATUS]), RowState.DELIVERED, None, 25, 0)
    for row in rows:
        assert row.output_offset is not None
        document = READER.output_document(Path(outcome.outputs[OutputName.TRADES]), row.output_offset)
        assert document["trade_id"] == row.trade_id


def test_timeline_stops_at_the_removing_stage(edge_runner: EngineRunner) -> None:
    outcome = edge_runner(Engine.DUCKDB, SourceFormat.CSV, tag="timeline")
    duplicate = READER.get(Path(outcome.outputs[OutputName.ROW_STATUS]), 5)
    assert duplicate is not None
    run = PipelineRun(
        run_id="r",
        comparison_id="c",
        dataset_id="edge",
        engine=Engine.DUCKDB,
        source_format=SourceFormat.CSV,
        params=TransformParams(),
        stages=outcome.stages,
    )
    steps = {step.key: step for step in build_timeline(run, duplicate, "trades.csv")}
    assert steps["ingest"].status == "done" and steps["ingest"].at is not None
    assert steps["clean"].status == "stopped" and "row 4" in steps["clean"].detail
    assert [steps[key].status for key in ("validate", "transform", "aggregate")] == ["skipped"] * 3
