"""Fixing an upload's columns as a new dataset version, and deleting runs, benchmarks and datasets."""

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.domain.contract import suggest_mapping
from app.domain.models import (
    Benchmark,
    Comparison,
    Engine,
    Entity,
    GenerationSpec,
    PipelineRun,
    SourceFormat,
    TransformParams,
    utc_now,
)
from tests.test_api import wait_for
from tests.test_edge_cases import CSV_FILES

RENAMED = {
    "trade_id": "tradeId",
    "previous_trade_id": "prevTradeId",
    "quantity": "qty",
    "price": "px",
    "currency": "ccy",
}


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("versions-data")


@pytest.fixture(scope="module")
def client(data_dir: Path) -> Iterator[TestClient]:
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("POC_DATA_DIR", str(data_dir))
        get_settings.cache_clear()
        from app.main import create_app

        with TestClient(create_app()) as test_client:
            yield test_client
    get_settings.cache_clear()


def settled(client: TestClient, dataset_id: str) -> dict[str, Any]:
    return wait_for(
        lambda: client.get(f"/api/datasets/{dataset_id}").json(), lambda d: d["status"] != "pending"
    )


def test_suggest_mapping_matches_names_case_and_synonyms() -> None:
    found = ["TRADE_ID", "qty", "Px", "ccy", "unrelated"]
    mapping = suggest_mapping(["trade_id", "quantity", "price", "currency", "side"], found)
    assert mapping == {
        "trade_id": "TRADE_ID",
        "quantity": "qty",
        "price": "Px",
        "currency": "ccy",
        "side": None,
    }


def test_fix_columns_as_a_new_version_then_run_it(client: TestClient) -> None:
    header, *rows = CSV_FILES[Entity.TRADES].splitlines()
    renamed = ",".join(RENAMED.get(column, column) for column in header.split(","))
    files = {
        str(entity): (
            f"{entity}.csv",
            content if entity is not Entity.TRADES else "\n".join([renamed, *rows]),
        )
        for entity, content in CSV_FILES.items()
    }
    upload = client.post("/api/datasets/upload", files=files).json()
    assert upload["number"] >= 1
    failed = settled(client, upload["dataset_id"])
    assert failed["status"] == "failed" and "trade_id" in str(failed["error"])

    columns = {
        item["entity"]: item for item in client.get(f"/api/datasets/{upload['dataset_id']}/columns").json()
    }
    trades = columns["trades"]
    assert {"trade_id", "quantity", "price", "currency"} <= set(trades["missing"])
    assert trades["suggested"]["trade_id"] == "tradeId" and trades["suggested"]["quantity"] == "qty"
    assert len(trades["samples"]["tradeId"]) == 3

    bad = client.post(
        f"/api/datasets/{upload['dataset_id']}/versions", json={"mapping": {"trades": {"trade_id": "nope"}}}
    )
    assert bad.status_code == 422 and "nope" in bad.text
    no_id = client.post(
        f"/api/datasets/{upload['dataset_id']}/versions", json={"mapping": {"trades": {"trade_id": None}}}
    )
    assert no_id.status_code == 422 and "trade_id" in no_id.text

    version = client.post(f"/api/datasets/{upload['dataset_id']}/versions", json={"mapping": {}}).json()
    assert version["dataset_id"] == f"{upload['dataset_id']}-v2"
    assert version["parent_id"] == upload["dataset_id"] and version["version"] == 2
    assert version["number"] == 0  # versions are named after their family, not numbered
    ready = settled(client, version["dataset_id"])
    assert ready["status"] == "ready", ready
    assert ready["row_counts"]["trades"] == len(rows)

    comparison_id = client.post("/api/comparisons", json={"dataset_id": version["dataset_id"]}).json()[
        "comparison"
    ]["comparison_id"]
    report = wait_for(
        lambda: client.get(f"/api/comparisons/{comparison_id}").json(),
        lambda r: r["comparison"]["status"] == "completed",
    )
    assert report["equivalent"] is True, report
    assert report["runs"][0]["quality"]["raw_rows"] == len(rows)


def test_delete_run_dataset_and_clear_history(client: TestClient, data_dir: Path) -> None:
    dataset_id = client.post("/api/datasets/generate", json={"rows": 400, "seed": 11}).json()["dataset_id"]
    assert settled(client, dataset_id)["status"] == "ready"

    comparison = client.post(
        "/api/comparisons", json={"dataset_id": dataset_id, "engines": ["duckdb"]}
    ).json()
    comparison_id = comparison["comparison"]["comparison_id"]
    run_id = comparison["runs"][0]["run_id"]
    number = comparison["comparison"]["number"]
    assert number >= 1 and client.get(f"/api/runs/{run_id}").json()["number"] == number
    wait_for(
        lambda: client.get(f"/api/comparisons/{comparison_id}").json(),
        lambda r: r["comparison"]["status"] == "completed",
    )
    assert (data_dir / "runs" / run_id).exists()
    assert client.delete(f"/api/comparisons/{comparison_id}").status_code == 204
    assert client.get(f"/api/comparisons/{comparison_id}").status_code == 404
    assert client.get(f"/api/runs/{run_id}").status_code == 404
    assert not (data_dir / "runs" / run_id).exists()

    benchmark = client.post(
        "/api/benchmarks", json={"sizes": [300], "engines": ["duckdb"], "seed": 11}
    ).json()["benchmark"]
    benchmark_id = benchmark["benchmark_id"]
    assert benchmark["number"] >= 1
    report = wait_for(
        lambda: client.get(f"/api/benchmarks/{benchmark_id}").json(),
        lambda r: r["benchmark"]["status"] in ("completed", "failed"),
    )
    # the deleted run's number is not handed out again
    assert report["rows"][0]["run_number"] == number + 1
    cleared = client.delete("/api/history").json()
    assert cleared["benchmarks"] >= 1 and cleared["runs"] >= 1 and cleared["kept"] == 0
    assert client.get("/api/benchmarks").json() == [] and client.get("/api/comparisons").json() == []

    assert client.delete(f"/api/datasets/{dataset_id}").status_code == 204
    assert client.get(f"/api/datasets/{dataset_id}").status_code == 404
    assert not (data_dir / "datasets" / dataset_id).exists()
    assert client.delete("/api/datasets/missing").status_code == 404


def test_records_saved_before_numbering_get_numbers_oldest_first(tmp_path: Path) -> None:
    """Runs, benchmarks and their engine runs from before numbering are numbered at startup."""
    state = tmp_path / "state"
    for folder in ("comparisons", "runs", "benchmarks"):
        (state / folder).mkdir(parents=True)
    params, now = TransformParams(), utc_now()
    for age, name in ((2, "old"), (1, "new")):
        run = PipelineRun(
            run_id=f"run-{name}",
            comparison_id=f"cmp-{name}",
            dataset_id="gone",
            engine=Engine.DUCKDB,
            source_format=SourceFormat.PARQUET,
            params=params,
        )
        comparison = Comparison(
            comparison_id=f"cmp-{name}",
            dataset_id="gone",
            source_format=SourceFormat.PARQUET,
            params=params,
            engines=[Engine.DUCKDB],
            run_ids={Engine.DUCKDB: run.run_id},
            created_at=now - timedelta(hours=age),
        )
        (state / "runs" / f"{run.run_id}.json").write_text(run.model_dump_json(exclude={"number"}))
        (state / "comparisons" / f"{comparison.comparison_id}.json").write_text(
            comparison.model_dump_json(exclude={"number"})
        )
    benchmark = Benchmark(
        benchmark_id="bench-old",
        specs=[GenerationSpec(rows=300)],
        engines=[Engine.DUCKDB],
        source_format=SourceFormat.PARQUET,
        params=params,
        verify=False,
        discard_outputs=True,
    )
    (state / "benchmarks" / "bench-old.json").write_text(benchmark.model_dump_json(exclude={"number"}))

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("POC_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        from app.main import create_app

        with TestClient(create_app()) as client:
            numbers = {
                r["comparison"]["comparison_id"]: r["comparison"]["number"]
                for r in client.get("/api/comparisons").json()
            }
            assert numbers == {"cmp-old": 1, "cmp-new": 2}
            assert [client.get(f"/api/runs/run-{name}").json()["number"] for name in ("old", "new")] == [1, 2]
            assert client.get("/api/benchmarks/bench-old").json()["benchmark"]["number"] == 1
    get_settings.cache_clear()
