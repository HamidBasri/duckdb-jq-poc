"""End-to-end through HTTP: generate -> compare (process pool + event bus) -> inspect outputs."""

import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings

POLL_SECONDS = 0.25
TIMEOUT_SECONDS = 120


@pytest.fixture(scope="module")
def client(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TestClient]:
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("POC_DATA_DIR", str(tmp_path_factory.mktemp("api-data")))
        get_settings.cache_clear()
        from app.main import create_app

        with TestClient(create_app()) as test_client:
            yield test_client
    get_settings.cache_clear()


def wait_for(fetch: Callable[[], dict[str, Any]], done: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if done(body := fetch()):
            return body
        time.sleep(POLL_SECONDS)
    raise TimeoutError(body)


def test_generate_compare_and_read_outputs(client: TestClient) -> None:
    response = client.post("/api/datasets/generate", json={"rows": 2_000, "seed": 3})
    assert response.status_code == 202
    dataset_id = response.json()["dataset_id"]
    dataset = wait_for(
        lambda: client.get(f"/api/datasets/{dataset_id}").json(), lambda d: d["status"] != "pending"
    )
    assert dataset["status"] == "ready"
    assert dataset["row_counts"]["trades"] == 2_000

    again = client.post("/api/datasets/generate", json={"rows": 2_000, "seed": 3}).json()
    assert again["created_at"] == dataset["created_at"]  # idempotent: same spec, same dataset

    comparison_id = client.post("/api/comparisons", json={"dataset_id": dataset_id}).json()["comparison"][
        "comparison_id"
    ]
    report = wait_for(
        lambda: client.get(f"/api/comparisons/{comparison_id}").json(),
        lambda r: r["comparison"]["status"] == "completed",
    )
    assert report["equivalent"] is True, report
    assert {run["engine"] for run in report["runs"]} == {"duckdb", "jq"}
    for run in report["runs"]:
        assert run["status"] == "succeeded"
        assert list(run["stages"]) == ["ingest", "clean", "validate", "transform", "aggregate"]
    assert report["slowdown_vs_duckdb"]["jq"]["total"] > 0

    run_id = report["runs"][0]["run_id"]
    preview = client.get(f"/api/runs/{run_id}/outputs/trades", params={"limit": 2}).json()
    assert len(preview) == 2 and "lineage" in preview[0]
    rows = client.get(f"/api/runs/{run_id}/rows", params={"state": "rejected", "limit": 5}).json()
    assert rows["total"] == report["runs"][0]["quality"]["rejected_rows"]
    detail = client.get(f"/api/runs/{run_id}/rows/{rows['rows'][0]['source_row']}").json()
    assert (
        detail["row"]["state"] == "rejected" and detail["source"] is not None and detail["reject"] is not None
    )
    assert detail["timeline"][0]["key"] == "queued"
    assert client.get("/tracker").status_code == 200
    assert any(run["run_id"] == run_id for run in client.get("/api/runs").json())

    download = client.get(f"/api/runs/{run_id}/outputs/trades/download")
    assert download.status_code == 200
    assert download.text.count("\n") == report["runs"][0]["quality"]["output_rows"]


def test_upload_reuses_contract(client: TestClient, tmp_path: Path) -> None:
    trades = tmp_path / "t.csv"
    trades.write_text(
        "trade_id,previous_trade_id,event_type,trade_date,settlement_date,instrument_id,"
        "counterparty_id,book_id,side,quantity,price,currency\nT1,,NEW,2025-01-02,,I1,C1,B1,BUY,1,2,USD\n"
    )
    with trades.open("rb") as handle:
        response = client.post("/api/datasets/upload", files={"trades": ("trades.csv", handle, "text/csv")})
    assert response.status_code == 202
    dataset_id = response.json()["dataset_id"]
    dataset = wait_for(
        lambda: client.get(f"/api/datasets/{dataset_id}").json(), lambda d: d["status"] != "pending"
    )
    assert dataset["status"] == "ready" and dataset["row_counts"] == {"trades": 1}


def test_upload_rejects_missing_columns(client: TestClient, tmp_path: Path) -> None:
    bad = tmp_path / "bad.csv"
    bad.write_text("trade_id,price\nT1,2\n")
    with bad.open("rb") as handle:
        dataset_id = client.post("/api/datasets/upload", files={"trades": ("bad.csv", handle)}).json()[
            "dataset_id"
        ]
    dataset = wait_for(
        lambda: client.get(f"/api/datasets/{dataset_id}").json(), lambda d: d["status"] != "pending"
    )
    assert dataset["status"] == "failed" and "missing columns" in dataset["error"]


def test_unknown_dataset_is_404(client: TestClient) -> None:
    assert client.post("/api/comparisons", json={"dataset_id": "nope"}).status_code == 404
