"""Row tracking: follow every input row through a pipeline run."""

import asyncio
from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse

from app.api.deps import ContainerDep
from app.application.tracking import RowDetail, RowPage
from app.domain.models import PipelineRun, RowState

router = APIRouter(tags=["row tracking"])

TRACKER_PAGE = Path(__file__).parents[1] / "static" / "tracker.html"
MAX_PAGE_ROWS = 200


@router.get("/tracker", include_in_schema=False)
async def tracker_page() -> FileResponse:
    return FileResponse(TRACKER_PAGE)


@router.get("/api/runs")
async def list_runs(container: ContainerDep, limit: int = Query(50, ge=1, le=500)) -> list[PipelineRun]:
    """Recent pipeline runs, newest first (failed runs are left out)."""
    return container.rows.tracked_runs()[:limit]


@router.get("/api/runs/{run_id}/rows")
async def list_rows(
    run_id: str,
    container: ContainerDep,
    state: RowState | None = None,
    q: str | None = Query(None, description="Trade id (partial) or source row number"),
    limit: int = Query(50, ge=1, le=MAX_PAGE_ROWS),
    offset: int = Query(0, ge=0),
) -> RowPage:
    """One page of the run's per-row status table."""
    return await asyncio.to_thread(container.rows.page, run_id, state, q, limit, offset)


@router.get("/api/runs/{run_id}/rows/{source_row}")
async def row_detail(run_id: str, source_row: int, container: ContainerDep) -> RowDetail:
    """One row's journey: timeline, the source record before, and the JSON document after."""
    return await asyncio.to_thread(container.rows.detail, run_id, source_row)
