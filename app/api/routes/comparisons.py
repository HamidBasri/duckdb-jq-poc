"""Service 2: cleaning, validation and transformation - run engines and compare them."""

import json
from itertools import islice
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import FileResponse

from app.api.deps import ContainerDep
from app.api.schemas import ComparisonRequest
from app.application.reporting import ComparisonReport
from app.domain.models import OutputName, PipelineRun

router = APIRouter(prefix="/api", tags=["comparisons"])

MAX_PREVIEW_ROWS = 500


def _output_path(run: PipelineRun, name: OutputName) -> Path:
    if name not in run.outputs or not (path := Path(run.outputs[name])).exists():
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run.run_id} has no {name} output (yet)")
    return path


@router.post("/comparisons", status_code=status.HTTP_202_ACCEPTED)
async def request_comparison(request: ComparisonRequest, container: ContainerDep) -> ComparisonReport:
    """Run the full clean -> validate -> transform -> aggregate pipeline on each engine."""
    comparison = await container.comparisons.request(
        request.dataset_id,
        request.engines,
        request.source_format,
        request.params,
        request.verify,
        request.parallel_engines,
    )
    return container.reports.comparison(comparison)


@router.get("/comparisons")
async def list_comparisons(container: ContainerDep) -> list[ComparisonReport]:
    return [container.reports.comparison(comparison) for comparison in container.comparisons.list_all()]


@router.get("/comparisons/{comparison_id}")
async def get_comparison(comparison_id: str, container: ContainerDep) -> ComparisonReport:
    return container.reports.comparison(container.comparisons.get(comparison_id))


@router.get("/runs/{run_id}")
async def get_run(run_id: str, container: ContainerDep) -> PipelineRun:
    return container.comparisons.get_run(run_id)


@router.get("/runs/{run_id}/outputs/{name}")
async def preview_output(
    run_id: str,
    name: OutputName,
    container: ContainerDep,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=MAX_PREVIEW_ROWS),
) -> Any:
    """A page of an output's JSON documents (the quality report is returned whole)."""
    path = _output_path(container.comparisons.get_run(run_id), name)
    if name is OutputName.QUALITY_REPORT:
        return json.loads(path.read_text())
    with path.open() as handle:
        return [json.loads(line) for line in islice(handle, offset, offset + limit)]


@router.get("/runs/{run_id}/outputs/{name}/download")
async def download_output(run_id: str, name: OutputName, container: ContainerDep) -> FileResponse:
    """Stream the complete output file (every row)."""
    path = _output_path(container.comparisons.get_run(run_id), name)
    media_type = "application/json" if name is OutputName.QUALITY_REPORT else "application/x-ndjson"
    return FileResponse(path, media_type=media_type, filename=f"{run_id}-{path.name}")
