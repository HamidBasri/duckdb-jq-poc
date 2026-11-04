"""Row tracking queries: a page of row statuses, and one row's full journey as a timeline."""

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from app.application.projections import Repositories
from app.application.services import NotFoundError
from app.domain.models import (
    PIPELINE_STAGES,
    CorrectionRule,
    Entity,
    OutputName,
    PipelineRun,
    RowState,
    RowStatus,
    RunStatus,
    SourceFormat,
    Stage,
    StageMetrics,
)
from app.domain.ports import RowStatusReader

StepStatus = Literal["done", "stopped", "skipped"]
CLEAN_WARNINGS = {"W_CURRENCY_IMPUTED": "currency", "W_SETTLEMENT_IMPUTED": "settlement date"}


class TimelineStep(BaseModel):
    key: str
    title: str
    status: StepStatus
    at: datetime | None
    seconds: float | None
    detail: str


class RowPage(BaseModel):
    total: int
    offset: int
    limit: int
    rows: list[RowStatus]
    complete: bool = True  # False: only the rejected rows, read before the row table exists


class RowDetail(BaseModel):
    run_id: str
    row: RowStatus
    timeline: list[TimelineStep]
    source: dict[str, Any] | None
    output: dict[str, Any] | None
    reject: dict[str, Any] | None
    output_note: str | None
    complete: bool = True


def _stage_detail(stage: Stage, row: RowStatus, source_file: str) -> tuple[StepStatus, str]:
    """What happened to `row` in `stage`, in plain words."""
    stopped_index = PIPELINE_STAGES.index(row.stopped_at) if row.stopped_at else len(PIPELINE_STAGES)
    index = PIPELINE_STAGES.index(stage)
    if index > stopped_index:
        return "skipped", "Not reached."
    match stage:
        case Stage.INGEST:
            return "done", f"Read from {source_file} as row {row.source_row:,}."
        case Stage.CLEAN if row.state is RowState.DUPLICATE:
            return "stopped", f"Removed: exact copy of row {row.duplicate_of_row:,}, which was kept."
        case Stage.CLEAN:
            filled = [label for code, label in CLEAN_WARNINGS.items() if code in row.warnings]
            suffix = f" Filled in the missing {' and '.join(filled)}." if filled else ""
            return "done", f"Text trimmed, dates and numbers parsed, kept as the first copy.{suffix}"
        case Stage.VALIDATE if row.state is RowState.REJECTED:
            count = len(row.errors)
            return "stopped", f"Rejected: broke {count} rule{'s' if count != 1 else ''}."
        case Stage.VALIDATE:
            return "done", "Passed all 13 rules."
        case Stage.TRANSFORM if row.state is RowState.FILTERED_OUT:
            return (
                "stopped",
                "Left out by the run filters (date range, instrument type, size or latest only).",
            )
        case Stage.TRANSFORM:
            version = f" as version {row.version}" if row.version else ""
            return "done", f"Links resolved and written as a nested JSON document{version}."
        case Stage.AGGREGATE if row.in_exposure:
            return "done", f"Counted in the exposure of group {row.exposure_group}."
        case Stage.AGGREGATE if row.event_type == "CANCEL":
            return "done", "Not counted: this event cancels the trade."
        case Stage.AGGREGATE:
            return "done", "Not counted: a later version of this trade replaces it."
    return "done", ""


FINAL_DETAIL = {
    RowState.DELIVERED: "Delivered in the output.",
    RowState.REJECTED: "Finished as rejected; written to the rejects file with its reasons.",
    RowState.DUPLICATE: "Finished as a duplicate; not part of any output.",
    RowState.FILTERED_OUT: "Finished as filtered out; valid but not requested.",
}


def build_timeline(run: PipelineRun, row: RowStatus, source_file: str) -> list[TimelineStep]:
    """Queued → five stages → finished, using the run's real stage timestamps."""
    metrics: dict[Stage, StageMetrics] = {m.stage: m for m in run.stages}
    steps = [
        TimelineStep(
            key="queued",
            title="Queued",
            status="done",
            at=run.created_at,
            seconds=None,
            detail="Waiting for a free worker process.",
        )
    ]
    for stage in PIPELINE_STAGES:
        status, detail = _stage_detail(stage, row, source_file)
        stage_metrics = metrics.get(stage)
        reached = status != "skipped" and stage_metrics is not None
        steps.append(
            TimelineStep(
                key=stage,
                title=stage.capitalize(),
                status=status,
                at=stage_metrics.finished_at if reached and stage_metrics else None,
                seconds=stage_metrics.wall_seconds if reached and stage_metrics else None,
                detail=detail,
            )
        )
    steps.append(
        TimelineStep(
            key="finished",
            title="Finished",
            status="done",
            at=run.finished_at,
            seconds=None,
            detail=FINAL_DETAIL[row.state],
        )
    )
    return steps


def _trade_rules(run: PipelineRun) -> list[CorrectionRule]:
    return run.rules.corrections_for(Entity.TRADES) if run.rules else []


class RowTrackingService:
    def __init__(self, repositories: Repositories, reader: RowStatusReader, datasets_dir: Path) -> None:
        self._repos = repositories
        self._reader = reader
        self._datasets_dir = datasets_dir

    def tracked_runs(self) -> list[PipelineRun]:
        runs = [run for run in self._repos.runs.list() if run.status is not RunStatus.FAILED]
        return sorted(runs, key=lambda run: run.created_at, reverse=True)

    def _run(self, run_id: str) -> PipelineRun:
        run = self._repos.runs.get(run_id)
        if run is None:
            raise NotFoundError(f"run {run_id!r} not found")
        return run

    def _source(self, run: PipelineRun) -> tuple[Path, SourceFormat] | None:
        dataset = self._repos.datasets.get(run.dataset_id)
        if dataset is None:
            return None
        resolved = dataset.source_path(
            self._datasets_dir / dataset.dataset_id, Entity.TRADES, run.source_format
        )
        return resolved if resolved is not None and resolved[0].exists() else None

    def _status_path(self, run: PipelineRun) -> Path | None:
        path = run.outputs.get(OutputName.ROW_STATUS)
        return Path(path) if path and Path(path).exists() else None

    def _rejects_so_far(self, run: PipelineRun) -> tuple[Path, SourceFormat, Path] | None:
        """Source and rejects files, when validate is done but there is no row table (yet)."""
        rejects = run.outputs.get(OutputName.REJECTS)
        source = self._source(run)
        if not rejects or not Path(rejects).exists() or source is None:
            return None
        return *source, Path(rejects)

    @staticmethod
    def _no_rows(run_id: str) -> NotFoundError:
        return NotFoundError(f"run {run_id!r} has no row tracking yet (rows appear once validate is done)")

    def page(
        self, run_id: str, state: RowState | None, search: str | None, limit: int, offset: int
    ) -> RowPage:
        run = self._run(run_id)
        if path := self._status_path(run):
            total, rows = self._reader.find(path, state, search, limit, offset)
            return RowPage(total=total, offset=offset, limit=limit, rows=rows)
        if files := self._rejects_so_far(run):
            total, rows = self._reader.find_rejects(*files, _trade_rules(run), state, search, limit, offset)
            return RowPage(total=total, offset=offset, limit=limit, rows=rows, complete=False)
        raise self._no_rows(run_id)

    def detail(self, run_id: str, source_row: int) -> RowDetail:
        run = self._run(run_id)
        complete = True
        if path := self._status_path(run):
            row = self._reader.get(path, source_row)
        elif files := self._rejects_so_far(run):
            row, complete = self._reader.get_reject(*files, _trade_rules(run), source_row), False
        else:
            raise self._no_rows(run_id)
        if row is None:
            raise NotFoundError(f"row {source_row} not found in run {run_id!r}")
        source = None
        source_name = "the source file"
        if resolved := self._source(run):
            source_name = resolved[0].name
            source = self._reader.source_record(resolved[0], resolved[1], source_row)

        output, output_note = None, None
        trades_file = run.outputs.get(OutputName.TRADES)
        if row.output_offset is not None:
            if trades_file and Path(trades_file).exists():
                output = self._reader.output_document(Path(trades_file), row.output_offset)
            else:
                output_note = "The output file was deleted after the run (benchmarks free disk this way)."
        reject = None
        rejects_file = run.outputs.get(OutputName.REJECTS)
        if row.state is RowState.REJECTED and row.trade_id and rejects_file and Path(rejects_file).exists():
            reject = self._reader.reject_record(Path(rejects_file), row.trade_id)
        return RowDetail(
            run_id=run.run_id,
            row=row,
            timeline=build_timeline(run, row, source_name),
            source=source,
            output=output,
            reject=reject,
            output_note=output_note,
            complete=complete,
        )
