"""Pipeline runner: drives an engine through the five stages and measures each one.

Runs inside a worker process. It knows nothing about HTTP or the event bus; it reports
progress through an `EventSink` and returns a `RunOutcome`.
"""

import json
import logging
import os
import shutil
from typing import Any

from app.config import EngineSettings
from app.domain.events import RunStarted, StageCompleted, StageStarted
from app.domain.models import (
    PIPELINE_STAGES,
    Engine,
    OutputName,
    RowTrackingSummary,
    RunOutcome,
    Stage,
    StageMetrics,
    utc_now,
)
from app.domain.ports import (
    EngineFactory,
    EventSink,
    MonitorFactory,
    RowTracker,
    RunContext,
    StageResult,
    TransformationEngine,
)

logger = logging.getLogger(__name__)


class ResourceLimitExceeded(RuntimeError):
    """A child process (jq) was killed for exceeding the configured memory budget."""


class PipelineRunner:
    """Template method: the stage sequence and measurement are fixed; engines vary."""

    def __init__(
        self,
        settings: EngineSettings,
        sink: EventSink,
        engine_factory: EngineFactory,
        monitor_factory: MonitorFactory,
        row_tracker: RowTracker | None = None,
    ) -> None:
        self._settings = settings
        self._sink = sink
        self._engine_factory = engine_factory
        self._monitor_factory = monitor_factory
        self._row_tracker = row_tracker

    def run(self, ctx: RunContext) -> RunOutcome:
        ctx.work_dir.mkdir(parents=True, exist_ok=True)
        ctx.output_dir.mkdir(parents=True, exist_ok=True)
        self._sink.put(RunStarted(run_id=ctx.run_id, worker_pid=os.getpid()))
        stages: list[StageMetrics] = []
        details: dict[str, dict[str, Any]] = {}
        outputs: dict[OutputName, str] = {}
        engine: TransformationEngine | None = None
        try:
            engine = self._engine_factory(ctx.engine, self._settings, ctx.work_dir)
            for stage in PIPELINE_STAGES:
                self._sink.put(StageStarted(run_id=ctx.run_id, stage=stage))
                result, metrics = self._measure(engine, ctx, stage)
                stages.append(metrics)
                details[stage] = result.detail
                outputs |= {name: str(path) for name, path in result.outputs.items()}
                self._sink.put(StageCompleted(run_id=ctx.run_id, metrics=metrics))
            quality = build_quality_report(details)
            quality_path = ctx.output_path(OutputName.QUALITY_REPORT)
            quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True))
            outputs[OutputName.QUALITY_REPORT] = str(quality_path)
            tracking = self._track_rows(ctx, outputs)
            return RunOutcome(
                succeeded=True, stages=stages, outputs=outputs, quality=quality, tracking=tracking
            )
        except Exception as exc:
            logger.exception("run %s failed", ctx.run_id)
            # Partial outputs are unusable and can be gigabytes (e.g. a disk-full failure).
            shutil.rmtree(ctx.output_dir, ignore_errors=True)
            return RunOutcome(
                succeeded=False, stages=stages, outputs={}, error=f"{type(exc).__name__}: {exc}"
            )
        finally:
            if engine is not None:
                engine.close()
            if not self._settings.keep_intermediates:
                shutil.rmtree(ctx.work_dir, ignore_errors=True)

    def _track_rows(self, ctx: RunContext, outputs: dict[OutputName, str]) -> RowTrackingSummary | None:
        """Build the per-row status table. Runs after the timed stages and never fails the run."""
        if self._row_tracker is None:
            return None
        try:
            summary = self._row_tracker(ctx, outputs)
        except Exception as exc:
            logger.exception("row tracking failed for run %s", ctx.run_id)
            error = f"{type(exc).__name__}: {exc}"
            return RowTrackingSummary(rows=0, states={}, in_exposure=0, seconds=0.0, error=error)
        outputs[OutputName.ROW_STATUS] = str(ctx.output_path(OutputName.ROW_STATUS))
        return summary

    def _measure(
        self, engine: TransformationEngine, ctx: RunContext, stage: Stage
    ) -> tuple[StageResult, StageMetrics]:
        child_limit = self._settings.jq_max_rss_mb if ctx.engine is Engine.JQ else None
        monitor = self._monitor_factory(self._settings.monitor_interval_seconds, child_limit)
        started_at = utc_now()
        try:
            with monitor:
                result = engine.run_stage(stage, ctx)
        except Exception as exc:
            if monitor.limit_exceeded:
                raise ResourceLimitExceeded(
                    f"{stage} stage killed: jq exceeded {self._settings.jq_max_rss_mb} MB "
                    f"(peak {monitor.peak_rss_mb:.0f} MB)"
                ) from exc
            raise
        metrics = StageMetrics(
            stage=stage,
            started_at=started_at,
            finished_at=utc_now(),
            wall_seconds=round(monitor.wall_seconds, 4),
            cpu_seconds=round(monitor.cpu_seconds, 4),
            peak_rss_mb=round(monitor.peak_rss_mb, 1),
            rows_out=result.rows_out,
            detail=result.detail,
        )
        return result, metrics


def build_quality_report(details: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Assemble the data-quality report from engine-computed stage statistics."""
    ingest, clean, validate = details["ingest"], details["clean"], details["validate"]
    return {
        "raw_rows": ingest["raw_rows"]["trades"],
        "duplicates_removed": clean["duplicates_removed"],
        "clean_rows": clean["clean_rows"],
        "valid_rows": validate["valid_rows"],
        "rejected_rows": validate["rejected_rows"],
        "error_counts": dict(sorted(validate["error_counts"].items())),
        "warning_counts": dict(sorted(validate["warning_counts"].items())),
        "output_rows": details["transform"]["output_rows"],
        "exposure_groups": details["aggregate"]["groups"],
    }
