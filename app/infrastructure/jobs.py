"""Worker-process entrypoints. Top-level functions so they pickle under the spawn start method."""

from pathlib import Path

import duckdb

from app.application.pipeline import PipelineRunner
from app.application.verification import verify_outputs
from app.config import EngineSettings
from app.domain.contract import RAW_COLUMNS
from app.domain.models import (
    DatasetManifest,
    Entity,
    GeneratedDataset,
    GenerationSpec,
    RunOutcome,
    SourceFormat,
)
from app.domain.ports import EventSink, RunContext, WorkerJobs
from app.engines import create_engine
from app.engines.sources import quote_literal
from app.infrastructure.generator import write_dataset
from app.infrastructure.resource_monitor import ResourceMonitor
from app.infrastructure.row_tracker import build_row_status


def generate_dataset_job(spec: GenerationSpec, target_dir: Path) -> GeneratedDataset:
    return write_dataset(spec, target_dir)


def inspect_upload_job(manifest: DatasetManifest, dataset_dir: Path) -> dict[Entity, int]:
    """Check uploaded files carry the contract columns and count their rows.

    Raises:
        ValueError: if a file misses required columns (DuckDB raises if it is unreadable).
    """
    row_counts: dict[Entity, int] = {}
    with duckdb.connect() as con:
        for entity, by_format in manifest.files.items():
            for fmt, filename in by_format.items():
                path = quote_literal(dataset_dir / filename)
                reader = (
                    f"read_parquet({path})"
                    if fmt is SourceFormat.PARQUET
                    else f"read_csv({path}, all_varchar = true)"
                )
                columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()}
                if missing := sorted(set(RAW_COLUMNS[entity]) - columns):
                    raise ValueError(f"{filename} is missing columns: {', '.join(missing)}")
                row = con.execute(f"SELECT count(*) FROM {reader}").fetchone()
                row_counts[entity] = int(row[0]) if row else 0
    return row_counts


def run_pipeline_job(ctx: RunContext, settings: EngineSettings, sink: EventSink) -> RunOutcome:
    tracker = build_row_status if settings.track_rows else None
    return PipelineRunner(settings, sink, create_engine, ResourceMonitor, tracker).run(ctx)


WORKER_JOBS = WorkerJobs(
    generate_dataset=generate_dataset_job,
    inspect_upload=inspect_upload_job,
    run_pipeline=run_pipeline_job,
    verify=verify_outputs,
)
