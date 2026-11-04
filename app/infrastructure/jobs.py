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
from app.engines.sources import numbered_reader_sql, quote_identifier, quote_literal, reader_sql
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
                reader = reader_sql(dataset_dir / filename, fmt)
                columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()}
                if missing := sorted(set(RAW_COLUMNS[entity]) - columns):
                    raise ValueError(f"{filename} is missing columns: {', '.join(missing)}")
                row = con.execute(f"SELECT count(*) FROM {reader}").fetchone()
                row_counts[entity] = int(row[0]) if row else 0
    return row_counts


def _edited_sql(column: str, base: str, edits: dict[int, str | None]) -> str:
    """`base` with hand-edited rows swapped in, keyed by source row."""
    if not edits:
        return base
    cases = " ".join(
        f"WHEN {row} THEN {'NULL' if value is None else quote_literal(value)}" for row, value in edits.items()
    )
    return f"CASE source_row {cases} ELSE {base} END"


def derive_dataset_job(
    parent: DatasetManifest, parent_dir: Path, target_dir: Path, child: DatasetManifest
) -> dict[Entity, dict[SourceFormat, str]]:
    """Write a new version of `parent` as Parquet: renamed columns, edited cells and deleted rows.

    With a column mapping, the files get exactly the contract columns and anything else is dropped.
    Without one, every source column is kept. Rows keep their order; edits refer to source rows.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    files: dict[Entity, dict[SourceFormat, str]] = {}
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false")
        for entity in parent.files:
            source = parent.source_path(parent_dir, entity, SourceFormat.PARQUET)
            if source is None:
                continue
            reader = numbered_reader_sql(*source)
            if child.column_mapping is not None:
                chosen = child.column_mapping.get(entity, {})
                sources = {column: chosen.get(column) for column in RAW_COLUMNS[entity]}
            else:
                found = [d[0] for d in con.execute(f"SELECT * FROM ({reader}) LIMIT 0").description or []]
                sources = {column: column for column in found if column != "source_row"}
            edits: dict[str, dict[int, str | None]] = {}
            for edit in (e for e in child.edits if e.entity is entity):
                edits.setdefault(edit.column, {})[edit.row] = edit.value
            projection = ", ".join(
                _edited_sql(
                    column,
                    f"CAST({quote_identifier(src)} AS VARCHAR)" if src else "CAST(NULL AS VARCHAR)",
                    edits.get(column, {}),
                )
                + f" AS {quote_identifier(column)}"
                for column, src in sources.items()
            )
            deleted = sorted({ref.row for ref in child.deleted_rows if ref.entity is entity})
            where = f"WHERE source_row NOT IN ({', '.join(map(str, deleted))})" if deleted else ""
            target = target_dir / f"{entity}.parquet"
            select = f"SELECT {projection} FROM ({reader}) {where} ORDER BY source_row"
            con.execute(f"COPY ({select}) TO {quote_literal(target)} (FORMAT parquet)")
            files[entity] = {SourceFormat.PARQUET: target.name}
    return files


def run_pipeline_job(ctx: RunContext, settings: EngineSettings, sink: EventSink) -> RunOutcome:
    tracker = build_row_status if settings.track_rows else None
    return PipelineRunner(settings, sink, create_engine, ResourceMonitor, tracker).run(ctx)


WORKER_JOBS = WorkerJobs(
    generate_dataset=generate_dataset_job,
    inspect_upload=inspect_upload_job,
    derive_dataset=derive_dataset_job,
    run_pipeline=run_pipeline_job,
    verify=verify_outputs,
)
