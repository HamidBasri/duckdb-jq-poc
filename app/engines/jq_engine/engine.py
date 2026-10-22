"""jq implementation of the five pipeline stages.

jq is a stream processor: one record in, zero or more out. Anything that needs to see
the whole dataset (de-duplication, reference resolution, forward references, grouping)
must hold state in memory, and anything that needs two views of the data needs two
passes. Each pass is a separate jq process over NDJSON files in the run's work dir.
"""

import json
from pathlib import Path

import duckdb

from app.common.files import count_lines
from app.config import EngineSettings
from app.domain.models import Engine, Entity, OutputName, Stage
from app.domain.ports import RunContext, StageResult
from app.engines.jq_engine.runner import JqRunner
from app.engines.sources import quote_literal, source_select_sql

PROGRAMS_DIR = Path(__file__).parent / "programs"
REFERENCE_ENTITIES = (Entity.COUNTERPARTIES, Entity.INSTRUMENTS, Entity.BOOKS, Entity.FX_RATES)


class JqEngine:
    name = Engine.JQ

    def __init__(self, settings: EngineSettings, work_dir: Path) -> None:
        self._runner = JqRunner(settings.jq_binary, PROGRAMS_DIR, settings.jq_timeout_seconds)
        self._keep_intermediates = settings.keep_intermediates
        self._raw = work_dir / "raw"
        self._clean = work_dir / "clean"
        self._work = work_dir
        for directory in (self._raw, self._clean):
            directory.mkdir(parents=True, exist_ok=True)

    def run_stage(self, stage: Stage, ctx: RunContext) -> StageResult:
        match stage:
            case Stage.INGEST:
                return self._ingest(ctx)
            case Stage.CLEAN:
                return self._clean_stage()
            case Stage.VALIDATE:
                return self._validate(ctx)
            case Stage.TRANSFORM:
                return self._transform(ctx)
            case Stage.AGGREGATE:
                return self._aggregate(ctx)

    def close(self) -> None:
        """Nothing to release: every jq pass is a short-lived subprocess."""

    # -- stages -------------------------------------------------------------------------
    def _ingest(self, ctx: RunContext) -> StageResult:
        """Convert sources to NDJSON. jq has no CSV/Parquet reader, so DuckDB acts as the adapter."""
        raw_rows: dict[str, int] = {}
        with duckdb.connect() as adapter:
            adapter.execute("SET enable_progress_bar = false")
            for entity in Entity:
                target = self._raw_path(entity)
                source = ctx.dataset.source_path(ctx.dataset_dir, entity, ctx.source_format)
                adapter.execute(
                    f"COPY ({source_select_sql(entity, source)}) TO {quote_literal(target)} (FORMAT json)"
                )
                raw_rows[entity] = count_lines(target)
        return StageResult(
            rows_out=raw_rows[Entity.TRADES],
            detail={"raw_rows": raw_rows, "adapter": "duckdb COPY -> NDJSON (jq cannot parse CSV/Parquet)"},
        )

    def _clean_stage(self) -> StageResult:
        for entity in REFERENCE_ENTITIES:
            self._runner.run(
                "clean_reference.jq",
                self._clean_path(entity),
                [self._raw_path(entity)],
                args={"entity": entity},
            )
        self._runner.run(
            "clean_trades.jq",
            self._clean_path(Entity.TRADES),
            [self._raw_path(Entity.TRADES)],
            slurpfiles={"instruments": self._clean_path(Entity.INSTRUMENTS)},
        )
        clean_rows = count_lines(self._clean_path(Entity.TRADES))
        raw_rows = count_lines(self._raw_path(Entity.TRADES))
        self._discard(*(self._raw_path(entity) for entity in Entity))
        return StageResult(
            rows_out=clean_rows,
            detail={
                "clean_rows": clean_rows,
                "duplicates_removed": raw_rows - clean_rows,
                "jq_passes": len(REFERENCE_ENTITIES) + 1,
            },
        )

    def _validate(self, ctx: RunContext) -> StageResult:
        trades = self._clean_path(Entity.TRADES)
        trade_ids = self._work / "trade_ids.json"
        validated = self._validated_path
        quality = self._work / "quality.json"
        self._runner.run("trade_ids.jq", trade_ids, [trades])
        self._runner.run(
            "validate.jq",
            validated,
            [trades],
            slurpfiles={"trade_ids": trade_ids, **self._reference_slurpfiles()},
        )
        rejects = ctx.output_path(OutputName.REJECTS)
        self._runner.run("rejects.jq", rejects, [validated])
        self._runner.run("quality.jq", quality, [validated])
        detail = json.loads(quality.read_text())
        detail["jq_passes"] = 4
        self._discard(trades, trade_ids, quality)
        return StageResult(
            rows_out=detail["valid_rows"], outputs={OutputName.REJECTS: rejects}, detail=detail
        )

    def _transform(self, ctx: RunContext) -> StageResult:
        dimensions = self._work / "dimensions.json"
        lineage = self._work / "lineage.json"
        trades = ctx.output_path(OutputName.TRADES)
        self._runner.run("dimensions.jq", dimensions, slurpfiles=self._reference_slurpfiles())
        self._runner.run("lineage_index.jq", lineage, [self._validated_path])
        self._runner.run(
            "trades.jq",
            trades,
            [self._validated_path],
            slurpfiles={"dimensions": dimensions, "lineage": lineage},
            json_args={"params": ctx.params.model_dump(mode="json")},
        )
        output_rows = count_lines(trades)
        self._discard(self._validated_path, dimensions, lineage)
        return StageResult(
            rows_out=output_rows,
            outputs={OutputName.TRADES: trades},
            detail={"output_rows": output_rows, "jq_passes": 3},
        )

    def _aggregate(self, ctx: RunContext) -> StageResult:
        exposures = ctx.output_path(OutputName.EXPOSURES)
        self._runner.run("exposures.jq", exposures, [ctx.output_path(OutputName.TRADES)])
        groups = count_lines(exposures)
        return StageResult(
            rows_out=groups, outputs={OutputName.EXPOSURES: exposures}, detail={"groups": groups}
        )

    # -- paths --------------------------------------------------------------------------
    def _discard(self, *paths: Path) -> None:
        """Delete intermediates as soon as no later pass needs them (multi-GB at 1M+ rows)."""
        if not self._keep_intermediates:
            for path in paths:
                path.unlink(missing_ok=True)

    def _raw_path(self, entity: Entity) -> Path:
        return self._raw / f"{entity}.ndjson"

    def _clean_path(self, entity: Entity) -> Path:
        return self._clean / f"{entity}.ndjson"

    @property
    def _validated_path(self) -> Path:
        return self._work / "validated.ndjson"

    def _reference_slurpfiles(self) -> dict[str, Path]:
        return {str(entity): self._clean_path(entity) for entity in REFERENCE_ENTITIES}
