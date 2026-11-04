"""Command-side use cases. Each validates input, persists a new record and publishes a
`*Requested` event; the matching orchestrator does the actual work in the background."""

import asyncio
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from pydantic import BaseModel

from app.application.projections import Repositories, next_number
from app.config import Settings
from app.domain.contract import LOCKED_RULES, RAW_COLUMNS, RULE_TEXT, ErrorCode, suggest_mapping
from app.domain.events import (
    BenchmarkRequested,
    ComparisonRequested,
    DatasetGenerationRequested,
    DatasetUploaded,
    DatasetVersionRequested,
    RecordsDeleted,
    RulesChanged,
)
from app.domain.models import (
    Benchmark,
    BenchmarkStatus,
    CellEdit,
    ColumnMapping,
    Comparison,
    ComparisonStatus,
    CorrectionRule,
    DatasetManifest,
    DatasetStatus,
    Engine,
    Entity,
    EntityColumns,
    GenerationSpec,
    PipelineRun,
    RowRef,
    RuleSet,
    SourceFormat,
    TransformParams,
    utc_now,
)
from app.domain.ports import ColumnReader, DataBrowser, EventPublisher

SAMPLE_ROWS = 3
ACTIVE_BENCHMARK = (BenchmarkStatus.PREPARING, BenchmarkStatus.RUNNING)


class NotFoundError(LookupError):
    pass


class ConflictError(RuntimeError):
    pass


def current_rules(repositories: Repositories) -> RuleSet:
    """The newest rule set, or an empty one (version 0) before anything was changed."""
    return max(repositories.rules.list(), key=lambda rules: rules.version, default=RuleSet())


def new_id(prefix: str) -> str:
    """Sortable, readable id such as `cmp-20261022-101500-3fa2c1`."""
    return f"{prefix}-{utc_now():%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"


class DatasetService:
    def __init__(
        self,
        settings: Settings,
        repositories: Repositories,
        publisher: EventPublisher,
        columns: ColumnReader,
        browser: DataBrowser,
    ) -> None:
        self._settings = settings
        self._repos = repositories
        self._datasets = repositories.datasets
        self._publisher = publisher
        self._columns = columns
        self._browser = browser

    def get(self, dataset_id: str) -> DatasetManifest:
        if (dataset := self._datasets.get(dataset_id)) is None:
            raise NotFoundError(f"dataset {dataset_id!r} not found")
        return dataset

    def list_all(self) -> list[DatasetManifest]:
        return sorted(self._datasets.list(), key=lambda dataset: dataset.created_at, reverse=True)

    async def request_generation(self, spec: GenerationSpec, overwrite: bool = False) -> DatasetManifest:
        """Idempotent: the id derives from the spec, so an existing usable dataset is returned as-is."""
        existing = self._datasets.get(spec.dataset_id)
        if existing is not None and existing.status is not DatasetStatus.FAILED and not overwrite:
            return existing
        if existing is not None and existing.status is DatasetStatus.PENDING:
            raise ConflictError(f"dataset {spec.dataset_id!r} is still being generated")
        manifest = DatasetManifest(dataset_id=spec.dataset_id, origin="generated", generation=spec)
        self._datasets.save(manifest)
        await self._publisher.publish(DatasetGenerationRequested(dataset_id=manifest.dataset_id, spec=spec))
        return manifest

    def _settled(self, dataset_id: str) -> DatasetManifest:
        dataset = self.get(dataset_id)
        if dataset.status is DatasetStatus.PENDING:
            raise ConflictError(f"dataset {dataset_id!r} is still being created")
        return dataset

    async def columns(self, dataset_id: str) -> list[EntityColumns]:
        """Each file's columns next to the expected ones, with suggested matches and sample values."""
        dataset = self._settled(dataset_id)
        dataset_dir = self._settings.datasets_dir / dataset_id
        result: list[EntityColumns] = []
        for entity in Entity:
            source = dataset.source_path(dataset_dir, entity, SourceFormat.PARQUET)
            if source is None or not source[0].exists():
                continue
            try:
                found, rows = await asyncio.to_thread(self._columns.read, *source, SAMPLE_ROWS)
            except Exception as exc:  # DuckDB raises a family of errors for unreadable files
                raise ValueError(f"could not read {source[0].name}: {exc}") from exc
            expected = list(RAW_COLUMNS[entity])
            result.append(
                EntityColumns(
                    entity=entity,
                    file=source[0].name,
                    found=found,
                    expected=expected,
                    missing=[column for column in expected if column not in found],
                    suggested=suggest_mapping(expected, found),
                    samples={name: [row[i] for row in rows] for i, name in enumerate(found)},
                )
            )
        return result

    async def request_version(self, dataset_id: str, mapping: ColumnMapping) -> DatasetManifest:
        """Queue a copy of the dataset with its columns renamed to the contract. The original is kept.

        Entities left out of `mapping` use the suggested matches.

        Raises:
            ValueError: a mapped column does not exist, or nothing is mapped to trade_id.
        """
        parent = self._settled(dataset_id)
        full: ColumnMapping = {}
        for info in await self.columns(dataset_id):
            chosen = mapping.get(info.entity, info.suggested)
            full[info.entity] = {column: chosen.get(column) or None for column in info.expected}
            if unknown := sorted({v for v in full[info.entity].values() if v} - set(info.found)):
                raise ValueError(f"{info.file} has no column named {', '.join(unknown)}")
        if not full.get(Entity.TRADES, {}).get("trade_id"):
            raise ValueError("map a column to trade_id; without it every trade is rejected")
        return await self._new_version(parent, column_mapping=full)

    async def request_edits(
        self, dataset_id: str, edits: list[CellEdit], deleted_rows: list[RowRef]
    ) -> DatasetManifest:
        """Queue a copy of the dataset with fields changed and rows removed by hand. The original is kept.

        Raises:
            ValueError: nothing to change, or an edit names a table, column or row that does not exist.
        """
        parent = self._settled(dataset_id)
        if not edits and not deleted_rows:
            raise ValueError("there are no changes to save")
        dataset_dir = self._settings.datasets_dir / dataset_id
        changes: list[CellEdit | RowRef] = [*edits, *deleted_rows]
        for entity in {change.entity for change in changes}:
            source = parent.source_path(dataset_dir, entity, SourceFormat.PARQUET)
            if source is None:
                raise ValueError(f"this dataset has no {entity} table")
            page = await asyncio.to_thread(self._browser.table_page, *source, 0, 1, None)
            for change in (c for c in changes if c.entity is entity):
                if change.row > page.total:
                    raise ValueError(f"{entity} has {page.total} rows; there is no row {change.row}")
                if isinstance(change, CellEdit) and change.column not in page.columns:
                    raise ValueError(f"{entity} has no column {change.column!r}")
        return await self._new_version(parent, edits=edits, deleted_rows=deleted_rows)

    async def _new_version(self, parent: DatasetManifest, **changes: object) -> DatasetManifest:
        root = parent.root_id or parent.dataset_id
        family = [d for d in self._datasets.list() if root in (d.dataset_id, d.root_id)]
        version = max(d.version for d in family) + 1
        manifest = DatasetManifest.model_validate(
            {
                "dataset_id": f"{root}-v{version}",
                "origin": "version",
                "version": version,
                "parent_id": parent.dataset_id,
                "root_id": root,
                **changes,
            }
        )
        self._datasets.save(manifest)
        await self._publisher.publish(
            DatasetVersionRequested(dataset_id=manifest.dataset_id, parent_id=parent.dataset_id)
        )
        return manifest

    async def delete(self, dataset_id: str) -> None:
        """Remove a dataset and its files. Finished runs on it are kept."""
        self._settled(dataset_id)
        if any(
            c.dataset_id == dataset_id and c.status is not ComparisonStatus.COMPLETED
            for c in self._repos.comparisons.list()
        ) or any(
            b.status in ACTIVE_BENCHMARK and any(spec.dataset_id == dataset_id for spec in b.specs)
            for b in self._repos.benchmarks.list()
        ):
            raise ConflictError(f"dataset {dataset_id!r} is in use by a run that is still going")
        await asyncio.to_thread(shutil.rmtree, self._settings.datasets_dir / dataset_id, ignore_errors=True)
        self._datasets.delete(dataset_id)
        await self._publisher.publish(RecordsDeleted(kind="dataset", ids=[dataset_id]))

    async def register_upload(self, uploads: Mapping[Entity, tuple[str, BinaryIO]]) -> DatasetManifest:
        """Store uploaded CSV/Parquet files (trades required) and queue their inspection.

        Raises:
            ValueError: if trades are missing or a file is neither CSV nor Parquet.
        """
        if Entity.TRADES not in uploads:
            raise ValueError("a trades file is required")
        dataset_id = new_id("upload")
        target_dir = self._settings.datasets_dir / dataset_id
        target_dir.mkdir(parents=True)
        files: dict[Entity, dict[SourceFormat, str]] = {}
        for entity, (filename, stream) in uploads.items():
            suffix = Path(filename).suffix.lower().lstrip(".")
            if suffix not in SourceFormat:
                raise ValueError(f"{filename}: expected a .csv or .parquet file")
            fmt = SourceFormat(suffix)
            target = target_dir / f"{entity}.{fmt}"
            with target.open("wb") as sink:
                await asyncio.to_thread(shutil.copyfileobj, stream, sink)
            files[entity] = {fmt: target.name}
        number = next_number(self._repos, "upload", (d.number for d in self._datasets.list()))
        manifest = DatasetManifest(dataset_id=dataset_id, origin="uploaded", number=number, files=files)
        self._datasets.save(manifest)
        await self._publisher.publish(DatasetUploaded(dataset_id=dataset_id))
        return manifest


class ComparisonService:
    def __init__(self, repositories: Repositories, publisher: EventPublisher) -> None:
        self._repos = repositories
        self._publisher = publisher

    def get(self, comparison_id: str) -> Comparison:
        if (comparison := self._repos.comparisons.get(comparison_id)) is None:
            raise NotFoundError(f"comparison {comparison_id!r} not found")
        return comparison

    def list_all(self) -> list[Comparison]:
        return sorted(
            self._repos.comparisons.list(), key=lambda comparison: comparison.created_at, reverse=True
        )

    def get_run(self, run_id: str) -> PipelineRun:
        if (run := self._repos.runs.get(run_id)) is None:
            raise NotFoundError(f"run {run_id!r} not found")
        return run

    def create(
        self,
        dataset_id: str,
        engines: list[Engine],
        source_format: SourceFormat,
        params: TransformParams,
        verify: bool,
        parallel_engines: bool,
        benchmark_id: str | None = None,
    ) -> Comparison:
        """Persist a comparison and one queued run per engine, without dispatching it.

        Raises:
            NotFoundError: unknown dataset.
            ConflictError: dataset not ready yet.
        """
        dataset = self._repos.datasets.get(dataset_id)
        if dataset is None:
            raise NotFoundError(f"dataset {dataset_id!r} not found")
        if dataset.status is not DatasetStatus.READY:
            raise ConflictError(f"dataset {dataset_id!r} is {dataset.status}, not ready")
        comparison_id = new_id("cmp")
        number = next_number(self._repos, "run", (c.number for c in self._repos.comparisons.list()))
        rules = current_rules(self._repos)
        unique_engines: list[Engine] = list(dict.fromkeys(engines))
        runs = {
            engine: PipelineRun(
                run_id=new_id(f"run-{engine}"),
                comparison_id=comparison_id,
                number=number,
                dataset_id=dataset_id,
                engine=engine,
                source_format=source_format,
                params=params,
                rules=rules,
            )
            for engine in unique_engines
        }
        for run in runs.values():
            self._repos.runs.save(run)
        comparison = Comparison(
            comparison_id=comparison_id,
            number=number,
            dataset_id=dataset_id,
            source_format=source_format,
            params=params,
            engines=unique_engines,
            run_ids={engine: run.run_id for engine, run in runs.items()},
            verify=verify,
            parallel_engines=parallel_engines,
            benchmark_id=benchmark_id,
            rules_version=rules.version,
        )
        self._repos.comparisons.save(comparison)
        return comparison

    async def request(
        self,
        dataset_id: str,
        engines: list[Engine],
        source_format: SourceFormat,
        params: TransformParams,
        verify: bool,
        parallel_engines: bool,
    ) -> Comparison:
        comparison = self.create(dataset_id, engines, source_format, params, verify, parallel_engines)
        await self._publisher.publish(ComparisonRequested(comparison_id=comparison.comparison_id))
        return comparison


class BenchmarkService:
    def __init__(self, repositories: Repositories, publisher: EventPublisher) -> None:
        self._repos = repositories
        self._benchmarks = repositories.benchmarks
        self._publisher = publisher

    def get(self, benchmark_id: str) -> Benchmark:
        if (benchmark := self._benchmarks.get(benchmark_id)) is None:
            raise NotFoundError(f"benchmark {benchmark_id!r} not found")
        return benchmark

    def list_all(self) -> list[Benchmark]:
        return sorted(self._benchmarks.list(), key=lambda benchmark: benchmark.created_at, reverse=True)

    async def request(
        self,
        specs: list[GenerationSpec],
        engines: list[Engine],
        source_format: SourceFormat,
        params: TransformParams,
        verify: bool,
        discard_outputs: bool,
    ) -> Benchmark:
        benchmark = Benchmark(
            benchmark_id=new_id("bench"),
            number=next_number(self._repos, "benchmark", (b.number for b in self._benchmarks.list())),
            specs=sorted(specs, key=lambda spec: spec.rows),
            engines=list(dict.fromkeys(engines)),
            source_format=source_format,
            params=params,
            verify=verify,
            discard_outputs=discard_outputs,
        )
        self._benchmarks.save(benchmark)
        await self._publisher.publish(BenchmarkRequested(benchmark_id=benchmark.benchmark_id))
        return benchmark


class HistoryCleared(BaseModel):
    runs: int
    benchmarks: int
    kept: int


class HistoryService:
    """Deletes finished runs and benchmarks together with their output files."""

    def __init__(self, repositories: Repositories, runs_dir: Path, publisher: EventPublisher) -> None:
        self._repos = repositories
        self._runs_dir = runs_dir
        self._publisher = publisher

    async def delete_comparison(self, comparison_id: str) -> None:
        comparison = self._repos.comparisons.get(comparison_id)
        if comparison is None:
            raise NotFoundError(f"run {comparison_id!r} not found")
        if comparison.benchmark_id and self._repos.benchmarks.get(comparison.benchmark_id):
            raise ConflictError(
                f"this run belongs to benchmark {comparison.benchmark_id}; delete that instead"
            )
        if comparison.status is not ComparisonStatus.COMPLETED:
            raise ConflictError("this run is still going; delete it when it finishes")
        await self._remove([comparison])
        await self._publisher.publish(RecordsDeleted(kind="comparison", ids=[comparison_id]))

    async def delete_benchmark(self, benchmark_id: str) -> None:
        benchmark = self._repos.benchmarks.get(benchmark_id)
        if benchmark is None:
            raise NotFoundError(f"benchmark {benchmark_id!r} not found")
        if benchmark.status in ACTIVE_BENCHMARK:
            raise ConflictError("this benchmark is still going; delete it when it finishes")
        await self._remove([c for c in self._repos.comparisons.list() if c.benchmark_id == benchmark_id])
        self._repos.benchmarks.delete(benchmark_id)
        await self._publisher.publish(RecordsDeleted(kind="benchmark", ids=[benchmark_id]))

    async def clear(self) -> HistoryCleared:
        """Delete every finished run and benchmark; anything still going is kept."""
        benchmarks = [b for b in self._repos.benchmarks.list() if b.status not in ACTIVE_BENCHMARK]
        finished = {b.benchmark_id for b in benchmarks}
        comparisons = [
            c
            for c in self._repos.comparisons.list()
            if c.benchmark_id in finished
            or (
                c.status is ComparisonStatus.COMPLETED
                and not self._repos.benchmarks.get(c.benchmark_id or "")
            )
        ]
        await self._remove(comparisons)
        for benchmark in benchmarks:
            self._repos.benchmarks.delete(benchmark.benchmark_id)
        kept = len(self._repos.comparisons.list()) + len(self._repos.benchmarks.list())
        await self._publisher.publish(
            RecordsDeleted(kind="history", ids=[c.comparison_id for c in comparisons] + list(finished))
        )
        return HistoryCleared(runs=len(comparisons), benchmarks=len(benchmarks), kept=kept)

    async def _remove(self, comparisons: list[Comparison]) -> None:
        for comparison in comparisons:
            for run_id in comparison.run_ids.values():
                await asyncio.to_thread(shutil.rmtree, self._runs_dir / run_id, ignore_errors=True)
                self._repos.runs.delete(run_id)
            self._repos.comparisons.delete(comparison.comparison_id)


class RuleCode(BaseModel):
    code: str
    text: str
    locked: bool
    reason: str | None


class RulesOverview(BaseModel):
    current: RuleSet
    codes: list[RuleCode]
    columns: dict[Entity, list[str]]


class RulesService:
    """Correction rules and validation switches. Each change is saved as a new version."""

    def __init__(self, repositories: Repositories, publisher: EventPublisher) -> None:
        self._repos = repositories
        self._publisher = publisher

    def overview(self) -> RulesOverview:
        return RulesOverview(
            current=current_rules(self._repos),
            codes=[
                RuleCode(
                    code=code,
                    text=RULE_TEXT[code],
                    locked=code in LOCKED_RULES,
                    reason=LOCKED_RULES.get(code),
                )
                for code in ErrorCode
            ],
            columns={entity: list(columns) for entity, columns in RAW_COLUMNS.items()},
        )

    def history(self) -> list[RuleSet]:
        return sorted(self._repos.rules.list(), key=lambda rules: rules.version, reverse=True)

    async def update(
        self, corrections: list[CorrectionRule], disabled_rules: list[str], note: str
    ) -> RuleSet:
        """Save a new version, unless nothing changed.

        Raises:
            ValueError: an unknown column or code, a locked rule, or two rules for the same value.
        """
        seen: set[tuple[Entity, str, str]] = set()
        for rule in corrections:
            if rule.column not in RAW_COLUMNS[rule.entity]:
                raise ValueError(f"{rule.entity} has no column {rule.column!r}")
            key = (rule.entity, rule.column, rule.match.strip())
            if key in seen:
                raise ValueError(f"two rules change {rule.entity}.{rule.column} = {rule.match.strip()!r}")
            seen.add(key)
        if unknown := sorted(set(disabled_rules) - {code.value for code in ErrorCode}):
            raise ValueError(f"unknown rule {', '.join(unknown)}")
        if locked := sorted(set(disabled_rules) & LOCKED_RULES.keys()):
            raise ValueError(f"{locked[0]} can't be switched off: {LOCKED_RULES[locked[0]]}")
        current = current_rules(self._repos)
        disabled = sorted(set(disabled_rules))
        if corrections == current.corrections and disabled == current.disabled_rules:
            return current
        rules = RuleSet(
            version=current.version + 1, corrections=corrections, disabled_rules=disabled, note=note
        )
        self._repos.rules.save(rules)
        await self._publisher.publish(RulesChanged(version=rules.version))
        return rules
