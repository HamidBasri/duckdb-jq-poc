"""Command-side use cases. Each validates input, persists a new record and publishes a
`*Requested` event; the matching orchestrator does the actual work in the background."""

import asyncio
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from app.application.projections import Repositories
from app.config import Settings
from app.domain.events import (
    BenchmarkRequested,
    ComparisonRequested,
    DatasetGenerationRequested,
    DatasetUploaded,
)
from app.domain.models import (
    Benchmark,
    Comparison,
    DatasetManifest,
    DatasetStatus,
    Engine,
    Entity,
    GenerationSpec,
    PipelineRun,
    SourceFormat,
    TransformParams,
    utc_now,
)
from app.domain.ports import EventPublisher


class NotFoundError(LookupError):
    pass


class ConflictError(RuntimeError):
    pass


def new_id(prefix: str) -> str:
    """Sortable, readable id such as `cmp-20261022-101500-3fa2c1`."""
    return f"{prefix}-{utc_now():%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"


class DatasetService:
    def __init__(self, settings: Settings, repositories: Repositories, publisher: EventPublisher) -> None:
        self._settings = settings
        self._datasets = repositories.datasets
        self._publisher = publisher

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
        manifest = DatasetManifest(dataset_id=dataset_id, origin="uploaded", files=files)
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
        unique_engines: list[Engine] = list(dict.fromkeys(engines))
        runs = {
            engine: PipelineRun(
                run_id=new_id(f"run-{engine}"),
                comparison_id=comparison_id,
                dataset_id=dataset_id,
                engine=engine,
                source_format=source_format,
                params=params,
            )
            for engine in unique_engines
        }
        for run in runs.values():
            self._repos.runs.save(run)
        comparison = Comparison(
            comparison_id=comparison_id,
            dataset_id=dataset_id,
            source_format=source_format,
            params=params,
            engines=unique_engines,
            run_ids={engine: run.run_id for engine, run in runs.items()},
            verify=verify,
            parallel_engines=parallel_engines,
            benchmark_id=benchmark_id,
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
