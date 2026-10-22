"""Event handlers that turn *Requested events into background jobs."""

import asyncio
import logging
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from app.application.messaging import EventBus, TaskSupervisor
from app.application.projections import Repositories
from app.application.services import ComparisonService, DatasetService
from app.config import Settings
from app.domain.events import (
    BenchmarkCompleted,
    BenchmarkProgressed,
    BenchmarkRequested,
    ComparisonCompleted,
    ComparisonRequested,
    ComparisonStarted,
    DatasetFailed,
    DatasetGenerationRequested,
    DatasetReady,
    DatasetUploaded,
    OutputsDiscarded,
    RunFinished,
    VerificationStarted,
)
from app.domain.models import (
    DatasetManifest,
    DatasetStatus,
    GenerationSpec,
    OutputName,
    PipelineRun,
    RunOutcome,
    RunStatus,
    SourceFormat,
    VerificationResult,
)
from app.domain.ports import EventSink, JobExecutor, RunContext, WorkerJobs

logger = logging.getLogger(__name__)

DISCARDABLE_OUTPUTS = (OutputName.TRADES, OutputName.REJECTS)


class DatasetOrchestrator:
    def __init__(
        self,
        settings: Settings,
        repositories: Repositories,
        executor: JobExecutor,
        jobs: WorkerJobs,
        bus: EventBus,
        supervisor: TaskSupervisor,
        datasets: DatasetService,
    ) -> None:
        self._settings = settings
        self._jobs = jobs
        self._repos = repositories
        self._executor = executor
        self._bus = bus
        self._supervisor = supervisor
        self._datasets = datasets
        self._in_flight: dict[str, asyncio.Task[None]] = {}

    def register(self) -> None:
        self._bus.subscribe(DatasetGenerationRequested, self._on_generation_requested)
        self._bus.subscribe(DatasetUploaded, self._on_uploaded)

    async def ensure_ready(self, spec: GenerationSpec) -> DatasetManifest:
        """Generate the dataset for `spec` unless it exists, and wait for it to settle."""
        manifest = await self._datasets.request_generation(spec)
        if (task := self._in_flight.get(manifest.dataset_id)) is not None:
            await asyncio.shield(task)
        return self._datasets.get(manifest.dataset_id)

    async def _on_generation_requested(self, event: DatasetGenerationRequested) -> None:
        self._track(event.dataset_id, self._generate(event.dataset_id, event.spec))

    async def _on_uploaded(self, event: DatasetUploaded) -> None:
        self._track(event.dataset_id, self._inspect(event.dataset_id))

    def _track(self, dataset_id: str, work: Coroutine[Any, Any, None]) -> None:
        task = self._supervisor.spawn(work, name=f"dataset:{dataset_id}")
        self._in_flight[dataset_id] = task
        task.add_done_callback(lambda _: self._in_flight.pop(dataset_id, None))

    async def _generate(self, dataset_id: str, spec: GenerationSpec) -> None:
        target_dir = self._settings.datasets_dir / dataset_id
        try:
            written = await self._executor.submit(self._jobs.generate_dataset, spec, target_dir)
        except Exception as exc:
            logger.exception("generation of %s failed", dataset_id)
            await self._bus.publish(DatasetFailed(dataset_id=dataset_id, error=str(exc)))
            return
        if (manifest := self._repos.datasets.get(dataset_id)) is not None:
            manifest.files = written.files
            self._repos.datasets.save(manifest)
        await self._bus.publish(
            DatasetReady(
                dataset_id=dataset_id,
                row_counts=written.row_counts,
                size_bytes=written.size_bytes,
                seconds=round(written.seconds, 3),
            )
        )

    async def _inspect(self, dataset_id: str) -> None:
        manifest = self._datasets.get(dataset_id)
        dataset_dir = self._settings.datasets_dir / dataset_id
        try:
            row_counts = await self._executor.submit(self._jobs.inspect_upload, manifest, dataset_dir)
        except Exception as exc:
            await self._bus.publish(DatasetFailed(dataset_id=dataset_id, error=str(exc)))
            return
        size_bytes: dict[SourceFormat, int] = {}
        for by_format in manifest.files.values():
            for fmt, filename in by_format.items():
                size_bytes[fmt] = size_bytes.get(fmt, 0) + (dataset_dir / filename).stat().st_size
        await self._bus.publish(
            DatasetReady(dataset_id=dataset_id, row_counts=row_counts, size_bytes=size_bytes, seconds=0.0)
        )


class ComparisonOrchestrator:
    def __init__(
        self,
        settings: Settings,
        repositories: Repositories,
        executor: JobExecutor,
        jobs: WorkerJobs,
        bus: EventBus,
        supervisor: TaskSupervisor,
        worker_sink: EventSink,
    ) -> None:
        self._settings = settings
        self._jobs = jobs
        self._repos = repositories
        self._executor = executor
        self._bus = bus
        self._supervisor = supervisor
        self._worker_sink = worker_sink

    def register(self) -> None:
        self._bus.subscribe(ComparisonRequested, self._on_requested)

    async def _on_requested(self, event: ComparisonRequested) -> None:
        self._supervisor.spawn(self.execute(event.comparison_id), name=f"comparison:{event.comparison_id}")

    async def execute(self, comparison_id: str) -> None:
        """Run every engine (sequentially unless `parallel_engines`), then verify equivalence."""
        comparison = self._repos.comparisons.get(comparison_id)
        if comparison is None:
            raise LookupError(f"comparison {comparison_id!r} vanished")
        await self._bus.publish(ComparisonStarted(comparison_id=comparison_id))
        runs = [self._run(run_id) for run_id in comparison.run_ids.values()]
        if comparison.parallel_engines:
            await asyncio.gather(*(self._execute_run(run) for run in runs))
        else:
            for run in runs:
                await self._execute_run(run)

        verification, note = None, None
        finished = [self._run(run_id) for run_id in comparison.run_ids.values()]
        if comparison.verify and len(finished) >= 2:
            if all(run.status is RunStatus.SUCCEEDED for run in finished):
                await self._bus.publish(VerificationStarted(comparison_id=comparison_id))
                verification, note = await self._verify(finished[0], finished[1])
            else:
                note = "verification skipped: not every run succeeded"
        await self._bus.publish(
            ComparisonCompleted(comparison_id=comparison_id, verification=verification, note=note)
        )

    async def discard_large_outputs(self, comparison_id: str) -> None:
        """Delete the big NDJSON outputs (benchmarks keep only metrics, quality and exposures)."""
        comparison = self._repos.comparisons.get(comparison_id)
        if comparison is None:
            return
        for run_id in comparison.run_ids.values():
            run = self._run(run_id)
            discarded = [name for name in DISCARDABLE_OUTPUTS if name in run.outputs]
            for name in discarded:
                Path(run.outputs[name]).unlink(missing_ok=True)
            if discarded:
                await self._bus.publish(OutputsDiscarded(run_id=run_id, outputs=discarded))

    def _run(self, run_id: str) -> PipelineRun:
        if (run := self._repos.runs.get(run_id)) is None:
            raise LookupError(f"run {run_id!r} vanished")
        return run

    async def _execute_run(self, run: PipelineRun) -> None:
        dataset = self._repos.datasets.get(run.dataset_id)
        if dataset is None:
            outcome = RunOutcome(succeeded=False, stages=[], error=f"dataset {run.dataset_id!r} not found")
        else:
            run_dir = self._settings.runs_dir / run.run_id
            ctx = RunContext(
                run_id=run.run_id,
                engine=run.engine,
                dataset=dataset,
                dataset_dir=self._settings.datasets_dir / dataset.dataset_id,
                source_format=run.source_format,
                params=run.params,
                work_dir=run_dir / "work",
                output_dir=run_dir / "output",
            )
            try:
                outcome = await self._executor.submit(
                    self._jobs.run_pipeline, ctx, self._settings.engine, self._worker_sink
                )
            except Exception as exc:
                logger.exception("run %s crashed", run.run_id)
                outcome = RunOutcome(succeeded=False, stages=[], error=f"{type(exc).__name__}: {exc}")
        await self._bus.publish(RunFinished(run_id=run.run_id, outcome=outcome))

    async def _verify(
        self, left: PipelineRun, right: PipelineRun
    ) -> tuple[VerificationResult | None, str | None]:
        try:
            result = await self._executor.submit(
                self._jobs.verify,
                left.engine,
                left.outputs,
                right.engine,
                right.outputs,
                self._settings.verify_full_max_rows,
                self._settings.verify_sample_rows,
            )
        except Exception as exc:
            logger.exception("verification failed")
            return None, f"verification failed: {exc}"
        return result, None


class BenchmarkOrchestrator:
    def __init__(
        self,
        repositories: Repositories,
        bus: EventBus,
        supervisor: TaskSupervisor,
        datasets: DatasetOrchestrator,
        comparisons: ComparisonService,
        comparison_runner: ComparisonOrchestrator,
    ) -> None:
        self._repos = repositories
        self._bus = bus
        self._supervisor = supervisor
        self._datasets = datasets
        self._comparisons = comparisons
        self._comparison_runner = comparison_runner

    def register(self) -> None:
        self._bus.subscribe(BenchmarkRequested, self._on_requested)

    async def _on_requested(self, event: BenchmarkRequested) -> None:
        self._supervisor.spawn(self.execute(event.benchmark_id), name=f"benchmark:{event.benchmark_id}")

    async def execute(self, benchmark_id: str) -> None:
        """Sizes run strictly one after another so measurements do not compete for CPU."""
        benchmark = self._repos.benchmarks.get(benchmark_id)
        if benchmark is None:
            raise LookupError(f"benchmark {benchmark_id!r} vanished")
        try:
            for spec in benchmark.specs:
                dataset = await self._datasets.ensure_ready(spec)
                if dataset.status is not DatasetStatus.READY:
                    raise RuntimeError(f"dataset {dataset.dataset_id} failed: {dataset.error}")
                comparison = self._comparisons.create(
                    dataset.dataset_id,
                    benchmark.engines,
                    benchmark.source_format,
                    benchmark.params,
                    verify=benchmark.verify,
                    parallel_engines=False,
                    benchmark_id=benchmark_id,
                )
                await self._bus.publish(
                    BenchmarkProgressed(benchmark_id=benchmark_id, comparison_id=comparison.comparison_id)
                )
                await self._comparison_runner.execute(comparison.comparison_id)
                if benchmark.discard_outputs:
                    await self._comparison_runner.discard_large_outputs(comparison.comparison_id)
        except Exception as exc:
            logger.exception("benchmark %s failed", benchmark_id)
            await self._bus.publish(BenchmarkCompleted(benchmark_id=benchmark_id, error=str(exc)))
            return
        await self._bus.publish(BenchmarkCompleted(benchmark_id=benchmark_id))
