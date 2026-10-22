"""Read-model projection: the only place that mutates persisted state in response to events."""

from dataclasses import dataclass

from app.domain.events import (
    BenchmarkCompleted,
    BenchmarkProgressed,
    ComparisonCompleted,
    ComparisonStarted,
    DatasetFailed,
    DatasetReady,
    DomainEvent,
    OutputsDiscarded,
    RunFinished,
    RunStarted,
    StageCompleted,
    StageStarted,
    VerificationStarted,
)
from app.domain.models import (
    Benchmark,
    BenchmarkStatus,
    Comparison,
    ComparisonStatus,
    DatasetManifest,
    DatasetStatus,
    PipelineRun,
    RunStatus,
    utc_now,
)
from app.domain.ports import Repository

TERMINAL_RUN_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED})


@dataclass(frozen=True)
class Repositories:
    datasets: Repository[DatasetManifest]
    runs: Repository[PipelineRun]
    comparisons: Repository[Comparison]
    benchmarks: Repository[Benchmark]


class StateProjector:
    """Applies events to records. Late progress events for finished runs are ignored, because
    worker progress travels through a queue and can arrive after the run's final outcome."""

    def __init__(self, repositories: Repositories) -> None:
        self._repos = repositories

    async def apply(self, event: DomainEvent) -> None:
        repos = self._repos
        match event:
            case DatasetReady(dataset_id=dataset_id) if dataset := repos.datasets.get(dataset_id):
                dataset.status = DatasetStatus.READY
                dataset.row_counts = event.row_counts
                dataset.size_bytes = event.size_bytes
                dataset.generation_seconds = event.seconds
                dataset.error = None
                repos.datasets.save(dataset)
            case DatasetFailed(dataset_id=dataset_id) if dataset := repos.datasets.get(dataset_id):
                dataset.status = DatasetStatus.FAILED
                dataset.error = event.error
                repos.datasets.save(dataset)
            case RunStarted(run_id=run_id) if run := repos.runs.get(run_id):
                if run.status not in TERMINAL_RUN_STATUSES:
                    run.status = RunStatus.RUNNING
                    run.started_at = event.occurred_at
                    run.worker_pid = event.worker_pid
                    repos.runs.save(run)
            case StageStarted(run_id=run_id) if run := repos.runs.get(run_id):
                if run.status not in TERMINAL_RUN_STATUSES:
                    run.current_stage = event.stage
                    repos.runs.save(run)
            case StageCompleted(run_id=run_id) if run := repos.runs.get(run_id):
                if run.status not in TERMINAL_RUN_STATUSES:
                    run.stages.append(event.metrics)
                    repos.runs.save(run)
            case RunFinished(run_id=run_id) if run := repos.runs.get(run_id):
                outcome = event.outcome
                run.status = RunStatus.SUCCEEDED if outcome.succeeded else RunStatus.FAILED
                run.stages = outcome.stages
                run.outputs = outcome.outputs
                run.quality = outcome.quality
                run.tracking = outcome.tracking
                run.error = outcome.error
                run.current_stage = None
                run.finished_at = event.occurred_at
                repos.runs.save(run)
            case OutputsDiscarded(run_id=run_id) if run := repos.runs.get(run_id):
                run.outputs = {name: path for name, path in run.outputs.items() if name not in event.outputs}
                repos.runs.save(run)
            case ComparisonStarted(comparison_id=comparison_id) if comparison := repos.comparisons.get(
                comparison_id
            ):
                comparison.status = ComparisonStatus.RUNNING
                repos.comparisons.save(comparison)
            case VerificationStarted(comparison_id=comparison_id) if comparison := repos.comparisons.get(
                comparison_id
            ):
                comparison.status = ComparisonStatus.VERIFYING
                repos.comparisons.save(comparison)
            case ComparisonCompleted(comparison_id=comparison_id) if comparison := repos.comparisons.get(
                comparison_id
            ):
                comparison.status = ComparisonStatus.COMPLETED
                comparison.verification = event.verification
                comparison.note = event.note
                comparison.finished_at = event.occurred_at
                repos.comparisons.save(comparison)
            case BenchmarkProgressed(benchmark_id=benchmark_id) if benchmark := repos.benchmarks.get(
                benchmark_id
            ):
                benchmark.status = BenchmarkStatus.RUNNING
                benchmark.comparison_ids.append(event.comparison_id)
                repos.benchmarks.save(benchmark)
            case BenchmarkCompleted(benchmark_id=benchmark_id) if benchmark := repos.benchmarks.get(
                benchmark_id
            ):
                benchmark.status = BenchmarkStatus.FAILED if event.error else BenchmarkStatus.COMPLETED
                benchmark.error = event.error
                benchmark.finished_at = event.occurred_at
                repos.benchmarks.save(benchmark)
            case _:
                pass


def recover_interrupted(repositories: Repositories) -> None:
    """Mark work that was in flight when the previous process stopped as failed."""
    reason = "interrupted by application restart"
    now = utc_now()
    for dataset in repositories.datasets.list():
        if dataset.status is DatasetStatus.PENDING:
            dataset.status, dataset.error = DatasetStatus.FAILED, reason
            repositories.datasets.save(dataset)
    for run in repositories.runs.list():
        if run.status not in TERMINAL_RUN_STATUSES:
            run.status, run.error, run.finished_at = RunStatus.FAILED, reason, now
            repositories.runs.save(run)
    for comparison in repositories.comparisons.list():
        if comparison.status is not ComparisonStatus.COMPLETED:
            comparison.status, comparison.note, comparison.finished_at = (
                ComparisonStatus.COMPLETED,
                reason,
                now,
            )
            repositories.comparisons.save(comparison)
    for benchmark in repositories.benchmarks.list():
        if benchmark.status not in (BenchmarkStatus.COMPLETED, BenchmarkStatus.FAILED):
            benchmark.status, benchmark.error, benchmark.finished_at = BenchmarkStatus.FAILED, reason, now
            repositories.benchmarks.save(benchmark)
