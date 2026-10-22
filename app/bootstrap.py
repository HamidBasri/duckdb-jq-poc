"""Composition root: the only module that knows every concrete class."""

import logging
from dataclasses import dataclass

from app.application.messaging import EventBus, TaskSupervisor
from app.application.orchestrators import BenchmarkOrchestrator, ComparisonOrchestrator, DatasetOrchestrator
from app.application.projections import Repositories, StateProjector, recover_interrupted
from app.application.queries import ReportQueries
from app.application.services import BenchmarkService, ComparisonService, DatasetService
from app.application.tracking import RowTrackingService
from app.config import Settings
from app.domain.events import DomainEvent
from app.domain.models import Benchmark, Comparison, DatasetManifest, PipelineRun
from app.infrastructure.jobs import WORKER_JOBS
from app.infrastructure.repositories import JsonFileRepository
from app.infrastructure.row_tracker import DuckDbRowStatusReader
from app.infrastructure.sse import EventBroadcaster
from app.infrastructure.workers import ProcessJobExecutor, WorkerEventBridge

logger = logging.getLogger("app.events")


async def log_event(event: DomainEvent) -> None:
    ids = {name: value for name, value in vars(event).items() if name.endswith("_id")}
    logger.info("%s %s", event.name, ids)


@dataclass
class Container:
    settings: Settings
    bus: EventBus
    supervisor: TaskSupervisor
    executor: ProcessJobExecutor
    bridge: WorkerEventBridge
    repositories: Repositories
    broadcaster: EventBroadcaster
    datasets: DatasetService
    comparisons: ComparisonService
    benchmarks: BenchmarkService
    reports: ReportQueries
    rows: RowTrackingService
    dataset_orchestrator: DatasetOrchestrator
    comparison_orchestrator: ComparisonOrchestrator
    benchmark_orchestrator: BenchmarkOrchestrator

    def start(self) -> None:
        self.bridge.start()

    async def stop(self) -> None:
        await self.supervisor.shutdown()
        await self.bridge.stop()
        self.executor.shutdown()


def build_container(settings: Settings) -> Container:
    """Wire the application. Must be called from within a running event loop's context."""
    repositories = Repositories(
        datasets=JsonFileRepository(
            settings.datasets_dir, DatasetManifest, lambda record: record.dataset_id, filename="manifest.json"
        ),
        runs=JsonFileRepository(settings.state_dir / "runs", PipelineRun, lambda record: record.run_id),
        comparisons=JsonFileRepository(
            settings.state_dir / "comparisons", Comparison, lambda record: record.comparison_id
        ),
        benchmarks=JsonFileRepository(
            settings.state_dir / "benchmarks", Benchmark, lambda record: record.benchmark_id
        ),
    )
    recover_interrupted(repositories)

    bus = EventBus()
    supervisor = TaskSupervisor()
    executor = ProcessJobExecutor(settings.max_workers)
    bridge = WorkerEventBridge(bus)
    broadcaster = EventBroadcaster()

    # Subscription order matters: state is projected before anything reacts to the event.
    bus.subscribe(DomainEvent, StateProjector(repositories).apply)
    bus.subscribe(DomainEvent, log_event)
    bus.subscribe(DomainEvent, broadcaster.on_event)

    datasets = DatasetService(settings, repositories, bus)
    comparisons = ComparisonService(repositories, bus)
    benchmarks = BenchmarkService(repositories, bus)
    dataset_orchestrator = DatasetOrchestrator(
        settings, repositories, executor, WORKER_JOBS, bus, supervisor, datasets
    )
    comparison_orchestrator = ComparisonOrchestrator(
        settings, repositories, executor, WORKER_JOBS, bus, supervisor, bridge.sink
    )
    benchmark_orchestrator = BenchmarkOrchestrator(
        repositories, bus, supervisor, dataset_orchestrator, comparisons, comparison_orchestrator
    )
    for orchestrator in (dataset_orchestrator, comparison_orchestrator, benchmark_orchestrator):
        orchestrator.register()

    return Container(
        settings=settings,
        bus=bus,
        supervisor=supervisor,
        executor=executor,
        bridge=bridge,
        repositories=repositories,
        broadcaster=broadcaster,
        datasets=datasets,
        comparisons=comparisons,
        benchmarks=benchmarks,
        reports=ReportQueries(repositories),
        rows=RowTrackingService(repositories, DuckDbRowStatusReader(), settings.datasets_dir),
        dataset_orchestrator=dataset_orchestrator,
        comparison_orchestrator=comparison_orchestrator,
        benchmark_orchestrator=benchmark_orchestrator,
    )
