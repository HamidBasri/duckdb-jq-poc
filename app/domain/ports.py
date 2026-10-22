"""Abstract ports. Application code depends on these, never on concrete adapters."""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Self

from pydantic import BaseModel

from app.domain.events import DomainEvent
from app.domain.models import (
    DatasetManifest,
    Engine,
    Entity,
    GeneratedDataset,
    GenerationSpec,
    OutputName,
    RowState,
    RowStatus,
    RowTrackingSummary,
    RunOutcome,
    SourceFormat,
    Stage,
    TransformParams,
    VerificationResult,
)

if TYPE_CHECKING:
    from app.config import EngineSettings


class EventSink(Protocol):
    """Fire-and-forget event emission; implemented by a cross-process queue in workers."""

    def put(self, event: DomainEvent) -> None: ...


class EventPublisher(Protocol):
    async def publish(self, event: DomainEvent) -> None: ...


class Repository[ModelT: BaseModel](Protocol):
    def get(self, key: str) -> ModelT | None: ...
    def save(self, record: ModelT) -> None: ...
    def list(self) -> list[ModelT]: ...


class JobExecutor(Protocol):
    """Runs CPU-heavy callables away from the event loop (a process pool in practice)."""

    async def submit[ResultT](self, job: Callable[..., ResultT], *args: Any) -> ResultT: ...


@dataclass(frozen=True)
class RunContext:
    """Everything an engine needs to execute one pipeline run."""

    run_id: str
    engine: Engine
    dataset: DatasetManifest
    dataset_dir: Path
    source_format: SourceFormat
    params: TransformParams
    work_dir: Path
    output_dir: Path

    def output_path(self, name: OutputName) -> Path:
        suffix = {OutputName.QUALITY_REPORT: "json", OutputName.ROW_STATUS: "parquet"}.get(name, "ndjson")
        return self.output_dir / f"{name}.{suffix}"


@dataclass
class StageResult:
    """What a stage reports back: its output row count, files and engine-computed stats."""

    rows_out: int
    outputs: dict[OutputName, Path] = field(default_factory=dict)
    detail: dict[str, Any] = field(default_factory=dict)


class TransformationEngine(Protocol):
    """Strategy interface: each engine implements the same five stages with the same semantics."""

    name: Engine

    def run_stage(self, stage: Stage, ctx: RunContext) -> StageResult: ...
    def close(self) -> None: ...


class StageMonitor(Protocol):
    """Measures one stage while used as a context manager."""

    wall_seconds: float
    cpu_seconds: float
    limit_exceeded: bool

    @property
    def peak_rss_mb(self) -> float: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *exc_info: object) -> None: ...


class RowStatusReader(Protocol):
    """Read side of the per-row status table (and the files it points into)."""

    def find(
        self, path: Path, state: RowState | None, search: str | None, limit: int, offset: int
    ) -> tuple[int, list[RowStatus]]: ...
    def get(self, path: Path, source_row: int) -> RowStatus | None: ...
    def source_record(self, path: Path, fmt: SourceFormat, source_row: int) -> dict[str, Any] | None: ...
    def output_document(self, path: Path, byte_offset: int) -> dict[str, Any]: ...
    def reject_record(self, path: Path, trade_id: str) -> dict[str, Any] | None: ...


EngineFactory = Callable[[Engine, "EngineSettings", Path], TransformationEngine]
RowTracker = Callable[[RunContext, dict[OutputName, str]], RowTrackingSummary]
MonitorFactory = Callable[[float, int | None], StageMonitor]


@dataclass(frozen=True)
class WorkerJobs:
    """Worker-process entrypoints, injected so orchestration never imports infrastructure.

    Each must be a module-level function (picklable under the spawn start method).
    """

    generate_dataset: Callable[[GenerationSpec, Path], GeneratedDataset]
    inspect_upload: Callable[[DatasetManifest, Path], dict[Entity, int]]
    run_pipeline: Callable[[RunContext, "EngineSettings", EventSink], RunOutcome]
    verify: Callable[
        [Engine, dict[OutputName, str], Engine, dict[OutputName, str], int, int], VerificationResult
    ]
