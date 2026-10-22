"""Domain events. Commands are expressed as `*Requested` events; everything else is a fact.

Events are frozen dataclasses so they pickle cleanly across the worker-process boundary.
"""

from dataclasses import dataclass, field
from datetime import datetime

from app.domain.models import (
    Entity,
    GenerationSpec,
    OutputName,
    RunOutcome,
    SourceFormat,
    Stage,
    StageMetrics,
    VerificationResult,
    utc_now,
)


@dataclass(frozen=True, kw_only=True)
class DomainEvent:
    occurred_at: datetime = field(default_factory=utc_now)

    @property
    def name(self) -> str:
        return type(self).__name__


# --- datasets ---------------------------------------------------------------------------
@dataclass(frozen=True, kw_only=True)
class DatasetGenerationRequested(DomainEvent):
    dataset_id: str
    spec: GenerationSpec


@dataclass(frozen=True, kw_only=True)
class DatasetUploaded(DomainEvent):
    dataset_id: str


@dataclass(frozen=True, kw_only=True)
class DatasetReady(DomainEvent):
    dataset_id: str
    row_counts: dict[Entity, int]
    size_bytes: dict[SourceFormat, int]
    seconds: float


@dataclass(frozen=True, kw_only=True)
class DatasetFailed(DomainEvent):
    dataset_id: str
    error: str


# --- pipeline runs (emitted from worker processes) --------------------------------------
@dataclass(frozen=True, kw_only=True)
class RunStarted(DomainEvent):
    run_id: str
    worker_pid: int


@dataclass(frozen=True, kw_only=True)
class StageStarted(DomainEvent):
    run_id: str
    stage: Stage


@dataclass(frozen=True, kw_only=True)
class StageCompleted(DomainEvent):
    run_id: str
    metrics: StageMetrics


@dataclass(frozen=True, kw_only=True)
class RunFinished(DomainEvent):
    run_id: str
    outcome: RunOutcome


@dataclass(frozen=True, kw_only=True)
class OutputsDiscarded(DomainEvent):
    run_id: str
    outputs: list[OutputName]


# --- comparisons & benchmarks -----------------------------------------------------------
@dataclass(frozen=True, kw_only=True)
class ComparisonRequested(DomainEvent):
    comparison_id: str


@dataclass(frozen=True, kw_only=True)
class ComparisonStarted(DomainEvent):
    comparison_id: str


@dataclass(frozen=True, kw_only=True)
class VerificationStarted(DomainEvent):
    comparison_id: str


@dataclass(frozen=True, kw_only=True)
class ComparisonCompleted(DomainEvent):
    comparison_id: str
    verification: VerificationResult | None
    note: str | None = None


@dataclass(frozen=True, kw_only=True)
class BenchmarkRequested(DomainEvent):
    benchmark_id: str


@dataclass(frozen=True, kw_only=True)
class BenchmarkProgressed(DomainEvent):
    benchmark_id: str
    comparison_id: str


@dataclass(frozen=True, kw_only=True)
class BenchmarkCompleted(DomainEvent):
    benchmark_id: str
    error: str | None = None
