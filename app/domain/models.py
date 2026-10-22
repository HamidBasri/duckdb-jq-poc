"""Domain records: datasets, pipeline runs, comparisons and benchmarks.

Pydantic models are used for records because they are persisted as JSON and exposed
over the API unchanged; keeping one representation avoids a mapping layer in a POC.
"""

from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator


def utc_now() -> datetime:
    return datetime.now(UTC)


class Engine(StrEnum):
    DUCKDB = "duckdb"
    JQ = "jq"


class SourceFormat(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"


class Entity(StrEnum):
    TRADES = "trades"
    COUNTERPARTIES = "counterparties"
    INSTRUMENTS = "instruments"
    BOOKS = "books"
    FX_RATES = "fx_rates"


class Stage(StrEnum):
    INGEST = "ingest"
    CLEAN = "clean"
    VALIDATE = "validate"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"


PIPELINE_STAGES: tuple[Stage, ...] = tuple(Stage)


class OutputName(StrEnum):
    TRADES = "trades"
    REJECTS = "rejects"
    EXPOSURES = "exposures"
    QUALITY_REPORT = "quality_report"
    ROW_STATUS = "row_status"


class RowState(StrEnum):
    """Where one raw input row ended up."""

    DELIVERED = "delivered"
    REJECTED = "rejected"
    DUPLICATE = "duplicate"
    FILTERED_OUT = "filtered_out"


class DatasetStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ComparisonStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    VERIFYING = "verifying"
    COMPLETED = "completed"


class BenchmarkStatus(StrEnum):
    PREPARING = "preparing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


SIZE_PRESETS: dict[str, int] = {
    "5k": 5_000,
    "50k": 50_000,
    "500k": 500_000,
    "1m": 1_000_000,
    "5m": 5_000_000,
}


def size_label(rows: int) -> str:
    """Return a compact label such as `50k` or `1m` for a row count."""
    if rows % 1_000_000 == 0:
        return f"{rows // 1_000_000}m"
    if rows % 1_000 == 0:
        return f"{rows // 1_000}k"
    return str(rows)


class GenerationSpec(BaseModel):
    """Parameters that fully determine a synthetic dataset (same spec => same bytes)."""

    rows: int = Field(ge=100, le=20_000_000)
    seed: int = 42
    dirty_rate: float = Field(default=0.02, ge=0.0, le=0.5)
    hierarchy_depth: int = Field(default=7, ge=1, le=1_000, description="Max counterparty parent-chain depth")
    max_chain_length: int = Field(default=5, ge=1, le=500, description="Max trade amendment chain length")
    formats: list[SourceFormat] = Field(default_factory=lambda: [SourceFormat.PARQUET, SourceFormat.CSV])

    @property
    def dataset_id(self) -> str:
        return (
            f"trades-{size_label(self.rows)}-s{self.seed}-d{round(self.dirty_rate * 1000)}"
            f"-h{self.hierarchy_depth}-c{self.max_chain_length}"
        )


class GeneratedDataset(BaseModel):
    """Result of writing a dataset to disk."""

    files: dict[Entity, dict[SourceFormat, str]]
    row_counts: dict[Entity, int]
    size_bytes: dict[SourceFormat, int]
    seconds: float


class DatasetManifest(BaseModel):
    dataset_id: str
    origin: str  # "generated" | "uploaded"
    status: DatasetStatus = DatasetStatus.PENDING
    created_at: datetime = Field(default_factory=utc_now)
    generation: GenerationSpec | None = None
    files: dict[Entity, dict[SourceFormat, str]] = Field(default_factory=dict)
    row_counts: dict[Entity, int] = Field(default_factory=dict)
    size_bytes: dict[SourceFormat, int] = Field(default_factory=dict)
    generation_seconds: float | None = None
    error: str | None = None

    def source_path(
        self, root: Path, entity: Entity, preferred: SourceFormat
    ) -> tuple[Path, SourceFormat] | None:
        """Resolve the file for `entity`, preferring `preferred` and falling back to any format."""
        available = self.files.get(entity, {})
        for fmt in (preferred, *SourceFormat):
            if fmt in available:
                return root / available[fmt], fmt
        return None


class TransformParams(BaseModel):
    """Row filters applied in the transform stage (identical semantics in both engines)."""

    trade_date_from: date | None = None
    trade_date_to: date | None = None
    instrument_types: list[str] | None = None
    min_notional_usd: float | None = None
    latest_only: bool = False

    @model_validator(mode="after")
    def _normalise_types(self) -> Self:
        if self.instrument_types is not None:
            self.instrument_types = sorted({value.strip().upper() for value in self.instrument_types})
        return self


class StageMetrics(BaseModel):
    stage: Stage
    started_at: datetime | None = None
    finished_at: datetime | None = None
    wall_seconds: float
    cpu_seconds: float
    peak_rss_mb: float
    rows_out: int
    detail: dict[str, Any] = Field(default_factory=dict)

    @property
    def cpu_utilization(self) -> float:
        """Average busy cores during the stage (1.0 = one core fully used)."""
        return self.cpu_seconds / self.wall_seconds if self.wall_seconds else 0.0


class RowTrackingSummary(BaseModel):
    """Counts from the per-row status table built after a run."""

    rows: int
    states: dict[RowState, int]
    in_exposure: int
    seconds: float
    error: str | None = None


class RowStatus(BaseModel):
    """The journey of one raw input row through a pipeline run."""

    source_row: int = Field(description="1-based position in the source trades file")
    trade_id: str | None
    state: RowState
    stopped_at: Stage | None = Field(description="Stage that removed the row; None when delivered")
    duplicate_of_row: int | None
    errors: list[str]
    warnings: list[str]
    event_type: str | None
    version: int | None
    is_latest: bool | None
    in_exposure: bool
    exposure_group: str | None
    output_offset: int | None = Field(description="Byte offset of the row's document in trades.ndjson")


class RunOutcome(BaseModel):
    """What a worker hands back when a pipeline run ends."""

    succeeded: bool
    stages: list[StageMetrics]
    outputs: dict[OutputName, str] = Field(default_factory=dict)
    quality: dict[str, Any] | None = None
    tracking: RowTrackingSummary | None = None
    error: str | None = None


class PipelineRun(BaseModel):
    run_id: str
    comparison_id: str
    dataset_id: str
    engine: Engine
    source_format: SourceFormat
    params: TransformParams
    status: RunStatus = RunStatus.QUEUED
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    worker_pid: int | None = None
    current_stage: Stage | None = None
    stages: list[StageMetrics] = Field(default_factory=list)
    outputs: dict[OutputName, str] = Field(default_factory=dict)
    quality: dict[str, Any] | None = None
    tracking: RowTrackingSummary | None = None
    error: str | None = None

    @property
    def total_seconds(self) -> float:
        return sum(stage.wall_seconds for stage in self.stages)

    @property
    def peak_rss_mb(self) -> float:
        return max((stage.peak_rss_mb for stage in self.stages), default=0.0)

    @property
    def cpu_seconds(self) -> float:
        return sum(stage.cpu_seconds for stage in self.stages)


class OutputCheck(BaseModel):
    output: OutputName
    left_rows: int
    right_rows: int
    keys_match: bool
    compared_docs: int
    mismatched_docs: int
    examples: list[dict[str, Any]] = Field(default_factory=list)

    @property
    def equivalent(self) -> bool:
        return self.left_rows == self.right_rows and self.keys_match and self.mismatched_docs == 0


class VerificationResult(BaseModel):
    left_engine: Engine
    right_engine: Engine
    checks: list[OutputCheck]
    quality_reports_match: bool
    sampled: bool
    seconds: float

    @property
    def equivalent(self) -> bool:
        return self.quality_reports_match and all(check.equivalent for check in self.checks)


class Comparison(BaseModel):
    comparison_id: str
    dataset_id: str
    source_format: SourceFormat
    params: TransformParams
    engines: list[Engine]
    run_ids: dict[Engine, str]
    verify: bool = True
    parallel_engines: bool = False
    benchmark_id: str | None = None
    status: ComparisonStatus = ComparisonStatus.QUEUED
    created_at: datetime = Field(default_factory=utc_now)
    finished_at: datetime | None = None
    verification: VerificationResult | None = None
    note: str | None = None


class Benchmark(BaseModel):
    benchmark_id: str
    specs: list[GenerationSpec]
    engines: list[Engine]
    source_format: SourceFormat
    params: TransformParams
    verify: bool
    discard_outputs: bool
    status: BenchmarkStatus = BenchmarkStatus.PREPARING
    created_at: datetime = Field(default_factory=utc_now)
    finished_at: datetime | None = None
    comparison_ids: list[str] = Field(default_factory=list)
    error: str | None = None
