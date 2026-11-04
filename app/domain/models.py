"""Records for datasets, runs, comparisons and benchmarks, saved as JSON and returned by the API."""

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


# Per entity: expected column -> column in the source file, or None to leave it empty.
ColumnMapping = dict[Entity, dict[str, str | None]]


class CellEdit(BaseModel):
    """One field changed by hand in a dataset version."""

    entity: Entity
    row: int = Field(ge=1, description="1-based row in the source file")
    column: str
    value: str | None = Field(description="New text; None empties the field")


class RowRef(BaseModel):
    entity: Entity
    row: int = Field(ge=1)


class CorrectionRule(BaseModel):
    """'When this column holds this value, use that one instead', applied while the data is read."""

    entity: Entity
    column: str
    match: str = Field(description="Compared after trimming spaces; an empty string matches a missing value")
    replace: str | None = Field(description="Value to use instead; None empties the field")
    note: str = ""
    enabled: bool = True


class RuleSet(BaseModel):
    """Correction rules and validation switches. Every change saves a new version; runs keep the one
    they used, so results can always be explained."""

    version: int = 0
    corrections: list[CorrectionRule] = Field(default_factory=list)
    disabled_rules: list[str] = Field(default_factory=list, description="Error codes that no longer reject")
    note: str = ""
    created_at: datetime = Field(default_factory=utc_now)

    def corrections_for(self, entity: Entity) -> list[CorrectionRule]:
        return [rule for rule in self.corrections if rule.enabled and rule.entity is entity]


class Counter(BaseModel):
    """The last display number handed out for one kind of record, so numbers are never reused."""

    name: str
    value: int = 0


class DatasetManifest(BaseModel):
    dataset_id: str
    origin: str  # "generated" | "uploaded" | "version"
    number: int = Field(default=0, description="Uploads only, shown as 'Upload 3'; never reused")
    status: DatasetStatus = DatasetStatus.PENDING
    created_at: datetime = Field(default_factory=utc_now)
    generation: GenerationSpec | None = None
    files: dict[Entity, dict[SourceFormat, str]] = Field(default_factory=dict)
    row_counts: dict[Entity, int] = Field(default_factory=dict)
    size_bytes: dict[SourceFormat, int] = Field(default_factory=dict)
    generation_seconds: float | None = None
    error: str | None = None
    # Versions: a copy of another dataset with its columns renamed to the contract.
    version: int = 1
    parent_id: str | None = None
    root_id: str | None = None
    column_mapping: ColumnMapping | None = None
    edits: list[CellEdit] = Field(default_factory=list)
    deleted_rows: list[RowRef] = Field(default_factory=list)

    def source_path(
        self, root: Path, entity: Entity, preferred: SourceFormat
    ) -> tuple[Path, SourceFormat] | None:
        """Resolve the file for `entity`, preferring `preferred` and falling back to any format."""
        available = self.files.get(entity, {})
        for fmt in (preferred, *SourceFormat):
            if fmt in available:
                return root / available[fmt], fmt
        return None


class EntityColumns(BaseModel):
    """One source file's columns next to the columns the pipeline expects."""

    entity: Entity
    file: str
    found: list[str]
    expected: list[str]
    missing: list[str]
    suggested: dict[str, str | None]
    samples: dict[str, list[str | None]]


class TablePage(BaseModel):
    """A page of rows from a source file, every value as text."""

    columns: list[str]
    rows: list[list[str | None]]
    row_numbers: list[int]
    total: int
    offset: int


class DocumentPage(BaseModel):
    """A page of documents from an NDJSON output file."""

    documents: list[dict[str, Any]]
    total: int
    offset: int


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
    number: int = Field(default=0, description="Its comparison's number, shown as 'Run 12'")
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
    rules: RuleSet | None = None

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
    number: int = Field(default=0, description="Shown as 'Run 12'; never reused, even after a delete")
    dataset_id: str
    source_format: SourceFormat
    params: TransformParams
    engines: list[Engine]
    run_ids: dict[Engine, str]
    verify: bool = True
    parallel_engines: bool = False
    benchmark_id: str | None = None
    rules_version: int = 0
    status: ComparisonStatus = ComparisonStatus.QUEUED
    created_at: datetime = Field(default_factory=utc_now)
    finished_at: datetime | None = None
    verification: VerificationResult | None = None
    note: str | None = None


class Benchmark(BaseModel):
    benchmark_id: str
    number: int = Field(default=0, description="Shown as 'Benchmark 3'; never reused, even after a delete")
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
