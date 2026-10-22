"""HTTP request bodies (responses reuse domain and reporting models)."""

from pydantic import BaseModel, Field, field_validator

from app.domain.models import SIZE_PRESETS, Engine, GenerationSpec, SourceFormat, TransformParams

DEFAULT_ENGINES = [Engine.DUCKDB, Engine.JQ]


def parse_size(value: str | int) -> int:
    """Accept a preset label (`5k`, `1m`) or a plain row count."""
    if isinstance(value, int):
        return value
    label = value.strip().lower()
    if label in SIZE_PRESETS:
        return SIZE_PRESETS[label]
    if label.isdigit():
        return int(label)
    raise ValueError(f"unknown size {value!r}; use one of {', '.join(SIZE_PRESETS)} or a row count")


class GenerateRequest(GenerationSpec):
    overwrite: bool = False


class DatasetProfile(BaseModel):
    """Generation knobs shared by preset and benchmark requests."""

    sizes: list[int] = Field(default_factory=lambda: list(SIZE_PRESETS.values()), min_length=1)
    seed: int = 42
    dirty_rate: float = Field(default=0.02, ge=0.0, le=0.5)
    hierarchy_depth: int = Field(default=7, ge=1, le=1_000)
    max_chain_length: int = Field(default=5, ge=1, le=500)
    formats: list[SourceFormat] = Field(default_factory=lambda: [SourceFormat.PARQUET, SourceFormat.CSV])

    @field_validator("sizes", mode="before")
    @classmethod
    def _parse_sizes(cls, values: list[str | int]) -> list[int]:
        return [parse_size(value) for value in values]

    def specs(self) -> list[GenerationSpec]:
        return [
            GenerationSpec(
                rows=rows,
                seed=self.seed,
                dirty_rate=self.dirty_rate,
                hierarchy_depth=self.hierarchy_depth,
                max_chain_length=self.max_chain_length,
                formats=self.formats,
            )
            for rows in self.sizes
        ]


class ComparisonRequest(BaseModel):
    dataset_id: str
    engines: list[Engine] = Field(default_factory=lambda: list(DEFAULT_ENGINES), min_length=1)
    source_format: SourceFormat = SourceFormat.PARQUET
    params: TransformParams = Field(default_factory=TransformParams)
    verify: bool = True
    parallel_engines: bool = Field(
        default=False, description="Run engines at the same time (faster, but they compete for CPU)"
    )


class BenchmarkRequest(DatasetProfile):
    sizes: list[int] = Field(default_factory=lambda: [5_000, 50_000, 500_000], min_length=1)
    engines: list[Engine] = Field(default_factory=lambda: list(DEFAULT_ENGINES), min_length=1)
    source_format: SourceFormat = SourceFormat.PARQUET
    params: TransformParams = Field(default_factory=TransformParams)
    verify: bool = True
    discard_outputs: bool = Field(
        default=True, description="Delete multi-GB trade/reject outputs after verifying"
    )
