"""Runtime configuration, overridable through `POC_*` environment variables."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# The jq engine is tested with exactly this version; `make jq` downloads it into .tools/.
JQ_VERSION = (PROJECT_ROOT / ".jq-version").read_text().strip()
PINNED_JQ = PROJECT_ROOT / ".tools" / "jq"


def default_jq_binary() -> str:
    """The jq from `make jq`, or whichever jq is on PATH; the jq engine checks its version either way."""
    return str(PINNED_JQ) if PINNED_JQ.exists() else "jq"


class EngineSettings(BaseModel):
    """The subset of settings shipped to worker processes (must stay picklable and small)."""

    duckdb_threads: int | None = None
    duckdb_memory_limit: str = "8GB"
    duckdb_storage: Literal["memory", "disk"] = "memory"
    duckdb_preserve_insertion_order: bool = True
    duckdb_lean: bool = False
    jq_binary: str = Field(default_factory=default_jq_binary)
    jq_max_rss_mb: int = 12_000
    jq_timeout_seconds: int = 3_600
    monitor_interval_seconds: float = 0.05
    keep_intermediates: bool = False
    track_rows: bool = True


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(env_prefix="POC_", env_nested_delimiter="__")

    data_dir: Path = Path("data")
    max_workers: int = 2
    verify_full_max_rows: int = 200_000
    verify_sample_rows: int = 50_000
    engine: EngineSettings = EngineSettings()

    @property
    def datasets_dir(self) -> Path:
        return self.data_dir / "datasets"

    @property
    def runs_dir(self) -> Path:
        return self.data_dir / "runs"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"


@lru_cache
def get_settings() -> Settings:
    return Settings()
