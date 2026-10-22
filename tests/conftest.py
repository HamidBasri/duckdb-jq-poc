import json
from pathlib import Path
from typing import Any

import pytest

from app.config import EngineSettings
from app.domain.events import DomainEvent
from app.domain.models import (
    DatasetManifest,
    DatasetStatus,
    Engine,
    GenerationSpec,
    OutputName,
    RunOutcome,
    SourceFormat,
    TransformParams,
)
from app.domain.ports import RunContext
from app.infrastructure.generator import write_dataset
from app.infrastructure.jobs import run_pipeline_job


class ListSink:
    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    def put(self, event: DomainEvent) -> None:
        self.events.append(event)


def read_ndjson(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open() as handle:
        return [json.loads(line) for line in handle]


class EngineRunner:
    """Runs the pipeline in-process on one dataset; each run gets `runs_dir/<engine>-<format>-<tag>`."""

    def __init__(self, dataset: DatasetManifest, dataset_dir: Path, runs_dir: Path) -> None:
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self.runs_dir = runs_dir

    def run_dir(self, engine: Engine, source_format: SourceFormat, tag: str) -> Path:
        return self.runs_dir / f"{engine}-{source_format}-{tag}"

    def __call__(
        self,
        engine: Engine,
        source_format: SourceFormat = SourceFormat.PARQUET,
        params: TransformParams | None = None,
        tag: str = "",
        settings: EngineSettings | None = None,
        check: bool = True,
    ) -> RunOutcome:
        run_dir = self.run_dir(engine, source_format, tag)
        ctx = RunContext(
            run_id=run_dir.name,
            engine=engine,
            dataset=self._dataset,
            dataset_dir=self._dataset_dir,
            source_format=source_format,
            params=params or TransformParams(),
            work_dir=run_dir / "work",
            output_dir=run_dir / "output",
        )
        outcome = run_pipeline_job(ctx, settings or EngineSettings(), ListSink())
        if check:
            assert outcome.succeeded, outcome.error
        return outcome


RunEngine = EngineRunner


@pytest.fixture(scope="session")
def generated(tmp_path_factory: pytest.TempPathFactory) -> RunEngine:
    """A 3k-row synthetic dataset (both formats) and a function that runs an engine on it."""
    spec = GenerationSpec(rows=3_000, seed=7, dirty_rate=0.05)
    root = tmp_path_factory.mktemp("generated")
    written = write_dataset(spec, root / "dataset")
    manifest = DatasetManifest(
        dataset_id=spec.dataset_id, origin="generated", status=DatasetStatus.READY, files=written.files
    )
    return EngineRunner(manifest, root / "dataset", root / "runs")


def outputs_of(outcome: RunOutcome, name: OutputName) -> list[dict[str, Any]]:
    return read_ndjson(outcome.outputs[name])
