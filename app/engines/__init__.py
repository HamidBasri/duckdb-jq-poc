"""Engine registry (factory). Adding an engine = implement `TransformationEngine` + register here."""

from collections.abc import Callable
from pathlib import Path

from app.config import EngineSettings
from app.domain.models import Engine
from app.domain.ports import TransformationEngine
from app.engines.duckdb_engine.engine import DuckDbEngine
from app.engines.jq_engine.engine import JqEngine

EngineFactory = Callable[[EngineSettings, Path], TransformationEngine]

ENGINE_FACTORIES: dict[Engine, EngineFactory] = {
    Engine.DUCKDB: DuckDbEngine,
    Engine.JQ: JqEngine,
}


def create_engine(engine: Engine, settings: EngineSettings, work_dir: Path) -> TransformationEngine:
    return ENGINE_FACTORIES[engine](settings, work_dir)
