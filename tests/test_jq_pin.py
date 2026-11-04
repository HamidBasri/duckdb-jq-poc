"""The jq engine only runs on the pinned jq version."""

from pathlib import Path

import pytest

from app.config import JQ_VERSION, EngineSettings
from app.engines.jq_engine.engine import JqEngine
from app.engines.jq_engine.runner import JqError, jq_version


def fake_jq(directory: Path, version: str) -> Path:
    script = directory / "jq"
    script.write_text(f"#!/bin/sh\necho jq-{version}\n")
    script.chmod(0o755)
    return script


def test_the_default_jq_is_the_pinned_version() -> None:
    assert jq_version(EngineSettings().jq_binary) == JQ_VERSION


def test_another_jq_version_is_refused(tmp_path: Path) -> None:
    apple = fake_jq(tmp_path, "1.7.1-apple")
    with pytest.raises(JqError, match=rf"is jq 1\.7\.1-apple, but this project needs jq {JQ_VERSION}"):
        JqEngine(EngineSettings(jq_binary=str(apple)), tmp_path / "work")
    assert not (tmp_path / "work").exists()


def test_a_missing_jq_says_how_to_get_it(tmp_path: Path) -> None:
    with pytest.raises(JqError, match="run `make jq`"):
        JqEngine(EngineSettings(jq_binary=str(tmp_path / "nope")), tmp_path / "work")
