"""Thin, typed wrapper around the jq CLI."""

import json
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

STDERR_TAIL_CHARS = 2_000


class JqError(RuntimeError):
    """jq exited non-zero, timed out or was killed."""


class JqRunner:
    """Runs jq programs from one directory with `-n -c`, so programs consume `inputs` lazily."""

    def __init__(self, binary: str, programs_dir: Path, timeout_seconds: float) -> None:
        self._binary = binary
        self._programs_dir = programs_dir
        self._timeout_seconds = timeout_seconds

    def run(
        self,
        program: str,
        output: Path,
        inputs: Sequence[Path] = (),
        slurpfiles: Mapping[str, Path] | None = None,
        args: Mapping[str, str] | None = None,
        json_args: Mapping[str, Any] | None = None,
    ) -> None:
        """Run `program` over `inputs`, writing its stdout to `output`.

        Raises:
            JqError: on a non-zero exit (including being killed by the memory watchdog) or timeout.
        """
        command = [self._binary, "-n", "-c", "-L", str(self._programs_dir)]
        for name, path in (slurpfiles or {}).items():
            command += ["--slurpfile", name, str(path)]
        for name, value in (args or {}).items():
            command += ["--arg", name, value]
        for name, value in (json_args or {}).items():
            command += ["--argjson", name, json.dumps(value)]
        command += ["-f", str(self._programs_dir / program), *map(str, inputs)]

        with output.open("wb") as sink:
            try:
                completed = subprocess.run(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=sink,
                    stderr=subprocess.PIPE,
                    timeout=self._timeout_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                raise JqError(f"{program} timed out after {self._timeout_seconds:.0f}s") from exc
        if completed.returncode != 0:
            stderr = completed.stderr.decode(errors="replace")[-STDERR_TAIL_CHARS:]
            raise JqError(f"{program} exited with {completed.returncode}: {stderr.strip()}")
