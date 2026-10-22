"""Per-stage resource measurement for the current process and its child processes.

DuckDB runs in-process (multi-threaded); jq runs as child processes. Sampling the whole
process tree measures both the same way. CPU time comes from `getrusage`, which (unlike
psutil on macOS) includes reaped children, so jq's CPU is counted too.
"""

import logging
import resource
import threading
import time
from typing import Self

import psutil

logger = logging.getLogger(__name__)

BYTES_PER_MB = 1024 * 1024


def cpu_seconds_used() -> float:
    """User + system CPU seconds of this process and every child it has waited for."""
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


class ResourceMonitor:
    """Context manager sampling wall time, CPU time and peak tree RSS.

    If `child_rss_limit_mb` is set and child processes together exceed it, they are killed
    (the in-process engine is bounded by its own memory limit instead).
    """

    def __init__(self, interval_seconds: float, child_rss_limit_mb: int | None = None) -> None:
        self._interval = interval_seconds
        self._child_limit_bytes = child_rss_limit_mb * BYTES_PER_MB if child_rss_limit_mb else None
        self._process = psutil.Process()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample_loop, name="resource-monitor", daemon=True)
        self._peak_rss_bytes = 0
        self._started_at = 0.0
        self._cpu_at_start = 0.0
        self.wall_seconds = 0.0
        self.cpu_seconds = 0.0
        self.limit_exceeded = False

    @property
    def peak_rss_mb(self) -> float:
        return self._peak_rss_bytes / BYTES_PER_MB

    def __enter__(self) -> Self:
        self._started_at = time.perf_counter()
        self._cpu_at_start = cpu_seconds_used()
        self._sample()
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._stop.set()
        self._thread.join()
        self._sample()
        self.wall_seconds = time.perf_counter() - self._started_at
        self.cpu_seconds = cpu_seconds_used() - self._cpu_at_start

    def _sample_loop(self) -> None:
        while not self._stop.wait(self._interval):
            self._sample()

    def _sample(self) -> None:
        try:
            children = self._process.children(recursive=True)
            child_rss = sum(self._rss(child) for child in children)
            self._peak_rss_bytes = max(self._peak_rss_bytes, self._rss(self._process) + child_rss)
        except psutil.Error:
            return
        if self._child_limit_bytes is not None and child_rss > self._child_limit_bytes:
            self.limit_exceeded = True
            logger.warning(
                "child processes use %.0f MB, over the limit; killing them", child_rss / BYTES_PER_MB
            )
            for child in children:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    continue

    @staticmethod
    def _rss(process: psutil.Process) -> int:
        try:
            return int(process.memory_info().rss)
        except psutil.NoSuchProcess:
            return 0
