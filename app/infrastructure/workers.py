"""Process pool for heavy jobs, plus the queue that brings worker events back to the bus.

Each job gets a fresh process, so memory numbers from one run never leak into the next.
"""

import asyncio
import logging
import multiprocessing
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any

from app.domain.events import DomainEvent
from app.domain.ports import EventPublisher, EventSink

logger = logging.getLogger(__name__)

_MP_CONTEXT = multiprocessing.get_context("spawn")


class WorkerCrashed(RuntimeError):
    """The worker process died (e.g. killed by the OS for memory) before returning."""


def configure_worker_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(processName)s %(name)s: %(message)s")


class ProcessJobExecutor:
    """`JobExecutor` backed by a spawn-context process pool that heals itself after a crash."""

    def __init__(self, max_workers: int) -> None:
        self._max_workers = max_workers
        self._executor = self._new_executor()
        self._in_flight = 0

    def _new_executor(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=self._max_workers,
            mp_context=_MP_CONTEXT,
            max_tasks_per_child=1,
            initializer=configure_worker_logging,
        )

    async def submit[ResultT](self, job: Callable[..., ResultT], *args: Any) -> ResultT:
        executor = self._executor
        self._in_flight += 1
        try:
            return await asyncio.wrap_future(executor.submit(job, *args))
        except BrokenProcessPool as exc:
            if executor is self._executor:
                logger.error("worker pool broken; recreating it")
                self._executor = self._new_executor()
            raise WorkerCrashed(f"{getattr(job, '__name__', job)} crashed its worker process") from exc
        finally:
            self._in_flight -= 1

    def shutdown(self) -> None:
        """Wait for a clean shutdown when idle; never block on a long-running job."""
        self._executor.shutdown(wait=self._in_flight == 0, cancel_futures=True)


class WorkerEventBridge:
    """Relays events that workers put on a managed queue onto the in-process event bus."""

    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher
        self._manager = _MP_CONTEXT.Manager()
        self._queue = self._manager.Queue()
        self._task: asyncio.Task[None] | None = None

    @property
    def sink(self) -> EventSink:
        """Picklable handle passed to worker jobs."""
        return self._queue

    def start(self) -> None:
        self._task = asyncio.create_task(self._pump(), name="worker-event-bridge")

    async def stop(self) -> None:
        self._queue.put(None)
        if self._task is not None:
            await self._task
        self._manager.shutdown()

    async def _pump(self) -> None:
        while True:
            event: DomainEvent | None = await asyncio.to_thread(self._queue.get)
            if event is None:
                return
            await self._publisher.publish(event)
