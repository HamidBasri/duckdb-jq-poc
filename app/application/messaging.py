"""In-process event bus and background-task supervision."""

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

from app.domain.events import DomainEvent

logger = logging.getLogger(__name__)


class EventBus:
    """Async publish/subscribe keyed by event type (subscribing to a base class receives subclasses).

    `publish` awaits handlers in subscription order, so state projections are applied in
    event order. Handlers must stay quick; long work is spawned on the `TaskSupervisor`.
    """

    def __init__(self) -> None:
        self._handlers: defaultdict[type[DomainEvent], list[Callable[[Any], Awaitable[None]]]] = defaultdict(
            list
        )

    def subscribe[EventT: DomainEvent](
        self, event_type: type[EventT], handler: Callable[[EventT], Awaitable[None]]
    ) -> None:
        self._handlers[event_type].append(handler)

    async def publish(self, event: DomainEvent) -> None:
        for event_type in type(event).__mro__:
            for handler in self._handlers.get(event_type, ()):
                try:
                    await handler(event)
                except Exception:
                    logger.exception(
                        "handler %s failed on %s", getattr(handler, "__qualname__", handler), event.name
                    )


class TaskSupervisor:
    """Owns fire-and-forget background tasks: keeps references, logs failures, cancels on shutdown."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()

    def spawn[ResultT](self, coroutine: Coroutine[Any, Any, ResultT], name: str) -> asyncio.Task[ResultT]:
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._on_done)
        return task

    def _on_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("background task %s failed", task.get_name(), exc_info=exc)

    async def shutdown(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
