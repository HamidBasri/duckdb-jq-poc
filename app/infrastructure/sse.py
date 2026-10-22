"""Fan-out of domain events to Server-Sent-Events clients."""

import asyncio
import dataclasses
from collections.abc import AsyncIterator

import pydantic_core

from app.domain.events import DomainEvent

CLIENT_QUEUE_SIZE = 1_000
HEARTBEAT_SECONDS = 15.0


def serialize_event(event: DomainEvent) -> str:
    payload = {"type": event.name, **{f.name: getattr(event, f.name) for f in dataclasses.fields(event)}}
    return pydantic_core.to_json(payload).decode()


class EventBroadcaster:
    """Each connected client gets a bounded queue; a slow client drops events, never blocks the bus."""

    def __init__(self) -> None:
        self._clients: set[asyncio.Queue[str]] = set()

    async def on_event(self, event: DomainEvent) -> None:
        message = f"event: {event.name}\ndata: {serialize_event(event)}\n\n"
        for queue in self._clients:
            if not queue.full():
                queue.put_nowait(message)

    async def stream(self) -> AsyncIterator[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=CLIENT_QUEUE_SIZE)
        self._clients.add(queue)
        try:
            while True:
                try:
                    yield await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
                except TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            self._clients.discard(queue)
