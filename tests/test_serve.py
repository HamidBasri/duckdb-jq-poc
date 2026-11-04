"""`make run` moves to the next free port when the asked one is taken."""

import socket
from collections.abc import Iterator

import pytest

from app.serve import open_listener

HOST = "127.0.0.1"


@pytest.fixture
def taken_port() -> Iterator[int]:
    """A port with a server listening on it, on all addresses (the case a plain bind can miss on macOS)."""
    with socket.socket() as server:
        server.bind(("0.0.0.0", 0))
        server.listen()
        yield server.getsockname()[1]


def test_a_free_port_is_used_as_asked() -> None:
    with socket.socket() as probe:
        probe.bind((HOST, 0))
        free = probe.getsockname()[1]
    with open_listener(HOST, free) as listener:
        assert listener.getsockname()[1] == free


def test_a_taken_port_moves_to_a_later_free_one(taken_port: int) -> None:
    with open_listener(HOST, taken_port) as listener:
        assert listener.getsockname()[1] > taken_port


def test_gives_up_with_a_hint_when_every_port_is_taken(taken_port: int) -> None:
    with pytest.raises(SystemExit, match="make run PORT="):
        open_listener(HOST, taken_port, attempts=1)
