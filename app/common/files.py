"""Small file helpers shared by engines and verification."""

from pathlib import Path

CHUNK_BYTES = 8 * 1024 * 1024


def count_lines(path: Path) -> int:
    """Count newline-terminated records without decoding (fast for multi-GB NDJSON)."""
    count = 0
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_BYTES):
            count += chunk.count(b"\n")
    return count
