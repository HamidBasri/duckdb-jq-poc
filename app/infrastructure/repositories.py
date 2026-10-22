"""Saves records as JSON files and keeps them in memory. Only the event loop thread uses it."""

import os
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel


class JsonFileRepository[ModelT: BaseModel]:
    """Stores each record as `{root}/{key}.json`, or `{root}/{key}/{filename}` when nested."""

    def __init__(
        self, root: Path, model: type[ModelT], key: Callable[[ModelT], str], filename: str | None = None
    ) -> None:
        self._root = root
        self._key = key
        self._filename = filename
        self._records: dict[str, ModelT] = {}
        root.mkdir(parents=True, exist_ok=True)
        pattern = f"*/{filename}" if filename else "*.json"
        for path in root.glob(pattern):
            record = model.model_validate_json(path.read_bytes())
            self._records[key(record)] = record

    def get(self, key: str) -> ModelT | None:
        return self._records.get(key)

    def list(self) -> list[ModelT]:
        return list(self._records.values())

    def save(self, record: ModelT) -> None:
        key = self._key(record)
        self._records[key] = record
        path = self._root / key / self._filename if self._filename else self._root / f"{key}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(record.model_dump_json(indent=2))
        os.replace(temporary, path)  # atomic: readers never see a half-written file
