"""Reads the columns and first rows of a source file, so a user can map them to the contract."""

from pathlib import Path

import duckdb

from app.domain.models import SourceFormat
from app.engines.sources import reader_sql

MAX_SAMPLE_CHARS = 60


class DuckDbColumnReader:
    """`ColumnReader` backed by DuckDB; reads only the header and the first rows."""

    def read(
        self, path: Path, fmt: SourceFormat, sample_rows: int
    ) -> tuple[list[str], list[list[str | None]]]:
        with duckdb.connect() as con:
            con.execute("SET enable_progress_bar = false")
            cursor = con.execute(f"SELECT * FROM {reader_sql(path, fmt)} LIMIT {int(sample_rows)}")
            columns = [column[0] for column in cursor.description or []]
            rows = [
                [None if value is None else str(value)[:MAX_SAMPLE_CHARS] for value in row]
                for row in cursor.fetchall()
            ]
        return columns, rows
