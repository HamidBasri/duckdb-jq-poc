"""Pages through source files and NDJSON outputs with DuckDB, for the data explorer."""

import json
from pathlib import Path

import duckdb

from app.domain.models import DocumentPage, SourceFormat, TablePage
from app.engines.sources import numbered_reader_sql, quote_identifier, quote_literal


class DuckDbDataBrowser:
    """`DataBrowser` backed by DuckDB. Every value comes back as text, as the file holds it."""

    def table_page(
        self, path: Path, fmt: SourceFormat, offset: int, limit: int, search: str | None
    ) -> TablePage:
        rows = numbered_reader_sql(path, fmt)
        with duckdb.connect() as con:
            con.execute("SET enable_progress_bar = false")
            described = con.execute(f"SELECT * FROM ({rows}) LIMIT 0").description or []
            columns = [column[0] for column in described if column[0] != "source_row"]
            texts = [f"CAST({quote_identifier(column)} AS VARCHAR)" for column in columns]
            where, params = _search(texts, search)
            total_row = con.execute(f"SELECT count(*) FROM ({rows}) {where}", params).fetchone()
            data = con.execute(
                f"SELECT source_row, {', '.join(texts) or 'NULL'} FROM ({rows}) {where} "
                "ORDER BY source_row LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        return TablePage(
            columns=columns,
            rows=[list(row[1:]) for row in data],
            row_numbers=[int(row[0]) for row in data],
            total=int(total_row[0]) if total_row else 0,
            offset=offset,
        )

    def document_page(self, path: Path, offset: int, limit: int, search: str | None) -> DocumentPage:
        source = f"read_ndjson_objects({quote_literal(path)})"
        where, params = (
            ("WHERE CAST(json AS VARCHAR) ILIKE ?", [f"%{search.strip()}%"]) if search else ("", [])
        )
        with duckdb.connect() as con:
            con.execute("SET enable_progress_bar = false")
            total_row = con.execute(f"SELECT count(*) FROM {source} {where}", params).fetchone()
            data = con.execute(
                f"SELECT json FROM {source} {where} LIMIT ? OFFSET ?", [*params, limit, offset]
            )
            documents = [json.loads(row[0]) for row in data.fetchall()]
        return DocumentPage(documents=documents, total=int(total_row[0]) if total_row else 0, offset=offset)


def _search(texts: list[str], search: str | None) -> tuple[str, list[object]]:
    """Match a row number exactly, or any field containing the text."""
    if not search or not search.strip() or not texts:
        return "", []
    term = search.strip()
    any_field = " OR ".join(f"{text} ILIKE ?" for text in texts)
    params: list[object] = [f"%{term}%"] * len(texts)
    if term.isdigit():
        return f"WHERE source_row = ? OR {any_field}", [int(term), *params]
    return f"WHERE {any_field}", params
