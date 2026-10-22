"""Builds the SQL that reads a raw source file into the all-text landing schema.

Both engines use it: DuckDB loads the result into a table; the jq engine has DuckDB write
it out as NDJSON, because jq cannot read CSV or Parquet itself (that conversion is timed
as part of jq's ingest stage).
"""

from pathlib import Path

from app.domain.contract import RAW_COLUMNS
from app.domain.models import Entity, SourceFormat


def quote_literal(value: str | Path) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def source_select_sql(entity: Entity, source: tuple[Path, SourceFormat] | None) -> str:
    """SELECT returning `entity` rows with exactly the contract columns, all as VARCHAR.

    A missing optional entity yields an empty relation so downstream joins still work.
    """
    columns = RAW_COLUMNS[entity]
    if source is None:
        projection = ", ".join(f"CAST(NULL AS VARCHAR) AS {quote_identifier(column)}" for column in columns)
        return f"SELECT {projection} WHERE false"

    path, fmt = source
    reader = (
        f"read_parquet({quote_literal(path)})"
        if fmt is SourceFormat.PARQUET
        else f"read_csv({quote_literal(path)}, header = true, all_varchar = true)"
    )
    projection = ", ".join(
        f"CAST({quote_identifier(c)} AS VARCHAR) AS {quote_identifier(c)}" for c in columns
    )
    return f"SELECT {projection} FROM {reader}"
