"""SQL that reads a source file into the raw schema, with every column as text.

The jq engine uses it too, through DuckDB, because jq can't read CSV or Parquet.
"""

from collections.abc import Sequence
from pathlib import Path

from app.domain.contract import RAW_COLUMNS
from app.domain.models import CorrectionRule, Entity, SourceFormat


def quote_literal(value: str | Path) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def reader_sql(path: Path, fmt: SourceFormat) -> str:
    """Table function reading one source file; CSV values all come back as text."""
    if fmt is SourceFormat.PARQUET:
        return f"read_parquet({quote_literal(path)})"
    return f"read_csv({quote_literal(path)}, header = true, all_varchar = true)"


def numbered_reader_sql(path: Path, fmt: SourceFormat) -> str:
    """Every row of a source file with its 1-based position in the file as `source_row`."""
    literal = quote_literal(path)
    if fmt is SourceFormat.PARQUET:
        return (
            "SELECT file_row_number + 1 AS source_row, * EXCLUDE (file_row_number) "
            f"FROM read_parquet({literal}, file_row_number = true)"
        )
    # CSV has no row-number column; insertion order is preserved, so row_number() follows the file.
    return (
        "SELECT row_number() OVER () AS source_row, * "
        f"FROM read_csv({literal}, header = true, all_varchar = true)"
    )


def _matches(value: str, match: str) -> str:
    match = match.strip()
    return f"nullif(trim({value}), '') IS NULL" if not match else f"trim({value}) = {quote_literal(match)}"


def corrected_sql(column: str, rules: Sequence[CorrectionRule]) -> str:
    """The column as text, with correction rules applied in order (the first match wins)."""
    value = f"CAST({quote_identifier(column)} AS VARCHAR)"
    cases = [
        f"WHEN {_matches(value, rule.match)} THEN "
        + ("NULL" if rule.replace is None else quote_literal(rule.replace))
        for rule in rules
        if rule.column == column
    ]
    return f"CASE {' '.join(cases)} ELSE {value} END" if cases else value


def source_select_sql(
    entity: Entity, source: tuple[Path, SourceFormat] | None, rules: Sequence[CorrectionRule] = ()
) -> str:
    """SELECT returning `entity` rows with exactly the contract columns, all as VARCHAR.

    Correction rules are applied here, so both engines read the same corrected values.
    A missing optional entity yields an empty relation so downstream joins still work.
    """
    columns = RAW_COLUMNS[entity]
    if source is None:
        projection = ", ".join(f"CAST(NULL AS VARCHAR) AS {quote_identifier(column)}" for column in columns)
        return f"SELECT {projection} WHERE false"

    projection = ", ".join(f"{corrected_sql(c, rules)} AS {quote_identifier(c)}" for c in columns)
    return f"SELECT {projection} FROM {reader_sql(*source)}"
