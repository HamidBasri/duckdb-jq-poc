"""Builds the per-row status table after a run and reads it back for the tracker.

The engines process whole sets of rows, so there is no per-row event to log. Instead this
compares the source file with the run's outputs and records where each row ended up. It only
reads files, so it works the same for both engines.
"""

import json
import re
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from app.domain.models import (
    CorrectionRule,
    Entity,
    OutputName,
    RowState,
    RowStatus,
    RowTrackingSummary,
    SourceFormat,
)
from app.domain.ports import RunContext
from app.engines.sources import corrected_sql, numbered_reader_sql, quote_literal

TRADE_ID_PATTERN = re.compile(rb'"trade_id":"((?:[^"\\]|\\.)*)"')
READ_CHUNK_LINES = 1 << 16

_REJECT_COLUMNS = "{'trade_id': 'VARCHAR', 'errors': 'VARCHAR[]', 'warnings': 'VARCHAR[]'}"
_TRADE_COLUMNS = (
    "{'trade_id': 'VARCHAR', 'event_type': 'VARCHAR', "
    "'lineage': 'STRUCT(version INTEGER, is_latest BOOLEAN)', "
    "'data_quality': 'STRUCT(warnings VARCHAR[])', "
    "'counterparty': 'STRUCT(counterparty_id VARCHAR, ultimate_parent STRUCT(counterparty_id VARCHAR))'}"
)


def _trade_id_sql(rules: Sequence[CorrectionRule]) -> str:
    """The trade id the engines saw: correction rules applied, then trimmed, blank as NULL."""
    return f"nullif(trim({corrected_sql('trade_id', rules)}), '')"


def _output_offsets(trades: Path) -> pa.Table:
    """Byte offset of every document in trades.ndjson, keyed by trade_id (first key in each line)."""
    trade_ids: list[str] = []
    offsets: list[int] = []
    position = 0
    with trades.open("rb") as handle:
        for line in handle:
            if match := TRADE_ID_PATTERN.search(line):
                trade_ids.append(json.loads(b'"' + match.group(1) + b'"'))
                offsets.append(position)
            position += len(line)
    return pa.table(
        {"trade_id": pa.array(trade_ids, pa.string()), "byte_offset": pa.array(offsets, pa.int64())}
    )


def build_row_status(ctx: RunContext, outputs: dict[OutputName, str]) -> RowTrackingSummary:
    """Write `row_status.parquet` for a finished run and return its counts."""
    started = time.perf_counter()
    source = ctx.dataset.source_path(ctx.dataset_dir, Entity.TRADES, ctx.source_format)
    if source is None:
        raise FileNotFoundError("the dataset has no trades file")
    target = ctx.output_path(OutputName.ROW_STATUS)
    offsets = _output_offsets(Path(outputs[OutputName.TRADES]))
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false")
        con.register("output_offsets", offsets)
        trade_id = _trade_id_sql(ctx.rules.corrections_for(Entity.TRADES))
        rows = numbered_reader_sql(*source)
        con.execute(f"CREATE TABLE raw AS SELECT source_row, {trade_id} AS trade_id FROM ({rows})")
        con.execute(
            f"CREATE TABLE rejects AS SELECT * FROM read_json({quote_literal(outputs[OutputName.REJECTS])}, "
            f"format = 'newline_delimited', columns = {_REJECT_COLUMNS})"
        )
        con.execute(
            "CREATE TABLE delivered AS SELECT trade_id, event_type, lineage.version AS version, "
            "lineage.is_latest AS is_latest, data_quality.warnings AS warnings, "
            "coalesce(counterparty.ultimate_parent.counterparty_id, counterparty.counterparty_id) "
            "AS exposure_group "
            f"FROM read_json({quote_literal(outputs[OutputName.TRADES])}, format = 'newline_delimited', "
            f"columns = {_TRADE_COLUMNS})"
        )
        con.execute(f"""
            COPY (
                WITH ranked AS (
                    SELECT source_row, trade_id,
                           CASE WHEN trade_id IS NOT NULL
                                THEN min(source_row) OVER (PARTITION BY trade_id) END AS first_row
                    FROM raw
                ), kept AS (
                    -- join_id is NULL for duplicates, so the joins below stay plain equi-joins (hash joins)
                    SELECT *,
                           trade_id IS NOT NULL AND source_row <> first_row AS is_duplicate,
                           CASE WHEN source_row = first_row THEN trade_id END AS join_id
                    FROM ranked
                )
                SELECT
                    k.source_row,
                    k.trade_id,
                    CASE WHEN k.is_duplicate THEN 'duplicate'
                         WHEN k.trade_id IS NULL OR j.trade_id IS NOT NULL THEN 'rejected'
                         WHEN d.trade_id IS NOT NULL THEN 'delivered'
                         ELSE 'filtered_out' END AS state,
                    CASE WHEN k.is_duplicate THEN 'clean'
                         WHEN k.trade_id IS NULL OR j.trade_id IS NOT NULL THEN 'validate'
                         WHEN d.trade_id IS NULL THEN 'transform' END AS stopped_at,
                    CASE WHEN k.is_duplicate THEN k.first_row END AS duplicate_of_row,
                    coalesce(j.errors, CASE WHEN k.trade_id IS NULL THEN ['E_TRADE_ID_MISSING'] END,
                             []::VARCHAR[]) AS errors,
                    coalesce(j.warnings, d.warnings, []::VARCHAR[]) AS warnings,
                    d.event_type,
                    d.version,
                    d.is_latest,
                    coalesce(d.is_latest AND d.event_type <> 'CANCEL', false) AS in_exposure,
                    CASE WHEN d.is_latest AND d.event_type <> 'CANCEL'
                         THEN d.exposure_group END AS exposure_group,
                    o.byte_offset AS output_offset
                FROM kept k
                LEFT JOIN rejects j ON j.trade_id = k.join_id
                LEFT JOIN delivered d ON d.trade_id = k.join_id
                LEFT JOIN output_offsets o ON o.trade_id = k.join_id
                ORDER BY k.source_row
            ) TO {quote_literal(target)} (FORMAT parquet, COMPRESSION zstd)
        """)
        counts = dict(
            con.execute(
                f"SELECT state, count(*) FROM read_parquet({quote_literal(target)}) GROUP BY state"
            ).fetchall()
        )
        in_exposure_row = con.execute(
            f"SELECT count(*) FROM read_parquet({quote_literal(target)}) WHERE in_exposure"
        ).fetchone()
    return RowTrackingSummary(
        rows=sum(counts.values()),
        states={state: counts.get(state.value, 0) for state in RowState},
        in_exposure=int(in_exposure_row[0]) if in_exposure_row else 0,
        seconds=round(time.perf_counter() - started, 3),
    )


def _rejects_so_far_sql(
    source: Path, fmt: SourceFormat, rejects: Path, rules: Sequence[CorrectionRule]
) -> str:
    """Row statuses for the rejected rows only, from the source file and the rejects file.

    Validate writes the rejects file before transform starts, so this works while a run is still
    going. The states match what `build_row_status` gives the same rows at the end.
    """
    return f"""
        WITH raw AS (
            SELECT source_row, {_trade_id_sql(rules)} AS trade_id
            FROM ({numbered_reader_sql(source, fmt)})
        ), first_rows AS (
            SELECT trade_id, min(source_row) AS source_row
            FROM raw WHERE trade_id IS NOT NULL GROUP BY trade_id
        ), rejected AS (
            SELECT f.source_row, f.trade_id, j.errors, coalesce(j.warnings, []::VARCHAR[]) AS warnings
            FROM first_rows f
            JOIN read_json({quote_literal(rejects)}, format = 'newline_delimited',
                           columns = {_REJECT_COLUMNS}) j ON j.trade_id = f.trade_id
            UNION ALL
            SELECT source_row, NULL, ['E_TRADE_ID_MISSING'], []::VARCHAR[] FROM raw WHERE trade_id IS NULL
        )
        SELECT source_row, trade_id, 'rejected' AS state, 'validate' AS stopped_at,
               NULL::BIGINT AS duplicate_of_row, errors, warnings, NULL::VARCHAR AS event_type,
               NULL::INTEGER AS version, NULL::BOOLEAN AS is_latest, false AS in_exposure,
               NULL::VARCHAR AS exposure_group, NULL::BIGINT AS output_offset
        FROM rejected
    """


class DuckDbRowStatusReader:
    """`RowStatusReader` backed by DuckDB queries over the run's files."""

    def find(
        self, path: Path, state: RowState | None, search: str | None, limit: int, offset: int
    ) -> tuple[int, list[RowStatus]]:
        return _find(f"read_parquet({quote_literal(path)})", state, search, limit, offset)

    def get(self, path: Path, source_row: int) -> RowStatus | None:
        return _get(f"read_parquet({quote_literal(path)})", source_row)

    def find_rejects(
        self,
        source: Path,
        fmt: SourceFormat,
        rejects: Path,
        rules: Sequence[CorrectionRule],
        state: RowState | None,
        search: str | None,
        limit: int,
        offset: int,
    ) -> tuple[int, list[RowStatus]]:
        return _find(f"({_rejects_so_far_sql(source, fmt, rejects, rules)})", state, search, limit, offset)

    def get_reject(
        self, source: Path, fmt: SourceFormat, rejects: Path, rules: Sequence[CorrectionRule], source_row: int
    ) -> RowStatus | None:
        return _get(f"({_rejects_so_far_sql(source, fmt, rejects, rules)})", source_row)

    def source_record(self, path: Path, fmt: SourceFormat, source_row: int) -> dict[str, Any] | None:
        with duckdb.connect() as con:
            con.execute("SET enable_progress_bar = false")
            cursor = con.execute(
                f"SELECT * EXCLUDE (source_row) FROM ({numbered_reader_sql(path, fmt)}) WHERE source_row = ?",
                [source_row],
            )
            rows = _records(cursor)
        return rows[0] if rows else None

    def output_document(self, path: Path, byte_offset: int) -> dict[str, Any]:
        with path.open("rb") as handle:
            handle.seek(byte_offset)
            document: dict[str, Any] = json.loads(handle.readline())
        return document

    def reject_record(self, path: Path, trade_id: str) -> dict[str, Any] | None:
        needle = json.dumps(trade_id).encode()
        with path.open("rb") as handle:
            for line in handle:
                if needle in line and (record := json.loads(line)).get("trade_id") == trade_id:
                    return dict(record)
        return None


def _find(
    relation: str, state: RowState | None, search: str | None, limit: int, offset: int
) -> tuple[int, list[RowStatus]]:
    conditions: list[str] = ["true"]
    params: list[object] = []
    if state is not None:
        conditions.append("state = ?")
        params.append(state.value)
    if search:
        term = search.strip()
        if term.isdigit():
            conditions.append("(source_row = ? OR trade_id ILIKE ?)")
            params += [int(term), f"%{term}%"]
        else:
            conditions.append("trade_id ILIKE ?")
            params.append(f"%{term}%")
    where = " AND ".join(conditions)
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false")
        total_row = con.execute(f"SELECT count(*) FROM {relation} WHERE {where}", params).fetchone()
        cursor = con.execute(
            f"SELECT * FROM {relation} WHERE {where} ORDER BY source_row LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        rows = _records(cursor)
    return (int(total_row[0]) if total_row else 0), [RowStatus.model_validate(row) for row in rows]


def _get(relation: str, source_row: int) -> RowStatus | None:
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false")
        rows = _records(con.execute(f"SELECT * FROM {relation} WHERE source_row = ?", [source_row]))
    return RowStatus.model_validate(rows[0]) if rows else None


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description or []]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
