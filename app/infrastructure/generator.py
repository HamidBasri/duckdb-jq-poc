"""Deterministic synthetic trade-dataset generator.

Produces five related entities with the cross-references the pipelines must resolve:

    trade -> counterparty -> parent counterparty -> ... (forest, configurable depth, injected cycles)
    trade -> instrument -> issuer counterparty / underlying instrument
    trade -> book, trade.currency -> fx rate
    trade -> previous trade (amendment chains; mostly backward, some forward references)

and with realistic dirt (format variants, missing values, broken references, duplicates)
proportional to `dirty_rate`. Everything is vectorised with numpy so 5M rows stay cheap,
and a fixed seed reproduces the exact same files.
"""

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import numpy as np
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from numpy.dtypes import StringDType
from numpy.typing import NDArray

from app.domain.contract import RAW_COLUMNS
from app.domain.models import Entity, GeneratedDataset, GenerationSpec, SourceFormat

logger = logging.getLogger(__name__)

STR: Final = StringDType(na_object=None)
# numpy stubs do not propagate StringDType through np.strings/np.where, so element type stays open.
StrArray = NDArray[Any]
IntArray = NDArray[np.int64]
BoolArray = NDArray[np.bool_]

FX_TO_USD: Final[dict[str, float]] = {
    "USD": 1.0,
    "EUR": 1.0842,
    "GBP": 1.2711,
    "JPY": 0.006712,
    "CHF": 1.1203,
    "CAD": 0.7314,
    "AUD": 0.6598,
    "SEK": 0.0951,
    "HKD": 0.1281,
}
CURRENCY_WEIGHTS: Final = (0.40, 0.25, 0.12, 0.08, 0.05, 0.04, 0.03, 0.015, 0.015)
INSTRUMENT_TYPES: Final = ("EQUITY", "BOND", "FUTURE", "OPTION", "SWAP")
INSTRUMENT_TYPE_WEIGHTS: Final = (0.40, 0.25, 0.15, 0.15, 0.05)
COUNTRIES: Final = ("US", "GB", "DE", "FR", "JP", "CH", "CA", "AU", "SE", "HK", "NL", "SG")
SECTORS: Final = ("Banking", "Asset Management", "Insurance", "Corporate", "Sovereign", "Hedge Fund")
RATINGS: Final = ("AAA", "AA", "A", "BBB", "BB", "B", "CCC")
NAME_PREFIXES: Final = ("Northern", "Atlas", "Crescent", "Summit", "Harbor", "Meridian", "Granite", "Aurora")
NAME_SUFFIXES: Final = ("Capital", "Holdings", "Partners", "Bank", "Securities", "Group", "Investments")
DESKS: Final = ("Rates", "Credit", "Equities", "FX", "Commodities", "Structured")
REGIONS: Final = ("EMEA", "AMER", "APAC")
TRADERS: Final = ("A. Rahimi", "J. Smith", "M. Rossi", "K. Tanaka", "L. Dubois", "S. Patel", "E. Novak")
BUY_VARIANTS: Final = ("buy", " Buy ", "B", " BUY")
SELL_VARIANTS: Final = ("sell", "S", " Sell ", "SELL ")
INVALID_DATES: Final = ("2025-13-45", "not-a-date", "31/02/2025")
FIRST_TRADE_DAY: Final = np.datetime64("2025-01-01")
SECONDS_PER_DAY: Final = 86_400
AMENDED_CHAIN_SHARE: Final = 0.15
ORPHAN_COUNTERPARTY_ID: Final = "CP9999999"
ORPHAN_INSTRUMENT_ID: Final = "IN999999"
ORPHAN_BOOK_ID: Final = "BK9999"
ORPHAN_TRADE_ID: Final = "TR9999999999"


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def _ids(prefix: str, numbers: IntArray, width: int) -> StrArray:
    """Format integers as zero-padded identifiers, e.g. CP0000042."""
    return np.strings.add(prefix, np.strings.zfill(numbers.astype(STR), width)).astype(STR)


def _values_or_null(mask: BoolArray, values: StrArray) -> StrArray:
    """`values` where `mask` is set, null elsewhere."""
    result = np.full(mask.size, None, dtype=STR)
    result[mask] = values[mask]
    return result


def _iso_dates(day_offsets: IntArray) -> StrArray:
    return np.datetime_as_string(FIRST_TRADE_DAY + day_offsets, unit="D").astype(STR)


def _to_ddmmyyyy(iso: StrArray) -> StrArray:
    day, month, year = np.strings.slice(iso, 8, 10), np.strings.slice(iso, 5, 7), np.strings.slice(iso, 0, 4)
    return np.strings.add(np.strings.add(np.strings.add(day, "/"), np.strings.add(month, "/")), year)


@dataclass(frozen=True)
class _Counterparties:
    ids: StrArray
    table: pa.Table


@dataclass(frozen=True)
class _Instruments:
    ids: StrArray
    currencies: StrArray
    base_prices: NDArray[np.float64]
    table: pa.Table


class TradeDatasetGenerator:
    """Builds the five entity tables for one `GenerationSpec`."""

    def __init__(self, spec: GenerationSpec) -> None:
        self._spec = spec
        self._rng = np.random.default_rng(spec.seed)

    def build(self) -> dict[Entity, pa.Table]:
        counterparties = self._counterparties()
        instruments = self._instruments(counterparties.ids)
        book_ids, books = self._books()
        return {
            Entity.TRADES: self._trades(counterparties.ids, instruments, book_ids),
            Entity.COUNTERPARTIES: counterparties.table,
            Entity.INSTRUMENTS: instruments.table,
            Entity.BOOKS: books,
            Entity.FX_RATES: self._fx_rates(),
        }

    # -- helpers ------------------------------------------------------------------------
    def _mask(self, size: int, share_of_dirty: float) -> BoolArray:
        """Rows to corrupt for one dirt type: `share_of_dirty` of the overall dirty rate."""
        return self._rng.random(size) < self._spec.dirty_rate * share_of_dirty

    def _pick(
        self, options: tuple[str, ...], size: int, weights: tuple[float, ...] | None = None
    ) -> StrArray:
        return np.array(options, dtype=STR)[self._rng.choice(len(options), size=size, p=weights)]

    def _at_least_one(self, expected: float) -> int:
        """Round an expected count down, but keep one instance of every dirt type when dirty."""
        return max(1, int(expected)) if self._spec.dirty_rate > 0 else 0

    def _duplicate_count(self, size: int) -> int:
        return int(size * self._spec.dirty_rate * 0.5)

    def _with_exact_duplicates(self, columns: dict[str, StrArray], duplicate_count: int) -> pa.Table:
        """Append exact copies of random rows, each placed shortly after its original."""
        size = len(next(iter(columns.values())))
        sources = self._rng.choice(size, size=duplicate_count, replace=False)
        sort_keys = np.concatenate(
            [np.arange(size, dtype=np.float64), sources + self._rng.integers(1, 500, duplicate_count) + 0.5]
        )
        order = np.argsort(sort_keys, kind="stable")
        rows = np.concatenate([np.arange(size), sources])[order]
        return pa.table({name: pa.array(values[rows], type=pa.string()) for name, values in columns.items()})

    # -- reference entities -------------------------------------------------------------
    def _counterparties(self) -> _Counterparties:
        depth = self._spec.hierarchy_depth
        count = max(_clamp(self._spec.rows // 50, 50, 50_000), depth * 2)
        level_weights = 0.55 ** np.arange(depth)
        levels = np.concatenate(
            [
                np.arange(depth),
                self._rng.choice(depth, size=count - depth, p=level_weights / level_weights.sum()),
            ]
        )
        parents = np.full(count, -1, dtype=np.int64)
        for level in range(1, depth):
            members = np.flatnonzero(levels == level)
            parents[members] = self._rng.choice(np.flatnonzero(levels == level - 1), size=members.size)

        ids = _ids("CP", self._rng.permutation(count) + 1, 7)  # permuted: parents often have higher ids
        parent_ids = _values_or_null(parents >= 0, ids[parents])

        has_children = np.zeros(count, dtype=bool)
        has_children[parents[parents >= 0]] = True
        leaves = np.flatnonzero(~has_children & (levels > 0))
        cycle_pairs = min(self._at_least_one(count * self._spec.dirty_rate * 0.05), leaves.size // 2)
        if cycle_pairs:
            pairs = self._rng.choice(leaves, size=cycle_pairs * 2, replace=False).reshape(-1, 2)
            parent_ids[pairs[:, 0]] = ids[pairs[:, 1]]  # A -> B and B -> A: a two-node cycle
            parent_ids[pairs[:, 1]] = ids[pairs[:, 0]]
            orphan_candidates = np.setdiff1d(leaves, pairs.ravel())
        else:
            orphan_candidates = leaves
        orphan_count = min(self._at_least_one(count * self._spec.dirty_rate * 0.05), orphan_candidates.size)
        parent_ids[self._rng.choice(orphan_candidates, size=orphan_count, replace=False)] = (
            ORPHAN_COUNTERPARTY_ID
        )

        names = np.strings.add(
            np.strings.add(self._pick(NAME_PREFIXES, count), " "),
            np.strings.add(
                np.strings.add(self._pick(NAME_SUFFIXES, count), " "), np.strings.slice(ids, 2, 9)
            ),
        )
        names = np.where(
            self._mask(count, 2.5), np.strings.add(np.strings.add(" ", names), "  "), names
        ).astype(STR)
        alphabet = np.frombuffer(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", dtype=np.uint8)
        lei_bytes = alphabet[self._rng.integers(0, alphabet.size, size=(count, 18))]
        leis = np.strings.add(np.frombuffer(lei_bytes.tobytes(), dtype="S18").astype(STR), "00")
        ratings = self._pick(RATINGS, count)
        ratings = np.where(self._mask(count, 2.5), np.strings.lower(ratings), ratings).astype(STR)

        table = self._with_exact_duplicates(
            {
                "counterparty_id": ids,
                "legal_name": names,
                "lei": leis,
                "country": self._pick(COUNTRIES, count),
                "sector": self._pick(SECTORS, count),
                "credit_rating": ratings,
                "parent_counterparty_id": parent_ids,
            },
            duplicate_count=self._duplicate_count(count),
        )
        return _Counterparties(ids=ids, table=table)

    def _instruments(self, counterparty_ids: StrArray) -> _Instruments:
        count = _clamp(self._spec.rows // 100, 40, 20_000)
        types = self._pick(INSTRUMENT_TYPES, count, INSTRUMENT_TYPE_WEIGHTS)
        ids = _ids("IN", self._rng.permutation(count) + 1, 6)
        currencies = self._pick(tuple(FX_TO_USD), count, CURRENCY_WEIGHTS)

        is_cash = (types == "EQUITY") | (types == "BOND")
        issuers = _values_or_null(
            is_cash, counterparty_ids[self._rng.integers(0, counterparty_ids.size, count)]
        )
        cash_ids = ids[is_cash]
        underlyings = _values_or_null(
            (types == "FUTURE") | (types == "OPTION"), cash_ids[self._rng.integers(0, cash_ids.size, count)]
        )
        maturities = _values_or_null(types != "EQUITY", _iso_dates(self._rng.integers(365, 365 * 20, count)))
        isins = np.strings.add(
            self._pick(COUNTRIES, count), np.strings.zfill(np.strings.slice(ids, 2, 8), 10)
        )
        base_prices = np.where(
            types == "BOND", self._rng.uniform(85, 115, count), self._rng.lognormal(3.5, 1.0, count)
        )
        dirty_types = np.where(self._mask(count, 2.5), np.strings.lower(types), types).astype(STR)

        table = pa.table(
            {
                "instrument_id": pa.array(ids, type=pa.string()),
                "isin": pa.array(isins, type=pa.string()),
                "instrument_type": pa.array(dirty_types, type=pa.string()),
                "currency": pa.array(currencies, type=pa.string()),
                "issuer_counterparty_id": pa.array(issuers, type=pa.string()),
                "underlying_instrument_id": pa.array(underlyings, type=pa.string()),
                "maturity_date": pa.array(maturities, type=pa.string()),
            }
        )
        return _Instruments(ids=ids, currencies=currencies, base_prices=base_prices, table=table)

    def _books(self) -> tuple[StrArray, pa.Table]:
        count = _clamp(self._spec.rows // 2_000, 20, 200)
        ids = _ids("BK", np.arange(1, count + 1), 4)
        table = pa.table(
            {
                "book_id": pa.array(ids, type=pa.string()),
                "desk": pa.array(self._pick(DESKS, count), type=pa.string()),
                "trader": pa.array(self._pick(TRADERS, count), type=pa.string()),
                "region": pa.array(self._pick(REGIONS, count), type=pa.string()),
            }
        )
        return ids, table

    @staticmethod
    def _fx_rates() -> pa.Table:
        return pa.table(
            {"currency": list(FX_TO_USD), "rate_to_usd": [str(rate) for rate in FX_TO_USD.values()]}
        )

    # -- trades -------------------------------------------------------------------------
    def _chain_lengths(self, trade_count: int) -> IntArray:
        """Split `trade_count` trades into amendment chains (mostly length 1)."""
        lengths = np.ones(trade_count, dtype=np.int64)
        if self._spec.max_chain_length > 1:
            amended = self._rng.random(trade_count) < AMENDED_CHAIN_SHARE
            lengths[amended] = self._rng.integers(2, self._spec.max_chain_length + 1, int(amended.sum()))
        totals = np.cumsum(lengths)
        last = int(np.searchsorted(totals, trade_count))
        lengths = lengths[: last + 1].copy()
        lengths[-1] -= totals[last] - trade_count
        return lengths

    def _trades(self, counterparty_ids: StrArray, instruments: _Instruments, book_ids: StrArray) -> pa.Table:
        rng, rows = self._rng, self._spec.rows
        base_count = rows - self._duplicate_count(rows)

        lengths = self._chain_lengths(base_count)
        chain_count = lengths.size
        chain_of = np.repeat(np.arange(chain_count), lengths)
        chain_starts = np.cumsum(lengths) - lengths
        seq = np.arange(base_count) - np.repeat(chain_starts, lengths)

        # Booking time decides file order; amendments follow their original by hours-days.
        chain_day = rng.integers(0, 365, chain_count)
        gaps = rng.integers(3_600, 5 * SECONDS_PER_DAY, base_count)
        gaps[seq == 0] = 0
        gap_totals = np.cumsum(gaps)
        booked_at = (
            np.repeat(chain_day * SECONDS_PER_DAY + rng.integers(8 * 3_600, 18 * 3_600, chain_count), lengths)
            + gap_totals
            - np.repeat(gap_totals[chain_starts], lengths)
        )
        late_original = (seq == 0) & (lengths[chain_of] > 1) & self._mask(base_count, 0.25)
        booked_at[late_original] += 6 * SECONDS_PER_DAY  # original lands after its amendment => forward ref
        file_order = np.argsort(booked_at, kind="stable")
        file_position = np.empty(base_count, dtype=np.int64)
        file_position[file_order] = np.arange(base_count)

        trade_ids = _ids("TR", file_position, 10)
        previous_ids = _values_or_null(seq > 0, np.roll(trade_ids, 1))
        is_last = seq == lengths[chain_of] - 1
        event_types = np.where(seq == 0, "NEW", "AMEND").astype(STR)
        event_types[is_last & (seq > 0) & (rng.random(base_count) < 0.25)] = "CANCEL"

        # Per-chain economics: same instrument, counterparty, book and side across versions.
        cp_weights = 1.0 / np.arange(1, counterparty_ids.size + 1) ** 0.7
        chain_cp = rng.permutation(counterparty_ids)[
            rng.choice(counterparty_ids.size, size=chain_count, p=cp_weights / cp_weights.sum())
        ]
        chain_instrument = rng.integers(0, instruments.ids.size, chain_count)
        instrument_index = chain_instrument[chain_of]
        quantities = rng.integers(1, 50_000, base_count)
        prices = np.round(
            instruments.base_prices[instrument_index] * (1 + rng.normal(0, 0.02, base_count)), 4
        )
        trade_dates = _iso_dates(chain_day[chain_of])
        settlement_dates = _iso_dates(chain_day[chain_of] + 2)
        sides = np.where(rng.random(chain_count) < 0.5, "BUY", "SELL").astype(STR)[chain_of]

        columns: dict[str, StrArray] = {
            "trade_id": trade_ids,
            "previous_trade_id": previous_ids,
            "event_type": event_types,
            "trade_date": trade_dates,
            "settlement_date": settlement_dates,
            "instrument_id": instruments.ids[instrument_index],
            "counterparty_id": chain_cp[chain_of],
            "book_id": book_ids[rng.integers(0, book_ids.size, chain_count)][chain_of],
            "side": sides,
            "quantity": quantities.astype(STR),
            "price": prices.astype(STR),
            "currency": instruments.currencies[instrument_index],
        }
        self._inject_format_variants(columns, quantities)
        self._inject_errors(columns, seq, chain_day[chain_of], prices)
        ordered = {name: values[file_order] for name, values in columns.items()}
        return self._with_exact_duplicates(ordered, rows - base_count).select(
            list(RAW_COLUMNS[Entity.TRADES])
        )

    def _inject_format_variants(self, columns: dict[str, StrArray], quantities: IntArray) -> None:
        """Harmless messiness the clean stage must normalise (not counted as errors)."""
        size, rng = quantities.size, self._rng
        variant = rng.integers(0, len(BUY_VARIANTS), size)
        styled = np.where(
            columns["side"] == "BUY",
            np.array(BUY_VARIANTS, dtype=STR)[variant],
            np.array(SELL_VARIANTS, dtype=STR)[variant],
        )
        columns["side"] = np.where(rng.random(size) < 0.2, styled, columns["side"]).astype(STR)
        columns["currency"] = np.where(
            rng.random(size) < 0.1, np.strings.lower(columns["currency"]), columns["currency"]
        ).astype(STR)
        columns["event_type"] = np.where(
            rng.random(size) < 0.05, np.strings.lower(columns["event_type"]), columns["event_type"]
        ).astype(STR)
        for column in ("trade_date", "settlement_date"):
            columns[column] = np.where(
                rng.random(size) < 0.1, _to_ddmmyyyy(columns[column]), columns[column]
            ).astype(STR)
        grouped = (quantities >= 1_000) & (rng.random(size) < 0.1)
        columns["quantity"][grouped] = [f"{quantity:,}" for quantity in quantities[grouped].tolist()]

    def _inject_errors(
        self,
        columns: dict[str, StrArray],
        seq: IntArray,
        trade_days: IntArray,
        prices: NDArray[np.float64],
    ) -> None:
        """Real data-quality problems: each `(column, share, value)` corrupts a slice of rows."""
        size = seq.size
        settlement_early = self._mask(size, 0.03)
        columns["settlement_date"][settlement_early] = _iso_dates(trade_days[settlement_early] - 3)
        columns["price"] = np.where(self._mask(size, 0.05), (-prices).astype(STR), columns["price"]).astype(
            STR
        )
        invalid_dates = self._mask(size, 0.05)
        columns["trade_date"][invalid_dates] = self._pick(INVALID_DATES, int(invalid_dates.sum()))

        corruptions: tuple[tuple[str, float, str | None], ...] = (
            ("currency", 0.15, None),
            ("currency", 0.03, "XXX"),
            ("settlement_date", 0.15, None),
            ("settlement_date", 0.05, "N/A"),
            ("side", 0.05, None),
            ("side", 0.02, "HOLD"),
            ("quantity", 0.05, "N/A"),
            ("quantity", 0.02, "0"),
            ("counterparty_id", 0.03, None),
            ("counterparty_id", 0.05, ORPHAN_COUNTERPARTY_ID),
            ("instrument_id", 0.05, ORPHAN_INSTRUMENT_ID),
            ("book_id", 0.02, ORPHAN_BOOK_ID),
        )
        for column, share, value in corruptions:
            columns[column][self._mask(size, share)] = value
        columns["previous_trade_id"][(seq > 0) & self._mask(size, 0.03)] = ORPHAN_TRADE_ID


def write_dataset(spec: GenerationSpec, target_dir: Path) -> GeneratedDataset:
    """Generate the dataset for `spec` and write every entity in each requested format."""
    started = time.perf_counter()
    target_dir.mkdir(parents=True, exist_ok=True)
    tables = TradeDatasetGenerator(spec).build()
    files: dict[Entity, dict[SourceFormat, str]] = {entity: {} for entity in tables}
    size_bytes = dict.fromkeys(spec.formats, 0)
    for entity, table in tables.items():
        for fmt in spec.formats:
            path = target_dir / f"{entity}.{fmt}"
            if fmt is SourceFormat.PARQUET:
                pq.write_table(table, path, compression="zstd")
            else:
                pa_csv.write_csv(table, path)
            files[entity][fmt] = path.name
            size_bytes[fmt] += path.stat().st_size
    seconds = time.perf_counter() - started
    logger.info("generated %s in %.1fs", spec.dataset_id, seconds)
    return GeneratedDataset(
        files=files,
        row_counts={entity: table.num_rows for entity, table in tables.items()},
        size_bytes=size_bytes,
        seconds=seconds,
    )
