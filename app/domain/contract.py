"""The data contract shared by both engines: raw landing schema and data-quality rule codes.

The SQL and jq implementations hard-code these codes; tests assert they stay in sync.
"""

from enum import StrEnum

from app.domain.models import Entity

# Raw landing zone: every column is text, whatever the source format, so that dirty
# values ("1,250", "N/A", " buy ") survive ingestion and are handled by the clean stage.
RAW_COLUMNS: dict[Entity, tuple[str, ...]] = {
    Entity.TRADES: (
        "trade_id",
        "previous_trade_id",
        "event_type",
        "trade_date",
        "settlement_date",
        "instrument_id",
        "counterparty_id",
        "book_id",
        "side",
        "quantity",
        "price",
        "currency",
    ),
    Entity.COUNTERPARTIES: (
        "counterparty_id",
        "legal_name",
        "lei",
        "country",
        "sector",
        "credit_rating",
        "parent_counterparty_id",
    ),
    Entity.INSTRUMENTS: (
        "instrument_id",
        "isin",
        "instrument_type",
        "currency",
        "issuer_counterparty_id",
        "underlying_instrument_id",
        "maturity_date",
    ),
    Entity.BOOKS: ("book_id", "desk", "trader", "region"),
    Entity.FX_RATES: ("currency", "rate_to_usd"),
}


# What each reject rule checks, in plain words (shown on the Rules page).
RULE_TEXT: dict[str, str] = {
    "E_TRADE_ID_MISSING": "No trade id",
    "E_EVENT_TYPE_INVALID": "Event type is not NEW, AMEND or CANCEL",
    "E_EVENT_LINEAGE_MISMATCH": "NEW with an earlier version, or AMEND / CANCEL without one",
    "E_TRADE_DATE_INVALID": "Trade date missing or not a real date",
    "E_SETTLEMENT_BEFORE_TRADE": "Settles before it was traded",
    "E_SIDE_INVALID": "Side is not BUY or SELL after cleaning",
    "E_QUANTITY_INVALID": "Quantity missing, zero or negative",
    "E_PRICE_INVALID": "Price missing, zero or negative",
    "E_CURRENCY_UNKNOWN": "Currency not in fx_rates, or its rate is not a positive number",
    "E_INSTRUMENT_UNRESOLVED": "Instrument not in instruments",
    "E_COUNTERPARTY_UNRESOLVED": "Counterparty not in counterparties",
    "E_BOOK_UNRESOLVED": "Book not in books",
    "E_PREVIOUS_TRADE_UNRESOLVED": "The earlier version it amends does not exist",
}

# Reject rules that can't be switched off, with the reason shown on the Rules page. The list comes
# from tests/test_parity.py, which switches every other rule off and checks both engines still agree.
LOCKED_RULES: dict[str, str] = {
    "E_TRADE_ID_MISSING": "Every output is keyed by trade id.",
    "E_QUANTITY_INVALID": "The amount is quantity times price; without a quantity it can't be calculated.",
    "E_PRICE_INVALID": "The amount is quantity times price; without a price it can't be calculated.",
    "E_CURRENCY_UNKNOWN": "Without a known currency the amount can't be converted to US dollars.",
    "E_INSTRUMENT_UNRESOLVED": "The output document is built around the instrument.",
    "E_COUNTERPARTY_UNRESOLVED": "Exposure is grouped by the counterparty's parent company.",
    "E_BOOK_UNRESOLVED": "The output document needs the book it was traded in.",
    "E_PREVIOUS_TRADE_UNRESOLVED": "A version chain can't be built without the earlier version.",
}

# Common alternative names, normalised (lower case, letters and digits only).
_SYNONYMS = {
    "qty": "quantity",
    "px": "price",
    "ccy": "currency",
    "cur": "currency",
    "cpty": "counterpartyid",
    "cptyid": "counterpartyid",
    "prevtradeid": "previoustradeid",
    "settledate": "settlementdate",
    "valuedate": "settlementdate",
    "parentid": "parentcounterpartyid",
    "rate": "ratetousd",
    "fxrate": "ratetousd",
}


def _normalise(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def suggest_mapping(expected: tuple[str, ...] | list[str], found: list[str]) -> dict[str, str | None]:
    """Match each expected column to a found one: same name, then same letters, then a known synonym."""
    unused = list(found)
    mapping: dict[str, str | None] = {}
    for column in expected:
        target = _normalise(column)
        match = next((name for name in unused if name == column), None)
        match = match or next((name for name in unused if _normalise(name) == target), None)
        match = match or next((name for name in unused if _SYNONYMS.get(_normalise(name)) == target), None)
        if match is not None:
            unused.remove(match)
        mapping[column] = match
    return mapping


class ErrorCode(StrEnum):
    """Rule violations that reject a trade. Evaluated, and listed, in this order."""

    TRADE_ID_MISSING = "E_TRADE_ID_MISSING"
    EVENT_TYPE_INVALID = "E_EVENT_TYPE_INVALID"
    EVENT_LINEAGE_MISMATCH = "E_EVENT_LINEAGE_MISMATCH"
    TRADE_DATE_INVALID = "E_TRADE_DATE_INVALID"
    SETTLEMENT_BEFORE_TRADE = "E_SETTLEMENT_BEFORE_TRADE"
    SIDE_INVALID = "E_SIDE_INVALID"
    QUANTITY_INVALID = "E_QUANTITY_INVALID"
    PRICE_INVALID = "E_PRICE_INVALID"
    CURRENCY_UNKNOWN = "E_CURRENCY_UNKNOWN"
    INSTRUMENT_UNRESOLVED = "E_INSTRUMENT_UNRESOLVED"
    COUNTERPARTY_UNRESOLVED = "E_COUNTERPARTY_UNRESOLVED"
    BOOK_UNRESOLVED = "E_BOOK_UNRESOLVED"
    PREVIOUS_TRADE_UNRESOLVED = "E_PREVIOUS_TRADE_UNRESOLVED"


class WarningCode(StrEnum):
    """Issues that are repaired or tolerated; the trade is kept and the warning attached."""

    CURRENCY_IMPUTED = "W_CURRENCY_IMPUTED"
    SETTLEMENT_IMPUTED = "W_SETTLEMENT_IMPUTED"
    COUNTERPARTY_HIERARCHY_CYCLE = "W_COUNTERPARTY_HIERARCHY_CYCLE"
    COUNTERPARTY_PARENT_UNRESOLVED = "W_COUNTERPARTY_PARENT_UNRESOLVED"
    LINEAGE_CYCLE = "W_LINEAGE_CYCLE"


SETTLEMENT_LAG_DAYS = 2
