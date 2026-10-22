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
