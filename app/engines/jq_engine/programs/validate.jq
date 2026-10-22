# Second validation pass (streaming). Rule order matches duckdb_engine/sql/validate.sql.
include "lib";

($trade_ids[0]) as $known_trades
| ($counterparties | index_by(.counterparty_id)) as $known_counterparties
| ($instruments | index_by(.instrument_id)) as $known_instruments
| ($books | index_by(.book_id)) as $known_books
| ($fx_rates | index_by(.currency)) as $known_currencies
| inputs
| (.event_type | IN("NEW", "AMEND", "CANCEL")) as $event_type_valid
| . + {
    errors: [
      if .trade_id == null then "E_TRADE_ID_MISSING" else empty end,
      if $event_type_valid | not then "E_EVENT_TYPE_INVALID" else empty end,
      if $event_type_valid and ((.event_type == "NEW") != (.previous_trade_id == null))
        then "E_EVENT_LINEAGE_MISMATCH" else empty end,
      if .trade_date == null then "E_TRADE_DATE_INVALID" else empty end,
      if .trade_date != null and .settlement_date != null and .settlement_date < .trade_date
        then "E_SETTLEMENT_BEFORE_TRADE" else empty end,
      if .side == null then "E_SIDE_INVALID" else empty end,
      if .quantity == null or .quantity <= 0 then "E_QUANTITY_INVALID" else empty end,
      if .price == null or .price <= 0 then "E_PRICE_INVALID" else empty end,
      if .currency | known($known_currencies) | not then "E_CURRENCY_UNKNOWN" else empty end,
      if .instrument_id | known($known_instruments) | not then "E_INSTRUMENT_UNRESOLVED" else empty end,
      if .counterparty_id | known($known_counterparties) | not then "E_COUNTERPARTY_UNRESOLVED" else empty end,
      if .book_id | known($known_books) | not then "E_BOOK_UNRESOLVED" else empty end,
      if .previous_trade_id != null and (.previous_trade_id | known($known_trades) | not)
        then "E_PREVIOUS_TRADE_UNRESOLVED" else empty end
    ]
  }
