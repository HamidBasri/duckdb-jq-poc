# Clean trades: normalise, parse, de-duplicate on trade_id (streaming), impute.
# $instruments: cleaned instruments (--slurpfile), needed to impute a missing currency.
include "lib";

def normalize_side:
  upper_or_null
  | if . == "B" or . == "BUY" then "BUY" elif . == "S" or . == "SELL" then "SELL" else null end;

def normalize:
  {
    trade_id: (.trade_id | nullify),
    previous_trade_id: (.previous_trade_id | nullify),
    event_type: (.event_type | upper_or_null),
    trade_date: (.trade_date | parse_date),
    settlement_date: (.settlement_date | parse_date),
    instrument_id: (.instrument_id | nullify),
    counterparty_id: (.counterparty_id | nullify),
    book_id: (.book_id | nullify),
    side: (.side | normalize_side),
    quantity: (.quantity | if . == null then null else (split(",") | join("")) end | to_number_or_null),
    price: (.price | to_number_or_null),
    currency: (.currency | upper_or_null)
  };

def impute($instrument_index):
  (.instrument_id | lookup($instrument_index) | .currency) as $instrument_currency
  | .warnings = []
  | if .currency == null and $instrument_currency != null
    then .currency = $instrument_currency | .warnings += ["W_CURRENCY_IMPUTED"]
    else . end
  | if .settlement_date == null and .trade_date != null
    then .settlement_date = (.trade_date | add_days(2)) | .warnings += ["W_SETTLEMENT_IMPUTED"]
    else . end;

($instruments | index_by(.instrument_id)) as $instrument_index
| first_by_key(inputs | normalize; .trade_id)
| impute($instrument_index)
