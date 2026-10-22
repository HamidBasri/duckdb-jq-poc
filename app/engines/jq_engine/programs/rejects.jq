inputs
| select(.errors != [])
| {
    trade_id,
    errors,
    warnings,
    record: {
      previous_trade_id, event_type, trade_date, settlement_date, instrument_id,
      counterparty_id, book_id, side, quantity, price, currency
    }
  }
