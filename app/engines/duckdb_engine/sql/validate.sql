-- Validate stage: every rule is evaluated; a trade is rejected when `errors` is non-empty.
CREATE OR REPLACE TABLE validated_trades AS
SELECT
    t.*,
    list_filter([
        CASE WHEN t.trade_id IS NULL THEN 'E_TRADE_ID_MISSING' END,
        CASE WHEN t.event_type IS NULL OR t.event_type NOT IN ('NEW', 'AMEND', 'CANCEL') THEN 'E_EVENT_TYPE_INVALID' END,
        CASE WHEN t.event_type IN ('NEW', 'AMEND', 'CANCEL')
              AND (t.event_type = 'NEW') <> (t.previous_trade_id IS NULL) THEN 'E_EVENT_LINEAGE_MISMATCH' END,
        CASE WHEN t.trade_date IS NULL THEN 'E_TRADE_DATE_INVALID' END,
        CASE WHEN t.settlement_date < t.trade_date THEN 'E_SETTLEMENT_BEFORE_TRADE' END,
        CASE WHEN t.side IS NULL THEN 'E_SIDE_INVALID' END,
        CASE WHEN t.quantity IS NULL OR t.quantity <= 0 THEN 'E_QUANTITY_INVALID' END,
        CASE WHEN t.price IS NULL OR t.price <= 0 THEN 'E_PRICE_INVALID' END,
        CASE WHEN fx.currency IS NULL THEN 'E_CURRENCY_UNKNOWN' END,
        CASE WHEN i.instrument_id IS NULL THEN 'E_INSTRUMENT_UNRESOLVED' END,
        CASE WHEN c.counterparty_id IS NULL THEN 'E_COUNTERPARTY_UNRESOLVED' END,
        CASE WHEN b.book_id IS NULL THEN 'E_BOOK_UNRESOLVED' END,
        CASE WHEN t.previous_trade_id IS NOT NULL AND p.trade_id IS NULL THEN 'E_PREVIOUS_TRADE_UNRESOLVED' END
    ], e -> e IS NOT NULL) AS errors
FROM clean_trades t
LEFT JOIN clean_fx_rates fx ON fx.currency = t.currency
LEFT JOIN clean_instruments i ON i.instrument_id = t.instrument_id
LEFT JOIN clean_counterparties c ON c.counterparty_id = t.counterparty_id
LEFT JOIN clean_books b ON b.book_id = t.book_id
LEFT JOIN (SELECT trade_id FROM clean_trades WHERE trade_id IS NOT NULL) p ON p.trade_id = t.previous_trade_id;

CREATE OR REPLACE VIEW rejected_trades AS
SELECT
    trade_id,
    errors,
    warnings,
    {
        'previous_trade_id': previous_trade_id, 'event_type': event_type, 'trade_date': trade_date,
        'settlement_date': settlement_date, 'instrument_id': instrument_id, 'counterparty_id': counterparty_id,
        'book_id': book_id, 'side': side, 'quantity': quantity, 'price': price, 'currency': currency
    } AS record
FROM validated_trades
WHERE len(errors) > 0;
