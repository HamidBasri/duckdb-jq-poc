-- Transform stage, part 2: shape valid trades into the nested target document and filter.
-- The engine wraps this in CREATE TABLE (default) or CREATE VIEW (lean mode); filters come from
-- the one-row run_params table because a view cannot hold prepared-statement parameters.
SELECT
    t.trade_id,
    t.event_type,
    t.trade_date,
    t.settlement_date,
    t.side,
    {
        'quantity': t.quantity, 'price': t.price, 'currency': t.currency,
        'notional': round(t.quantity * t.price, 2),
        'fx_rate_to_usd': fx.rate_to_usd,
        'notional_usd': round(t.quantity * t.price * fx.rate_to_usd, 2)
    } AS economics,
    {
        'instrument_id': i.instrument_id, 'isin': i.isin, 'instrument_type': i.instrument_type,
        'currency': i.currency, 'maturity_date': i.maturity_date, 'issuer': i.issuer, 'underlying': i.underlying
    } AS instrument,
    {
        'counterparty_id': c.counterparty_id, 'legal_name': c.legal_name, 'lei': c.lei, 'country': c.country,
        'sector': c.sector, 'credit_rating': c.credit_rating, 'ultimate_parent': c.ultimate_parent,
        'hierarchy_depth': c.hierarchy_depth, 'hierarchy_path': c.hierarchy_path, 'parent': c.parent
    } AS counterparty,
    {'book_id': b.book_id, 'desk': b.desk, 'trader': b.trader, 'region': b.region} AS book,
    {
        'root_trade_id': l.root_trade_id, 'version': l.version, 'chain': l.chain,
        'previous_trade_id': t.previous_trade_id, 'superseded_by': l.superseded_by,
        'is_latest': l.superseded_by IS NULL
    } AS lineage,
    {
        'warnings': list_concat(
            t.warnings,
            c.warnings,
            list_filter([CASE WHEN l.version IS NULL THEN 'W_LINEAGE_CYCLE' END], w -> w IS NOT NULL)
        )
    } AS data_quality
FROM validated_trades t
JOIN clean_fx_rates fx ON fx.currency = t.currency
JOIN dim_instrument i ON i.instrument_id = t.instrument_id
JOIN dim_counterparty c ON c.counterparty_id = t.counterparty_id
JOIN clean_books b ON b.book_id = t.book_id
JOIN lineage l ON l.trade_id = t.trade_id
CROSS JOIN run_params p
WHERE len(t.errors) = 0
  AND (p.trade_date_from IS NULL OR t.trade_date >= p.trade_date_from)
  AND (p.trade_date_to IS NULL OR t.trade_date <= p.trade_date_to)
  AND (p.instrument_types IS NULL OR list_contains(p.instrument_types, i.instrument_type))
  AND (p.min_notional_usd IS NULL OR round(t.quantity * t.price * fx.rate_to_usd, 2) >= p.min_notional_usd)
  AND (NOT p.latest_only OR l.superseded_by IS NULL)
