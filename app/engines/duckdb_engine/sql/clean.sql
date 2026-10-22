-- Clean stage: trim/normalise text, parse types, deduplicate (first occurrence wins), impute.
CREATE OR REPLACE TABLE clean_counterparties AS
SELECT
    nullify(counterparty_id) AS counterparty_id,
    nullify(legal_name) AS legal_name,
    nullify(lei) AS lei,
    upper_or_null(country) AS country,
    nullify(sector) AS sector,
    upper_or_null(credit_rating) AS credit_rating,
    nullify(parent_counterparty_id) AS parent_counterparty_id
FROM raw_counterparties
WHERE nullify(counterparty_id) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY nullify(counterparty_id) ORDER BY rowid) = 1;

CREATE OR REPLACE TABLE clean_instruments AS
SELECT
    nullify(instrument_id) AS instrument_id,
    nullify(isin) AS isin,
    upper_or_null(instrument_type) AS instrument_type,
    upper_or_null(currency) AS currency,
    nullify(issuer_counterparty_id) AS issuer_counterparty_id,
    nullify(underlying_instrument_id) AS underlying_instrument_id,
    parse_date(maturity_date) AS maturity_date
FROM raw_instruments
WHERE nullify(instrument_id) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY nullify(instrument_id) ORDER BY rowid) = 1;

CREATE OR REPLACE TABLE clean_books AS
SELECT nullify(book_id) AS book_id, nullify(desk) AS desk, nullify(trader) AS trader, upper_or_null(region) AS region
FROM raw_books
WHERE nullify(book_id) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY nullify(book_id) ORDER BY rowid) = 1;

CREATE OR REPLACE TABLE clean_fx_rates AS
SELECT upper_or_null(currency) AS currency, to_double(rate_to_usd) AS rate_to_usd
FROM raw_fx_rates
WHERE upper_or_null(currency) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY upper_or_null(currency) ORDER BY rowid) = 1;

CREATE OR REPLACE TABLE clean_trades AS
WITH normalized AS (
    SELECT
        rowid AS source_row,
        nullify(trade_id) AS trade_id,
        nullify(previous_trade_id) AS previous_trade_id,
        upper_or_null(event_type) AS event_type,
        parse_date(trade_date) AS trade_date,
        parse_date(settlement_date) AS settlement_date,
        nullify(instrument_id) AS instrument_id,
        nullify(counterparty_id) AS counterparty_id,
        nullify(book_id) AS book_id,
        CASE upper_or_null(side) WHEN 'B' THEN 'BUY' WHEN 'BUY' THEN 'BUY' WHEN 'S' THEN 'SELL' WHEN 'SELL' THEN 'SELL' END
            AS side,
        to_double(replace(quantity, ',', '')) AS quantity,
        to_double(price) AS price,
        upper_or_null(currency) AS currency
    FROM raw_trades
), deduplicated AS (
    SELECT * FROM normalized
    QUALIFY trade_id IS NULL OR row_number() OVER (PARTITION BY trade_id ORDER BY source_row) = 1
)
SELECT
    d.* EXCLUDE (currency, settlement_date),
    coalesce(d.currency, i.currency) AS currency,
    coalesce(d.settlement_date, (d.trade_date + INTERVAL 2 DAY)::DATE) AS settlement_date,
    list_filter([
        CASE WHEN d.currency IS NULL AND i.currency IS NOT NULL THEN 'W_CURRENCY_IMPUTED' END,
        CASE WHEN d.settlement_date IS NULL AND d.trade_date IS NOT NULL THEN 'W_SETTLEMENT_IMPUTED' END
    ], w -> w IS NOT NULL) AS warnings
FROM deduplicated d
LEFT JOIN clean_instruments i ON i.instrument_id = d.instrument_id;
