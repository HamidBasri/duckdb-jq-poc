-- Aggregate stage: live exposure per ultimate parent, with nested currency/counterparty breakdowns.
CREATE OR REPLACE TABLE out_exposures AS
WITH base AS (
    SELECT
        coalesce(counterparty.ultimate_parent.counterparty_id, counterparty.counterparty_id) AS group_id,
        coalesce(counterparty.ultimate_parent.legal_name, counterparty.legal_name) AS group_name,
        counterparty.counterparty_id AS counterparty_id,
        economics.currency AS currency,
        economics.notional_usd AS notional_usd,
        CASE WHEN side = 'BUY' THEN economics.notional_usd ELSE -economics.notional_usd END AS signed_notional_usd
    FROM out_trades
    WHERE lineage.is_latest AND event_type <> 'CANCEL'
), totals AS (
    SELECT
        group_id,
        any_value(group_name) AS group_name,
        count(*) AS trade_count,
        round(sum(notional_usd), 2) AS gross_notional_usd,
        round(sum(signed_notional_usd), 2) AS net_notional_usd
    FROM base
    GROUP BY group_id
), by_currency AS (
    SELECT group_id,
           list({'currency': currency, 'trade_count': trade_count, 'gross_notional_usd': gross} ORDER BY currency)
               AS by_currency
    FROM (SELECT group_id, currency, count(*) AS trade_count, round(sum(notional_usd), 2) AS gross
          FROM base GROUP BY group_id, currency)
    GROUP BY group_id
), by_counterparty AS (
    SELECT group_id,
           list({'counterparty_id': counterparty_id, 'trade_count': trade_count, 'gross_notional_usd': gross}
                ORDER BY counterparty_id) AS by_counterparty
    FROM (SELECT group_id, counterparty_id, count(*) AS trade_count, round(sum(notional_usd), 2) AS gross
          FROM base GROUP BY group_id, counterparty_id)
    GROUP BY group_id
)
SELECT
    t.group_id AS ultimate_parent_id,
    t.group_name AS ultimate_parent_name,
    t.trade_count,
    t.gross_notional_usd,
    t.net_notional_usd,
    c.by_currency,
    p.by_counterparty
FROM totals t
JOIN by_currency c ON c.group_id = t.group_id
JOIN by_counterparty p ON p.group_id = t.group_id
ORDER BY ultimate_parent_id;
