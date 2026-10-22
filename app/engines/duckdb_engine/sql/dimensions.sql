-- Transform stage, part 1: resolve cross-references into dimension tables.

-- Counterparty hierarchy. A STRUCT can't contain itself, so the nested parent object is built
-- as JSON, starting at the roots. Rows inside a parent loop are never reached from a root,
-- which stops the recursion and lets us flag them.
CREATE OR REPLACE TABLE dim_counterparty AS
WITH RECURSIVE tree(counterparty_id, depth, path, ultimate_id, nested, parent_nested) AS (
    SELECT
        c.counterparty_id, 0, [c.counterparty_id], c.counterparty_id,
        json_object('counterparty_id', c.counterparty_id, 'legal_name', c.legal_name, 'country', c.country,
                    'parent', NULL::JSON),
        NULL::JSON
    FROM clean_counterparties c
    WHERE c.parent_counterparty_id IS NULL
       OR NOT EXISTS (SELECT 1 FROM clean_counterparties p WHERE p.counterparty_id = c.parent_counterparty_id)
    UNION ALL
    SELECT
        c.counterparty_id, t.depth + 1, list_append(t.path, c.counterparty_id), t.ultimate_id,
        json_object('counterparty_id', c.counterparty_id, 'legal_name', c.legal_name, 'country', c.country,
                    'parent', t.nested),
        t.nested
    FROM clean_counterparties c
    JOIN tree t ON c.parent_counterparty_id = t.counterparty_id
)
SELECT
    c.counterparty_id, c.legal_name, c.lei, c.country, c.sector, c.credit_rating,
    CASE WHEN t.counterparty_id IS NOT NULL
         THEN {'counterparty_id': u.counterparty_id, 'legal_name': u.legal_name} END AS ultimate_parent,
    t.depth AS hierarchy_depth,
    t.path AS hierarchy_path,
    t.parent_nested AS parent,
    list_filter([
        CASE WHEN t.counterparty_id IS NULL THEN 'W_COUNTERPARTY_HIERARCHY_CYCLE' END,
        CASE WHEN c.parent_counterparty_id IS NOT NULL AND p.counterparty_id IS NULL
             THEN 'W_COUNTERPARTY_PARENT_UNRESOLVED' END
    ], w -> w IS NOT NULL) AS warnings
FROM clean_counterparties c
LEFT JOIN tree t ON t.counterparty_id = c.counterparty_id
LEFT JOIN clean_counterparties u ON u.counterparty_id = t.ultimate_id
LEFT JOIN clean_counterparties p ON p.counterparty_id = c.parent_counterparty_id;

CREATE OR REPLACE TABLE dim_instrument AS
SELECT
    i.instrument_id, i.isin, i.instrument_type, i.currency, i.maturity_date,
    CASE WHEN iss.counterparty_id IS NOT NULL
         THEN {'counterparty_id': iss.counterparty_id, 'legal_name': iss.legal_name, 'lei': iss.lei} END AS issuer,
    CASE WHEN u.instrument_id IS NOT NULL
         THEN {'instrument_id': u.instrument_id, 'isin': u.isin, 'instrument_type': u.instrument_type} END AS underlying
FROM clean_instruments i
LEFT JOIN clean_counterparties iss ON iss.counterparty_id = i.issuer_counterparty_id
LEFT JOIN clean_instruments u ON u.instrument_id = i.underlying_instrument_id;

-- Trade lineage over every validated trade (rejected ones still anchor chains).
-- `previous_trade_id` may point backward or forward in the file; the recursive CTE walks
-- from originals down to the latest amendment, so file order does not matter.
CREATE OR REPLACE TABLE lineage AS
WITH RECURSIVE nodes AS (
    SELECT t.trade_id, CASE WHEN p.trade_id IS NOT NULL THEN t.previous_trade_id END AS parent_id
    FROM validated_trades t
    LEFT JOIN validated_trades p ON p.trade_id = t.previous_trade_id
    WHERE t.trade_id IS NOT NULL
), chain(trade_id, root_trade_id, version, path) AS (
    SELECT trade_id, trade_id, 1, [trade_id] FROM nodes WHERE parent_id IS NULL
    UNION ALL
    SELECT n.trade_id, c.root_trade_id, c.version + 1, list_append(c.path, n.trade_id)
    FROM nodes n
    JOIN chain c ON n.parent_id = c.trade_id
), successors AS (
    SELECT parent_id AS trade_id, min(trade_id) AS superseded_by
    FROM nodes
    WHERE parent_id IS NOT NULL
    GROUP BY parent_id
)
SELECT n.trade_id, c.root_trade_id, c.version, c.path AS chain, s.superseded_by
FROM nodes n
LEFT JOIN chain c ON c.trade_id = n.trade_id
LEFT JOIN successors s ON s.trade_id = n.trade_id;
