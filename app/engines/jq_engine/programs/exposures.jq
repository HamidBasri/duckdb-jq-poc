# Aggregate (streaming reduce): live exposure per ultimate parent with nested breakdowns.
#
# jq performance notes, measured on 100k real trade documents:
#   * Project each document to the few fields needed BEFORE the reduce: ~4.5x faster than
#     reducing over the full ~1.2 KB documents.
#   * Use direct path updates (.[$k].x += n), not `.[$k] |= (...)` (copies the sub-object).
#   * Avoid flat composite string keys (.m["\($a)|\($b)"]): ~20x slower than nested paths.
include "lib";

def breakdown($key):
  to_entries
  | sort_by(.key)
  | map({($key): .key, trade_count: .value.trade_count, gross_notional_usd: (.value.gross | round2)});

reduce (
  inputs
  | select(.lineage.is_latest and .event_type != "CANCEL")
  | (.counterparty.ultimate_parent // .counterparty) as $group
  | {
      key: $group.counterparty_id,
      name: $group.legal_name,
      counterparty: .counterparty.counterparty_id,
      currency: .economics.currency,
      notional: .economics.notional_usd,
      signed: (if .side == "BUY" then .economics.notional_usd else 0 - .economics.notional_usd end)
    }
) as $t ({};
  .[$t.key].ultimate_parent_id = $t.key
  | .[$t.key].ultimate_parent_name = $t.name
  | .[$t.key].trade_count += 1
  | .[$t.key].gross += $t.notional
  | .[$t.key].net += $t.signed
  | .[$t.key].by_currency[$t.currency].trade_count += 1
  | .[$t.key].by_currency[$t.currency].gross += $t.notional
  | .[$t.key].by_counterparty[$t.counterparty].trade_count += 1
  | .[$t.key].by_counterparty[$t.counterparty].gross += $t.notional)
| to_entries
| sort_by(.key)
| .[].value
| {
    ultimate_parent_id,
    ultimate_parent_name,
    trade_count,
    gross_notional_usd: (.gross | round2),
    net_notional_usd: (.net | round2),
    by_currency: (.by_currency | breakdown("currency")),
    by_counterparty: (.by_counterparty | breakdown("counterparty_id"))
  }
