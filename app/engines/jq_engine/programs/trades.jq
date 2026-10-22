# Transform pass 3 (streaming): shape each valid trade into the nested target document.
include "lib";

($dimensions[0]) as $dims
| ($lineage[0]) as $links
| def selected:
    ($params.trade_date_from == null or .trade_date >= $params.trade_date_from)
    and ($params.trade_date_to == null or .trade_date <= $params.trade_date_to)
    and ($params.instrument_types == null or (.instrument.instrument_type | IN($params.instrument_types[])))
    and ($params.min_notional_usd == null or .economics.notional_usd >= $params.min_notional_usd)
    and (($params.latest_only | not) or .lineage.is_latest);
  inputs
  | select(.errors == [])
  | ($dims.counterparties[.counterparty_id]) as $counterparty
  | ($dims.fx_rates[.currency].rate_to_usd) as $rate
  | (.trade_id | path_to_root($links.previous)) as $chain
  | ($links.next[.trade_id]) as $successor
  | (.quantity * .price) as $notional
  | {
      trade_id, event_type, trade_date, settlement_date, side,
      economics: {
        quantity, price, currency,
        notional: ($notional | round2),
        fx_rate_to_usd: $rate,
        notional_usd: ($notional * $rate | round2)
      },
      instrument: $dims.instruments[.instrument_id],
      counterparty: ($counterparty | del(.warnings)),
      book: $dims.books[.book_id],
      lineage: {
        root_trade_id: (if $chain == null then null else $chain[0] end),
        version: (if $chain == null then null else ($chain | length) end),
        chain: $chain,
        previous_trade_id,
        superseded_by: $successor,
        is_latest: ($successor == null)
      },
      data_quality: {
        warnings: (.warnings + $counterparty.warnings + (if $chain == null then ["W_LINEAGE_CYCLE"] else [] end))
      }
    }
  | select(selected)
