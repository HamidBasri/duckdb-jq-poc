# Transform pass 2: amendment links in both directions. A trade can reference a trade that
# appears later in the file (forward reference), so lineage needs this full-stream index.
# Documents are projected to the two ids before the reduce (see exposures.jq for why).
reduce (
  inputs
  | select(.trade_id != null and .previous_trade_id != null)
  | select(any(.errors[]; . == "E_PREVIOUS_TRADE_UNRESOLVED") | not)
  | {id: .trade_id, previous: .previous_trade_id}
) as $link ({previous: {}, next: {}};
  .previous[$link.id] = $link.previous
  | .next[$link.previous] |= (if . == null or $link.id < . then $link.id else . end))
