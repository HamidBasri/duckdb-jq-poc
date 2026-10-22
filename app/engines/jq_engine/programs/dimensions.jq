# Transform pass 1: resolve reference cross-links into lookup tables (one JSON object).
include "lib";

($counterparties | index_by(.counterparty_id)) as $cps
| ($cps | map_values(.parent_counterparty_id | select(. != null and $cps[.] != null))) as $parent_of
| ($cps | map_values(.counterparty_id | path_to_root($parent_of))) as $paths
# Nested `parent` objects are built root-first so every node reuses its parent's object.
# jq values are immutable and shared by reference, so this costs O(n), not O(n * depth).
| (reduce ($paths | to_entries | map(select(.value != null)) | sort_by(.value | length) | .[]) as $entry ({};
    $cps[$entry.key] as $cp
    | .[$entry.key] = {
        counterparty_id: $cp.counterparty_id,
        legal_name: $cp.legal_name,
        country: $cp.country,
        parent: (if ($entry.value | length) > 1 then .[$entry.value[-2]] else null end)
      })) as $nested
| ($instruments | index_by(.instrument_id)) as $ins
| {
    counterparties: ($cps | map_values(
      ($paths[.counterparty_id]) as $path
      | {
          counterparty_id, legal_name, lei, country, sector, credit_rating,
          ultimate_parent: (if $path == null then null else ($cps[$path[0]] | {counterparty_id, legal_name}) end),
          hierarchy_depth: (if $path == null then null else ($path | length) - 1 end),
          hierarchy_path: $path,
          parent: (if $path == null or ($path | length) < 2 then null else $nested[$path[-2]] end),
          warnings: [
            if $path == null then "W_COUNTERPARTY_HIERARCHY_CYCLE" else empty end,
            if .parent_counterparty_id != null and $cps[.parent_counterparty_id] == null
              then "W_COUNTERPARTY_PARENT_UNRESOLVED" else empty end
          ]
        })),
    instruments: ($ins | map_values({
      instrument_id, isin, instrument_type, currency, maturity_date,
      issuer: (.issuer_counterparty_id | lookup($cps) | if . == null then null else {counterparty_id, legal_name, lei} end),
      underlying: (.underlying_instrument_id | lookup($ins)
                   | if . == null then null else {instrument_id, isin, instrument_type} end)
    })),
    books: ($books | index_by(.book_id)),
    fx_rates: ($fx_rates | index_by(.currency))
  }
