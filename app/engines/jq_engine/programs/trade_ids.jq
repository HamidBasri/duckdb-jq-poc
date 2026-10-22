# First validation pass: the set of known trade ids, so previous_trade_id can be checked in
# a second streaming pass. jq cannot look ahead in a stream, so this needs O(n) memory.
reduce (inputs | .trade_id | select(. != null)) as $id ({}; .[$id] = true)
