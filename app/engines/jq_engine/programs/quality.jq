reduce (inputs | {errors, warnings}) as $trade ({valid_rows: 0, rejected_rows: 0, error_counts: {}, warning_counts: {}};
  (if $trade.errors == [] then .valid_rows += 1 else .rejected_rows += 1 end)
  | reduce $trade.errors[] as $code (.; .error_counts[$code] += 1)
  | reduce $trade.warnings[] as $code (.; .warning_counts[$code] += 1))
