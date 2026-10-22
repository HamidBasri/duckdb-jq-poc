# Clean one reference entity ($entity). Records with no key are dropped; first key wins.
include "lib";

def normalize:
  if $entity == "counterparties" then
    {
      counterparty_id: (.counterparty_id | nullify),
      legal_name: (.legal_name | nullify),
      lei: (.lei | nullify),
      country: (.country | upper_or_null),
      sector: (.sector | nullify),
      credit_rating: (.credit_rating | upper_or_null),
      parent_counterparty_id: (.parent_counterparty_id | nullify)
    }
  elif $entity == "instruments" then
    {
      instrument_id: (.instrument_id | nullify),
      isin: (.isin | nullify),
      instrument_type: (.instrument_type | upper_or_null),
      currency: (.currency | upper_or_null),
      issuer_counterparty_id: (.issuer_counterparty_id | nullify),
      underlying_instrument_id: (.underlying_instrument_id | nullify),
      maturity_date: (.maturity_date | parse_date)
    }
  elif $entity == "books" then
    {book_id: (.book_id | nullify), desk: (.desk | nullify), trader: (.trader | nullify), region: (.region | upper_or_null)}
  elif $entity == "fx_rates" then
    {currency: (.currency | upper_or_null), rate_to_usd: (.rate_to_usd | to_number_or_null)}
  else
    error("unknown entity \($entity)")
  end;

{counterparties: "counterparty_id", instruments: "instrument_id", books: "book_id", fx_rates: "currency"}[$entity]
  as $key
| first_by_key(inputs | normalize | select(.[$key] != null); .[$key])
