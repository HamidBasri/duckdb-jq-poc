# Shared helpers. Semantics mirror duckdb_engine/sql/macros.sql exactly.

def nullify: if . == null then null else (tostring | trim | if . == "" then null else . end) end;
def upper_or_null: nullify | if . == null then null else ascii_upcase end;
def to_number_or_null: nullify | if . == null then null else (try tonumber catch null) end;

# ISO or DD/MM/YYYY. strptime silently rolls invalid dates over (2025-02-31 -> 2025-03-03),
# so the parsed value must format back to the input to count as valid (strict, like DuckDB).
def parse_date:
  nullify
  | if . == null then null
    else . as $text
      | first(
          ("%Y-%m-%d", "%d/%m/%Y") as $format
          | try ($text | strptime($format) | mktime) catch empty
          | select(strftime($format) == $text)
          | strftime("%Y-%m-%d")
        ) // null
    end;

def add_days($days): strptime("%Y-%m-%d") | mktime + ($days * 86400) | strftime("%Y-%m-%d");
def index_by(f): reduce .[] as $row ({}; .[$row | f] = $row);
def lookup($table): if . == null then null else $table[.] end;
def known($table): . != null and $table[.] != null;
def round2: . * 100 | round / 100;

# Ids from the root of a parent chain down to `.`, or null when the walk hits a cycle.
# `$parent_of` maps id -> parent id; a missing or unknown parent ends the chain.
def path_to_root($parent_of):
  def climb($seen):
    . as $id
    | if $seen[$id] then null
      else ($parent_of[$id]) as $parent
        | if $parent == null then [$id]
          else ($parent | climb($seen + {($id): true})) as $upper
            | if $upper == null then null else $upper + [$id] end
          end
      end;
  climb({});

# Streaming de-duplication: keeps the first record per key, holding only the keys in memory.
def first_by_key(stream; key):
  foreach stream as $row ({seen: {}, emit: null};
    ($row | key) as $key
    | if $key == null then .emit = $row
      elif .seen[$key] then .emit = null
      else .seen[$key] = true | .emit = $row
      end;
    .emit // empty);
