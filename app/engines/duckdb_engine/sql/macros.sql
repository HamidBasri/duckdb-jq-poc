-- Scalar helpers shared by every stage. jq equivalents live in jq_engine/programs/lib.jq.
CREATE OR REPLACE MACRO nullify(s) AS nullif(trim(s), '');
CREATE OR REPLACE MACRO upper_or_null(s) AS upper(nullif(trim(s), ''));
CREATE OR REPLACE MACRO to_double(s) AS try_cast(nullif(trim(s), '') AS DOUBLE);
-- try_strptime is strict (2025-02-31 -> NULL); accepts ISO and DD/MM/YYYY.
CREATE OR REPLACE MACRO parse_date(s) AS
    coalesce(try_strptime(nullif(trim(s), ''), '%Y-%m-%d'), try_strptime(nullif(trim(s), ''), '%d/%m/%Y'))::DATE;
