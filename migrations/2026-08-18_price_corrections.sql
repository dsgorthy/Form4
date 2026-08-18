-- trades.price_as_filed / value_as_filed / correction_method — show our work.
--
-- WHY
--
-- Some Form 4s are filled out wrong. ConnectM put the trade's total in the
-- price-per-share field, so a $65,122 purchase is stored as
-- price 65,122 x qty 73,680 = $4,798,188,960. Repairing that is a real part of
-- the service, but silently rewriting a number that came off a public SEC
-- filing is not: a reader comparing our page to EDGAR must be able to see that
-- the two differ and why.
--
-- suspect_reason already carries a sentence, but it is prose. The filing page
-- needs the original figures as figures, and parsing them back out of a
-- human-readable string in the frontend is the fragile version of this.
--
--   price_as_filed     what the filer submitted, NULL if we never touched it
--   value_as_filed     likewise
--   correction_method  which failure it was — price_is_total_value,
--                      power_of_10_shift, price_equals_qty
--
-- NULL means untouched, which is the overwhelming majority, so the column
-- doubles as the flag: `WHERE price_as_filed IS NOT NULL` is every corrected
-- filing.
--
-- Written by strategies/insider_catalog/price_validator.py. Nothing else
-- should write them — the validator is the one place that decides a filing is
-- wrong, and it records the evidence in the same statement that applies the
-- fix.
--
-- Applied via: psql -d form4 -f migrations/2026-08-18_price_corrections.sql

ALTER TABLE trades ADD COLUMN IF NOT EXISTS price_as_filed    DOUBLE PRECISION;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS value_as_filed    DOUBLE PRECISION;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS correction_method TEXT;

COMMENT ON COLUMN trades.price_as_filed IS
    'Price as submitted by the filer, before correction. NULL = never corrected.';
COMMENT ON COLUMN trades.value_as_filed IS
    'Value as submitted by the filer, before correction. NULL = never corrected.';
COMMENT ON COLUMN trades.correction_method IS
    'Which parse failure was corrected: price_is_total_value | power_of_10_shift | price_equals_qty.';

-- Partial: corrected rows are a rounding error against 1.7M, and the only
-- query shape is "is this one corrected" / "list the corrected ones".
CREATE INDEX IF NOT EXISTS idx_trades_corrected
    ON trades (correction_method)
    WHERE correction_method IS NOT NULL;
