#!/usr/bin/env python3
"""Does the product hold every DECISION the plane parsed? Exit non-zero if not.

WHY THIS EXISTS, AND WHY record_parity.py DOES NOT COVER IT

`scripts/record_parity.py` gates on `recall = matched / distinct_b`, which asks
"does the PLANE have everything the PRODUCT has". That is the correct question
for its own purpose — retiring the form4 bridge — and it answers 100.000% almost
every day, so it PASSES daily and always will. Nothing gated the other
direction, and the other direction is the one where data loss lives.

It records the reverse figure as `coverage_a`, which has run 87.8-97.3%. Pointing
the existing gate at that number would be wrong, and would produce a
permanently-red alarm that everyone learns to ignore. Measured 2026-09-29: of 464
accessions present in Silver and absent from `trades` over six trading days,

    325  derivative-only        the product is RIGHT to skip these
    127  grants and exercises   non-derivative, no P or S
     12  real purchases/sales   and 13 of their 14 rows were ALREADY STORED
                                under a different accession

Genuine loss: ONE trade. So `coverage_a` is not a loss rate; it is mostly
by-design skipping plus accession churn.

TWO THINGS THIS CHECK DOES DIFFERENTLY

1. **Population.** Only non-derivative purchases and sales — the rows that reach
   a track record, a grade or a strategy. A derivative warrant purchase the
   product deliberately excludes is not a gap.
2. **Identity, not accession.** A trade counts as present if `trades` holds the
   same (ticker, trade_date, DIRECTION) at the same value or share count. The
   unique index on `trades` is
   (insider_id, ticker, trade_date, trade_type, value), NOT accession, so the
   same economic trade legitimately arrives under a different accession and an
   accession-keyed test calls it missing. That single confusion produced three
   wrong conclusions in one afternoon.

EXITS NON-ZERO above the threshold, so the Dagster failure sensor pages. Silence
was the actual defect being fixed here: the loss was recorded daily in a column
nobody gated on.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

#: How many genuinely-absent decision rows are tolerated before this fails.
#:
#: MEASURED BASELINE on 2026-09-29 over a 7-day window: FIVE. And all five share
#: one cause — a malformed issuer ticker, which the product's ingest does not
#: resolve while Silver keeps whatever the filing said:
#:
#:     N/A    Calamos Aksia Hedged Strategies Fund   P  $7,477,500
#:     cv     Shen Ching Hang                        P  $4,999,996
#:     emyb   Boyer Geoffrey F                       S  three sells
#:
#: 15 is three times that steady state. Set ABOVE the baseline rather than at it,
#: deliberately: a threshold equal to the normal reading flaps, and an alarm that
#: cries most weeks is one people mute — which is the failure this whole check
#: exists to undo. It still catches a doubling.
#:
#: THE RESIDUAL LOSS IS FIXABLE AND IS NOT FIXED. `gold.ticker_by_issuer` maps
#: issuer_cik to ticker over 15,545 issuers, so the ingest could resolve these
#: instead of storing them unusable. That is the next change here, not this one.
DEFAULT_MAX_MISSING = 15

#: Trailing trading window. Long enough that one quiet day cannot hide a problem,
#: short enough that a fixed historical gap does not keep the alarm red forever.
DEFAULT_DAYS = 7

SQL = """
WITH silver_decisions AS (
    SELECT s.accession, s.ticker, s.trans_date::date AS trade_date,
           s.trans_code, s.rptowner_name,
           ROUND(s.value::numeric, 2)  AS value,
           ROUND(s.shares::numeric, 4) AS shares
      FROM silver.form4_transaction s
     WHERE s.filed_at >= (CURRENT_DATE - %s)
       AND NOT s.is_derivative
       AND s.trans_code IN ('P', 'S')
       AND s.ticker IS NOT NULL AND s.ticker <> '' AND s.ticker <> 'NONE'
       AND s.trans_date IS NOT NULL
)
SELECT d.accession, d.ticker, d.rptowner_name, d.trans_code,
       d.trade_date, d.value, d.shares
  FROM silver_decisions d
 WHERE NOT EXISTS (
        SELECT 1 FROM trades t
         WHERE t.ticker = d.ticker
           AND t.trade_date = d.trade_date::text
           -- DIRECTION, not the exact transaction code. Matching on
           -- trans_code produced false positives: measured over 30 days to
           -- 2026-09-29, 10 of 1,534 Silver `P` rows appear in `trades` as `A`
           -- with the same ticker, date and quantity, under a different
           -- accession. `trade_type` agrees in every one of those, so the trade
           -- IS held and only the coding differs. That disagreement is a real
           -- but separate and much smaller issue; it must not make this check
           -- cry loss when the row is present.
           AND t.trade_type = CASE d.trans_code WHEN 'P' THEN 'buy' ELSE 'sell' END
           -- Value OR share match: a filing reported in shares with no price
           -- has a NULL value on one side and must not count as missing.
           AND (
                 (d.value IS NOT NULL AND t.value IS NOT NULL
                  AND ABS(ROUND(t.value::numeric, 2) - d.value) <= 0.02)
              OR (d.shares IS NOT NULL AND t.qty IS NOT NULL
                  AND ABS(ROUND(t.qty::numeric, 4) - d.shares) <= 0.0001)
               )
       )
 ORDER BY d.value DESC NULLS LAST
"""


def find_missing(days: int) -> list[tuple]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '900s'")
        cur.execute(SQL, (days,))
        return list(cur.fetchall())
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS)
    ap.add_argument("--max-missing", type=int, default=DEFAULT_MAX_MISSING)
    args = ap.parse_args()

    missing = find_missing(args.days)
    print(f"ingest completeness — non-derivative P/S decisions, "
          f"last {args.days} days, matched on trade identity not accession")
    print(f"  genuinely absent from trades: {len(missing)} "
          f"(threshold {args.max_missing})")

    for row in missing[:25]:
        acc, ticker, who, code, td, val, sh = row
        amount = f"${val:,.0f}" if val is not None else f"{sh} sh"
        print(f"    {acc}  {ticker:<8} {code}  {td}  {amount:>14}  {who}")
    if len(missing) > 25:
        print(f"    ... and {len(missing) - 25} more")

    if len(missing) > args.max_missing:
        print(f"FAIL: {len(missing)} decision row(s) parsed by Silver are absent "
              f"from trades, above the threshold of {args.max_missing}. These are "
              f"purchases and sales the product cannot show and no strategy can "
              f"see.")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
