#!/usr/bin/env python3
"""price_quality for Silver rows: an assessment, never an edit.

The criterion is the one measured for migrations/2026-09-06_price_quality.sql
on `trades`: compare the FILED per-share price to the ticker's price band for
the month of the transaction.

    ratio = price_per_share / band_high

      >= 100x           implausible     no corporate action moves a price 100x
                                        in a month; the largest split in the
                                        data is 50:1. The IHT 22,625 filer error
                                        is ~15,000x.
      3x .. 100x        outside_band    splits, ADRs, volatility. Real more
      or < band_low/3                   often than not; the value is shown but
                                        a reader deserves the label.
      inside            ok
      no prices that    no_reference    we cannot say, and say so
      month
      price filed as 0  no_price        a grant, exercise or gift; not a
                                        market price, so not judged against one

The band is [min(low), max(high)] over prices.daily_prices for the calendar
month of trans_date. It is a SEPARATE pass from the parse so a parse without
prices is still a complete parse, and so re-assessing (better prices, a
different threshold) never touches the facts.

    python3 pipelines/silver/assess.py --limit 50000
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402
from framework.observability.pipeline_runner import pipeline_run  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("assess_silver")

IMPLAUSIBLE_X = Decimal(100)
OUTSIDE_X = Decimal(3)

# One statement does the whole batch: the band is a correlated aggregate over
# the month, and the CASE is the table above. daily_prices.date is TEXT
# (YYYY-MM-DD), hence the substr.
# The band is built ONCE per run into a temp table and joined. The first
# version computed it as a correlated subquery per row -- min(low)/max(high)
# over daily_prices filtered by substr(date, 1, 7), which no index serves --
# and one 100,000-row batch ran past twelve minutes on the full 11.4M-row
# table. daily_prices.date is TEXT (YYYY-MM-DD), hence the substr.
BAND_SQL = """
    CREATE TEMP TABLE band AS
    SELECT ticker, substr(date, 1, 7) AS ym, min(low) AS lo, max(high) AS hi
      FROM prices.daily_prices
     WHERE low IS NOT NULL AND high IS NOT NULL
     GROUP BY 1, 2;
    CREATE INDEX band_ticker_ym ON band (ticker, ym);
    ANALYZE band;
"""

ASSESS_SQL = """
    WITH todo AS (
        SELECT accession, rptowner_cik, line_no, ticker, to_char(trans_date, 'YYYY-MM') AS ym, price_per_share
          FROM silver.form4_transaction
         WHERE price_quality IS NULL AND price_per_share IS NOT NULL
         LIMIT ?
    ), scored AS (
        SELECT t.accession, t.rptowner_cik, t.line_no, t.price_per_share, b.lo, b.hi
          FROM todo t LEFT JOIN band b ON b.ticker = t.ticker AND b.ym = t.ym
    )
    UPDATE silver.form4_transaction s
       SET price_quality = CASE
             -- A price of 0 is a grant, exercise or gift: the filer reported
             -- no price, and comparing 0 to a band said 'outside_band' on
             -- 1,766,274 lines of the first full run. No price is not a
             -- bad price.
             WHEN b.price_per_share <= 0                THEN 'no_price'
             WHEN b.hi IS NULL OR b.hi <= 0            THEN 'no_reference'
             WHEN b.price_per_share >= b.hi * ?        THEN 'implausible'
             WHEN b.price_per_share >= b.hi * ?
               OR b.price_per_share * ? < b.lo          THEN 'outside_band'
             ELSE 'ok' END,
           price_quality_note = CASE
             WHEN b.price_per_share <= 0 THEN 'no price filed (0): a grant, exercise, gift or similar, not a market transaction'
             WHEN b.hi IS NULL OR b.hi <= 0 THEN 'no daily prices for this ticker in the month of the transaction'
             -- concatenation, not format(): a %s inside the SQL string is a
             -- psycopg2 placeholder and the first run died on it
             ELSE 'filed ' || b.price_per_share || ' against a ' || round(b.lo::numeric, 4)
                  || '..' || round(b.hi::numeric, 4) || ' band that month ('
                  || round((b.price_per_share / b.hi)::numeric, 1) || 'x the high)' END
      FROM scored b
     WHERE s.accession = b.accession AND s.rptowner_cik = b.rptowner_cik AND s.line_no = b.line_no
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100000, help="rows per batch")
    ap.add_argument("--max-batches", type=int, default=None)
    args = ap.parse_args()
    with pipeline_run("assess_silver") as run:
        conn = get_connection()
        t0 = time.monotonic()
        for stmt in BAND_SQL.strip().split(";"):
            if stmt.strip():
                conn.execute(stmt)
        nb = conn.execute("SELECT count(*) AS n FROM band").fetchone()["n"]
        logger.info("band table: %d ticker-months in %.1fs", nb, time.monotonic() - t0)
        total = batches = 0
        while args.max_batches is None or batches < args.max_batches:
            cur = conn.execute(ASSESS_SQL, (args.limit, IMPLAUSIBLE_X, OUTSIDE_X, OUTSIDE_X))
            n = cur.rowcount if cur.rowcount is not None else 0
            conn.commit()
            total += n
            batches += 1
            logger.info("assessed %d (total %d)", n, total)
            if n < args.limit:
                break
        dist = conn.execute(
            "SELECT price_quality, count(*) AS n FROM silver.form4_transaction "
            "WHERE price_per_share IS NOT NULL GROUP BY 1 ORDER BY 2 DESC").fetchall()
        for r in dist:
            logger.info("  %-14s %d", r["price_quality"], r["n"])
        run.set_rows_written(total)
        run.set_metadata({r["price_quality"] or "unassessed": r["n"] for r in dist})
    return 0


if __name__ == "__main__":
    sys.exit(main())
