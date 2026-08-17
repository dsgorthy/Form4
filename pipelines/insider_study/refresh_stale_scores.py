#!/usr/bin/env python3
"""Refresh V2 ("Recent Form") scores that were written before their returns existed.

WHY THIS EXISTS

insider_ticker_scores rows are created at ingest, keyed on as_of_date = the new
trade's filing_date. That is the one moment when, by definition, the newest
trade has no observable forward return — and for a great many rows the returns
of the insider's EARLIER trades had not landed in trade_returns yet either,
because that backfill runs nightly and the 90-day window closes 100 days after
the trade. So the row was written with blended_score NULL and sufficient_data 0,
and nothing ever went back to look again.

The V3 career fields do not have this problem: compute_career_grades.py runs
daily and recomputes them at run time, by which point the prior trades have
matured. It writes only career_blended_score and career_grade, leaving the V2
fields exactly as ingest left them.

HOW MUCH THIS ACTUALLY RECOVERS — READ BEFORE TRUSTING AN ESTIMATE

Almost nothing, most days. The first full run (2026-08-17, --since 2016-01-01)
processed all 81,468 unscored rows and recovered 0. That is the correct result,
not a failure: trade_returns is already caught up to its ceiling, so there is no
backlog of matured-but-unread returns to collect.

Sampling why each unscored row is unscored:

    84.4%  insider had no prior buy at all as of that date
    13.2%  prior buys exist, but no price data for any of their tickers
     2.5%  prices exist, return not computed  <- the only real gap
     0.0%  should have scored and didn't

The 13.2% is mostly not a defect. Ticker 'NONE' (5,912 trades, 1,174 insiders,
388 companies) is non-traded issuers filing Form 4 — private BDCs, interval
funds, employee-owned companies: Audax Private Credit, HPS Real Assets Lending,
Fundrise, West Bay BDC, Publix. No ticker, no market price, never scoreable.
The remainder are OTC microcaps (ENDV, IMTL, VPRB) with no price coverage.

This script was originally written to fix profiles reading "awaiting returns"
next to a career grade. That turned out to be a display bug, not a data one —
the rows were scored the whole time, and the card said "awaiting returns"
whenever the insider had no prior trades in that specific ticker. Fixed in
api/pit_helpers.py + frontend insider page; nothing here was the cause.

What the script is still good for: 2026 rows, whose returns are 74.8% complete
and still landing. Re-running periodically picks those up as they mature. Expect
single-digit or low-hundreds recoveries, and treat a large number as a signal
that the returns backfill had stalled.

THIS IS NOT A PIT VIOLATION

Recomputing an old as_of_date today produces a MORE correct point-in-time value,
not a leaky one. _get_returns enforces both guards independently of when it runs:

    trade_date  <= as_of_date - lag   (the forward return endpoint is observable)
    filing_date <= as_of_date         (we actually knew about the trade)

Nothing after as_of_date can enter the score. The original write was simply
missing rows it was entitled to include, because trade_returns had not caught up
yet. Running later sees the same eligible set, more completely.

SCOPE

Touches only the V2 columns on insider_ticker_scores. Does not write
trades.pit_grade, does not touch the career_* columns, and no live strategy
reads any of it — quality_momentum filters on trades.career_grade, reversal_dip
and tenb51_surprise use no grade at all. cw_runner reads pit_grade only to label
an alert, never to select one.

USAGE

    python3 pipelines/insider_study/refresh_stale_scores.py --since 2016-01-01
    python3 pipelines/insider_study/refresh_stale_scores.py --since 2026-01-01 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--since", required=True,
        help="Only revisit score rows with as_of_date >= this (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--rebuild", action="store_true",
        help="Recompute every row in the window, not just the unscored ones",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would change without writing",
    )
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Commit every N rows")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N rows (for a bounded first pass)")
    args = parser.parse_args()

    sys.path.insert(
        0, str(Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog")
    )
    from pit_scoring import compute_insider_ticker_score  # noqa: E402

    conn = get_connection()

    where = ["as_of_date >= ?"]
    params: list = [args.since]
    if not args.rebuild:
        where.append("blended_score IS NULL")
    where_sql = " AND ".join(where)

    sql = f"""SELECT insider_id, ticker, as_of_date
                FROM insider_ticker_scores
               WHERE {where_sql}
            ORDER BY as_of_date ASC"""
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"

    rows = conn.execute(sql, tuple(params)).fetchall()
    logger.info("Score rows to revisit: %d (since=%s, rebuild=%s, dry_run=%s)",
                len(rows), args.since, args.rebuild, args.dry_run)
    if not rows:
        logger.info("Nothing to do.")
        return 0

    t0 = time.time()
    processed = 0
    recovered = 0   # went from unscored to scored
    failed = 0

    for insider_id, ticker, as_of_date in rows:
        try:
            r = compute_insider_ticker_score(conn, insider_id, ticker, as_of_date)
        except Exception as exc:
            logger.warning("score failed for %s/%s @ %s: %s",
                           insider_id, ticker, as_of_date, exc)
            failed += 1
            processed += 1
            continue

        # Only claim a row when the scorer says it has enough to stand on;
        # writing a score it flagged as insufficient would trade one misleading
        # display for another.
        if r.sufficient_data:
            if not args.dry_run:
                conn.execute(
                    """UPDATE insider_ticker_scores
                          SET blended_score = ?,
                              ticker_score = ?,
                              global_score = ?,
                              ticker_win_rate_7d = ?,
                              ticker_avg_abnormal_7d = ?,
                              global_win_rate_7d = ?,
                              global_avg_abnormal_7d = ?,
                              ticker_trade_count = ?,
                              global_trade_count = ?,
                              sufficient_data = 1
                        WHERE insider_id = ? AND ticker = ? AND as_of_date = ?""",
                    (r.blended_score, r.ticker_score, r.global_score,
                     r.ticker_win_rate_7d, r.ticker_avg_abnormal_7d,
                     r.global_win_rate_7d, r.global_avg_abnormal_7d,
                     r.ticker_trade_count, r.global_trade_count,
                     insider_id, ticker, as_of_date),
                )
            recovered += 1

        processed += 1
        if not args.dry_run and processed % args.batch_size == 0:
            conn.commit()
            rate = processed / max(time.time() - t0, 1e-9)
            logger.info("  %d/%d processed, %d recovered (%.0f rows/s)",
                        processed, len(rows), recovered, rate)

    if not args.dry_run:
        conn.commit()

    logger.info("Done in %.1fs — processed=%d recovered=%d failed=%d",
                time.time() - t0, processed, recovered, failed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
