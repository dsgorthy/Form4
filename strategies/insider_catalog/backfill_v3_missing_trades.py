#!/usr/bin/env python3
"""Fill career_grade for trades that don't yet have an insider_ticker_scores
row predating their filing_date.

For these trades, we compute a fresh V3 score at filing_date and:
  1. Insert/update insider_ticker_scores with career_blended_score + career_grade
  2. Update trades.career_grade

Read-only against existing scoring data; only writes to the new career_*
columns and creates new rows for missing as_of_dates.

Usage (on Studio):
    python3 -m strategies.insider_catalog.backfill_v3_missing_trades
    python3 -m strategies.insider_catalog.backfill_v3_missing_trades --limit 1000   # smoke test
"""
from __future__ import annotations

import argparse
import time

from config.database import get_connection
from strategies.insider_catalog.pit_scoring import (
    SCORER_V3,
    compute_insider_ticker_score,
    pit_score_to_grade,
)


def _fetch_targets(conn, limit=None):
    sql = """
        SELECT t.trade_id, t.insider_id, t.ticker, t.filing_date
        FROM trades t
        WHERE t.career_grade IS NULL
          AND t.superseded_by IS NULL
          AND t.trade_type = 'buy'
        ORDER BY t.filing_date DESC, t.trade_id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return conn.execute(sql).fetchall()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--commit-every", type=int, default=500)
    args = parser.parse_args()

    started = time.time()
    with get_connection() as conn:
        targets = _fetch_targets(conn, args.limit)

    n = len(targets)
    print(f"Backfilling {n:,} trades without career_grade")
    if n == 0:
        return

    with get_connection() as conn:
        ok = 0
        err = 0
        last_log = time.time()
        for i, row in enumerate(targets):
            trade_id = row["trade_id"]
            insider_id = row["insider_id"]
            ticker = row["ticker"]
            filing_date = row["filing_date"]
            try:
                v3 = compute_insider_ticker_score(
                    conn, insider_id, ticker, filing_date, scorer=SCORER_V3,
                )
                career_grade = (
                    pit_score_to_grade(v3.blended_score)
                    if v3.sufficient_data else None
                )
                # Upsert into insider_ticker_scores (career columns only — leave
                # blended_score / pit_grade alone for V2 path)
                conn.execute("""
                    INSERT INTO insider_ticker_scores
                        (insider_id, ticker, as_of_date,
                         career_blended_score, career_grade)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (insider_id, ticker, as_of_date) DO UPDATE SET
                        career_blended_score = EXCLUDED.career_blended_score,
                        career_grade = EXCLUDED.career_grade
                """, (insider_id, ticker, filing_date, v3.blended_score, career_grade))
                conn.execute("""
                    UPDATE trades SET career_grade = ?
                    WHERE trade_id = ?
                """, (career_grade, trade_id))
                ok += 1
            except Exception as e:
                err += 1
                if err < 20:
                    print(f"  ERR {insider_id}/{ticker}/{filing_date}: {e}")

            if (i + 1) % args.commit_every == 0:
                conn.commit()

            now = time.time()
            if now - last_log > 10.0:
                pct = 100.0 * (i + 1) / n
                rate = (i + 1) / max(1, now - started)
                eta_min = (n - i - 1) / max(1.0, rate) / 60
                print(f"  [{i+1:>7,}/{n:>7,}] {pct:5.1f}% ok={ok:,} err={err}, "
                      f"rate={rate:.1f}/s eta={eta_min:.1f}min")
                last_log = now

        conn.commit()

    print(f"\nDone in {(time.time()-started)/60:.1f}min. ok={ok:,} err={err}")


if __name__ == "__main__":
    main()
