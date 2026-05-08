#!/usr/bin/env python3
"""V3 backfill: re-score every (insider_id, ticker, as_of_date) row from
insider_ticker_scores using BayesianScorerV3, write to insider_ticker_scores_v3.

Read-only against insider_ticker_scores; writes only to *_v3 scratch table.
Production untouched.

Usage (on Studio):
    python3 -m strategies.insider_catalog.backfill_v3 --mode latest    # ~10-15 min
    python3 -m strategies.insider_catalog.backfill_v3 --mode all       # ~1-2 hr
    python3 -m strategies.insider_catalog.backfill_v3 --mode all --resume

`latest`  — only the most-recent as_of_date per (insider, ticker). Sufficient
            for distribution comparison; doesn't enable historical backtest.
`all`     — every existing row. Required for backtest. Resumes from last
            written row when --resume is passed.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime

from config.database import get_connection
from strategies.insider_catalog.pit_scoring import (
    SCORER_V3,
    compute_insider_ticker_score,
)


def _fetch_targets(conn, mode: str, resume: bool) -> list[tuple[int, str, str]]:
    """Return list of (insider_id, ticker, as_of_date) tuples to backfill."""
    if mode == "latest":
        query = """
            SELECT DISTINCT ON (insider_id, ticker)
                   insider_id, ticker, as_of_date
            FROM insider_ticker_scores
            ORDER BY insider_id, ticker, as_of_date DESC
        """
        rows = conn.execute(query).fetchall()
    elif mode == "all":
        query = """
            SELECT insider_id, ticker, as_of_date
            FROM insider_ticker_scores
            ORDER BY insider_id, ticker, as_of_date
        """
        rows = conn.execute(query).fetchall()
    else:
        raise ValueError(f"Unknown mode: {mode}")

    targets = [(r["insider_id"], r["ticker"], r["as_of_date"]) for r in rows]

    if resume:
        existing = {
            (r["insider_id"], r["ticker"], r["as_of_date"])
            for r in conn.execute(
                "SELECT insider_id, ticker, as_of_date FROM insider_ticker_scores_v3"
            ).fetchall()
        }
        before = len(targets)
        targets = [t for t in targets if t not in existing]
        skipped = before - len(targets)
        print(f"Resume: {skipped} already-backfilled rows skipped, {len(targets)} remaining.")

    return targets


def _upsert_v3(conn, result):
    s = result
    conn.execute("""
        INSERT INTO insider_ticker_scores_v3 (
            insider_id, ticker, as_of_date,
            ticker_trade_count, ticker_win_rate_7d, ticker_avg_abnormal_7d, ticker_score,
            global_trade_count, global_win_rate_7d, global_avg_abnormal_7d, global_score,
            blended_score, role_at_ticker, role_weight, is_primary_company, sufficient_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (insider_id, ticker, as_of_date) DO UPDATE SET
            ticker_trade_count = EXCLUDED.ticker_trade_count,
            ticker_win_rate_7d = EXCLUDED.ticker_win_rate_7d,
            ticker_avg_abnormal_7d = EXCLUDED.ticker_avg_abnormal_7d,
            ticker_score = EXCLUDED.ticker_score,
            global_trade_count = EXCLUDED.global_trade_count,
            global_win_rate_7d = EXCLUDED.global_win_rate_7d,
            global_avg_abnormal_7d = EXCLUDED.global_avg_abnormal_7d,
            global_score = EXCLUDED.global_score,
            blended_score = EXCLUDED.blended_score,
            role_at_ticker = EXCLUDED.role_at_ticker,
            role_weight = EXCLUDED.role_weight,
            is_primary_company = EXCLUDED.is_primary_company,
            sufficient_data = EXCLUDED.sufficient_data
    """, (
        s.insider_id, s.ticker, s.as_of_date,
        s.ticker_trade_count, s.ticker_win_rate_7d, s.ticker_avg_abnormal_7d, s.ticker_score,
        s.global_trade_count, s.global_win_rate_7d, s.global_avg_abnormal_7d, s.global_score,
        s.blended_score, s.role_at_ticker, s.role_weight,
        s.is_primary_company, s.sufficient_data,
    ))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["latest", "all"], default="latest")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--commit-every", type=int, default=500)
    parser.add_argument("--limit", type=int, default=None, help="cap targets for testing")
    args = parser.parse_args()

    started = time.time()

    with get_connection() as conn:
        targets = _fetch_targets(conn, args.mode, args.resume)

    if args.limit:
        targets = targets[: args.limit]

    n = len(targets)
    print(f"V3 backfill: mode={args.mode}, resume={args.resume}, target_rows={n:,}")
    if n == 0:
        print("Nothing to do.")
        return

    with get_connection() as conn:
        ok = 0
        err = 0
        last_log = time.time()
        for i, (insider_id, ticker, as_of_date) in enumerate(targets):
            try:
                result = compute_insider_ticker_score(
                    conn, insider_id, ticker, as_of_date, scorer=SCORER_V3,
                )
                _upsert_v3(conn, result)
                ok += 1
            except Exception as e:
                err += 1
                if err < 20:
                    print(f"  ERR {insider_id}/{ticker}/{as_of_date}: {e}")

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

    elapsed = time.time() - started
    print(f"\nDone in {elapsed/60:.1f}min. ok={ok:,} err={err}")


if __name__ == "__main__":
    main()
