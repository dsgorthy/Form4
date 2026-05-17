#!/usr/bin/env python3
"""Compute trades.week52_proximity for P/S trades — PostgreSQL recurring writer.

Replaces the pre-2026-04 SQLite implementation that read from the abandoned
`insiders.db` + `prices.db`. After the PG migration the script was orphaned;
column was 43.8% populated then frozen, and active product consumers
(`api/trade_grade.py`, `breaking-signal`, `daily-content`) started silently
returning NULL for any trade filed after 2026-04-09.

proximity = (trade_price - 52w_low) / (52w_high - 52w_low)
  0.0 = at 52-week low, 1.0 = at 52-week high.

Uses `prices.daily_prices` (schema-qualified PG table) for the rolling
252-day window per ticker.

Idempotent / incremental: by default fills only `WHERE week52_proximity IS
NULL`. Pass `--rebuild` to overwrite within the `--since` window.

Usage:
    python3 pipelines/insider_study/compute_week52_proximity.py --since 2026-04-01
    python3 pipelines/insider_study/compute_week52_proximity.py --since 2026-04-01 --rebuild
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.database import get_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000
WINDOW = 252            # trading days in a year
MIN_DAYS = 100          # require ≥100 trading days in the window for stability
MAX_CALENDAR_SPAN = 400 # halt-aware: skip if window spans > 400 calendar days


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", required=True,
                        help="Only UPDATE trades with filing_date >= this date (YYYY-MM-DD)")
    parser.add_argument("--rebuild", action="store_true",
                        help="Overwrite existing week52_proximity in the window (default: NULL-only)")
    args = parser.parse_args()

    conn = get_connection()
    t_start = time.time()

    # Source: trades to update. We need full ticker history of prices but only
    # update rows in --since (cheap daily cron).
    where_clauses = ["trans_code IN ('P','S')", "price > 0", "filing_date >= ?"]
    params: list = [args.since]
    if not args.rebuild:
        where_clauses.append("week52_proximity IS NULL")
    where_sql = " AND ".join(where_clauses)

    target_total = conn.execute(
        f"SELECT COUNT(*) FROM trades WHERE {where_sql}", tuple(params)
    ).fetchone()[0]
    logger.info("Trades to update: %d (since=%s, rebuild=%s)",
                target_total, args.since, args.rebuild)
    if target_total == 0:
        logger.info("Nothing to do.")
        return 0

    # Load target trades, grouped by ticker.
    target_rows = conn.execute(
        f"""SELECT trade_id, ticker, trade_date, price
              FROM trades WHERE {where_sql}
          ORDER BY ticker, trade_date""",
        tuple(params),
    ).fetchall()
    targets_by_ticker: dict[str, list[tuple]] = defaultdict(list)
    for r in target_rows:
        targets_by_ticker[r["ticker"]].append((r["trade_id"], r["trade_date"], r["price"]))

    tickers = sorted(targets_by_ticker.keys())
    logger.info("Tickers to process: %d", len(tickers))

    updates: list[tuple] = []
    tickers_no_prices = 0
    trades_matched = 0
    trades_no_date = 0
    trades_no_range = 0

    # Pull each ticker's daily price history once. Pull the FULL series (no
    # date cap) — we need 252 days BEFORE the earliest target trade_date.
    for idx, ticker in enumerate(tickers):
        prices = conn.execute(
            """SELECT date::text AS date, high, low
                  FROM prices.daily_prices
                 WHERE ticker = ?
              ORDER BY date""",
            (ticker,),
        ).fetchall()
        if not prices:
            tickers_no_prices += 1
            continue

        dates = [p["date"] for p in prices]
        highs = [p["high"] for p in prices]
        lows = [p["low"] for p in prices]

        # Per-trade computation. We compute the 52w window on demand for the
        # target trade_date (not for every price row) — much faster than the
        # legacy implementation when the ticker has years of prices but few
        # in-window trades.
        for trade_id, trade_date, price in targets_by_ticker[ticker]:
            # Binary search for the latest date <= trade_date
            lo, hi = 0, len(dates) - 1
            best = -1
            while lo <= hi:
                mid = (lo + hi) // 2
                if dates[mid] <= trade_date:
                    best = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            if best < 0:
                trades_no_date += 1
                continue
            # Snap-back tolerance: if exact trade_date isn't in the price
            # series (weekend / halted), use the nearest prior date within 5d.
            try:
                td = datetime.strptime(trade_date, "%Y-%m-%d")
                pd = datetime.strptime(dates[best], "%Y-%m-%d")
                if (td - pd).days > 5:
                    trades_no_date += 1
                    continue
            except (ValueError, TypeError):
                pass

            start = max(0, best - WINDOW + 1)
            window_size = best - start + 1
            if window_size < MIN_DAYS:
                trades_no_date += 1
                continue
            try:
                d_start = datetime.strptime(dates[start], "%Y-%m-%d")
                d_end = datetime.strptime(dates[best], "%Y-%m-%d")
                if (d_end - d_start).days > MAX_CALENDAR_SPAN:
                    trades_no_date += 1
                    continue
            except (ValueError, TypeError):
                pass

            w_high = max(highs[start:best + 1])
            w_low = min(lows[start:best + 1])
            if w_high <= w_low:
                trades_no_range += 1
                continue
            proximity = (price - w_low) / (w_high - w_low)
            proximity = max(0.0, min(1.0, proximity))
            updates.append((proximity, trade_id))
            trades_matched += 1

        if len(updates) >= BATCH_SIZE * 4:
            _flush(conn, updates)
            updates = []
        if (idx + 1) % 500 == 0:
            elapsed = time.time() - t_start
            logger.info("processed %d/%d tickers, %d matched (%.1fs)",
                        idx + 1, len(tickers), trades_matched, elapsed)

    _flush(conn, updates)

    elapsed = time.time() - t_start
    logger.info("Done in %.1fs: matched=%d, no_price=%d ticker(s), no_date=%d, no_range=%d",
                elapsed, trades_matched, tickers_no_prices, trades_no_date, trades_no_range)

    try:
        from framework.contracts.freshness_writer import write_freshness
        write_freshness(
            conn, table="trades", column="week52_proximity",
            n_rows_affected=trades_matched,
            populated_by="pipelines/insider_study/compute_week52_proximity.py",
        )
        conn.commit()
    except Exception as exc:
        logger.warning("freshness write failed: %s", exc)

    conn.close()
    return 0


def _flush(conn, updates: list[tuple]) -> None:
    if not updates:
        return
    conn.executemany(
        "UPDATE trades SET week52_proximity = ? WHERE trade_id = ?",
        updates,
    )
    conn.commit()


if __name__ == "__main__":
    sys.exit(main())
