#!/usr/bin/env python3
"""Refresh strategies/insider_catalog/prices.db (SQLite) from PG prices.daily_prices.

compute_cw_indicators.py reads prices from this SQLite cache, not from PG —
a holdover from the pre-PG-migration architecture (2026-04-07). Without a
periodic sync, the cache goes stale and SMA / dip computations on fresh trades
return NULL, which silently kills the strategy runners' filter pass-rate.

This script: pulls the last `--days` (default 240) of daily prices from PG and
INSERT-OR-REPLACE-es them into the SQLite cache. Idempotent.

Run on Studio. Daily, after daily-prices completes.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras

SQLITE_PATH = Path(__file__).resolve().parent / "prices.db"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=240,
                   help="Days of history to sync (covers SMA200 lookback).")
    p.add_argument("--db-url", default=None,
                   help="PG DSN (default: dbname=form4 via local socket)")
    args = p.parse_args()

    since = (date.today() - timedelta(days=args.days)).isoformat()
    t0 = time.time()
    pg_dsn = args.db_url or "dbname=form4"
    pg = psycopg2.connect(pg_dsn, connect_timeout=10)
    pg.set_session(readonly=True)
    cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ticker, date::text AS date, open, high, low, close, volume
          FROM prices.daily_prices
         WHERE date::date >= %s
         ORDER BY ticker, date
    """, (since,))
    rows = cur.fetchall()
    pg.close()
    print(f"PG: {len(rows):,} rows since {since} in {time.time()-t0:.1f}s")

    # FAIL CLOSED: refuse to write 0 rows. The April 2026 outage taught us
    # that an empty PG fetch (network blip, schema drift, transient PG issue)
    # would silently land an empty cache and starve compute_cw_indicators
    # of price data — turning every fresh trade's SMA columns to NULL.
    # Far better to abort + alert + retry next cycle than poison the cache.
    MIN_EXPECTED_ROWS = 100   # ~1 day × 100 tickers; below this is anomalous
    if len(rows) < MIN_EXPECTED_ROWS:
        msg = (
            f"sync_prices_sqlite: PG returned only {len(rows):,} rows since {since} "
            f"(min expected {MIN_EXPECTED_ROWS:,}). Refusing to overwrite cache."
        )
        print(f"ERROR: {msg}", file=sys.stderr)
        with __import__("contextlib").suppress(Exception):
            sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
            from framework.alerts.log import alert
            alert.critical("sync_prices_sqlite", msg, rows_returned=len(rows))
        sys.exit(2)

    sl = sqlite3.connect(str(SQLITE_PATH))
    sl.execute("PRAGMA journal_mode=WAL")
    sl.execute("PRAGMA synchronous=NORMAL")

    batch = [
        (r["ticker"], r["date"],
         float(r["open"])  if r["open"]  is not None else None,
         float(r["high"])  if r["high"]  is not None else None,
         float(r["low"])   if r["low"]   is not None else None,
         float(r["close"]) if r["close"] is not None else None,
         int(r["volume"])  if r["volume"] is not None else None)
        for r in rows
    ]

    CHUNK = 50_000
    written = 0
    for i in range(0, len(batch), CHUNK):
        sl.executemany(
            "INSERT OR REPLACE INTO daily_prices (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch[i:i+CHUNK],
        )
        sl.commit()
        written += len(batch[i:i+CHUNK])
        if i % (CHUNK * 5) == 0 and i > 0:
            print(f"  wrote {written:,}/{len(batch):,}")

    print(f"  wrote {written:,}/{len(batch):,}")
    sl.close()
    print(f"Done in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
