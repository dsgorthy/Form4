#!/usr/bin/env python3
"""
Backfill 5-minute intraday bars from Alpaca into prices.db.

Memory-safe: writes each API page directly to DB, never accumulates.
Streams 30-day chunks, commits after each chunk, checkpoints every 50 tickers.

Usage:
    python3 pipelines/insider_study/backfill_intraday_events.py --cw-only
    python3 pipelines/insider_study/backfill_intraday_events.py --ticker AAPL
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
# Write intraday bars to a SEPARATE DB to avoid corruption from concurrent access
# prices.db is read by the API containers — writing to it while they read causes corruption
INTRADAY_DB = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "intraday.db"
# prices.db only used if we need to check existing data (read-only)
PRICES_DB = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "prices.db"

ALPACA_BASE = "https://data.alpaca.markets/v2"
MIN_INTERVAL = 0.31
_last_request = 0.0


def _throttle():
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_request = time.time()


def fetch_and_write_bars(conn, ticker, start, end, api_key, api_secret, timeframe="5Min"):
    """Fetch bars from Alpaca and write directly to DB per page. Never accumulates."""
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
    page_token = None
    total_written = 0

    while True:
        _throttle()
        params = {
            "start": start, "end": end, "timeframe": timeframe,
            "feed": "sip", "adjustment": "split", "limit": 10000,
        }
        if page_token:
            params["page_token"] = page_token
        try:
            resp = requests.get(f"{ALPACA_BASE}/stocks/{ticker}/bars",
                                headers=headers, params=params, timeout=30)
            if resp.status_code == 429:
                time.sleep(10)
                continue
            if resp.status_code != 200:
                break
            data = resp.json()
            bars = data.get("bars") or []
            if not bars:
                break

            # Write this page immediately — never hold more than 10K bars
            rows = [(ticker, b["t"][:19], timeframe, b["o"], b["h"], b["l"], b["c"],
                      b["v"], b.get("vw"), b.get("n")) for b in bars]
            conn.executemany("""
                INSERT OR IGNORE INTO intraday_bars
                (ticker, timestamp, timeframe, open, high, low, close, volume, vwap, trade_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            total_written += len(rows)
            del rows, bars, data  # explicit free

            page_token = resp.json().get("next_page_token") if resp.status_code == 200 else None
            if not page_token:
                break
        except Exception as e:
            logger.warning("Error %s: %s", ticker, e)
            break

    return total_written


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intraday_bars (
            ticker TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            timeframe TEXT NOT NULL DEFAULT '5Min',
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER, vwap REAL, trade_count INTEGER,
            PRIMARY KEY (ticker, timestamp, timeframe)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intraday_ticker_ts ON intraday_bars (ticker, timestamp)")
    conn.commit()


def get_ticker_date_ranges(cw_only=False):
    conn = sqlite3.connect(str(DB_PATH))
    cw_filter = ""
    if cw_only:
        cw_filter = """AND (t.is_rare_reversal = 1
            OR (t.dip_1mo <= -0.15 OR t.dip_3mo <= -0.25)
            OR (t.above_sma50 = 1 AND t.above_sma200 = 1 AND t.is_largest_ever = 1))
            AND COALESCE(t.is_recurring, 0) = 0
            AND COALESCE(t.is_tax_sale, 0) = 0"""
    rows = conn.execute(f"""
        SELECT ticker, MIN(filing_date) as first_filing, MAX(filing_date) as last_filing
        FROM trades t
        WHERE t.trans_code = 'P' AND t.filing_date >= '2020-01-01'
          AND COALESCE(t.cohen_routine, 0) = 0
          {cw_filter}
        GROUP BY ticker ORDER BY ticker
    """).fetchall()
    conn.close()
    ranges = []
    for ticker, first, last in rows:
        start = datetime.strptime(first, "%Y-%m-%d")
        end = datetime.strptime(last, "%Y-%m-%d") + timedelta(days=95)
        ranges.append((ticker, start, end))
    return ranges


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cw-only", action="store_true")
    parser.add_argument("--ticker")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    api_key = os.getenv("ALPACA_API_KEY", "")
    api_secret = os.getenv("ALPACA_API_SECRET", "")
    if not api_key:
        logger.error("ALPACA_API_KEY required")
        return

    conn = sqlite3.connect(str(INTRADAY_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-50000")  # 50MB cache only
    ensure_table(conn)

    if args.ticker:
        ranges = [(args.ticker, datetime(2020, 1, 1), datetime.now())]
    else:
        ranges = get_ticker_date_ranges(cw_only=args.cw_only)

    logger.info("Pulling 5-min bars for %d tickers", len(ranges))

    pulled = 0
    skipped = 0
    total_bars = 0

    for i, (ticker, start_dt, end_dt) in enumerate(ranges):
        # Check if already covered
        if not args.force:
            r = conn.execute(
                "SELECT COUNT(*) FROM intraday_bars WHERE ticker = ? AND timeframe = '5Min'",
                (ticker,)).fetchone()
            if r[0] > 100:
                skipped += 1
                continue

        # Pull in 30-day chunks, write each page immediately
        chunk_start = start_dt
        ticker_bars = 0

        while chunk_start < end_dt:
            chunk_end = min(chunk_start + timedelta(days=30), end_dt)
            written = fetch_and_write_bars(
                conn, ticker,
                chunk_start.strftime("%Y-%m-%dT00:00:00Z"),
                chunk_end.strftime("%Y-%m-%dT23:59:59Z"),
                api_key, api_secret,
            )
            ticker_bars += written
            chunk_start = chunk_end

        # Commit after each ticker
        conn.commit()
        total_bars += ticker_bars
        if ticker_bars > 0:
            pulled += 1

        # WAL checkpoint and gc every 50 tickers to keep memory flat
        if (i + 1) % 50 == 0:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            gc.collect()
            logger.info("  %d/%d tickers (%d pulled, %d skipped, %d bars)",
                        i + 1, len(ranges), pulled, skipped, total_bars)

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.commit()
    conn.close()
    logger.info("Done: %d pulled, %d skipped, %d bars total", pulled, skipped, total_bars)


if __name__ == "__main__":
    main()
