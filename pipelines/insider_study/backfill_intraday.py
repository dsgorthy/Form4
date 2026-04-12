#!/usr/bin/env python3
"""
Backfill intraday stock bars from Alpaca into prices.db.

Pulls 5-minute bars for tickers with recent insider activity.
Stores in a new `intraday_bars` table in prices.db.

Strategy: pull 30-day trailing window for tickers with insider buys
in the last 90 days. Runs incrementally — skips tickers already pulled
for the current date range.

Usage:
    python3 pipelines/insider_study/backfill_intraday.py
    python3 pipelines/insider_study/backfill_intraday.py --days 60
    python3 pipelines/insider_study/backfill_intraday.py --ticker SPY --days 365
    python3 pipelines/insider_study/backfill_intraday.py --all-insider-tickers --days 30
"""

from __future__ import annotations

import argparse
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
PRICES_DB = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "prices.db"

ALPACA_BASE = "https://data.alpaca.markets/v2"
ALPACA_KEY = os.getenv("ALPACA_DATA_API_KEY", "")
ALPACA_SECRET = os.getenv("ALPACA_DATA_API_SECRET", "")

# Rate limiting: 200 req/min
MIN_INTERVAL = 0.31  # seconds between requests
_last_request = 0.0


def _throttle():
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_request = time.time()


def fetch_bars(ticker: str, start: str, end: str, timeframe: str = "5Min") -> list[dict]:
    """Fetch bars from Alpaca with pagination."""
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("ALPACA_DATA_API_KEY/SECRET required (set in .env) — shared read-only data credentials")

    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    all_bars = []
    page_token = None

    while True:
        _throttle()
        params = {
            "start": start,
            "end": end,
            "timeframe": timeframe,
            "feed": "sip",
            "adjustment": "raw",
            "limit": 10000,
        }
        if page_token:
            params["page_token"] = page_token

        try:
            resp = requests.get(
                f"{ALPACA_BASE}/stocks/{ticker}/bars",
                headers=headers,
                params=params,
                timeout=30,
            )
            if resp.status_code == 429:
                logger.warning("Rate limited, sleeping 10s")
                time.sleep(10)
                continue
            if resp.status_code != 200:
                logger.warning("Alpaca %d for %s: %s", resp.status_code, ticker, resp.text[:100])
                break

            data = resp.json()
            bars = data.get("bars", [])
            if not bars:
                break

            for b in bars:
                all_bars.append({
                    "ticker": ticker,
                    "timestamp": b["t"][:19],  # truncate to second
                    "open": b["o"],
                    "high": b["h"],
                    "low": b["l"],
                    "close": b["c"],
                    "volume": b["v"],
                    "vwap": b.get("vw"),
                    "trade_count": b.get("n"),
                })

            page_token = data.get("next_page_token")
            if not page_token:
                break

        except Exception as e:
            logger.error("Error fetching %s: %s", ticker, e)
            break

    return all_bars


def ensure_table(conn: sqlite3.Connection):
    """Create intraday_bars table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intraday_bars (
            ticker TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            timeframe TEXT NOT NULL DEFAULT '5Min',
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            vwap REAL,
            trade_count INTEGER,
            PRIMARY KEY (ticker, timestamp, timeframe)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_intraday_ticker_ts
        ON intraday_bars (ticker, timestamp)
    """)
    conn.commit()


def get_insider_tickers(lookback_days: int = 90) -> list[str]:
    """Get tickers with recent insider buy activity."""
    conn = sqlite3.connect(str(DB_PATH))
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT DISTINCT ticker FROM trades
        WHERE trans_code = 'P' AND filing_date >= ?
        ORDER BY ticker
    """, (cutoff,)).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_already_pulled(conn: sqlite3.Connection, ticker: str, start: str) -> bool:
    """Check if we already have recent data for this ticker."""
    r = conn.execute(
        "SELECT COUNT(*) FROM intraday_bars WHERE ticker = ? AND timestamp >= ?",
        (ticker, start),
    ).fetchone()
    return r[0] > 0


def main():
    parser = argparse.ArgumentParser(description="Backfill intraday bars from Alpaca")
    parser.add_argument("--days", type=int, default=30, help="Days of history to pull (default 30)")
    parser.add_argument("--ticker", help="Pull a single ticker")
    parser.add_argument("--all-insider-tickers", action="store_true", help="Pull all tickers with recent insider buys")
    parser.add_argument("--timeframe", default="5Min", help="Bar timeframe (default 5Min)")
    parser.add_argument("--force", action="store_true", help="Re-pull even if data exists")
    args = parser.parse_args()

    # Load env if not already set
    _key = os.getenv("ALPACA_DATA_API_KEY", "") or ALPACA_KEY
    _secret = os.getenv("ALPACA_DATA_API_SECRET", "") or ALPACA_SECRET
    if not _key:
        try:
            from dotenv import load_dotenv
            load_dotenv(Path(__file__).resolve().parents[2] / ".env")
            _key = os.getenv("ALPACA_DATA_API_KEY", "")
            _secret = os.getenv("ALPACA_DATA_API_SECRET", "")
        except ImportError:
            pass

    conn = sqlite3.connect(str(PRICES_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_table(conn)

    end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%dT00:00:00Z")
    start_check = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    if args.ticker:
        tickers = [args.ticker]
    elif args.all_insider_tickers:
        tickers = get_insider_tickers(lookback_days=90)
    else:
        # Default: top insider tickers from last 30 days
        tickers = get_insider_tickers(lookback_days=30)

    logger.info("Pulling %s bars for %d tickers (%d days)", args.timeframe, len(tickers), args.days)

    pulled = 0
    skipped = 0
    total_bars = 0

    for i, ticker in enumerate(tickers):
        if not args.force and get_already_pulled(conn, ticker, start_check):
            skipped += 1
            continue

        bars = fetch_bars(ticker, start_date, end_date, args.timeframe)
        if bars:
            conn.executemany("""
                INSERT OR IGNORE INTO intraday_bars
                (ticker, timestamp, timeframe, open, high, low, close, volume, vwap, trade_count)
                VALUES (:ticker, :timestamp, ?, :open, :high, :low, :close, :volume, :vwap, :trade_count)
            """.replace("?", f"'{args.timeframe}'"),
                bars,
            )
            conn.commit()
            total_bars += len(bars)
            pulled += 1

        if (i + 1) % 50 == 0:
            logger.info("  %d/%d tickers (%d pulled, %d skipped, %d bars)", i + 1, len(tickers), pulled, skipped, total_bars)

    logger.info("Done: %d tickers pulled, %d skipped, %d total bars stored", pulled, skipped, total_bars)
    conn.close()


if __name__ == "__main__":
    main()
