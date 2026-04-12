#!/usr/bin/env python3
"""Refresh prices.daily_prices directly from Alpaca.

Backfills gaps for tickers that have insider trades in the recent window. The
original daily_prices ingest path (CSV -> sync) stopped updating on 2026-03-13
because no process refreshes the CSV files. This script bypasses the CSV layer
and goes straight to Alpaca, upserting into prices.daily_prices.

Usage:
    python3 pipelines/insider_study/update_daily_prices.py                    # last 30d of trades
    python3 pipelines/insider_study/update_daily_prices.py --since 2026-01-01 # trades since date
    python3 pipelines/insider_study/update_daily_prices.py --tickers SPY,QQQ  # specific tickers
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection
from framework.data.alpaca_client import AlpacaClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_alpaca_credentials() -> tuple[str, str]:
    """Load shared read-only data credentials from .env. This script only fetches
    bars — never places orders — so the data credentials are correct."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    creds: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            creds[k.strip()] = v.strip().strip('"').strip("'")

    key = os.environ.get("ALPACA_DATA_API_KEY") or creds.get("ALPACA_DATA_API_KEY")
    secret = os.environ.get("ALPACA_DATA_API_SECRET") or creds.get("ALPACA_DATA_API_SECRET")
    if key and secret:
        return key, secret

    logger.error("ALPACA_DATA_API_KEY / ALPACA_DATA_API_SECRET not found in env or .env")
    sys.exit(1)


def get_tickers_to_update(conn, since: str, explicit: list[str] | None) -> list[str]:
    if explicit:
        return sorted({t.strip().upper() for t in explicit if t.strip()})
    rows = conn.execute(
        """
        SELECT DISTINCT ticker FROM trades
        WHERE filing_date >= ?
          AND trans_code IN ('P','S')
          AND ticker IS NOT NULL
          AND ticker != 'NONE'
        """,
        (since,),
    ).fetchall()
    tickers = [r["ticker"] for r in rows if r["ticker"]]
    return sorted(set(tickers))


def get_latest_dates(conn, tickers: list[str]) -> dict[str, str]:
    if not tickers:
        return {}
    placeholders = ",".join(["?"] * len(tickers))
    rows = conn.execute(
        f"SELECT ticker, MAX(date) AS max_date FROM daily_prices "
        f"WHERE ticker IN ({placeholders}) GROUP BY ticker",
        tuple(tickers),
    ).fetchall()
    return {r["ticker"]: r["max_date"] for r in rows}


def upsert_bars(conn, ticker: str, bars) -> int:
    if bars is None or bars.empty:
        return 0
    rows_to_insert = []
    for ts, row in bars.iterrows():
        d = ts.date().isoformat() if hasattr(ts, "date") else str(ts)[:10]
        rows_to_insert.append((
            ticker, d,
            float(row.get("open") or 0),
            float(row.get("high") or 0),
            float(row.get("low") or 0),
            float(row.get("close") or 0),
            int(row.get("volume") or 0),
        ))
    if not rows_to_insert:
        return 0
    conn.executemany(
        "INSERT OR IGNORE INTO daily_prices (ticker, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows_to_insert,
    )
    conn.commit()
    return len(rows_to_insert)


def main():
    parser = argparse.ArgumentParser(description="Update daily_prices from Alpaca")
    parser.add_argument("--since", default=None, help="Only update tickers with trades since this date (default: 30 days ago)")
    parser.add_argument("--tickers", default=None, help="Comma-separated tickers to force-update (overrides --since)")
    parser.add_argument("--max-tickers", type=int, default=2000, help="Cap number of tickers per run")
    parser.add_argument("--lookback-days", type=int, default=60, help="Days of bars to fetch for tickers with no existing data")
    args = parser.parse_args()

    since = args.since or (date.today() - timedelta(days=30)).isoformat()
    end = date.today().isoformat()

    explicit_tickers = args.tickers.split(",") if args.tickers else None
    conn = get_connection()

    tickers = get_tickers_to_update(conn, since, explicit_tickers)
    if len(tickers) > args.max_tickers:
        logger.warning("Capping %d tickers → %d", len(tickers), args.max_tickers)
        tickers = tickers[: args.max_tickers]

    logger.info("Updating %d tickers (since=%s end=%s)", len(tickers), since, end)

    latest = get_latest_dates(conn, tickers)

    key, secret = load_alpaca_credentials()
    client = AlpacaClient(api_key=key, api_secret=secret)

    total_rows = 0
    updated = 0
    skipped_current = 0
    failed: list[str] = []

    for i, ticker in enumerate(tickers, 1):
        max_date = latest.get(ticker)
        if max_date:
            # Start from the day after last stored date
            start_dt = (datetime.fromisoformat(max_date) + timedelta(days=1)).date()
        else:
            start_dt = date.today() - timedelta(days=args.lookback_days)

        if start_dt.isoformat() > end:
            skipped_current += 1
            continue

        try:
            df = client.get_daily_bars(ticker, start_dt.isoformat(), end, adjustment="split")
            n = upsert_bars(conn, ticker, df)
            total_rows += n
            if n > 0:
                updated += 1
        except Exception as exc:
            logger.warning("Failed %s: %s", ticker, exc)
            failed.append(ticker)

        if i % 50 == 0:
            logger.info("  progress: %d/%d (rows=%d, updated=%d, failed=%d)", i, len(tickers), total_rows, updated, len(failed))

    logger.info("Done. Inserted %d rows across %d tickers (skipped %d current, failed %d)",
                total_rows, updated, skipped_current, len(failed))
    if failed[:10]:
        logger.info("First failures: %s", ", ".join(failed[:10]))

    max_date_row = conn.execute("SELECT MAX(date) AS d FROM daily_prices").fetchone()
    logger.info("daily_prices MAX(date) after update: %s", max_date_row["d"] if max_date_row else "unknown")
    conn.close()


if __name__ == "__main__":
    main()
