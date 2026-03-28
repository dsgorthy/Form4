#!/usr/bin/env python3
"""
Targeted options pull for high-conviction portfolio trades missing options data.

Pulls options chains from ThetaData for specific ticker/date combos where
our portfolio simulator needs options data for the hybrid model.

Usage:
    python3 pipelines/insider_study/options_pull_targeted.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from theta_client import ThetaClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"


def get_target_events() -> list[dict]:
    """Get high-conviction portfolio trades that need options data."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")

    # Trades that are Q8+ or rare reversal, have stock price > $5, and DON'T have
    # matching options on the entry date
    rows = conn.execute("""
        SELECT sp.ticker, sp.entry_date, sp.exit_date, sp.entry_price, sp.signal_quality,
               t.is_rare_reversal
        FROM strategy_portfolio sp
        JOIN trades t ON sp.trade_id = t.trade_id
        WHERE sp.strategy = 'form4_insider' AND sp.status = 'closed'
          AND (sp.signal_quality >= 8 OR t.is_rare_reversal = 1)
          AND sp.entry_price > 5
          AND sp.ticker NOT IN (
              SELECT DISTINCT op.ticker FROM prices.option_prices op
              WHERE op.trade_date = sp.entry_date AND op.right = 'C'
                AND op.strike BETWEEN sp.entry_price * 0.80 AND sp.entry_price * 1.20
          )
        ORDER BY sp.signal_quality DESC, sp.entry_date
    """).fetchall()

    events = [dict(r) for r in rows]
    conn.close()
    return events


def get_prices_conn():
    conn = sqlite3.connect(str(PRICES_DB))
    conn.execute("PRAGMA journal_mode=wal")
    return conn


async def pull_options_for_event(
    client: ThetaClient,
    prices_conn: sqlite3.Connection,
    ticker: str,
    entry_date: str,
    exit_date: str,
    stock_price: float,
) -> int:
    """Pull call options chain for a single event. Returns count of rows inserted."""
    entry_d = datetime.strptime(entry_date, "%Y-%m-%d").date()
    exit_d = datetime.strptime(exit_date, "%Y-%m-%d").date() if exit_date else entry_d + timedelta(days=30)

    # Get available expirations
    expirations = await client.get_expirations(ticker)
    if not expirations:
        logger.warning(f"  {ticker}: no expirations found")
        return 0

    # Filter to expirations 7-120 days from entry
    target_exps = [
        e for e in expirations
        if timedelta(days=7) <= (e - entry_d) <= timedelta(days=120)
    ]

    if not target_exps:
        logger.warning(f"  {ticker} {entry_date}: no expirations in 7-120d range")
        return 0

    # For each expiration, get strikes near the money
    inserted = 0
    for exp in target_exps[:5]:  # Max 5 expirations per event
        strikes = await client.get_strikes(ticker, exp)
        if not strikes:
            continue

        # Filter strikes to 80%-120% of stock price
        near_strikes = [s for s in strikes if stock_price * 0.80 <= s <= stock_price * 1.20]
        if not near_strikes:
            continue

        # Pull EOD for calls at each strike
        # Date range: entry_date to min(exit_date, expiration)
        end_d = min(exit_d, exp)
        start_d = entry_d - timedelta(days=1)  # day before entry for reference

        for strike in near_strikes[:10]:  # Max 10 strikes per expiration
            for right in ["C"]:  # Calls only for buy signals
                rows = await client.get_option_eod(
                    ticker, exp, strike, right, start_d, end_d
                )
                if not rows:
                    continue

                for r in rows:
                    try:
                        trade_date = r.get("date", "").strip().strip('"')
                        if not trade_date:
                            continue
                        # Normalize date format
                        if len(trade_date) == 10:
                            pass  # already YYYY-MM-DD
                        else:
                            continue

                        close_val = float(r.get("close", 0) or 0)
                        if close_val <= 0:
                            continue

                        prices_conn.execute("""
                            INSERT OR IGNORE INTO option_prices
                            (ticker, expiration, strike, right, trade_date,
                             open, high, low, close, volume, bid, ask, source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'thetadata')
                        """, (
                            ticker,
                            exp.strftime("%Y-%m-%d"),
                            strike,
                            right,
                            trade_date,
                            float(r.get("open", 0) or 0),
                            float(r.get("high", 0) or 0),
                            float(r.get("low", 0) or 0),
                            close_val,
                            int(float(r.get("volume", 0) or 0)),
                            float(r.get("bid", 0) or 0),
                            float(r.get("ask", 0) or 0),
                        ))
                        inserted += 1
                    except (ValueError, KeyError) as e:
                        continue

        prices_conn.commit()

    return inserted


async def main():
    events = get_target_events()
    logger.info(f"Found {len(events)} high-conviction trades needing options data")

    if not events:
        logger.info("Nothing to pull")
        return

    prices_conn = get_prices_conn()

    async with ThetaClient(max_concurrent=6) as client:
        total_inserted = 0

        for i, evt in enumerate(events):
            ticker = evt["ticker"]
            entry = evt["entry_date"]
            exit_d = evt["exit_date"]
            price = evt["entry_price"]
            quality = evt["signal_quality"]

            logger.info(
                f"[{i+1}/{len(events)}] {ticker} entry={entry} price=${price:.2f} Q={quality}"
            )

            try:
                n = await pull_options_for_event(
                    client, prices_conn, ticker, entry, exit_d, price
                )
                total_inserted += n
                logger.info(f"  → {n} option rows inserted")
            except Exception as e:
                logger.error(f"  → Error: {e}")

        logger.info(
            f"\nDone: {total_inserted} total rows inserted. "
            f"Requests: {client.requests_made} made, {client.requests_cached} cached, "
            f"{client.requests_failed} failed"
        )

    prices_conn.close()


if __name__ == "__main__":
    asyncio.run(main())
