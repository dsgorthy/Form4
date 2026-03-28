#!/usr/bin/env python3
"""
Targeted long-DTE options pull for proven seller events.

Pulls EOD put option data for all expirations up to 180 DTE for proven seller
discretionary sell events. Designed to fill the data gap for testing 30-90d hold
periods with longer-dated puts.

Uses the existing options_pull.py infrastructure (theta_client, OptionPriceWriter,
checkpointing).

Usage:
    python options_pull_longdte.py --test              # 10 events
    python options_pull_longdte.py --full               # all proven seller events
    python options_pull_longdte.py --full --min-acc 0.65 # stricter filter
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from theta_client import ThetaClient, get_eod_date, find_nearest_expiration, find_nearest_strike, add_trading_days
from options_pull import OptionPriceWriter, load_events_from_db

INSIDERS_DB = SCRIPT_DIR.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = INSIDERS_DB.parent / "prices.db"  # daily_prices, option_prices
THETA_CACHE = SCRIPT_DIR / "data" / "theta_cache.db"

# Pull config
MAX_DTE = 180
STRIKE_RATIOS = {
    "5pct_itm": 1.05,
    "atm": 1.00,
    "5pct_otm": 0.95,
}
# Hold periods we want to test
HOLD_DAYS_LIST = [30, 60, 90]
# For each hold period, we need daily option prices from entry through exit
# Plus we need the entry-day and exit-day prices for P&L calculation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_proven_seller_events(min_acc: float = 0.60) -> list[dict]:
    """Get discretionary sell events from proven sellers."""
    conn = sqlite3.connect(str(INSIDERS_DB))
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT DISTINCT t.ticker, t.filing_date, t.trade_date, t.value,
               MIN(t.trade_id) AS trade_id
        FROM trades t
        JOIN insider_track_records itr ON t.insider_id = itr.insider_id
        WHERE t.trans_code = 'S'
          AND (t.is_routine != 1 OR t.is_routine IS NULL)
          AND (t.is_10b5_1 != 1 OR t.is_10b5_1 IS NULL)
          AND itr.sell_win_rate_7d >= ?
          AND t.trade_date >= '2016-01-01'
        GROUP BY t.filing_key
        ORDER BY t.filing_date
    """, (min_acc,)).fetchall()

    events = []
    for r in rows:
        events.append({
            "ticker": r["ticker"],
            "filing_date": r["filing_date"],
            "trade_date": r["trade_date"],
            "trade_id": r["trade_id"],
            "value": r["value"],
            "trade_type": "sell",
        })

    conn.close()
    return events


def get_already_pulled(ticker: str, trade_date: str) -> set[str]:
    """Check which expirations we already have option data for."""
    conn = sqlite3.connect(str(INSIDERS_DB))
    conn.row_factory = sqlite3.Row
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")

    rows = conn.execute("""
        SELECT DISTINCT expiration FROM option_prices
        WHERE ticker = ? AND right = 'P' AND trade_date = ?
    """, (ticker, trade_date)).fetchall()

    conn.close()
    return {r["expiration"] for r in rows}


async def pull_event_longdte(client: ThetaClient, writer: OptionPriceWriter,
                              event: dict, semaphore: asyncio.Semaphore):
    """Pull all put expirations up to MAX_DTE for a single event."""
    ticker = event["ticker"]
    filing_date = event["filing_date"]
    entry_date_str = event.get("trade_date") or filing_date

    try:
        entry_dt = datetime.strptime(entry_date_str[:10], "%Y-%m-%d").date()
    except ValueError:
        return 0

    # T+1 entry
    entry_date = add_trading_days(entry_dt, 1)

    # Get stock price at entry for strike calculation
    async with semaphore:
        try:
            stock_eod = await client.get_eod(ticker, entry_date)
        except Exception as e:
            logger.debug("No stock EOD for %s on %s: %s", ticker, entry_date, e)
            return 0

    if not stock_eod or stock_eod.get("close", 0) <= 0:
        return 0

    stock_price = stock_eod["close"]

    # Get available expirations
    async with semaphore:
        try:
            expirations = await client.get_expirations(ticker)
        except Exception:
            return 0

    if not expirations:
        return 0

    # Filter to expirations between 30 and MAX_DTE days out
    target_exps = []
    for exp_date in expirations:
        dte = (exp_date - entry_date).days
        if 30 <= dte <= MAX_DTE:
            target_exps.append(exp_date)

    if not target_exps:
        return 0

    # Check what we already have
    already = get_already_pulled(ticker, entry_date.isoformat())
    new_exps = [e for e in target_exps if e.isoformat() not in already]

    if not new_exps:
        return 0

    # For each new expiration, pull EOD data for entry date through max hold period
    contracts_pulled = 0
    max_exit = add_trading_days(entry_date, max(HOLD_DAYS_LIST))

    for exp_date in new_exps:
        # Get available strikes near our targets
        async with semaphore:
            try:
                strikes = await client.get_strikes(ticker, exp_date)
            except Exception:
                continue

        if not strikes:
            continue

        # Find nearest strikes to our targets
        target_strikes = set()
        for ratio_name, ratio in STRIKE_RATIOS.items():
            target = stock_price * ratio
            nearest = min(strikes, key=lambda s: abs(s - target))
            target_strikes.add(nearest)

        # Pull EOD for each strike from entry to max exit
        for strike in target_strikes:
            async with semaphore:
                try:
                    eod_data = await client.get_option_eod(
                        ticker, exp_date, strike, "P",
                        entry_date, max_exit,
                    )
                except Exception as e:
                    logger.debug("EOD failed %s %s %.2f P: %s", ticker, exp_date, strike, e)
                    continue

            if eod_data:
                # Inject expiration and strike into records for the writer
                for row in eod_data:
                    row["expiration"] = exp_date.isoformat()
                    row["strike"] = strike
                writer.write_eod_records(ticker, "P", eod_data)
                contracts_pulled += 1

    return contracts_pulled


async def main_async(events: list[dict], batch_size: int = 4):
    """Pull long-DTE options for all events."""
    client = ThetaClient()
    writer = OptionPriceWriter()
    semaphore = asyncio.Semaphore(8)

    total_contracts = 0
    t0 = time.time()

    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        tasks = [pull_event_longdte(client, writer, ev, semaphore) for ev in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, int):
                total_contracts += r

        elapsed = time.time() - t0
        rate = (i + len(batch)) / elapsed * 60 if elapsed > 0 else 0
        logger.info(
            "Progress: %d/%d events (%.1f%%), %d contracts, %.0f events/min",
            i + len(batch), len(events),
            100 * (i + len(batch)) / len(events),
            total_contracts, rate,
        )

    try:
        await client.close()
    except AttributeError:
        pass
    try:
        writer.close()
    except AttributeError:
        pass
    logger.info("Done. %d contracts pulled for %d events.", total_contracts, len(events))


def main():
    parser = argparse.ArgumentParser(description="Pull long-DTE options for proven sellers")
    parser.add_argument("--test", action="store_true", help="Test with 10 events")
    parser.add_argument("--full", action="store_true", help="Pull all events")
    parser.add_argument("--min-acc", type=float, default=0.60, help="Min sell accuracy (default: 0.60)")
    parser.add_argument("--batch-size", type=int, default=4, help="Concurrent events (default: 4)")
    args = parser.parse_args()

    if not args.test and not args.full:
        parser.error("Specify --test or --full")

    events = get_proven_seller_events(min_acc=args.min_acc)
    logger.info("Proven seller events (acc >= %.0f%%): %d", args.min_acc * 100, len(events))

    if args.test:
        # Use recent events for test (older ones may lack options data)
        recent = [e for e in events if e["filing_date"] >= "2020-01-01"]
        events = recent[:10] if recent else events[:10]
        logger.info("Test mode: using 10 events (from 2020+)")

    asyncio.run(main_async(events, batch_size=args.batch_size))


if __name__ == "__main__":
    main()
