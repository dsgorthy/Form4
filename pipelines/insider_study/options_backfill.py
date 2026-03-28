#!/usr/bin/env python3
"""
Options Data Backfill — Pull missing 2016-2025 options data from Theta Data.

Queries insiders.db directly for V3.3-qualifying events that are missing from
theta_cache.db, then pulls options data using the existing infrastructure.

Features:
  - Pulls both buy and sell events
  - Skips already-completed events (checkpoint-based resume)
  - Multi-event batching (--batch-size) for throughput
  - Telegram progress updates every N events
  - Prioritizes by year (oldest first to fill the biggest gap)

Usage:
    # Full backfill (both legs, all missing events)
    python options_backfill.py --batch-size 4

    # Sells only (put leg gap is larger)
    python options_backfill.py --sells-only --batch-size 4

    # Buys only
    python options_backfill.py --buys-only --batch-size 4

    # Test with 10 events
    python options_backfill.py --test
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

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from theta_client import ThetaClient, CacheDB
from options_pull import pull_event, run_pull

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(SCRIPT_DIR / "data" / "backfill.log"),
    ],
)
logger = logging.getLogger(__name__)

INSIDERS_DB = SCRIPT_DIR.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
THETA_CACHE_DB = SCRIPT_DIR / "data" / "theta_cache.db"

# Telegram config (same as V3.3 paper runner)
TG_BOT_TOKEN = "8738253569:AAGHNvkFuyVEaZzGlUesgP5AN9F4vT24mtA"
TG_CHAT_ID = "8585305446"


def send_telegram(text: str) -> bool:
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as e:
        logger.warning("Telegram send failed: %s", e)
        return False


def get_done_events(cache_db_path: Path) -> set[tuple[str, str, str]]:
    """Get all completed events from theta_cache.db."""
    conn = sqlite3.connect(str(cache_db_path))
    rows = conn.execute(
        "SELECT cache_key FROM cache WHERE cache_key LIKE 'event_done%'"
    ).fetchall()
    conn.close()

    done = set()
    for (key,) in rows:
        parts = key.split("|")
        if len(parts) >= 4:
            done.add((parts[1], parts[2], parts[3]))  # ticker, date, type
    return done


def get_entry_price(ticker: str, filing_date: str, insiders_conn) -> float:
    """Get entry price (T+1 open) for an event. Falls back to trade price."""
    # Try daily prices CSV first
    prices_dir = SCRIPT_DIR.parent.parent / "pipelines" / "insider_study" / "data" / "prices"
    price_file = prices_dir / f"{ticker}.csv"

    if price_file.exists():
        import csv
        target = datetime.strptime(filing_date, "%Y-%m-%d").date()
        best_date = None
        best_price = None
        with open(price_file) as f:
            for row in csv.DictReader(f):
                try:
                    d = datetime.strptime(row["timestamp"][:10], "%Y-%m-%d").date()
                    if d > target and (best_date is None or d < best_date):
                        best_date = d
                        best_price = float(row["open"])
                except (ValueError, KeyError):
                    continue
        if best_price:
            return best_price

    # Fallback: use avg trade price from insiders.db
    row = insiders_conn.execute(
        """SELECT AVG(t.price) FROM trades t
           WHERE t.ticker = ? AND t.filing_date = ? AND t.price > 0""",
        (ticker, filing_date),
    ).fetchone()
    return float(row[0]) if row and row[0] else 0.0


def load_missing_buy_events(
    insiders_conn, done: set, min_value: float = 2_000_000
) -> list[dict]:
    """Load buy events from insiders.db that are missing from theta_cache."""
    cursor = insiders_conn.cursor()

    # Get distinct (ticker, filing_date) with aggregate value >= min_value
    cursor.execute(
        """
        SELECT t.ticker, t.filing_date, SUM(t.value) as total_value, AVG(t.price) as avg_price
        FROM trades t
        WHERE t.trade_type = 'buy'
          AND t.filing_date >= '2016-01-01' AND t.filing_date < '2026-01-01'
          AND t.price > 0
        GROUP BY t.ticker, t.filing_date
        HAVING total_value >= ?
        ORDER BY t.filing_date
        """,
        (min_value,),
    )

    events = []
    for ticker, filing_date, total_value, avg_price in cursor.fetchall():
        if (ticker, filing_date, "buy") in done:
            continue
        if avg_price <= 0:
            continue

        events.append(
            {
                "ticker": ticker,
                "filing_date": filing_date,
                "total_value": total_value,
                "_ticker": ticker,
                "_entry_date": datetime.strptime(filing_date, "%Y-%m-%d").date()
                + timedelta(days=1),
                "_entry_price": avg_price,
                "_signal": "buy",
                "_right": "C",
            }
        )

    # Fix entry dates that land on weekends
    for e in events:
        while e["_entry_date"].weekday() >= 5:
            e["_entry_date"] += timedelta(days=1)

    return events


def load_missing_sell_events(
    insiders_conn, done: set, min_insiders: int = 2, min_value: float = 5_000_000
) -> list[dict]:
    """Load V3.3-qualifying sell cluster events missing from theta_cache."""
    cursor = insiders_conn.cursor()

    cursor.execute(
        """
        SELECT t.ticker, t.filing_date,
               COUNT(DISTINCT i.name_normalized) as n_sellers,
               SUM(t.value) as total_value,
               AVG(t.price) as avg_price
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trade_type = 'sell'
          AND t.filing_date >= '2016-01-01' AND t.filing_date < '2026-01-01'
          AND t.price > 0
        GROUP BY t.ticker, t.filing_date
        HAVING n_sellers >= ? AND total_value >= ?
        ORDER BY t.filing_date
        """,
        (min_insiders, min_value),
    )

    events = []
    for ticker, filing_date, n_sellers, total_value, avg_price in cursor.fetchall():
        if (ticker, filing_date, "sell") in done:
            continue
        if avg_price <= 0:
            continue

        events.append(
            {
                "ticker": ticker,
                "filing_date": filing_date,
                "n_sellers": n_sellers,
                "total_value": total_value,
                "_ticker": ticker,
                "_entry_date": datetime.strptime(filing_date, "%Y-%m-%d").date()
                + timedelta(days=1),
                "_entry_price": avg_price,
                "_signal": "sell",
                "_right": "P",
            }
        )

    for e in events:
        while e["_entry_date"].weekday() >= 5:
            e["_entry_date"] += timedelta(days=1)

    return events


async def run_backfill(events: list[dict], label: str, batch_size: int = 4,
                       telegram_interval: int = 500):
    """
    Run backfill pull with Telegram progress updates.

    Args:
        events: List of event dicts
        label: Label for logging
        batch_size: Events to process concurrently
        telegram_interval: Send Telegram update every N events
    """
    if not events:
        logger.info(f"No events to pull for {label}")
        send_telegram(f"*Backfill {label}*: No events to pull")
        return []

    cache_db = CacheDB(str(THETA_CACHE_DB))
    total = len(events)
    start_time = time.monotonic()

    send_telegram(
        f"*Options Backfill Started*\n"
        f"Label: `{label}`\n"
        f"Events: {total:,}\n"
        f"Batch size: {batch_size}\n"
        f"Estimated time: {total / 2 / 60:.0f}–{total / 0.5 / 60:.0f} min"
    )

    results = []
    events_done = 0
    total_contracts_ok = 0
    total_contracts_attempted = 0
    errors = 0

    async with ThetaClient(max_concurrent=8) as client:
        for batch_start in range(0, total, batch_size):
            batch = events[batch_start : batch_start + batch_size]

            try:
                batch_results = await asyncio.gather(
                    *[pull_event(client, ev) for ev in batch],
                    return_exceptions=True,
                )
            except Exception as e:
                logger.error("Batch failed: %s", e)
                errors += len(batch)
                continue

            for ev, summary in zip(batch, batch_results):
                if isinstance(summary, Exception):
                    logger.error("Event %s failed: %s", ev["_ticker"], summary)
                    errors += 1
                    events_done += 1
                    continue

                results.append(summary)
                ok = summary["contracts_with_data"]
                total_c = summary["contracts_attempted"]
                total_contracts_ok += ok
                total_contracts_attempted += total_c

                # Checkpoint
                event_key = f"{ev['_ticker']}|{ev['_entry_date']}|{ev['_signal']}"
                cache_db.put(
                    f"event_done|{event_key}",
                    {"status": "done", "ok": ok, "total": total_c},
                )

                events_done += 1

            # Telegram progress update
            if events_done > 0 and events_done % telegram_interval == 0:
                elapsed = time.monotonic() - start_time
                rate = events_done / elapsed if elapsed > 0 else 0
                remaining = total - events_done
                eta_min = (remaining / rate / 60) if rate > 0 else 0
                pct = events_done / total * 100

                send_telegram(
                    f"*Backfill Progress — {label}*\n"
                    f"Events: {events_done:,}/{total:,} ({pct:.0f}%)\n"
                    f"Contracts: {total_contracts_ok:,}/{total_contracts_attempted:,} with data\n"
                    f"Rate: {rate:.1f} events/sec ({rate * 60:.0f}/min)\n"
                    f"ETA: {eta_min:.0f} min ({eta_min/60:.1f} hr)\n"
                    f"Errors: {errors}"
                )

            # Also log to console every 100 events
            if events_done % 100 == 0:
                elapsed = time.monotonic() - start_time
                rate = events_done / elapsed if elapsed > 0 else 0
                remaining = total - events_done
                eta_min = (remaining / rate / 60) if rate > 0 else 0
                logger.info(
                    "PROGRESS [%s]: %d/%d (%.0f%%) — %.1f evt/s — ETA %.0f min — %d errors",
                    label, events_done, total, events_done / total * 100,
                    rate, eta_min, errors,
                )

    # Final summary
    elapsed = time.monotonic() - start_time
    rate = events_done / elapsed if elapsed > 0 else 0

    summary_msg = (
        f"*Backfill COMPLETE — {label}*\n"
        f"Events: {events_done:,}/{total:,}\n"
        f"Contracts: {total_contracts_ok:,}/{total_contracts_attempted:,} with data\n"
        f"Rate: {rate:.1f} events/sec\n"
        f"Time: {elapsed/3600:.1f} hours\n"
        f"Errors: {errors}"
    )
    send_telegram(summary_msg)
    logger.info(summary_msg.replace("*", ""))

    return results


async def main():
    parser = argparse.ArgumentParser(description="Options Data Backfill (2016-2025)")
    parser.add_argument("--test", action="store_true", help="Test with 10 events")
    parser.add_argument("--buys-only", action="store_true", help="Only pull buy events")
    parser.add_argument("--sells-only", action="store_true", help="Only pull sell events")
    parser.add_argument(
        "--batch-size", type=int, default=4,
        help="Events to process concurrently (default 4)",
    )
    parser.add_argument(
        "--telegram-interval", type=int, default=500,
        help="Send Telegram update every N events (default 500)",
    )
    parser.add_argument(
        "--year-start", type=int, default=2016,
        help="Start year (default 2016)",
    )
    parser.add_argument(
        "--year-end", type=int, default=2025,
        help="End year inclusive (default 2025)",
    )
    args = parser.parse_args()

    # Load completed events
    logger.info("Loading completed events from theta_cache.db...")
    done = get_done_events(THETA_CACHE_DB)
    logger.info("  %d events already completed", len(done))

    # Connect to insiders.db
    insiders_conn = sqlite3.connect(str(INSIDERS_DB))

    all_events = []

    if not args.sells_only:
        logger.info("Loading missing buy events ($2M+)...")
        buy_events = load_missing_buy_events(insiders_conn, done)
        # Filter to year range
        buy_events = [
            e for e in buy_events
            if args.year_start <= e["_entry_date"].year <= args.year_end
        ]
        logger.info("  %d missing buy events", len(buy_events))
        all_events.extend(buy_events)

    if not args.buys_only:
        logger.info("Loading missing sell events (2+ insiders, $5M+)...")
        sell_events = load_missing_sell_events(insiders_conn, done)
        sell_events = [
            e for e in sell_events
            if args.year_start <= e["_entry_date"].year <= args.year_end
        ]
        logger.info("  %d missing sell events", len(sell_events))
        all_events.extend(sell_events)

    insiders_conn.close()

    # Sort by date (oldest first)
    all_events.sort(key=lambda e: e["_entry_date"])

    if args.test:
        all_events = all_events[:10]
        logger.info("TEST MODE: pulling %d events", len(all_events))

    # Print year breakdown
    from collections import Counter
    year_counts = Counter()
    type_counts = Counter()
    for e in all_events:
        year_counts[e["_entry_date"].year] += 1
        type_counts[e["_signal"]] += 1

    logger.info("\nBackfill summary:")
    logger.info("  Total events: %d (buy=%d, sell=%d)",
                len(all_events), type_counts["buy"], type_counts["sell"])
    for y in sorted(year_counts):
        logger.info("  %d: %d events", y, year_counts[y])

    if not all_events:
        logger.info("Nothing to pull!")
        return

    label = "backfill"
    if args.buys_only:
        label = "backfill-buys"
    elif args.sells_only:
        label = "backfill-sells"

    await run_backfill(
        all_events,
        label=label,
        batch_size=args.batch_size,
        telegram_interval=args.telegram_interval,
    )


if __name__ == "__main__":
    asyncio.run(main())
