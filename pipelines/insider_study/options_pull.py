#!/usr/bin/env python3
"""
Phase 0c: Theta Data Options Pull — Dual-DTE Strategy
------------------------------------------------------
Pulls EOD option data for insider events using the locked pull strategy:
- 4 hold periods (7/14/30/60d) × 2 DTE types (tight/comfortable) = 8 hold-DTE pairs
- 4 strikes per pair (5% ITM, ATM, 5% OTM, 10% OTM)
- 32 EOD calls per event + expiration/strike lookups
- Calls for buy events, puts for sell events

Uses theta_client.py for async concurrent pulls (8 concurrent, Pro plan).
Results cached in SQLite with per-event checkpointing.
Multi-event concurrency (--batch-size) processes N events in parallel for higher throughput.

Usage:
    # Full pull from insiders.db (preferred — all events, deduplicated)
    python options_pull.py --full --from-db --batch-size 4

    # Pull a specific date range from DB
    python options_pull.py --full --from-db --start 2016-01-01 --end 2019-12-31 --buys-only

    # Test run from DB (10 events)
    python options_pull.py --test --from-db

    # Legacy: pull from CSV (cluster events only, default)
    python options_pull.py --full

    # Legacy: pull ALL events including individual from CSV
    python options_pull.py --full --all

    # Resume from checkpoint (automatic — completed events always skipped)
    python options_pull.py --full --from-db

Author: Claude Opus 4.6
Date: 2026-03-09 (updated 2026-03-11: --all flag, multi-event batching)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# Add parent paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_MD = os.path.join(SCRIPT_DIR, "PROGRESS.md")
sys.path.insert(0, SCRIPT_DIR)

import sqlite3 as _sqlite3

from theta_client import ThetaClient, CacheDB, find_nearest_expiration, find_nearest_strike

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuration — LOCKED 2026-03-09
# ─────────────────────────────────────────────

STRIKES = {
    "5pct_itm": 0.95,
    "atm": 1.00,
    "5pct_otm": 1.05,
    "10pct_otm": 1.10,
}

# Hold period → (tight DTE, comfortable DTE)
HOLD_DTE_MAP = {
    7:  (14, 21),
    14: (28, 45),
    30: (50, 60),
    60: (90, 120),
    90: (120, 150),
    180: (210, 240),
}

BUY_EVENTS_CSV = os.path.join(SCRIPT_DIR, "data", "results_bulk_7d.csv")  # deprecated — use --from-db
SELL_EVENTS_CSV = os.path.join(SCRIPT_DIR, "data", "results_sells_7d.csv")  # deprecated — use --from-db

INSIDERS_DB = os.path.join(
    os.path.dirname(SCRIPT_DIR),  # pipelines/
    os.pardir, "strategies", "insider_catalog", "insiders.db",
)
PRICES_DB = os.path.join(os.path.dirname(os.path.normpath(INSIDERS_DB)), "prices.db")


class OptionPriceWriter:
    """Writes structured option price data to insiders.db as events are pulled."""

    def __init__(self):
        db_path = os.path.normpath(INSIDERS_DB)
        self._conn = _sqlite3.connect(db_path, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        if os.path.exists(PRICES_DB):
            self._conn.execute(f"ATTACH DATABASE '{PRICES_DB}' AS prices")
        self._rows_written = 0

    def _execute_with_retry(self, sql, params=None, many=False, max_retries=5):
        """Execute SQL with retry on database lock."""
        for attempt in range(max_retries):
            try:
                if many and params:
                    self._conn.executemany(sql, params)
                elif params:
                    self._conn.execute(sql, params)
                else:
                    self._conn.execute(sql)
                return
            except _sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < max_retries - 1:
                    import time as _time
                    _time.sleep(1 + attempt)
                    continue
                raise

    def write_eod_records(self, ticker, right, eod_records):
        """Write raw EOD records from ThetaData into option_prices table."""
        if not eod_records:
            return
        batch = []
        right_code = "C" if right == "C" or right == "CALL" else "P"
        for r in eod_records:
            created = r.get("created", "")
            if not created:
                continue
            trade_date = created[:10]
            try:
                batch.append((
                    ticker,
                    r.get("expiration", ""),
                    float(r.get("strike", 0)),
                    right_code,
                    trade_date,
                    float(r["open"]) if r.get("open") else None,
                    float(r["high"]) if r.get("high") else None,
                    float(r["low"]) if r.get("low") else None,
                    float(r["close"]) if r.get("close") else None,
                    int(r["volume"]) if r.get("volume") else None,
                    float(r["bid"]) if r.get("bid") else None,
                    float(r["ask"]) if r.get("ask") else None,
                    int(r["bid_size"]) if r.get("bid_size") else None,
                    int(r["ask_size"]) if r.get("ask_size") else None,
                    "thetadata",
                ))
            except (ValueError, KeyError, TypeError):
                continue
        if batch:
            self._execute_with_retry(
                """INSERT OR IGNORE INTO option_prices
                   (ticker, expiration, strike, right, trade_date,
                    open, high, low, close, volume, bid, ask,
                    bid_size, ask_size, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                batch, many=True,
            )
            self._rows_written += len(batch)

    def write_pull_status(self, ticker, trade_date, trade_type, contracts_found, contracts_empty):
        """Record that an event's options have been pulled."""
        self._execute_with_retry(
            """INSERT OR REPLACE INTO option_pull_status
               (ticker, trade_date, trade_type, contracts_found, contracts_empty)
               VALUES (?, ?, ?, ?, ?)""",
            (ticker, str(trade_date), trade_type, contracts_found, contracts_empty),
        )
        self._conn.commit()

    def close(self):
        self._conn.close()
        logger.info(f"OptionPriceWriter: {self._rows_written:,} rows written to option_prices")

# ─────────────────────────────────────────────
# PROGRESS.md Auto-Update
# ─────────────────────────────────────────────

def update_progress_md(label: str, events_done: int, total_events: int,
                       contracts_ok: int, contracts_attempted: int,
                       contracts_skipped: int, rate_events: float,
                       eta_seconds: float, cache_entries: int = 0,
                       buy_checkpoints: int = 0, sell_checkpoints: int = 0,
                       completed: bool = False):
    """Update Phase 0c section of PROGRESS.md with current pull stats.

    Rewrites only the auto-updated stats block between markers, preserving
    the rest of the file. Safe to call from a running pull every progress_interval.
    """
    try:
        with open(PROGRESS_MD, "r") as f:
            content = f.read()
    except FileNotFoundError:
        logger.warning("PROGRESS.md not found, skipping auto-update")
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    pct = (events_done / total_events * 100) if total_events > 0 else 0
    eta_min = eta_seconds / 60 if eta_seconds > 0 else 0
    status = "COMPLETED" if completed else f"IN PROGRESS ({pct:.0f}%)"

    # Build the auto-update block
    block = (
        f"<!-- AUTO-UPDATE START -->\n"
        f"- **Auto-Updated**: {now_str}\n"
        f"- **Current Run**: `{label}` — {status}\n"
        f"- **Events**: {events_done:,}/{total_events:,} ({pct:.1f}%)\n"
        f"- **Contracts**: {contracts_ok:,}/{contracts_attempted:,} with data, {contracts_skipped:,} skipped/no-vol\n"
        f"- **Rate**: {rate_events:.2f} events/sec ({rate_events*60:.1f} events/min)\n"
        f"- **ETA**: {eta_min:.0f} min\n"
        f"- **Cache**: {cache_entries:,} total entries | {buy_checkpoints:,} buy + {sell_checkpoints:,} sell event_done\n"
        f"<!-- AUTO-UPDATE END -->"
    )

    # Replace existing block or insert after "## Phase 0c" status line
    start_marker = "<!-- AUTO-UPDATE START -->"
    end_marker = "<!-- AUTO-UPDATE END -->"

    if start_marker in content:
        # Replace existing block
        start_idx = content.index(start_marker)
        end_idx = content.index(end_marker) + len(end_marker)
        content = content[:start_idx] + block + content[end_idx:]
    else:
        # Insert after the first "- **Status**:" line under Phase 0c
        phase_0c_marker = "## Phase 0c: Theta Data Full Options Pull"
        if phase_0c_marker in content:
            marker_idx = content.index(phase_0c_marker)
            # Find the first status line after Phase 0c header
            status_line_start = content.index("- **Status**:", marker_idx)
            status_line_end = content.index("\n", status_line_start)
            content = content[:status_line_end + 1] + block + "\n" + content[status_line_end + 1:]
        else:
            logger.warning("Phase 0c section not found in PROGRESS.md, skipping auto-update")
            return

    # Also update the top-level "Last Updated" line
    import re
    content = re.sub(
        r"^## Last Updated:.*$",
        f"## Last Updated: {now_str} (auto)",
        content,
        count=1,
        flags=re.MULTILINE,
    )

    try:
        with open(PROGRESS_MD, "w") as f:
            f.write(content)
    except OSError as e:
        logger.warning(f"Failed to write PROGRESS.md: {e}")

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def add_trading_days(start: date, n_days: int) -> date:
    """Add n trading days (skip weekends)."""
    current = start
    added = 0
    while added < n_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def load_events_from_db(trade_type: str = "buy", start_date: str = None,
                        end_date: str = None) -> list[dict]:
    """Load events from insiders.db, deduplicated by (ticker, trade_date).

    Multiple insider trades on the same ticker+date become one event,
    using the volume-weighted average price.
    """
    import sqlite3
    db_path = os.path.normpath(INSIDERS_DB)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT ticker, trade_date,
               SUM(price * qty) / SUM(qty) as avg_price,
               SUM(value) as total_value,
               COUNT(*) as n_trades
        FROM trades
        WHERE trade_type = ?
          AND price > 0
          AND qty > 0
    """
    params = [trade_type]

    if start_date:
        query += " AND trade_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND trade_date <= ?"
        params.append(end_date)

    query += " GROUP BY ticker, trade_date ORDER BY trade_date"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    right = "C" if trade_type == "buy" else "P"
    events = []
    for r in rows:
        events.append({
            "ticker": r["ticker"],
            "entry_date": r["trade_date"],
            "entry_price": str(r["avg_price"]),
            "total_value": str(r["total_value"]),
            "is_cluster": "True" if r["n_trades"] > 1 else "False",
            "_ticker": r["ticker"],
            "_entry_date": datetime.strptime(r["trade_date"], "%Y-%m-%d").date(),
            "_entry_price": r["avg_price"],
            "_signal": trade_type,
            "_right": right,
        })

    logger.info(f"Loaded {len(events)} {trade_type} events from insiders.db"
                f" ({start_date or 'all'} to {end_date or 'all'})")
    return events


def load_buy_events(cluster_only: bool = True, csv_path: str = None) -> list[dict]:
    """Load buy events from CSV (legacy). Use load_events_from_db instead."""
    src = csv_path or BUY_EVENTS_CSV
    with open(src) as f:
        events = list(csv.DictReader(f))
    if cluster_only:
        events = [e for e in events if e["is_cluster"] == "True"]
    for e in events:
        e["_ticker"] = e["ticker"]
        e["_entry_date"] = datetime.strptime(e["entry_date"], "%Y-%m-%d").date()
        e["_entry_price"] = float(e["entry_price"])
        e["_signal"] = "buy"
        e["_right"] = "C"
    return events


def load_sell_events(cluster_only: bool = True) -> list[dict]:
    """Load sell events from CSV (legacy). Use load_events_from_db instead."""
    with open(SELL_EVENTS_CSV) as f:
        events = list(csv.DictReader(f))
    if cluster_only:
        events = [e for e in events if e["is_cluster"] == "True"]
    for e in events:
        e["_ticker"] = e["ticker"]
        e["_entry_date"] = datetime.strptime(e["entry_date"], "%Y-%m-%d").date()
        e["_entry_price"] = float(e["entry_price"])
        e["_signal"] = "sell"
        e["_right"] = "P"
    return events


# ─────────────────────────────────────────────
# Core Pull Logic
# ─────────────────────────────────────────────

async def pull_hold_dte_pair(client: ThetaClient, ticker: str, entry_date: date,
                             entry_price: float, right: str, expirations: list[date],
                             hold_days: int, dte_type: str, target_dte: int) -> list[dict]:
    """
    Pull option data for a single hold-DTE pair. Returns a list of detail dicts.
    Designed to run concurrently across all 8 hold-DTE pairs.

    ATM is fetched first as a liquidity probe — if ATM has no volume,
    remaining strikes for this pair are skipped.
    """
    exit_date = add_trading_days(entry_date, hold_days)
    pull_start = entry_date - timedelta(days=2)
    pull_end = exit_date + timedelta(days=5)
    details = []

    matched_exp = find_nearest_expiration(expirations, entry_date, target_dte)
    if matched_exp is None:
        details.append({
            "hold": hold_days, "dte_type": dte_type, "target_dte": target_dte,
            "status": "no_expiry_match",
            "_ok": 0, "_attempted": 0, "_skipped": 0, "_no_data": 0,
        })
        return details

    if exit_date >= matched_exp:
        details.append({
            "hold": hold_days, "dte_type": dte_type, "target_dte": target_dte,
            "matched_exp": str(matched_exp),
            "status": "exit_after_expiry",
            "_ok": 0, "_attempted": 0, "_skipped": 0, "_no_data": 0,
        })
        return details

    strikes = await client.get_strikes(ticker, matched_exp)
    if not strikes:
        details.append({
            "hold": hold_days, "dte_type": dte_type, "matched_exp": str(matched_exp),
            "status": "no_strikes",
            "_ok": 0, "_attempted": 0, "_skipped": 0, "_no_data": 0,
        })
        return details

    # ATM first as liquidity probe
    atm_target = entry_price * STRIKES["atm"]
    atm_strike = find_nearest_strike(strikes, atm_target)
    atm_has_volume = True  # assume yes unless proven otherwise

    if atm_strike is not None:
        atm_eod = await client.get_option_eod(
            symbol=ticker, expiration=matched_exp, strike=atm_strike,
            right=right, start_date=pull_start, end_date=pull_end,
        )
        if atm_eod and len(atm_eod) > 0:
            rows_with_vol = sum(1 for r in atm_eod if int(r.get("volume", "0")) > 0)
            entry_vol = 0
            for r in atm_eod:
                try:
                    row_date = r.get("created", "")[:10]
                    if row_date >= str(entry_date):
                        entry_vol = int(r.get("volume", "0"))
                        break
                except (ValueError, KeyError):
                    pass
            atm_has_volume = entry_vol > 0
            details.append({
                "hold": hold_days, "dte_type": dte_type, "strike_type": "atm",
                "real_strike": atm_strike, "matched_exp": str(matched_exp),
                "right": right, "rows": len(atm_eod), "rows_with_volume": rows_with_vol,
                "pull_window": f"{pull_start}→{pull_end}",
                "remaining_dte_at_exit": (matched_exp - exit_date).days,
                "status": "ok",
                "_ok": 1, "_attempted": 1, "_skipped": 0, "_no_data": 0,
                "_eod_records": atm_eod,
            })
        else:
            atm_has_volume = False
            details.append({
                "hold": hold_days, "dte_type": dte_type, "strike_type": "atm",
                "real_strike": atm_strike, "matched_exp": str(matched_exp),
                "right": right, "status": "no_data",
                "_ok": 0, "_attempted": 1, "_skipped": 0, "_no_data": 1,
            })

    # If ATM has no volume, skip remaining strikes
    other_strikes = ["5pct_itm", "5pct_otm", "10pct_otm"]
    if not atm_has_volume:
        for strike_name in other_strikes:
            target_strike = entry_price * STRIKES[strike_name]
            real_strike = find_nearest_strike(strikes, target_strike)
            if real_strike is not None:
                details.append({
                    "hold": hold_days, "dte_type": dte_type, "strike_type": strike_name,
                    "real_strike": real_strike, "matched_exp": str(matched_exp),
                    "right": right, "status": "skipped_no_atm_volume",
                    "_ok": 0, "_attempted": 0, "_skipped": 1, "_no_data": 0,
                })
        return details

    # ATM has volume — pull remaining 3 strikes concurrently
    async def pull_strike(strike_name: str):
        target_strike = entry_price * STRIKES[strike_name]
        real_strike = find_nearest_strike(strikes, target_strike)
        if real_strike is None:
            return None
        eod = await client.get_option_eod(
            symbol=ticker, expiration=matched_exp, strike=real_strike,
            right=right, start_date=pull_start, end_date=pull_end,
        )
        if eod and len(eod) > 0:
            rows_with_vol = sum(1 for r in eod if int(r.get("volume", "0")) > 0)
            return {
                "hold": hold_days, "dte_type": dte_type, "strike_type": strike_name,
                "real_strike": real_strike, "matched_exp": str(matched_exp),
                "right": right, "rows": len(eod), "rows_with_volume": rows_with_vol,
                "pull_window": f"{pull_start}→{pull_end}",
                "remaining_dte_at_exit": (matched_exp - exit_date).days,
                "status": "ok",
                "_ok": 1, "_attempted": 1, "_skipped": 0, "_no_data": 0,
                "_eod_records": eod,
            }
        else:
            return {
                "hold": hold_days, "dte_type": dte_type, "strike_type": strike_name,
                "real_strike": real_strike, "matched_exp": str(matched_exp),
                "right": right, "status": "no_data",
                "_ok": 0, "_attempted": 1, "_skipped": 0, "_no_data": 1,
            }

    strike_results = await asyncio.gather(*[pull_strike(s) for s in other_strikes])
    for r in strike_results:
        if r is not None:
            details.append(r)

    return details


async def pull_event(client: ThetaClient, event: dict) -> dict:
    """
    Pull all option data for a single insider event.
    Parallelizes across all 8 hold-DTE pairs for maximum throughput.

    Returns a summary dict with counts and any issues.
    """
    ticker = event["_ticker"]
    entry_date = event["_entry_date"]
    entry_price = event["_entry_price"]
    right = event["_right"]
    signal = event["_signal"]

    summary = {
        "ticker": ticker,
        "entry_date": str(entry_date),
        "signal": signal,
        "right": right,
        "entry_price": entry_price,
        "contracts_attempted": 0,
        "contracts_with_data": 0,
        "contracts_no_data": 0,
        "contracts_skipped_no_vol": 0,
        "expirations_found": 0,
        "details": [],
    }

    # Step 1: Get expirations
    expirations = await client.get_expirations(ticker)
    if not expirations:
        summary["error"] = "no expirations found"
        logger.warning(f"  {ticker}: no expirations found")
        return summary
    summary["expirations_found"] = len(expirations)

    # Step 2: Fire all 8 hold-DTE pairs concurrently
    tasks = []
    for hold_days, (tight_dte, comf_dte) in HOLD_DTE_MAP.items():
        for dte_type, target_dte in [("tight", tight_dte), ("comfortable", comf_dte)]:
            tasks.append(pull_hold_dte_pair(
                client, ticker, entry_date, entry_price, right, expirations,
                hold_days, dte_type, target_dte,
            ))

    pair_results = await asyncio.gather(*tasks)

    # Merge results
    all_eod_records = []
    for detail_list in pair_results:
        for d in detail_list:
            summary["contracts_attempted"] += d.pop("_attempted", 0)
            summary["contracts_with_data"] += d.pop("_ok", 0)
            summary["contracts_no_data"] += d.pop("_no_data", 0)
            summary["contracts_skipped_no_vol"] += d.pop("_skipped", 0)
            eod = d.pop("_eod_records", None)
            if eod:
                all_eod_records.extend(eod)
            summary["details"].append(d)

    summary["_eod_records"] = all_eod_records
    return summary


async def run_pull(events: list[dict], label: str = "pull", checkpoint_db: CacheDB = None,
                   progress_interval: int = 300, batch_size: int = 1,
                   price_writer: OptionPriceWriter = None):
    """
    Run the pull for a list of events.

    Args:
        events: List of event dicts with _ticker, _entry_date, _entry_price, _signal, _right
        label: Label for logging
        checkpoint_db: If provided, use for per-event checkpointing (skip completed events)
        progress_interval: Seconds between progress summary logs (default 300 = 5 min)
        batch_size: Number of events to process concurrently (default 1).
                    Higher values improve throughput by overlapping API waits across events.
                    The 8-request semaphore in ThetaClient still enforces the API concurrency limit.
    """
    logger.info(f"Starting {label}: {len(events)} events (batch_size={batch_size})")
    logger.info(f"Grid: 4 holds × 2 DTE types × 4 strikes = 32 EOD calls/event")
    logger.info(f"Max API calls: {len(events) * 39} (32 EOD + ~7 lookups per event)")

    # Filter out already-completed events if checkpoint_db is provided
    if checkpoint_db:
        events_to_pull = []
        skipped_count = 0
        for ev in events:
            event_key = f"{ev['_ticker']}|{ev['_entry_date']}|{ev['_signal']}"
            if checkpoint_db.has(f"event_done|{event_key}"):
                skipped_count += 1
            else:
                events_to_pull.append(ev)
        if skipped_count > 0:
            logger.info(f"Resuming: skipping {skipped_count} already-completed events, {len(events_to_pull)} remaining")
        events = events_to_pull

    if not events:
        logger.info(f"No events to pull for {label}")
        return []

    results = []
    last_progress_time = time.monotonic()
    total_events = len(events)
    events_done = 0

    # Aggregate stats for progress reporting
    total_contracts_ok = 0
    total_contracts_attempted = 0
    total_contracts_skipped = 0

    async with ThetaClient(max_concurrent=8) as client:
        # Process events in batches for higher throughput
        for batch_start in range(0, total_events, batch_size):
            batch = events[batch_start:batch_start + batch_size]

            for ev in batch:
                logger.info(f"[{batch_start + 1}-{batch_start + len(batch)}/{total_events}] "
                           f"{ev['_ticker']} ({ev['_signal']}) entry={ev['_entry_date']} price=${ev['_entry_price']:.2f}")

            # Run batch concurrently — semaphore inside ThetaClient keeps API calls ≤8
            batch_results = await asyncio.gather(*[pull_event(client, ev) for ev in batch])

            for ev, summary in zip(batch, batch_results):
                results.append(summary)

                ok = summary["contracts_with_data"]
                no = summary["contracts_no_data"]
                skipped = summary["contracts_skipped_no_vol"]
                total = summary["contracts_attempted"]
                total_contracts_ok += ok
                total_contracts_attempted += total
                total_contracts_skipped += skipped

                logger.info(f"  {summary['ticker']} → {ok}/{total} contracts with data ({no} empty, {skipped} skipped/no-vol)")

                # Write option prices to insiders.db
                if price_writer:
                    eod_records = summary.pop("_eod_records", [])
                    if eod_records:
                        price_writer.write_eod_records(ev["_ticker"], ev["_right"], eod_records)
                    price_writer.write_pull_status(
                        ev["_ticker"], ev["_entry_date"], ev["_signal"], ok, no,
                    )
                else:
                    summary.pop("_eod_records", None)

                # Checkpoint this event as done
                if checkpoint_db:
                    event_key = f"{ev['_ticker']}|{ev['_entry_date']}|{ev['_signal']}"
                    checkpoint_db.put(f"event_done|{event_key}", {"status": "done", "ok": ok, "total": total})

                events_done += 1

            # Periodic progress summary
            now = time.monotonic()
            if now - last_progress_time >= progress_interval:
                elapsed = client.elapsed
                events_remaining = total_events - events_done
                rate_events = events_done / elapsed if elapsed > 0 else 0
                eta_seconds = events_remaining / rate_events if rate_events > 0 else 0
                logger.info(
                    f"\n{'='*80}\n"
                    f"  PROGRESS UPDATE ({label})\n"
                    f"  Events: {events_done}/{total_events} ({events_done/total_events*100:.1f}%)\n"
                    f"  Contracts: {total_contracts_ok}/{total_contracts_attempted} with data, {total_contracts_skipped} skipped/no-vol\n"
                    f"  {client.stats_summary()}\n"
                    f"  Event rate: {rate_events:.2f} events/sec ({rate_events*60:.1f} events/min)\n"
                    f"  ETA: {eta_seconds/60:.0f} min ({eta_seconds/3600:.1f} hours)\n"
                    f"{'='*80}"
                )
                last_progress_time = now

                # Auto-update PROGRESS.md
                cache_count = 0
                buy_ckpts = 0
                sell_ckpts = 0
                if checkpoint_db:
                    try:
                        import sqlite3
                        conn = sqlite3.connect(checkpoint_db.db_path)
                        cache_count = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
                        buy_ckpts = conn.execute("SELECT COUNT(*) FROM cache WHERE cache_key LIKE 'event_done|%buy'").fetchone()[0]
                        sell_ckpts = conn.execute("SELECT COUNT(*) FROM cache WHERE cache_key LIKE 'event_done|%sell'").fetchone()[0]
                        conn.close()
                    except Exception:
                        pass
                update_progress_md(
                    label=label, events_done=events_done, total_events=total_events,
                    contracts_ok=total_contracts_ok, contracts_attempted=total_contracts_attempted,
                    contracts_skipped=total_contracts_skipped, rate_events=rate_events,
                    eta_seconds=eta_seconds, cache_entries=cache_count,
                    buy_checkpoints=buy_ckpts, sell_checkpoints=sell_ckpts,
                )

        # Final summary
        elapsed = client.elapsed
        final_rate = total_events / elapsed if elapsed > 0 else 0
        logger.info(
            f"\n{'='*80}\n"
            f"  COMPLETED: {label}\n"
            f"  Events: {total_events}\n"
            f"  Contracts: {total_contracts_ok}/{total_contracts_attempted} with data, {total_contracts_skipped} skipped/no-vol\n"
            f"  {client.stats_summary()}\n"
            f"  Total time: {elapsed/60:.1f} min ({elapsed/3600:.1f} hours)\n"
            f"{'='*80}"
        )

        # Final PROGRESS.md update
        cache_count = 0
        buy_ckpts = 0
        sell_ckpts = 0
        if checkpoint_db:
            try:
                import sqlite3
                conn = sqlite3.connect(checkpoint_db.db_path)
                cache_count = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
                buy_ckpts = conn.execute("SELECT COUNT(*) FROM cache WHERE cache_key LIKE 'event_done|%buy'").fetchone()[0]
                sell_ckpts = conn.execute("SELECT COUNT(*) FROM cache WHERE cache_key LIKE 'event_done|%sell'").fetchone()[0]
                conn.close()
            except Exception:
                pass
        update_progress_md(
            label=label, events_done=total_events, total_events=total_events,
            contracts_ok=total_contracts_ok, contracts_attempted=total_contracts_attempted,
            contracts_skipped=total_contracts_skipped, rate_events=final_rate,
            eta_seconds=0, cache_entries=cache_count,
            buy_checkpoints=buy_ckpts, sell_checkpoints=sell_ckpts,
            completed=True,
        )

    return results


def print_results(results: list[dict]):
    """Print a summary table of results."""
    print("\n" + "=" * 100)
    print("PULL RESULTS SUMMARY")
    print("=" * 100)

    for r in results:
        print(f"\n{'─'*80}")
        print(f"  {r['ticker']} | {r['signal'].upper()} ({r['right']}) | entry={r['entry_date']} | ${r['entry_price']:.2f}")
        print(f"  Expirations found: {r['expirations_found']}")
        print(f"  Contracts: {r['contracts_with_data']}/{r['contracts_attempted']} with data")

        if r.get("error"):
            print(f"  ERROR: {r['error']}")
            continue

        # Group details by hold period
        from collections import defaultdict
        by_hold = defaultdict(list)
        for d in r["details"]:
            by_hold[d["hold"]].append(d)

        for hold in sorted(by_hold.keys()):
            details = by_hold[hold]
            print(f"\n  {hold}d hold:")
            for d in details:
                if d["status"] == "ok":
                    print(f"    {d['dte_type']:>11} | {d['strike_type']:>10} ${d['real_strike']:<8} | "
                          f"exp={d['matched_exp']} | {d['rows']:>3} rows ({d['rows_with_volume']} vol) | "
                          f"remaining={d['remaining_dte_at_exit']}d | {d['pull_window']}")
                elif d["status"] in ("no_expiry_match", "exit_after_expiry", "no_strikes"):
                    print(f"    {d['dte_type']:>11} | {d['status']}"
                          + (f" (exp={d.get('matched_exp', '?')})" if "matched_exp" in d else ""))
                elif d["status"] == "no_data":
                    print(f"    {d['dte_type']:>11} | {d['strike_type']:>10} ${d['real_strike']:<8} | "
                          f"exp={d['matched_exp']} | NO DATA")

    # Overall stats
    total_attempted = sum(r["contracts_attempted"] for r in results)
    total_ok = sum(r["contracts_with_data"] for r in results)
    total_no = sum(r["contracts_no_data"] for r in results)
    total_skipped = sum(r["contracts_skipped_no_vol"] for r in results)
    print(f"\n{'='*100}")
    print(f"TOTALS: {total_ok}/{total_attempted} contracts with data "
          f"({total_no} empty, {total_skipped} skipped/no-vol)")
    print(f"Hit rate: {total_ok/total_attempted*100:.1f}%" if total_attempted > 0 else "")
    if total_skipped > 0:
        print(f"API calls saved by volume filter: ~{total_skipped} EOD pulls skipped")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Phase 0c: Theta Data Options Pull")
    parser.add_argument("--test", action="store_true", help="Test run: 5 buys + 5 sells")
    parser.add_argument("--full", action="store_true", help="Full pull (cluster events by default)")
    parser.add_argument("--all", action="store_true", help="Include individual (non-cluster) events too")
    parser.add_argument("--resume", action="store_true", help="(Deprecated — resume is automatic via checkpointing)")
    parser.add_argument("--buys-only", action="store_true", help="Only pull buy events")
    parser.add_argument("--sells-only", action="store_true", help="Only pull sell events")
    parser.add_argument("--batch-size", type=int, default=4,
                        help="Events to process concurrently (default 4). "
                             "Higher = better throughput, same API concurrency limit.")
    parser.add_argument("--buy-csv", type=str, default=None,
                        help="(Legacy) Override buy events CSV path")
    parser.add_argument("--from-db", action="store_true",
                        help="Load events from insiders.db instead of CSV files")
    parser.add_argument("--start", type=str, default=None,
                        help="Start date filter YYYY-MM-DD (requires --from-db)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date filter YYYY-MM-DD (requires --from-db)")
    args = parser.parse_args()

    cluster_only = not args.all
    mode_label = "all" if args.all else "cluster-only"

    if args.test:
        # Pick 5 diverse buy events (different price ranges, tickers)
        if args.from_db:
            all_buys = load_events_from_db("buy", start_date=args.start, end_date=args.end)
        else:
            all_buys = load_buy_events(cluster_only=cluster_only, csv_path=args.buy_csv)

        # Select liquid, diverse buys
        test_buys = []
        seen_tickers = set()
        priority = ["JPM", "TSLA", "AAPL", "MSFT", "NVDA", "META", "BAC", "XOM", "HD", "PFE"]
        for target in priority:
            for e in all_buys:
                if e["_ticker"] == target and target not in seen_tickers:
                    test_buys.append(e)
                    seen_tickers.add(target)
                    break
            if len(test_buys) >= 5:
                break

        # Fill remaining with high-value events
        if len(test_buys) < 5:
            remaining = [e for e in all_buys
                        if e["_ticker"] not in seen_tickers
                        and e["_entry_price"] and float(e["_entry_price"]) >= 20]
            remaining.sort(key=lambda e: float(e["total_value"]), reverse=True)
            for e in remaining:
                if e["_ticker"] not in seen_tickers:
                    test_buys.append(e)
                    seen_tickers.add(e["_ticker"])
                    if len(test_buys) >= 5:
                        break

        # Pick 5 diverse sell events
        if args.from_db:
            all_sells = load_events_from_db("sell", start_date=args.start, end_date=args.end)
        else:
            all_sells = load_sell_events(cluster_only=cluster_only)
        test_sells = []
        seen_sell_tickers = set()
        sell_priority = ["TSLA", "AMZN", "AAPL", "MSFT", "NVDA", "META", "NFLX", "GOOGL", "BLK", "JPM"]
        for target in sell_priority:
            for e in all_sells:
                if e["_ticker"] == target and target not in seen_sell_tickers and e["_entry_price"] >= 20:
                    test_sells.append(e)
                    seen_sell_tickers.add(target)
                    break
            if len(test_sells) >= 5:
                break
        if len(test_sells) < 5:
            remaining_sells = [e for e in all_sells
                              if e["_ticker"] not in seen_sell_tickers
                              and e["_entry_price"] >= 20
                              and float(e.get("total_value", "0")) >= 1_000_000]
            remaining_sells.sort(key=lambda e: float(e.get("total_value", "0")), reverse=True)
            for e in remaining_sells:
                if e["_ticker"] not in seen_sell_tickers:
                    test_sells.append(e)
                    seen_sell_tickers.add(e["_ticker"])
                    if len(test_sells) >= 5:
                        break

        logger.info(f"Mode: {mode_label} | Test buys: {[e['_ticker'] for e in test_buys]}")
        logger.info(f"Test sells: {[e['_ticker'] for e in test_sells]}")

        buy_results = await run_pull(test_buys, "test-buys", batch_size=args.batch_size)
        sell_results = await run_pull(test_sells, "test-sells", batch_size=args.batch_size)

        print("\n\n" + "#" * 100)
        print("# BUY EVENTS (CALLS)")
        print("#" * 100)
        print_results(buy_results)

        print("\n\n" + "#" * 100)
        print("# SELL EVENTS (PUTS)")
        print("#" * 100)
        print_results(sell_results)

        output_path = os.path.join(SCRIPT_DIR, "data", "test_pull_results.json")
        with open(output_path, "w") as f:
            json.dump({"buys": buy_results, "sells": sell_results}, f, indent=2, default=str)
        logger.info(f"Results saved to {output_path}")

    elif args.full:
        checkpoint_db = CacheDB()
        price_writer = OptionPriceWriter()
        logger.info(f"Mode: {mode_label} | Batch size: {args.batch_size}"
                     + (f" | Source: insiders.db" if args.from_db else " | Source: CSV")
                     + " | Writing to: option_prices + option_pull_status")

        if not args.sells_only:
            if args.from_db:
                all_buys = load_events_from_db("buy", start_date=args.start, end_date=args.end)
            else:
                all_buys = load_buy_events(cluster_only=cluster_only, csv_path=args.buy_csv)
                logger.info(f"Loaded {len(all_buys)} buy events ({mode_label}) from {args.buy_csv or BUY_EVENTS_CSV}")
            buy_results = await run_pull(all_buys, "full-buys", checkpoint_db=checkpoint_db,
                                         batch_size=args.batch_size, price_writer=price_writer)

            output_path = os.path.join(SCRIPT_DIR, "data", "full_pull_buys.json")
            with open(output_path, "w") as f:
                json.dump(buy_results, f, indent=2, default=str)
            logger.info(f"Buy results saved to {output_path}")

        if not args.buys_only:
            if args.from_db:
                all_sells = load_events_from_db("sell", start_date=args.start, end_date=args.end)
            else:
                all_sells = load_sell_events(cluster_only=cluster_only)
                logger.info(f"Loaded {len(all_sells)} sell events ({mode_label})")
            sell_results = await run_pull(all_sells, "full-sells", checkpoint_db=checkpoint_db,
                                          batch_size=args.batch_size, price_writer=price_writer)

            output_path = os.path.join(SCRIPT_DIR, "data", "full_pull_sells.json")
            with open(output_path, "w") as f:
                json.dump(sell_results, f, indent=2, default=str)
            logger.info(f"Sell results saved to {output_path}")

        price_writer.close()

    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
