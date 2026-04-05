#!/usr/bin/env python3
"""
Compute CEO Watcher-inspired indicator columns on the trades table.

Adds: dip_1mo, dip_3mo, dip_1yr, sma50_rel, sma200_rel,
above_sma50, above_sma200, purchase_size_ratio, is_largest_ever,
is_tax_sale, is_recurring, recurring_period, consecutive_sells_before.

All computations are point-in-time: only use data available as of trade_date.

Usage:
    python3 pipelines/insider_study/compute_cw_indicators.py
    python3 pipelines/insider_study/compute_cw_indicators.py --indicator dip
    python3 pipelines/insider_study/compute_cw_indicators.py --indicator sma
    python3 pipelines/insider_study/compute_cw_indicators.py --indicator size
    python3 pipelines/insider_study/compute_cw_indicators.py --indicator tax
    python3 pipelines/insider_study/compute_cw_indicators.py --indicator recurring
    python3 pipelines/insider_study/compute_cw_indicators.py --indicator consecutive
"""

from __future__ import annotations

import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
from pipelines.insider_study.db_lock import db_write_lock

import argparse
import csv
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

try:
    from pipelines.insider_study.price_utils import (
        load_prices, find_price, compute_period_change, available_tickers, PRICES_DIR,
    )
except ModuleNotFoundError:
    from price_utils import (
        load_prices, find_price, compute_period_change, available_tickers, PRICES_DIR,
    )

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
MIN_DATE = "2016-01-01"
BATCH_SIZE = 50_000


def _load_prices_fresh(ticker: str) -> dict[str, float]:
    """Load prices from prices.db (primary) or CSV (fallback).
    Clears from cache after use to keep memory bounded when processing many tickers."""
    from pipelines.insider_study.price_utils import load_prices as _lp, clear_cache
    prices = _lp(ticker)
    # Don't accumulate in global cache — caller processes one ticker at a time
    result = dict(prices)
    clear_cache()
    return result


def _period_change(prices: dict[str, float], trade_date: str, days: int) -> float | None:
    """Compute price change over N calendar days."""
    return compute_period_change(prices, trade_date, days)


def _find_nearest(prices: dict[str, float], dt: datetime, offsets: range) -> float | None:
    """Find nearest price at or BEFORE dt. Never looks forward (PIT-safe)."""
    for off in offsets:
        check = (dt - timedelta(days=off)).strftime("%Y-%m-%d")
        if check in prices:
            return prices[check]
    return None

# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

COLUMNS = {
    "dip_1mo": "REAL",
    "dip_3mo": "REAL",
    "dip_1yr": "REAL",
    "sma50_rel": "REAL",
    "sma200_rel": "REAL",
    "above_sma50": "INTEGER",
    "above_sma200": "INTEGER",
    "purchase_size_ratio": "REAL",
    "is_largest_ever": "INTEGER DEFAULT 0",
    "is_tax_sale": "INTEGER DEFAULT 0",
    "is_recurring": "INTEGER DEFAULT 0",
    "recurring_period": "TEXT",
    "consecutive_sells_before": "INTEGER",
}


def ensure_columns(conn: sqlite3.Connection):
    existing = {r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()}
    for col, dtype in COLUMNS.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {dtype}")
            print(f"  Added column: {col}")
    conn.commit()


def flush_updates(conn, table, col_names, updates):
    """Batch UPDATE trades with (val1, val2, ..., trade_id) tuples."""
    if not updates:
        return
    set_clause = ", ".join(f"{c} = ?" for c in col_names)
    for attempt in range(5):
        try:
            conn.executemany(
                f"UPDATE {table} SET {set_clause} WHERE trade_id = ?",
                updates,
            )
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < 4:
                import time
                time.sleep(2 ** attempt)
                continue
            raise


# ---------------------------------------------------------------------------
# Indicator 1: Dip indicators (dip_1mo, dip_3mo, dip_1yr)
# ---------------------------------------------------------------------------

def compute_dip_indicators(conn: sqlite3.Connection) -> int:
    """Compute price change 30d/90d/365d before each trade.
    Processes ticker-by-ticker to avoid OOM from price cache."""
    print("\n=== Dip Indicators ===")
    avail = available_tickers()
    if not avail:
        print("  No price files found!")
        return 0

    # Get distinct tickers with trades
    trade_tickers = {r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM trades WHERE trade_date >= ?", (MIN_DATE,)
    ).fetchall()}
    tickers_to_process = sorted(trade_tickers & avail)
    print(f"  {len(tickers_to_process):,} tickers with both trades and price data")

    total = 0
    t0 = time.time()

    for i, ticker in enumerate(tickers_to_process):
        rows = conn.execute("""
            SELECT trade_id, trade_date FROM trades
            WHERE ticker = ? AND trade_date >= ?
            ORDER BY trade_date
        """, (ticker, MIN_DATE)).fetchall()
        if not rows:
            continue

        prices = _load_prices_fresh(ticker)
        updates = []
        for trade_id, trade_date in rows:
            dip_1mo = _period_change(prices, trade_date, 30)
            dip_3mo = _period_change(prices, trade_date, 90)
            dip_1yr = _period_change(prices, trade_date, 365)
            updates.append((dip_1mo, dip_3mo, dip_1yr, trade_id))

        flush_updates(conn, "trades", ["dip_1mo", "dip_3mo", "dip_1yr"], updates)
        total += len(updates)

        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(tickers_to_process)} tickers, {total:,} trades ({time.time()-t0:.1f}s)")

    print(f"  Done: {total:,} trades across {len(tickers_to_process):,} tickers in {time.time()-t0:.1f}s")
    return total


# ---------------------------------------------------------------------------
# Indicator 2: SMA context (sma50_rel, sma200_rel, above_sma*)
# ---------------------------------------------------------------------------

def _compute_sma_series(prices: dict[str, float], window: int) -> dict[str, float]:
    """Compute SMA series from {date: close} dict. Returns {date: sma_value}."""
    if len(prices) < window:
        return {}
    sorted_dates = sorted(prices.keys())
    closes = [prices[d] for d in sorted_dates]
    sma = {}
    running_sum = sum(closes[:window])
    sma[sorted_dates[window - 1]] = running_sum / window
    for i in range(window, len(closes)):
        running_sum += closes[i] - closes[i - window]
        sma[sorted_dates[i]] = running_sum / window
    return sma


def compute_sma_context(conn: sqlite3.Connection) -> int:
    """Compute SMA-relative positioning at time of each trade.
    Processes ticker-by-ticker to avoid OOM."""
    print("\n=== SMA Context ===")
    avail = available_tickers()
    trade_tickers = {r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM trades WHERE trade_date >= ?", (MIN_DATE,)
    ).fetchall()}
    tickers_to_process = sorted(trade_tickers & avail)
    print(f"  {len(tickers_to_process):,} tickers to process")

    total = 0
    t0 = time.time()

    for i, ticker in enumerate(tickers_to_process):
        rows = conn.execute("""
            SELECT trade_id, trade_date, price FROM trades
            WHERE ticker = ? AND trade_date >= ?
            ORDER BY trade_date
        """, (ticker, MIN_DATE)).fetchall()
        if not rows:
            continue

        prices = _load_prices_fresh(ticker)
        sma50 = _compute_sma_series(prices, 50)
        sma200 = _compute_sma_series(prices, 200)

        updates = []
        for trade_id, trade_date, trade_price in rows:
            price = trade_price
            if not price or price <= 0:
                price = _find_nearest(prices, datetime.strptime(trade_date, "%Y-%m-%d"), range(4)) if trade_date else None
            if not price or price <= 0:
                updates.append((None, None, None, None, trade_id))
                continue

            s50 = _find_sma_at_date(sma50, trade_date)
            s200 = _find_sma_at_date(sma200, trade_date)

            sma50_rel = (price - s50) / s50 if s50 and s50 > 0 else None
            sma200_rel = (price - s200) / s200 if s200 and s200 > 0 else None
            a50 = 1 if s50 and price > s50 else (0 if s50 else None)
            a200 = 1 if s200 and price > s200 else (0 if s200 else None)
            updates.append((sma50_rel, sma200_rel, a50, a200, trade_id))

        flush_updates(conn, "trades",
                      ["sma50_rel", "sma200_rel", "above_sma50", "above_sma200"],
                      updates)
        total += len(updates)

        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(tickers_to_process)} tickers, {total:,} trades ({time.time()-t0:.1f}s)")

    print(f"  Done: {total:,} trades across {len(tickers_to_process):,} tickers in {time.time()-t0:.1f}s")
    return total


def _find_sma_at_date(sma: dict[str, float], trade_date: str) -> float | None:
    """Find SMA value at or just before trade_date (max 5 day lookback)."""
    try:
        td = datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        return None
    for offset in range(6):
        check = (td - timedelta(days=offset)).strftime("%Y-%m-%d")
        if check in sma:
            return sma[check]
    return None


# ---------------------------------------------------------------------------
# Indicator 3: Purchase size metrics
# ---------------------------------------------------------------------------

def compute_purchase_size_metrics(conn: sqlite3.Connection) -> int:
    """Compute purchase_size_ratio and is_largest_ever.
    PIT: only compares against trades with trade_date < current trade_date."""
    print("\n=== Purchase Size Metrics ===")
    t0 = time.time()

    # Load all trades grouped by (insider_id, ticker, trans_code)
    rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trans_code, trade_date, value
        FROM trades
        WHERE trade_date >= ? AND value > 0
        ORDER BY insider_id, ticker, trans_code, trade_date, trade_id
    """, (MIN_DATE,)).fetchall()
    print(f"  Loaded {len(rows):,} trades with value > 0")

    # Group by (insider_id, ticker, trans_code)
    groups = defaultdict(list)
    for row in rows:
        trade_id, insider_id, ticker, trans_code, trade_date, value = row
        groups[(insider_id, ticker, trans_code)].append((trade_id, trade_date, value))

    updates = []
    for key, trades in groups.items():
        # trades already sorted by trade_date
        prior_values = []
        prior_max = 0.0
        for trade_id, trade_date, value in trades:
            if prior_values:
                avg_prior = sum(prior_values) / len(prior_values)
                ratio = value / avg_prior if avg_prior > 0 else None
                largest = 1 if value > prior_max else 0
            else:
                ratio = None  # first trade — no prior reference
                largest = 1   # trivially the largest
            updates.append((ratio, largest, trade_id))
            prior_values.append(value)
            prior_max = max(prior_max, value)

        if len(updates) >= BATCH_SIZE:
            flush_updates(conn, "trades", ["purchase_size_ratio", "is_largest_ever"], updates)
            updates = []

    flush_updates(conn, "trades", ["purchase_size_ratio", "is_largest_ever"], updates)
    print(f"  Done: {sum(len(v) for v in groups.values()):,} trades in {time.time()-t0:.1f}s")
    return sum(len(v) for v in groups.values())


# ---------------------------------------------------------------------------
# Indicator 4: Tax sale classification
# ---------------------------------------------------------------------------

def compute_tax_sale_flag(conn: sqlite3.Connection) -> int:
    """Classify tax-motivated sales.
    Heuristics:
      1. S-code trade in Nov or Dec
      2. Not flagged as 10b5-1 (cohen_routine != 1)
      3. Sale price < average purchase price (realized loss)
    """
    print("\n=== Tax Sale Flag ===")
    t0 = time.time()

    # Get all S-code trades in Nov/Dec that aren't 10b5-1
    sell_rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trade_date, price
        FROM trades
        WHERE trans_code = 'S'
          AND trade_date >= ?
          AND CAST(strftime('%m', trade_date) AS INTEGER) IN (11, 12)
          AND COALESCE(cohen_routine, 0) != 1
    """, (MIN_DATE,)).fetchall()
    print(f"  Found {len(sell_rows):,} Nov/Dec S-code trades (non-10b5-1)")

    # For each, check if selling at a loss vs prior purchase prices
    updates = []
    flagged = 0
    for trade_id, insider_id, ticker, trade_date, sell_price in sell_rows:
        if not sell_price or sell_price <= 0:
            continue
        # PIT: avg purchase price from prior P-codes
        avg_buy = conn.execute("""
            SELECT AVG(price) FROM trades
            WHERE insider_id = ? AND ticker = ? AND trans_code = 'P'
              AND trade_date < ? AND price > 0
        """, (insider_id, ticker, trade_date)).fetchone()[0]

        if avg_buy and sell_price < avg_buy:
            updates.append((1, trade_id))
            flagged += 1
        else:
            updates.append((0, trade_id))

    flush_updates(conn, "trades", ["is_tax_sale"], updates)
    print(f"  Flagged {flagged:,} tax sales in {time.time()-t0:.1f}s")
    return flagged


# ---------------------------------------------------------------------------
# Indicator 5: Recurring purchase detection
# ---------------------------------------------------------------------------

def compute_recurring_purchase(conn: sqlite3.Connection) -> int:
    """Detect insiders buying on a regular schedule (monthly/quarterly/yearly)
    without a 10b5-1 flag. Requires 3+ instances at regular intervals."""
    print("\n=== Recurring Purchase Detection ===")
    t0 = time.time()

    # Load all P-code trades grouped by (insider_id, ticker)
    rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trade_date
        FROM trades
        WHERE trans_code = 'P' AND trade_date >= ?
        ORDER BY insider_id, ticker, trade_date
    """, (MIN_DATE,)).fetchall()

    groups = defaultdict(list)
    for trade_id, insider_id, ticker, trade_date in rows:
        groups[(insider_id, ticker)].append((trade_id, trade_date))

    # Check interval patterns
    PATTERNS = {
        "monthly": (20, 40),
        "quarterly": (75, 105),
        "yearly": (335, 400),
    }

    updates = []
    flagged = 0

    for key, trades in groups.items():
        if len(trades) < 3:
            for trade_id, _ in trades:
                updates.append((0, None, trade_id))
            continue

        # Compute intervals between consecutive trades
        dates = []
        for _, td in trades:
            try:
                dates.append(datetime.strptime(td, "%Y-%m-%d"))
            except ValueError:
                dates.append(None)

        intervals = []
        for i in range(1, len(dates)):
            if dates[i] and dates[i - 1]:
                intervals.append((dates[i] - dates[i - 1]).days)
            else:
                intervals.append(None)

        # For each trade, check if PIT intervals match a pattern
        for idx, (trade_id, _) in enumerate(trades):
            pit_intervals = [iv for iv in intervals[:idx] if iv is not None]
            if len(pit_intervals) < 2:
                updates.append((0, None, trade_id))
                continue

            matched_period = None
            for period_name, (lo, hi) in PATTERNS.items():
                matching = sum(1 for iv in pit_intervals if lo <= iv <= hi)
                if matching >= 2 and matching / len(pit_intervals) >= 0.6:
                    matched_period = period_name
                    break

            if matched_period:
                updates.append((1, matched_period, trade_id))
                flagged += 1
            else:
                updates.append((0, None, trade_id))

        if len(updates) >= BATCH_SIZE:
            flush_updates(conn, "trades", ["is_recurring", "recurring_period"], updates)
            updates = []

    flush_updates(conn, "trades", ["is_recurring", "recurring_period"], updates)
    print(f"  Flagged {flagged:,} recurring purchases in {time.time()-t0:.1f}s")
    return flagged


# ---------------------------------------------------------------------------
# Indicator 6: Consecutive sells before buy
# ---------------------------------------------------------------------------

def compute_consecutive_sells(conn: sqlite3.Connection) -> int:
    """For each P-code buy, count consecutive S-code sells immediately prior
    by the same insider at the same ticker. Enhances reversal detection."""
    print("\n=== Consecutive Sells Before Buy ===")
    t0 = time.time()

    # Load all trades by insider+ticker, ordered by date
    rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trade_type, trade_date
        FROM trades
        WHERE trade_date >= ?
        ORDER BY insider_id, ticker, trade_date, trade_id
    """, (MIN_DATE,)).fetchall()
    print(f"  Loaded {len(rows):,} trades")

    groups = defaultdict(list)
    for trade_id, insider_id, ticker, trade_type, trade_date in rows:
        groups[(insider_id, ticker)].append((trade_id, trade_type))

    updates = []
    for key, trades in groups.items():
        for idx, (trade_id, trade_type) in enumerate(trades):
            if trade_type != "buy":
                continue
            # Count consecutive sells immediately before this buy
            count = 0
            for j in range(idx - 1, -1, -1):
                if trades[j][1] == "sell":
                    count += 1
                else:
                    break
            updates.append((count, trade_id))

        if len(updates) >= BATCH_SIZE:
            flush_updates(conn, "trades", ["consecutive_sells_before"], updates)
            updates = []

    flush_updates(conn, "trades", ["consecutive_sells_before"], updates)
    print(f"  Done in {time.time()-t0:.1f}s")
    return len(updates)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

INDICATOR_MAP = {
    "dip": compute_dip_indicators,
    "sma": compute_sma_context,
    "size": compute_purchase_size_metrics,
    "tax": compute_tax_sale_flag,
    "recurring": compute_recurring_purchase,
    "consecutive": compute_consecutive_sells,
}


def main():
    parser = argparse.ArgumentParser(description="Compute CW-inspired indicators on trades table")
    parser.add_argument("--indicator", choices=list(INDICATOR_MAP.keys()),
                        help="Compute only this indicator (default: all)")
    args = parser.parse_args()

    with db_write_lock(timeout_msg="compute_cw_indicators"):
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA cache_size=-200000")

        print(f"Database: {DB_PATH}")
        ensure_columns(conn)

        if args.indicator:
            INDICATOR_MAP[args.indicator](conn)
        else:
            for name, fn in INDICATOR_MAP.items():
                fn(conn)

        # Summary
        print("\n=== Summary ===")
        for col in ["dip_1mo", "dip_3mo", "dip_1yr", "sma50_rel", "above_sma50",
                    "purchase_size_ratio", "is_largest_ever", "is_tax_sale",
                    "is_recurring", "consecutive_sells_before"]:
            count = conn.execute(
                f"SELECT COUNT(*) FROM trades WHERE {col} IS NOT NULL AND trade_date >= ?",
                (MIN_DATE,)
            ).fetchone()[0]
            print(f"  {col}: {count:,} populated")

        conn.close()
        print("\nDone.")


if __name__ == "__main__":
    main()
