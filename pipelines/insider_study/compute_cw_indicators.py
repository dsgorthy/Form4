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

import argparse
import csv
import time
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from config.database import get_connection

try:
    from pipelines.insider_study.price_utils import (
        load_prices, find_price, compute_period_change, available_tickers, PRICES_DIR,
    )
except ModuleNotFoundError:
    from price_utils import (
        load_prices, find_price, compute_period_change, available_tickers, PRICES_DIR,
    )

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


def ensure_columns(conn):
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
        except Exception as e:
            if "locked" in str(e) and attempt < 4:
                import time
                time.sleep(2 ** attempt)
                continue
            raise


# ---------------------------------------------------------------------------
# Indicator 1: Dip indicators (dip_1mo, dip_3mo, dip_1yr)
# ---------------------------------------------------------------------------

def compute_dip_indicators(conn) -> int:
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


def compute_sma_context(conn) -> int:
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

def compute_purchase_size_metrics(conn) -> int:
    """Compute purchase_size_ratio and is_largest_ever.
    PIT: only compares against trades with trade_date < current trade_date."""
    print("\n=== Purchase Size Metrics ===")
    t0 = time.time()

    # HISTORY IS NOT THE UPDATE WINDOW.
    #
    # This used to load `trade_date >= MIN_DATE` and then mark the first trade
    # it saw in each group as "trivially the largest". That is fine on a full
    # run, where MIN_DATE is 2016 and only 1,471 P-trades predate it — and
    # badly wrong on the incremental one, because fetch_latest calls this every
    # five minutes with `--since <7 days ago>`, which overrides MIN_DATE. Each
    # insider's first purchase inside a seven-day window was being crowned
    # their largest ever.
    #
    # 23.7% of is_largest_ever=1 flags had a bigger purchase by the same
    # insider in the same ticker already filed before them. Scott Gordon's
    # LIEN buy was published as "largest purchase they've ever made in it"
    # against his own $490,336 in 2022 — this one was $421K.
    #
    # So: load the FULL history for comparison, and restrict only which rows
    # get written.
    rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trans_code, trade_date,
               COALESCE(filing_key, accession) AS filing_key, value
        FROM trades
        WHERE value > 0
        ORDER BY insider_id, ticker, trans_code, trade_date, trade_id
    """).fetchall()
    print(f"  Loaded {len(rows):,} trades with value > 0 (full history for comparison)")

    # COMPARE FILINGS, NOT LOTS.
    #
    # This compared individual execution rows, so "the largest purchase they
    # have ever made" meant "a bigger tranche than any previous tranche".
    # Benjamin Wood's August CDNL buy filled in two lots, $534,451 and
    # $480,143; the larger one was flagged largest-ever and the page said so.
    # His actual August purchase was $1,014,594 — and his May purchase, filled
    # in five lots, was $1,025,900. August was NOT his largest, and we
    # published that it was.
    #
    # 26.6% of flags flip once filings are compared. That is the same order as
    # the 23.7% error the --since window bug produced in August, from a
    # different cause.
    #
    # Every lot of a filing inherits the filing's verdict, so downstream
    # readers that look at any single row still get the right answer.
    filings = defaultdict(list)
    for row in rows:
        trade_id, insider_id, ticker, trans_code, trade_date, filing_key, value = row
        key = (insider_id, ticker, trans_code)
        filings[key].append((filing_key or str(trade_date), trade_id, trade_date, value))

    updates = []
    for key, lots in filings.items():
        # Collapse to one entry per filing, preserving first-seen order.
        by_filing = OrderedDict()
        for fk, trade_id, trade_date, value in lots:
            if fk not in by_filing:
                by_filing[fk] = {"date": trade_date, "value": 0.0, "ids": []}
            by_filing[fk]["value"] += value
            by_filing[fk]["ids"].append(trade_id)
            by_filing[fk]["date"] = min(by_filing[fk]["date"], trade_date)

        prior_values = []
        prior_max = 0.0
        for fk, f in by_filing.items():
            value, trade_date = f["value"], f["date"]
            if prior_values:
                avg_prior = sum(prior_values) / len(prior_values)
                ratio = value / avg_prior if avg_prior > 0 else None
                largest = 1 if value > prior_max else 0
            else:
                ratio = None  # first filing — no prior reference
                largest = 1   # genuinely their first, so genuinely the largest
            # Only write rows inside the requested window. The comparison above
            # always walks the whole history, so a short --since no longer
            # rewrites the past OR mistakes a window boundary for a career start.
            if trade_date >= MIN_DATE:
                for trade_id in f["ids"]:
                    updates.append((ratio, largest, trade_id))
            prior_values.append(value)
            prior_max = max(prior_max, value)

        if len(updates) >= BATCH_SIZE:
            flush_updates(conn, "trades", ["purchase_size_ratio", "is_largest_ever"], updates)
            updates = []

    flush_updates(conn, "trades", ["purchase_size_ratio", "is_largest_ever"], updates)
    # `filings`, not `groups` — renamed when this moved to filing-level
    # comparison. The stale name survived here because it is only read in the
    # summary, after every write has flushed, so the function wrote correct
    # data and then raised NameError on its own last line.
    n = sum(len(v) for v in filings.values())
    print(f"  Done: {n:,} trades in {time.time()-t0:.1f}s")
    return n


# ---------------------------------------------------------------------------
# Indicator 4: Tax sale classification
# ---------------------------------------------------------------------------

def compute_tax_sale_flag(conn) -> int:
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

def compute_recurring_purchase(conn) -> int:
    """Detect insiders buying on a regular schedule (monthly/quarterly/yearly)
    without a 10b5-1 flag. Requires 3+ instances at regular intervals."""
    print("\n=== Recurring Purchase Detection ===")
    t0 = time.time()

    # Full history, same reason as compute_purchase_size_metrics and
    # compute_consecutive_sells. This one was the worst of the three: detecting
    # a monthly/quarterly/yearly cadence needs 3+ instances, which a seven-day
    # --since window can never contain, and the loop below writes 0 for any
    # group with fewer than three trades. So the five-minute incremental run
    # could not ever SET is_recurring and actively ERASED whatever a full run
    # had set. 42 flags survive across 66,711 trades filed this year.
    rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trade_date,
               COALESCE(filing_key, accession, CAST(trade_date AS TEXT)) AS filing_key
        FROM trades
        WHERE trans_code = 'P'
        ORDER BY insider_id, ticker, trade_date
    """).fetchall()

    # ONE ENTRY PER FILING. The ">= 3 purchases" gate below counted execution
    # rows, so a single purchase filled in three tranches cleared it — 3,108
    # insider/ticker pairs qualified on lots alone. The gap-pattern test that
    # follows is meaningless between two lots of one filing anyway: they share
    # a trade_date, so the interval is zero.
    groups = defaultdict(list)
    seen = defaultdict(set)
    for trade_id, insider_id, ticker, trade_date, filing_key in rows:
        key = (insider_id, ticker)
        if filing_key in seen[key]:
            continue
        seen[key].add(filing_key)
        groups[key].append((trade_id, trade_date))

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
            for trade_id, td in trades:
                if td >= MIN_DATE:
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
        for idx, (trade_id, _td) in enumerate(trades):
            if _td < MIN_DATE:
                continue          # outside the write window; history only
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

def compute_consecutive_sells(conn) -> int:
    """For each P-code buy, count consecutive S-code sells immediately prior
    by the same insider at the same ticker.

    Counting requires the *full* prior-trade history per (insider, ticker),
    so we always load every trade regardless of `--since`. We only emit
    UPDATEs for rows whose trade_date >= MIN_DATE — this keeps daily refresh
    cheap while making the count correct.

    Previously the load query was also gated by `WHERE trade_date >= MIN_DATE`,
    which truncated the prior-trade window when running with `--since 30d`
    and collapsed `consecutive_sells_before` (max dropped from 176 in March
    to 4 in May — the bug that silenced reversal_dip for 8 weeks)."""
    print("\n=== Consecutive Sells Before Buy ===")
    t0 = time.time()

    # Load ALL trades for accurate counting. UPDATE filter happens below.
    rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trade_type, trade_date,
               COALESCE(filing_key, accession, CAST(trade_date AS TEXT)) AS filing_key
        FROM trades
        ORDER BY insider_id, ticker, trade_date, trade_id
    """).fetchall()
    print(f"  Loaded {len(rows):,} trades (full history)")

    groups = defaultdict(list)
    for trade_id, insider_id, ticker, trade_type, trade_date, filing_key in rows:
        groups[(insider_id, ticker)].append(
            (trade_id, trade_type, trade_date, filing_key))

    # COUNT SELL DECISIONS, NOT EXECUTION LOTS.
    #
    # A sale filled in five tranches is five rows and one decision, so counting
    # rows made "10 consecutive sells" reachable by two or three actual sales.
    # That gate is reversal_dip's primary filter. Collapsing lots changes 19.2%
    # of stored values and takes the >=10 population from 5,643 to 3,799 — a
    # third of the signals that book fires on were inflated.
    #
    # Consecutive lots of ONE filing collapse into a single event; two separate
    # filings on the same day stay two, which is why the key is the filing
    # rather than the date.
    updates = []
    for key, trades in groups.items():
        events = []
        for trade_id, trade_type, trade_date, filing_key in trades:
            if events and events[-1][0] == filing_key and events[-1][1] == trade_type:
                events[-1][2].append((trade_id, trade_date))
            else:
                events.append([filing_key, trade_type, [(trade_id, trade_date)]])

        for idx, (filing_key, trade_type, members) in enumerate(events):
            if trade_type != "buy":
                continue
            count = 0
            for j in range(idx - 1, -1, -1):
                if events[j][1] == "sell":
                    count += 1
                else:
                    break
            # Only update recent trades — the count uses the full prior list
            # but writes only roll forward.
            for trade_id, trade_date in members:
                if trade_date < MIN_DATE:
                    continue
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

# Maps each indicator to the columns it populates. Used to write
# signal_freshness rows after each indicator completes — every contracted
# column gets its own freshness timestamp reflecting when this script
# last touched it.
INDICATOR_TO_COLUMNS = {
    "dip": ["dip_1mo", "dip_3mo"],
    "sma": ["above_sma50", "above_sma200"],
    "size": ["purchase_size_ratio", "is_largest_ever"],
    "tax": ["is_tax_sale"],
    "recurring": ["is_recurring"],
    # Note: compute_consecutive_sells only writes `consecutive_sells_before`.
    # `is_rare_reversal` is written by compute_switch_rate.py (separate step).
    # `is_10b5_1` is set during Form 4 ingestion, not by this script.
    "consecutive": ["consecutive_sells_before"],
}


def _write_freshness_for_indicator(conn, indicator: str) -> None:
    """Write signal_freshness rows for every column the indicator populated.
    Best-effort — n_rows_affected is recomputed from the trades table since
    the indicator functions don't return their write counts uniformly."""
    from framework.contracts.freshness_writer import write_freshness
    cols = INDICATOR_TO_COLUMNS.get(indicator, [])
    for col in cols:
        try:
            n = conn.execute(
                f"SELECT COUNT(*) FROM trades WHERE {col} IS NOT NULL AND trade_date >= ?",
                (MIN_DATE,),
            ).fetchone()[0]
        except Exception:
            n = 0
        if n > 0:
            write_freshness(
                conn,
                table="trades",
                column=col,
                n_rows_affected=n,
                populated_by="pipelines/insider_study/compute_cw_indicators.py",
            )
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Compute CW-inspired indicators on trades table")
    parser.add_argument("--indicator", choices=list(INDICATOR_MAP.keys()),
                        help="Compute only this indicator (default: all)")
    parser.add_argument("--since",
                        help="Only process trades with trade_date >= this YYYY-MM-DD "
                             "(default: 2016-01-01). Use to backfill recent trades quickly.")
    args = parser.parse_args()

    if args.since:
        # Mutate the module-global so all indicator functions pick it up.
        global MIN_DATE
        MIN_DATE = args.since
        print(f"--since override: MIN_DATE = {MIN_DATE}")

    conn = get_connection()

    print(f"Database: PostgreSQL")
    ensure_columns(conn)

    if args.indicator:
        INDICATOR_MAP[args.indicator](conn)
        _write_freshness_for_indicator(conn, args.indicator)
    else:
        for name, fn in INDICATOR_MAP.items():
            fn(conn)
            _write_freshness_for_indicator(conn, name)

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
