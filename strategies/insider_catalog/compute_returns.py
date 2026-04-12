#!/usr/bin/env python3
"""
Compute 30d and 90d forward returns for all buy trades in the insider catalog.

Uses the existing price CSVs in pipelines/insider_study/data/prices/.
Also recomputes 7d returns for trades that are missing them.

For each trade:
  - Entry: T+1 open after filing_date (same as run_event_study.py)
  - Exit 7d:  close at T+7 trading days after entry
  - Exit 30d: close at T+30 calendar days after filing (nearest trading day)
  - Exit 90d: close at T+90 calendar days after filing (nearest trading day)
  - SPY benchmark return over same period

Usage:
  python compute_returns.py              # compute all windows
  python compute_returns.py --window 30  # only 30d
  python compute_returns.py --window 90  # only 90d
  python compute_returns.py --dry-run    # show counts, don't write
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from config.database import get_connection

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Minimum calendar days that must have elapsed since trade_date before
# we consider a return window valid.  Keyed by window label.
WINDOW_MIN_DAYS = {"7d": 7, "14d": 14, "30d": 30, "60d": 60, "90d": 90, "180d": 180, "365d": 365}

# Ticker aliases: map tickers that share price data but have different symbols
# e.g., GOOGL (Class A) and GOOG (Class C) trade at nearly identical prices
TICKER_ALIASES = {
    "GOOGL": "GOOG",
    "FB": "META",
    "TWTR": "X",
    "GAP": "GPS",
}

# ---------------------------------------------------------------------------
# Price lookups — pure SQL, no DataFrames, no memory accumulation
# ---------------------------------------------------------------------------

_prices_conn = None


def _get_prices_conn():
    """Lazy singleton read-only connection to PostgreSQL (prices schema)."""
    global _prices_conn
    if _prices_conn is not None:
        return _prices_conn
    _prices_conn = get_connection(readonly=True)
    return _prices_conn


def _resolve_ticker(ticker: str) -> str:
    """Resolve ticker aliases."""
    upper = ticker.upper()
    return TICKER_ALIASES.get(upper, upper)


def load_prices(ticker: str) -> Optional[str]:
    """Check if a ticker has price data. Returns the resolved ticker name or None.
    No DataFrame, no memory — just a DB existence check."""
    pconn = _get_prices_conn()
    if pconn is None:
        return None
    resolved = _resolve_ticker(ticker)
    for t in [resolved, ticker.upper()]:
        r = pconn.execute("SELECT 1 FROM daily_prices WHERE ticker = ? LIMIT 1", (t,)).fetchone()
        if r:
            return t
    return None


def get_price_at_offset(
    ticker_resolved: str,
    ref_date,
    calendar_days: int = 0,
    trading_days: int = 0,
    price_col: str = "close",
) -> Optional[tuple[float, pd.Timestamp]]:
    """Get price at an offset from ref_date using pure SQL.

    ticker_resolved: resolved ticker string (not a DataFrame).
    ref_date: pd.Timestamp or string date.

    If trading_days > 0: count N trading days after ref_date.
    If calendar_days > 0: find nearest trading day on or after ref_date + calendar_days.

    Returns (price, actual_date) or None. Zero memory overhead.
    """
    pconn = _get_prices_conn()
    if pconn is None:
        return None

    ref_str = str(ref_date)[:10] if not isinstance(ref_date, str) else ref_date[:10]

    if trading_days > 0:
        # Find the Nth trading day after ref_date
        # T+1: start from the day after ref_date
        r = pconn.execute(
            f"SELECT date, {price_col} FROM daily_prices "
            f"WHERE ticker = ? AND date > ? ORDER BY date LIMIT 1 OFFSET ?",
            (ticker_resolved, ref_str, trading_days - 1),
        ).fetchone()
    elif calendar_days > 0:
        # Find the first trading day on or after ref_date + calendar_days
        target_date = (pd.Timestamp(ref_str) + timedelta(days=calendar_days)).strftime("%Y-%m-%d")
        r = pconn.execute(
            f"SELECT date, {price_col} FROM daily_prices "
            f"WHERE ticker = ? AND date >= ? ORDER BY date LIMIT 1",
            (ticker_resolved, target_date),
        ).fetchone()
    else:
        return None

    if r and r[1] and r[1] > 0:
        return (float(r[1]), pd.Timestamp(r[0]))
    return None


def compute_return(entry_price: float, exit_price: float) -> float:
    """Compute fractional return."""
    if entry_price <= 0:
        return 0.0
    return (exit_price - entry_price) / entry_price


def process_trades(conn, windows: list[str], dry_run: bool = False, trade_type: str = "buy"):
    """
    Compute forward returns for trades.

    windows: list of '7d', '30d', '90d'
    trade_type: 'buy' or 'sell'
    """
    today = date.today()

    spy_ticker = load_prices("SPY")
    if spy_ticker is None:
        logger.error("SPY price data not found")
        return

    # Get trades that need return computation
    trades = conn.execute("""
        SELECT t.trade_id, t.ticker, t.filing_date, t.trade_date
        FROM trades t
        WHERE t.trade_type = ?
        ORDER BY t.filing_date
    """, (trade_type,)).fetchall()

    logger.info("Processing %d %s trades for windows: %s", len(trades), trade_type, ", ".join(windows))

    # Check what already exists in trade_returns
    existing = {}
    for row in conn.execute("SELECT trade_id, return_7d, return_14d, return_30d, return_60d, return_90d, return_180d, return_365d FROM trade_returns").fetchall():
        existing[row[0]] = {
            "return_7d": row[1],
            "return_14d": row[2],
            "return_30d": row[3],
            "return_60d": row[4],
            "return_90d": row[5],
            "return_180d": row[6],
            "return_365d": row[7],
        }

    stats = {w: {"computed": 0, "skipped_exists": 0, "skipped_nodata": 0} for w in windows}
    batch = []
    batch_size = 5000

    for i, (trade_id, ticker, filing_date_str, trade_date_str) in enumerate(trades):
        if i % 10000 == 0 and i > 0:
            logger.info("  %d/%d (%.0f%%)", i, len(trades), 100 * i / len(trades))

        prices = load_prices(ticker)
        if prices is None:
            for w in windows:
                stats[w]["skipped_nodata"] += 1
            continue

        try:
            filing_date = pd.Timestamp(filing_date_str)
        except Exception:
            for w in windows:
                stats[w]["skipped_nodata"] += 1
            continue

        ex = existing.get(trade_id, {})

        # Compute entry price (T+1 open) — same for all windows
        entry_result = get_price_at_offset(prices, filing_date, trading_days=1, price_col="open")
        if entry_result is None:
            for w in windows:
                stats[w]["skipped_nodata"] += 1
            continue

        entry_price, entry_date = entry_result

        # SPY entry
        spy_entry = get_price_at_offset(spy_ticker, filing_date, trading_days=1, price_col="open")
        spy_entry_price = spy_entry[0] if spy_entry else None

        update = {"trade_id": trade_id, "entry_price": entry_price}

        for w in windows:
            col_return = f"return_{w}"
            col_exit = f"exit_price_{w}"
            col_spy = f"spy_return_{w}"
            col_abnormal = f"abnormal_{w}"

            # Skip if already computed for this specific window
            existing_val = ex.get(col_return)
            if existing_val is not None:
                stats[w]["skipped_exists"] += 1
                update[col_return] = existing_val
                continue

            # Skip if trade is too recent for this window
            try:
                td = pd.Timestamp(trade_date_str).date() if trade_date_str else filing_date.date()
            except Exception:
                td = filing_date.date()
            if (today - td).days < WINDOW_MIN_DAYS[w]:
                stats[w]["skipped_nodata"] += 1
                update[col_return] = None
                continue

            # Compute exit price
            if w == "7d":
                exit_result = get_price_at_offset(prices, entry_date, trading_days=7, price_col="close")
            elif w == "14d":
                exit_result = get_price_at_offset(prices, filing_date, calendar_days=14, price_col="close")
            elif w == "30d":
                exit_result = get_price_at_offset(prices, filing_date, calendar_days=30, price_col="close")
            elif w == "60d":
                exit_result = get_price_at_offset(prices, filing_date, calendar_days=60, price_col="close")
            elif w == "90d":
                exit_result = get_price_at_offset(prices, filing_date, calendar_days=90, price_col="close")
            elif w == "180d":
                exit_result = get_price_at_offset(prices, filing_date, calendar_days=180, price_col="close")
            elif w == "365d":
                exit_result = get_price_at_offset(prices, filing_date, calendar_days=365, price_col="close")
            else:
                continue

            if exit_result is None:
                stats[w]["skipped_nodata"] += 1
                update[col_return] = None
                continue

            exit_price, exit_date = exit_result
            ret = compute_return(entry_price, exit_price)

            # SPY return over same period
            spy_ret = 0.0
            if spy_entry_price:
                if w == "7d":
                    spy_exit = get_price_at_offset(spy_ticker, entry_date, trading_days=7, price_col="close")
                elif w in ("14d", "30d", "60d", "90d", "180d", "365d"):
                    cal_days = int(w.replace("d", ""))
                    spy_exit = get_price_at_offset(spy_ticker, filing_date, calendar_days=cal_days, price_col="close")
                else:
                    spy_exit = None

                if spy_exit:
                    spy_ret = compute_return(spy_entry_price, spy_exit[0])

            abnormal = ret - spy_ret

            update[col_exit] = round(exit_price, 4)
            update[col_return] = round(ret, 6)
            update[col_spy] = round(spy_ret, 6)
            update[col_abnormal] = round(abnormal, 6)
            stats[w]["computed"] += 1

        batch.append(update)

        if len(batch) >= batch_size:
            if not dry_run:
                _flush_returns(conn, batch)
            batch = []

    if batch and not dry_run:
        _flush_returns(conn, batch)

    conn.commit()

    # Print stats
    for w in windows:
        s = stats[w]
        logger.info(
            "  %s: computed=%d, already_existed=%d, no_data=%d",
            w, s["computed"], s["skipped_exists"], s["skipped_nodata"],
        )


def _flush_returns(conn, batch: list):
    """Upsert trade_returns rows."""
    for row in batch:
        trade_id = row["trade_id"]

        # Check if row exists
        exists = conn.execute(
            "SELECT 1 FROM trade_returns WHERE trade_id = ?", (trade_id,)
        ).fetchone()

        if exists:
            # Update only the columns we computed
            sets = []
            vals = []
            for key in ["entry_price",
                        "exit_price_7d", "return_7d", "spy_return_7d", "abnormal_7d",
                        "exit_price_14d", "return_14d", "spy_return_14d", "abnormal_14d",
                        "exit_price_30d", "return_30d", "spy_return_30d", "abnormal_30d",
                        "exit_price_60d", "return_60d", "spy_return_60d", "abnormal_60d",
                        "exit_price_90d", "return_90d", "spy_return_90d", "abnormal_90d",
                        "exit_price_180d", "return_180d", "spy_return_180d", "abnormal_180d",
                        "exit_price_365d", "return_365d", "spy_return_365d", "abnormal_365d"]:
                if key in row and row[key] is not None:
                    sets.append(f"{key} = ?")
                    vals.append(row[key])
            if sets:
                vals.append(trade_id)
                conn.execute(
                    f"UPDATE trade_returns SET {', '.join(sets)}, computed_at = datetime('now') WHERE trade_id = ?",
                    vals,
                )
        else:
            conn.execute("""
                INSERT INTO trade_returns
                    (trade_id, entry_price,
                     exit_price_7d, return_7d, spy_return_7d, abnormal_7d,
                     exit_price_30d, return_30d, spy_return_30d, abnormal_30d,
                     exit_price_90d, return_90d, spy_return_90d, abnormal_90d)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_id, row.get("entry_price"),
                row.get("exit_price_7d"), row.get("return_7d"), row.get("spy_return_7d"), row.get("abnormal_7d"),
                row.get("exit_price_30d"), row.get("return_30d"), row.get("spy_return_30d"), row.get("abnormal_30d"),
                row.get("exit_price_90d"), row.get("return_90d"), row.get("spy_return_90d"), row.get("abnormal_90d"),
            ))


def cleanup_premature_returns(conn):
    """NULL out return values for trades that are too recent for their window.

    This fixes already-stored bad data where returns were computed before
    enough calendar days had elapsed since the trade date.
    """
    today_str = date.today().isoformat()
    cleanup_queries = [
        (
            "7d",
            f"""
            UPDATE trade_returns SET return_7d = NULL, abnormal_7d = NULL, spy_return_7d = NULL,
                                     exit_price_7d = NULL
            WHERE trade_id IN (
                SELECT t.trade_id FROM trades t
                WHERE julianday('{today_str}') - julianday(t.trade_date) < 7
            ) AND return_7d IS NOT NULL
            """,
        ),
        (
            "30d",
            f"""
            UPDATE trade_returns SET return_30d = NULL, abnormal_30d = NULL, spy_return_30d = NULL,
                                     exit_price_30d = NULL
            WHERE trade_id IN (
                SELECT t.trade_id FROM trades t
                WHERE julianday('{today_str}') - julianday(t.trade_date) < 30
            ) AND return_30d IS NOT NULL
            """,
        ),
        (
            "90d",
            f"""
            UPDATE trade_returns SET return_90d = NULL, abnormal_90d = NULL, spy_return_90d = NULL,
                                     exit_price_90d = NULL
            WHERE trade_id IN (
                SELECT t.trade_id FROM trades t
                WHERE julianday('{today_str}') - julianday(t.trade_date) < 90
            ) AND return_90d IS NOT NULL
            """,
        ),
    ]

    total_cleaned = 0
    for label, sql in cleanup_queries:
        cursor = conn.execute(sql)
        n = cursor.rowcount
        if n > 0:
            logger.info("Cleanup: NULLed %d premature %s return rows", n, label)
        total_cleaned += n

    if total_cleaned > 0:
        conn.commit()
        logger.info("Cleanup: %d total rows corrected", total_cleaned)
    else:
        logger.info("Cleanup: no premature return data found")


def print_summary(conn):
    """Print coverage stats."""
    total = conn.execute("SELECT COUNT(*) FROM trades WHERE trade_type = 'buy'").fetchone()[0]

    for window in ["7d", "30d", "90d"]:
        col = f"return_{window}"
        count = conn.execute(
            f"SELECT COUNT(*) FROM trade_returns WHERE {col} IS NOT NULL"
        ).fetchone()[0]

        # Avg return and win rate
        stats = conn.execute(f"""
            SELECT
                AVG({col}) * 100 AS avg_ret,
                AVG(CASE WHEN {col} > 0 THEN 1.0 ELSE 0.0 END) * 100 AS win_rate,
                AVG(abnormal_{window}) * 100 AS avg_abn
            FROM trade_returns
            WHERE {col} IS NOT NULL
        """).fetchone()

        avg_ret = (stats["avg_ret"] if stats else None) or 0
        wr = (stats["win_rate"] if stats else None) or 0
        avg_abn = (stats["avg_abn"] if stats else None) or 0

        print(f"  {window}: {count:,}/{total:,} trades ({100*count/total:.0f}%) | "
              f"Avg ret: {avg_ret:+.2f}% | WR: {wr:.1f}% | Avg alpha: {avg_abn:+.2f}%")


def main():
    parser = argparse.ArgumentParser(description="Compute multi-window forward returns")
    parser.add_argument("--window", choices=["7d", "14d", "30d", "60d", "90d", "180d", "365d"],
                        help="Only compute this window (default: all)")
    parser.add_argument("--trade-type", choices=["buy", "sell", "both"], default="buy",
                        help="Trade type to process (default: buy)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be computed without writing")
    args = parser.parse_args()

    windows = [args.window] if args.window else ["7d", "14d", "30d", "60d", "90d", "180d", "365d"]
    trade_types = ["buy", "sell"] if args.trade_type == "both" else [args.trade_type]

    conn = get_connection()

    for tt in trade_types:
        process_trades(conn, windows, dry_run=args.dry_run, trade_type=tt)

    # Clean up any previously-stored premature returns
    if not args.dry_run:
        cleanup_premature_returns(conn)

    print(f"\n{'='*60}")
    print("RETURN COVERAGE SUMMARY")
    print(f"{'='*60}")
    print_summary(conn)
    print(f"{'='*60}\n")

    conn.close()


if __name__ == "__main__":
    main()
