#!/usr/bin/env python3
"""
Backfill 14d and 60d forward returns in trade_returns table.

Follows the same conventions as compute_returns.py:
  - Entry: T+1 open after filing_date
  - Exit 14d: close at T+14 calendar days after filing (nearest trading day)
  - Exit 60d: close at T+60 calendar days after filing (nearest trading day)
  - SPY benchmark return over same period
  - Abnormal return = return - SPY return

Usage:
  python backfill_14d_60d.py
  python backfill_14d_60d.py --dry-run
"""

from __future__ import annotations

import logging
import sqlite3
import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CATALOG_DIR = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog"
DB_PATH = CATALOG_DIR / "insiders.db"
PRICES_DIR = Path(__file__).resolve().parent / "data" / "prices"

_price_cache: dict[str, Optional[pd.DataFrame]] = {}


def load_prices(ticker: str) -> Optional[pd.DataFrame]:
    """Load daily OHLCV from CSV, cached in memory."""
    if ticker in _price_cache:
        return _price_cache[ticker]

    path = PRICES_DIR / f"{ticker.upper()}.csv"
    if not path.exists():
        _price_cache[ticker] = None
        return None

    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
        df.columns = [c.lower() for c in df.columns]
        _price_cache[ticker] = df
        return df
    except Exception:
        _price_cache[ticker] = None
        return None


def get_price_at_offset(
    df: pd.DataFrame,
    ref_date: pd.Timestamp,
    calendar_days: int = 0,
    trading_days: int = 0,
    price_col: str = "close",
) -> Optional[tuple[float, pd.Timestamp]]:
    """Get price at an offset from ref_date."""
    if trading_days > 0:
        future = df.index[df.index > ref_date]
        if len(future) < trading_days:
            return None
        target = future[trading_days - 1]
    elif calendar_days > 0:
        target_date = ref_date + pd.Timedelta(days=calendar_days)
        future = df.index[df.index >= target_date]
        if len(future) == 0:
            return None
        target = future[0]
    else:
        return None

    try:
        price = float(df.loc[target, price_col])
        if not np.isfinite(price) or price <= 0:
            return None
        return (price, target)
    except (KeyError, ValueError):
        return None


def compute_return(entry_price: float, exit_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    return (exit_price - entry_price) / entry_price


def main():
    parser = argparse.ArgumentParser(description="Backfill 14d and 60d returns")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        logger.error("DB not found at %s", DB_PATH)
        return
    if not PRICES_DIR.exists():
        logger.error("Prices dir not found at %s", PRICES_DIR)
        return

    spy_df = load_prices("SPY")
    if spy_df is None:
        logger.error("SPY price data not found in %s", PRICES_DIR)
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Get all trades that have a trade_returns row but NULL 14d or 60d
    trades = conn.execute("""
        SELECT t.trade_id, t.ticker, t.filing_date, t.trade_type,
               tr.return_14d, tr.return_60d, tr.entry_price
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE tr.return_14d IS NULL OR tr.return_60d IS NULL
        ORDER BY t.filing_date
    """).fetchall()

    logger.info("Found %d trades needing 14d/60d backfill", len(trades))

    stats = {
        "14d": {"computed": 0, "skipped_exists": 0, "skipped_nodata": 0},
        "60d": {"computed": 0, "skipped_exists": 0, "skipped_nodata": 0},
    }
    batch = []
    batch_size = 1000
    committed = 0

    for i, (trade_id, ticker, filing_date_str, trade_type, ret_14d, ret_60d, entry_price_cached) in enumerate(trades):
        if i % 5000 == 0 and i > 0:
            logger.info("  Progress: %d/%d (%.0f%%) | 14d computed=%d | 60d computed=%d",
                        i, len(trades), 100 * i / len(trades),
                        stats["14d"]["computed"], stats["60d"]["computed"])

        prices = load_prices(ticker)
        if prices is None:
            stats["14d"]["skipped_nodata"] += 1
            stats["60d"]["skipped_nodata"] += 1
            continue

        try:
            filing_date = pd.Timestamp(filing_date_str)
        except Exception:
            stats["14d"]["skipped_nodata"] += 1
            stats["60d"]["skipped_nodata"] += 1
            continue

        # Entry price (T+1 open after filing)
        entry_result = get_price_at_offset(prices, filing_date, trading_days=1, price_col="open")
        if entry_result is None:
            stats["14d"]["skipped_nodata"] += 1
            stats["60d"]["skipped_nodata"] += 1
            continue
        entry_price, entry_date = entry_result

        # SPY entry
        spy_entry = get_price_at_offset(spy_df, filing_date, trading_days=1, price_col="open")
        spy_entry_price = spy_entry[0] if spy_entry else None

        update_cols = {}

        # 14d: calendar days from filing
        if ret_14d is None:
            exit_result = get_price_at_offset(prices, filing_date, calendar_days=14, price_col="close")
            if exit_result:
                exit_price, exit_date = exit_result
                ret = compute_return(entry_price, exit_price)
                spy_ret = 0.0
                if spy_entry_price:
                    spy_exit = get_price_at_offset(spy_df, filing_date, calendar_days=14, price_col="close")
                    if spy_exit:
                        spy_ret = compute_return(spy_entry_price, spy_exit[0])
                abnormal = ret - spy_ret

                update_cols["exit_price_14d"] = round(exit_price, 4)
                update_cols["return_14d"] = round(ret, 6)
                update_cols["spy_return_14d"] = round(spy_ret, 6)
                update_cols["abnormal_14d"] = round(abnormal, 6)
                stats["14d"]["computed"] += 1
            else:
                stats["14d"]["skipped_nodata"] += 1
        else:
            stats["14d"]["skipped_exists"] += 1

        # 60d: calendar days from filing
        if ret_60d is None:
            exit_result = get_price_at_offset(prices, filing_date, calendar_days=60, price_col="close")
            if exit_result:
                exit_price, exit_date = exit_result
                ret = compute_return(entry_price, exit_price)
                spy_ret = 0.0
                if spy_entry_price:
                    spy_exit = get_price_at_offset(spy_df, filing_date, calendar_days=60, price_col="close")
                    if spy_exit:
                        spy_ret = compute_return(spy_entry_price, spy_exit[0])
                abnormal = ret - spy_ret

                update_cols["exit_price_60d"] = round(exit_price, 4)
                update_cols["return_60d"] = round(ret, 6)
                update_cols["spy_return_60d"] = round(spy_ret, 6)
                update_cols["abnormal_60d"] = round(abnormal, 6)
                stats["60d"]["computed"] += 1
            else:
                stats["60d"]["skipped_nodata"] += 1
        else:
            stats["60d"]["skipped_exists"] += 1

        if update_cols:
            batch.append((trade_id, update_cols))

        if len(batch) >= batch_size:
            if not args.dry_run:
                _flush_batch(conn, batch)
                committed += len(batch)
            batch = []

    if batch and not args.dry_run:
        _flush_batch(conn, batch)
        committed += len(batch)

    conn.commit()

    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    for w in ["14d", "60d"]:
        s = stats[w]
        logger.info("  %s: computed=%d, already_existed=%d, no_data=%d",
                     w, s["computed"], s["skipped_exists"], s["skipped_nodata"])
    logger.info("  Total rows updated: %d", committed)

    # Verify
    for w in ["14d", "60d"]:
        count = conn.execute(f"SELECT COUNT(*) FROM trade_returns WHERE return_{w} IS NOT NULL").fetchone()[0]
        avg = conn.execute(f"SELECT AVG(return_{w}) * 100 FROM trade_returns WHERE return_{w} IS NOT NULL").fetchone()[0]
        avg_abn = conn.execute(f"SELECT AVG(abnormal_{w}) * 100 FROM trade_returns WHERE return_{w} IS NOT NULL").fetchone()[0]
        logger.info("  %s coverage: %d rows | Avg return: %.2f%% | Avg abnormal: %.2f%%",
                     w, count, avg or 0, avg_abn or 0)

    conn.close()


def _flush_batch(conn: sqlite3.Connection, batch: list):
    """Update trade_returns rows with 14d/60d data."""
    for trade_id, cols in batch:
        sets = []
        vals = []
        for key, val in cols.items():
            sets.append(f"{key} = ?")
            vals.append(val)
        if sets:
            vals.append(trade_id)
            conn.execute(
                f"UPDATE trade_returns SET {', '.join(sets)}, computed_at = datetime('now') WHERE trade_id = ?",
                vals,
            )


if __name__ == "__main__":
    main()
