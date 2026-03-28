#!/usr/bin/env python3
"""
Post-import price validation for insider trades.

SEC Form 4 filings frequently contain corrupted price/qty data — especially from
micro/nano-cap filers. Common errors:
  - Total dollar value in the price-per-share field (e.g. $2,261,327 instead of $226.18)
  - Share counts in the price field (price == qty)
  - Decimal point shifts (100x or 10000x the real price)

This validator cross-references reported trade prices against known stock price ranges
to flag and quarantine suspect trades. It runs as a post-import stage — called after
backfill.py or backfill_live.py inserts new trades.

Strategy:
  1. For each ticker, compute the MEDIAN price from all trades (robust to outliers)
  2. Flag any trade where the price deviates >20x from the ticker's median
  3. For flagged trades, attempt auto-correction if the error pattern is recognizable
  4. Quarantine trades that can't be corrected (set value to NULL or move to bad_trades)

Usage:
  # Dry run — report only, no changes
  python price_validator.py --dry-run

  # Fix bad trades (quarantine unfixable, correct fixable)
  python price_validator.py

  # Run on a specific ticker
  python price_validator.py --ticker MSFT

  # Show stats only
  python price_validator.py --stats
"""

from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import statistics
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"

# Trades with price-per-share above this are almost certainly wrong.
# BRK-A is the only US stock that trades above $500K/share.
ABSOLUTE_PRICE_CAP = 999_999.0
BRK_TICKERS = {"BRK-A", "BRK.A", "BRKA", "BRK/A"}

# If a trade's price deviates more than this factor from the ticker's median,
# it's flagged as suspect. 20x is generous — a stock doubling twice in a year
# would only be 4x. 20x catches parsing errors without false positives.
MEDIAN_DEVIATION_FACTOR = 20.0

# Minimum trades needed to compute a reliable median for a ticker
MIN_TRADES_FOR_MEDIAN = 3


def ensure_schema(conn: sqlite3.Connection):
    """Add the suspect_reason column if it doesn't exist."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
    if "suspect_reason" not in cols:
        conn.execute("ALTER TABLE trades ADD COLUMN suspect_reason TEXT")
        conn.commit()
        logger.info("Added suspect_reason column to trades table")

    # Create quarantine table for unfixable trades
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bad_trades (
            trade_id INTEGER PRIMARY KEY,
            insider_id INTEGER,
            ticker TEXT,
            company TEXT,
            title TEXT,
            trade_type TEXT,
            trade_date TEXT,
            filing_date TEXT,
            original_price REAL,
            original_qty INTEGER,
            original_value REAL,
            suspect_reason TEXT,
            median_price REAL,
            deviation_factor REAL,
            source TEXT,
            accession TEXT,
            quarantined_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def compute_ticker_medians(conn: sqlite3.Connection, ticker: str = None) -> dict:
    """
    Compute median price-per-share for each ticker using all trades.

    Uses a two-pass approach:
      1. First pass: raw median across all trades for the ticker
      2. Second pass: exclude outliers (>10x from raw median), recompute

    Returns {ticker: median_price}
    """
    if ticker:
        rows = conn.execute(
            "SELECT ticker, price FROM trades WHERE ticker = ? AND price > 0",
            (ticker,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT ticker, price FROM trades WHERE price > 0"
        ).fetchall()

    # Group prices by ticker
    prices_by_ticker: dict[str, list[float]] = {}
    for tk, price in rows:
        prices_by_ticker.setdefault(tk, []).append(price)

    medians = {}
    for tk, prices in prices_by_ticker.items():
        if len(prices) < MIN_TRADES_FOR_MEDIAN:
            continue

        # First pass: raw median
        raw_med = statistics.median(prices)
        if raw_med <= 0:
            continue

        # Second pass: exclude >10x outliers, recompute
        filtered = [p for p in prices if p / raw_med < 10 and raw_med / p < 10]
        if len(filtered) >= MIN_TRADES_FOR_MEDIAN:
            medians[tk] = statistics.median(filtered)
        else:
            medians[tk] = raw_med

    return medians


def find_suspect_trades(conn: sqlite3.Connection, ticker: str = None) -> list[dict]:
    """
    Find trades with suspect prices. Returns list of suspect trade dicts.

    Detection rules (in order):
      1. Price > $1M and ticker is not BRK-A → absolute cap violation
      2. Price > 20x the ticker's median price → statistical outlier
      3. Price == qty (common parsing error where shares end up in price field)
    """
    medians = compute_ticker_medians(conn, ticker)

    if ticker:
        all_trades = conn.execute(
            """SELECT trade_id, ticker, trade_date, price, qty, value, insider_id,
                      source, accession
               FROM trades WHERE ticker = ? AND price > 0""",
            (ticker,),
        ).fetchall()
    else:
        all_trades = conn.execute(
            """SELECT trade_id, ticker, trade_date, price, qty, value, insider_id,
                      source, accession
               FROM trades WHERE price > 0"""
        ).fetchall()

    suspects = []
    for trade_id, tk, trade_date, price, qty, value, insider_id, source, accession in all_trades:
        reason = None
        deviation = None

        # Rule 1: Absolute price cap
        if price > ABSOLUTE_PRICE_CAP and tk.upper() not in BRK_TICKERS:
            reason = f"price_exceeds_cap: ${price:,.2f} > ${ABSOLUTE_PRICE_CAP:,.0f}"
            median = medians.get(tk)
            deviation = price / median if median and median > 0 else None

        # Rule 2: Statistical outlier vs ticker median
        elif tk in medians:
            median = medians[tk]
            if median > 0:
                ratio = price / median
                if ratio > MEDIAN_DEVIATION_FACTOR:
                    reason = f"price_outlier: ${price:,.2f} is {ratio:.1f}x median ${median:,.2f}"
                    deviation = ratio

        # Rule 3: Price equals qty (common corruption pattern)
        if reason is None and qty > 0 and abs(price - qty) < 0.01 and price > 1000:
            reason = f"price_equals_qty: price=${price:,.2f} qty={qty:,}"
            median = medians.get(tk)
            deviation = price / median if median and median > 0 else None

        if reason:
            suspects.append({
                "trade_id": trade_id,
                "ticker": tk,
                "trade_date": trade_date,
                "price": price,
                "qty": qty,
                "value": value,
                "insider_id": insider_id,
                "source": source,
                "accession": accession,
                "reason": reason,
                "median_price": medians.get(tk),
                "deviation": deviation,
            })

    return suspects


def attempt_correction(trade: dict) -> dict | None:
    """
    Try to auto-correct a suspect trade price.

    Correction strategies:
      1. If price is ~N * median where N is a power of 10 → divide by N
      2. If price / median is close to qty → price field has total value, divide by qty
      3. If price == qty and median exists → use median as price

    Returns corrected dict with 'corrected_price' and 'corrected_value', or None.
    """
    median = trade.get("median_price")
    if not median or median <= 0:
        return None

    price = trade["price"]
    qty = trade["qty"]

    # Strategy 1: Power-of-10 shift (price = real_price * 10^N)
    ratio = price / median
    log_ratio = math.log10(ratio) if ratio > 0 else 0
    nearest_power = round(log_ratio)
    if nearest_power >= 1 and abs(log_ratio - nearest_power) < 0.3:
        divisor = 10 ** nearest_power
        corrected = price / divisor
        # Verify corrected price is within 3x of median
        if 0.33 < corrected / median < 3.0:
            return {
                "corrected_price": round(corrected, 4),
                "corrected_value": round(corrected * qty, 2),
                "method": f"power_of_10_shift: /{divisor:.0f}",
            }

    # Strategy 2: Price field contains total value (price = price_per_share * qty)
    if qty > 1:
        candidate = price / qty
        if 0.33 < candidate / median < 3.0:
            return {
                "corrected_price": round(candidate, 4),
                "corrected_value": round(candidate * qty, 2),
                "method": "price_is_total_value: price/qty",
            }

    # Strategy 3: Price == qty and we have a good median
    if abs(price - qty) < 0.01:
        return {
            "corrected_price": round(median, 4),
            "corrected_value": round(median * qty, 2),
            "method": "price_equals_qty: used_median",
        }

    return None


def quarantine_trade(conn: sqlite3.Connection, trade: dict):
    """Move a trade to the bad_trades table and delete from trades."""
    conn.execute("""
        INSERT OR REPLACE INTO bad_trades
            (trade_id, insider_id, ticker, company, title, trade_type, trade_date,
             filing_date, original_price, original_qty, original_value,
             suspect_reason, median_price, deviation_factor, source, accession)
        SELECT
            trade_id, insider_id, ticker, company, title, trade_type, trade_date,
            filing_date, price, qty, value,
            ?, ?, ?, source, accession
        FROM trades WHERE trade_id = ?
    """, (trade["reason"], trade.get("median_price"), trade.get("deviation"), trade["trade_id"]))

    conn.execute("DELETE FROM trades WHERE trade_id = ?", (trade["trade_id"],))


def correct_trade(conn: sqlite3.Connection, trade: dict, correction: dict):
    """Apply a price correction to a trade in-place."""
    conn.execute("""
        UPDATE trades
        SET price = ?,
            value = ?,
            suspect_reason = ?
        WHERE trade_id = ?
    """, (
        correction["corrected_price"],
        correction["corrected_value"],
        f"corrected: {correction['method']} (was ${trade['price']:,.2f})",
        trade["trade_id"],
    ))


def print_stats(conn: sqlite3.Connection):
    """Print summary of data quality status."""
    total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]

    cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
    if "suspect_reason" in cols:
        corrected = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE suspect_reason LIKE 'corrected:%'"
        ).fetchone()[0]
    else:
        corrected = 0

    # Check if bad_trades table exists
    has_bad_trades = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='bad_trades'"
    ).fetchone()[0]
    quarantined = conn.execute("SELECT COUNT(*) FROM bad_trades").fetchone()[0] if has_bad_trades else 0

    # Current suspect count
    suspects = find_suspect_trades(conn)

    print(f"\n{'='*60}")
    print("PRICE VALIDATION STATS")
    print(f"{'='*60}")
    print(f"Total trades:           {total:,}")
    print(f"Currently suspect:      {len(suspects):,}")
    print(f"Previously corrected:   {corrected:,}")
    print(f"Quarantined:            {quarantined:,}")

    if suspects:
        # Breakdown by reason type
        by_reason = {}
        for s in suspects:
            key = s["reason"].split(":")[0]
            by_reason[key] = by_reason.get(key, 0) + 1
        print(f"\nSuspect breakdown:")
        for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count:,}")

        # Top 10 worst offenders
        suspects.sort(key=lambda s: s.get("deviation") or 0, reverse=True)
        print(f"\nTop 10 worst offenders:")
        for s in suspects[:10]:
            name = conn.execute(
                "SELECT name FROM insiders WHERE insider_id = ?", (s["insider_id"],)
            ).fetchone()[0]
            dev = f"{s['deviation']:.0f}x" if s.get("deviation") else "N/A"
            print(f"  {s['ticker']} {s['trade_date']} ${s['price']:,.2f} "
                  f"(median ${s.get('median_price', 0):,.2f}, {dev}) -- {name}")

    print(f"{'='*60}\n")


def _rebuild_insider_companies(conn: sqlite3.Connection):
    """Rebuild insider_companies aggregates from current trades table."""
    logger.info("Rebuilding insider_companies aggregates after corrections...")
    conn.execute("DELETE FROM insider_companies")
    conn.execute("""
        INSERT INTO insider_companies
            (insider_id, ticker, company, title, trade_count, total_value, first_trade, last_trade)
        SELECT
            t.insider_id,
            t.ticker,
            MAX(t.company),
            (SELECT t2.title FROM trades t2
             WHERE t2.insider_id = t.insider_id AND t2.ticker = t.ticker
             ORDER BY t2.trade_date DESC LIMIT 1),
            COUNT(*),
            SUM(t.value),
            MIN(t.trade_date),
            MAX(t.trade_date)
        FROM trades t
        GROUP BY t.insider_id, t.ticker
    """)
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM insider_companies").fetchone()[0]
    logger.info("Rebuilt %d insider-company mappings", count)


def run_validation(
    conn: sqlite3.Connection,
    dry_run: bool = False,
    ticker: str = None,
):
    """
    Main validation pipeline:
      1. Find all suspect trades
      2. Attempt auto-correction where possible
      3. Quarantine unfixable trades
    """
    if not dry_run:
        ensure_schema(conn)
    suspects = find_suspect_trades(conn, ticker)

    if not suspects:
        logger.info("No suspect trades found%s", f" for {ticker}" if ticker else "")
        return

    logger.info("Found %d suspect trades%s", len(suspects), f" for {ticker}" if ticker else "")

    corrected_count = 0
    quarantined_count = 0

    for trade in suspects:
        correction = attempt_correction(trade)

        if correction:
            if dry_run:
                logger.info(
                    "  [WOULD CORRECT] %s %s: $%.2f -> $%.4f (%s)",
                    trade["ticker"], trade["trade_date"],
                    trade["price"], correction["corrected_price"],
                    correction["method"],
                )
            else:
                try:
                    correct_trade(conn, trade, correction)
                    corrected_count += 1
                except sqlite3.IntegrityError:
                    # Corrected value duplicates an existing trade — quarantine instead
                    quarantine_trade(conn, trade)
                    quarantined_count += 1
        else:
            if dry_run:
                logger.info(
                    "  [WOULD QUARANTINE] %s %s: $%.2f (no correction possible, %s)",
                    trade["ticker"], trade["trade_date"],
                    trade["price"], trade["reason"],
                )
            else:
                quarantine_trade(conn, trade)
                quarantined_count += 1

    if not dry_run:
        conn.commit()

        # Rebuild insider_companies aggregates so company pages reflect corrections
        if corrected_count > 0 or quarantined_count > 0:
            _rebuild_insider_companies(conn)

    logger.info(
        "Validation complete: %d corrected, %d quarantined, %d total suspect",
        corrected_count, quarantined_count, len(suspects),
    )


def main():
    parser = argparse.ArgumentParser(description="Validate trade prices against market data")
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't modify DB")
    parser.add_argument("--ticker", type=str, help="Validate a specific ticker only")
    parser.add_argument("--stats", action="store_true", help="Show validation stats and exit")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    if not (args.dry_run or args.stats):
        ensure_schema(conn)

    if args.dry_run or args.stats:
        # Open read-only to avoid lock conflicts with the API server
        conn.close()
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)

    if args.stats:
        print_stats(conn)
    else:
        run_validation(conn, dry_run=args.dry_run, ticker=args.ticker)
        if not args.dry_run:
            print_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
