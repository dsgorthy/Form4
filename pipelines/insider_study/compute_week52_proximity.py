#!/usr/bin/env python3
"""
Compute week52_proximity for all P/S trades with price > 0.

proximity = (trade_price - 52w_low) / (52w_high - 52w_low)
0.0 = at 52-week low, 1.0 = at 52-week high.

Uses daily_prices table for rolling 252-day high/low per ticker.
"""

import sqlite3
import time
from collections import defaultdict

from pathlib import Path as _Path

DB_PATH = "/Users/openclaw/trading-framework/strategies/insider_catalog/insiders.db"
PRICES_DB = _Path(DB_PATH).parent / "prices.db"  # daily_prices, option_prices
BATCH_SIZE = 50_000
WINDOW = 252  # trading days in a year


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    cur = conn.cursor()

    # Add column if not exist
    existing = {r[1] for r in cur.execute("PRAGMA table_info(trades)").fetchall()}
    if "week52_proximity" not in existing:
        cur.execute("ALTER TABLE trades ADD COLUMN week52_proximity REAL")
        print("Added column: week52_proximity")
        conn.commit()

    # Get all P/S trades with price > 0, grouped by ticker
    print("Loading trades...")
    t0 = time.time()
    cur.execute("""
        SELECT trade_id, ticker, trade_date, price
        FROM trades
        WHERE trans_code IN ('P', 'S') AND price > 0
        ORDER BY ticker, trade_date
    """)
    all_trades = cur.fetchall()
    print(f"Loaded {len(all_trades):,} P/S trades in {time.time()-t0:.1f}s")

    trades_by_ticker = defaultdict(list)
    for trade_id, ticker, trade_date, price in all_trades:
        trades_by_ticker[ticker].append((trade_id, trade_date, price))

    print(f"Unique tickers with trades: {len(trades_by_ticker):,}")

    # Process per ticker
    updates = []
    tickers_with_prices = 0
    tickers_no_prices = 0
    trades_matched = 0
    trades_no_match = 0
    trades_no_range = 0
    t0 = time.time()

    ticker_list = sorted(trades_by_ticker.keys())
    for idx, ticker in enumerate(ticker_list):
        ticker_trades = trades_by_ticker[ticker]

        # Load daily prices for this ticker
        cur.execute("""
            SELECT date, high, low
            FROM daily_prices
            WHERE ticker = ?
            ORDER BY date
        """, (ticker,))
        prices = cur.fetchall()

        if not prices:
            tickers_no_prices += 1
            trades_no_match += len(ticker_trades)
            continue

        tickers_with_prices += 1

        # Build rolling 252-day high/low lookup
        # Store as dict: date -> (52w_high, 52w_low)
        highs = [p[1] for p in prices]
        lows = [p[2] for p in prices]
        dates = [p[0] for p in prices]

        # Compute rolling max high and min low over 252 days
        # Require: (a) at least 100 trading days in the window, AND
        # (b) the window spans no more than 400 calendar days (catches halted stocks with gaps)
        MIN_DAYS = 100
        MAX_CALENDAR_SPAN = 400
        week52 = {}
        for i in range(len(dates)):
            start = max(0, i - WINDOW + 1)
            window_size = i - start + 1
            if window_size < MIN_DAYS:
                continue
            # Check for large gaps: calendar span of the window shouldn't exceed ~400 days
            from datetime import datetime
            d_start = datetime.strptime(dates[start], "%Y-%m-%d")
            d_end = datetime.strptime(dates[i], "%Y-%m-%d")
            if (d_end - d_start).days > MAX_CALENDAR_SPAN:
                continue  # trading was halted/sparse — range is unreliable
            w_high = max(highs[start : i + 1])
            w_low = min(lows[start : i + 1])
            week52[dates[i]] = (w_high, w_low)

        # Match trades to dates
        for trade_id, trade_date, price in ticker_trades:
            if trade_date in week52:
                w_high, w_low = week52[trade_date]
                if w_high > w_low:
                    proximity = (price - w_low) / (w_high - w_low)
                    # Clamp to [0, 1] — trade price may exceed intraday high/low
                    proximity = max(0.0, min(1.0, proximity))
                    updates.append((proximity, trade_id))
                    trades_matched += 1
                else:
                    trades_no_range += 1
            else:
                # Try nearest prior date
                # Binary search for the closest date <= trade_date
                lo, hi = 0, len(dates) - 1
                best = None
                while lo <= hi:
                    mid = (lo + hi) // 2
                    if dates[mid] <= trade_date:
                        best = mid
                        lo = mid + 1
                    else:
                        hi = mid - 1

                if best is not None and (len(trade_date) == 10) and dates[best] in week52:
                    # Use nearest prior date's 52w range if within 5 days
                    from datetime import datetime, timedelta
                    td = datetime.strptime(trade_date, "%Y-%m-%d")
                    pd = datetime.strptime(dates[best], "%Y-%m-%d")
                    if (td - pd).days <= 5:
                        w_high, w_low = week52[dates[best]]
                        if w_high > w_low:
                            proximity = (price - w_low) / (w_high - w_low)
                            proximity = max(0.0, min(1.0, proximity))
                            updates.append((proximity, trade_id))
                            trades_matched += 1
                        else:
                            trades_no_range += 1
                    else:
                        trades_no_match += 1
                else:
                    trades_no_match += 1

        if (idx + 1) % 500 == 0:
            elapsed = time.time() - t0
            print(f"  Processed {idx+1:,} / {len(ticker_list):,} tickers, {trades_matched:,} matched ({elapsed:.1f}s)")

    elapsed = time.time() - t0
    print(f"\nComputed all tickers in {elapsed:.1f}s")
    print(f"  Tickers with prices: {tickers_with_prices:,}")
    print(f"  Tickers without prices: {tickers_no_prices:,}")
    print(f"  Trades matched: {trades_matched:,}")
    print(f"  Trades no date match: {trades_no_match:,}")
    print(f"  Trades no range (high==low): {trades_no_range:,}")

    # Batch update
    print(f"\nWriting {len(updates):,} updates to database...")
    t0 = time.time()
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i : i + BATCH_SIZE]
        cur.executemany(
            "UPDATE trades SET week52_proximity = ? WHERE trade_id = ?",
            batch,
        )
        conn.commit()
        print(f"  Written {min(i + BATCH_SIZE, len(updates)):,} / {len(updates):,}")

    elapsed = time.time() - t0
    print(f"All updates written in {elapsed:.1f}s")

    # Stats
    cur.execute("SELECT COUNT(*) FROM trades WHERE week52_proximity IS NOT NULL")
    print(f"\nTrades with week52_proximity: {cur.fetchone()[0]:,}")
    cur.execute("SELECT AVG(week52_proximity) FROM trades WHERE week52_proximity IS NOT NULL")
    print(f"Mean proximity: {cur.fetchone()[0]:.4f}")
    cur.execute("""
        SELECT
            CASE WHEN week52_proximity < 0.2 THEN '0.0-0.2 (near low)'
                 WHEN week52_proximity < 0.4 THEN '0.2-0.4'
                 WHEN week52_proximity < 0.6 THEN '0.4-0.6'
                 WHEN week52_proximity < 0.8 THEN '0.6-0.8'
                 ELSE '0.8-1.0 (near high)' END AS bucket,
            COUNT(*)
        FROM trades
        WHERE week52_proximity IS NOT NULL
        GROUP BY bucket ORDER BY bucket
    """)
    print("\nProximity distribution:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    # Buys vs sells near extremes
    cur.execute("""
        SELECT trade_type,
               AVG(week52_proximity),
               COUNT(*)
        FROM trades
        WHERE week52_proximity IS NOT NULL AND trans_code IN ('P','S')
        GROUP BY trade_type
    """)
    print("\nMean proximity by trade type:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:.4f} (n={row[2]:,})")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
