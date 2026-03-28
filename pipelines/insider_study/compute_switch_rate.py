#!/usr/bin/env python3
"""
Compute insider_switch_rate and is_rare_reversal for all trades >= 2016.

Point-in-time: for each trade event, only considers the insider's prior trades.
Groups by filing_key to avoid counting lot-splits as separate events.
"""

import sqlite3
import time
from collections import defaultdict

DB_PATH = "/Users/openclaw/trading-framework/strategies/insider_catalog/insiders.db"
BATCH_SIZE = 50_000
MIN_DATE = "2016-01-01"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")  # 200MB cache
    cur = conn.cursor()

    # Add columns if not exist
    existing = {r[1] for r in cur.execute("PRAGMA table_info(trades)").fetchall()}
    if "insider_switch_rate" not in existing:
        cur.execute("ALTER TABLE trades ADD COLUMN insider_switch_rate REAL")
        print("Added column: insider_switch_rate")
    if "is_rare_reversal" not in existing:
        cur.execute("ALTER TABLE trades ADD COLUMN is_rare_reversal INTEGER DEFAULT 0")
        print("Added column: is_rare_reversal")
    conn.commit()

    # Load all trades grouped by insider, ordered by filing_date
    # We need: trade_id, insider_id, trade_type, filing_date, filing_key
    print("Loading trades...")
    t0 = time.time()
    cur.execute("""
        SELECT trade_id, insider_id, trade_type, filing_date, filing_key
        FROM trades
        WHERE filing_date >= ?
        ORDER BY insider_id, filing_date, trade_id
    """, (MIN_DATE,))
    all_trades = cur.fetchall()
    print(f"Loaded {len(all_trades):,} trades in {time.time()-t0:.1f}s")

    # Group by insider
    insider_trades = defaultdict(list)
    for row in all_trades:
        trade_id, insider_id, trade_type, filing_date, filing_key = row
        insider_trades[insider_id].append(row)
    print(f"Unique insiders: {len(insider_trades):,}")

    # Compute switch_rate and rare_reversal for each trade
    updates = []  # (switch_rate, is_rare_reversal, trade_id)
    processed = 0
    t0 = time.time()

    for insider_id, trades in insider_trades.items():
        # Deduplicate by filing_key to get events (not lots)
        # For each event, take the first trade's direction
        seen_keys = {}  # filing_key -> (direction, filing_date)
        events_order = []  # list of (filing_key, direction, filing_date)
        trade_to_event_idx = {}  # trade_id -> index into events_order at time of that trade

        # First pass: identify unique events in order
        for trade_id, _, trade_type, filing_date, filing_key in trades:
            key = filing_key if filing_key else f"_nofk_{trade_id}"
            if key not in seen_keys:
                direction = "buy" if trade_type == "buy" else "sell"
                seen_keys[key] = (direction, filing_date)
                events_order.append((key, direction, filing_date))

        # Second pass: for each trade, compute PIT metrics
        # Build event list incrementally
        event_idx = 0
        key_to_event_idx = {}
        for i, (key, direction, fdate) in enumerate(events_order):
            key_to_event_idx[key] = i

        for trade_id, _, trade_type, filing_date, filing_key in trades:
            key = filing_key if filing_key else f"_nofk_{trade_id}"
            current_event_idx = key_to_event_idx[key]
            current_direction = "buy" if trade_type == "buy" else "sell"

            # PIT: only events before this one
            prior_events = events_order[:current_event_idx]

            if len(prior_events) < 1:
                # No prior history
                updates.append((None, 0, trade_id))
            else:
                # Compute switch rate: count direction changes / (n_events - 1)
                prior_directions = [d for _, d, _ in prior_events]
                n_prior = len(prior_directions)

                if n_prior < 2:
                    switch_rate = 0.0
                else:
                    switches = sum(
                        1 for i in range(1, n_prior)
                        if prior_directions[i] != prior_directions[i - 1]
                    )
                    switch_rate = switches / (n_prior - 1)

                # Rare reversal: last 5+ events all same direction, this trade is opposite
                is_rare = 0
                if n_prior >= 5:
                    last_dir = prior_directions[-1]
                    # Count consecutive same-direction events from the end
                    streak = 0
                    for d in reversed(prior_directions):
                        if d == last_dir:
                            streak += 1
                        else:
                            break
                    if streak >= 5 and current_direction != last_dir:
                        is_rare = 1

                updates.append((switch_rate, is_rare, trade_id))

            processed += 1
            if processed % 200_000 == 0:
                elapsed = time.time() - t0
                print(f"  Computed {processed:,} trades ({elapsed:.1f}s)")

    print(f"Computed all {processed:,} trades in {time.time()-t0:.1f}s")

    # Batch update
    print("Writing updates to database...")
    t0 = time.time()
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i : i + BATCH_SIZE]
        cur.executemany(
            "UPDATE trades SET insider_switch_rate = ?, is_rare_reversal = ? WHERE trade_id = ?",
            batch,
        )
        conn.commit()
        print(f"  Written {min(i + BATCH_SIZE, len(updates)):,} / {len(updates):,}")

    elapsed = time.time() - t0
    print(f"All updates written in {elapsed:.1f}s")

    # Stats
    cur.execute("SELECT COUNT(*) FROM trades WHERE insider_switch_rate IS NOT NULL")
    print(f"\nTrades with switch_rate: {cur.fetchone()[0]:,}")
    cur.execute("SELECT AVG(insider_switch_rate) FROM trades WHERE insider_switch_rate IS NOT NULL")
    print(f"Mean switch_rate: {cur.fetchone()[0]:.4f}")
    cur.execute("SELECT COUNT(*) FROM trades WHERE is_rare_reversal = 1")
    print(f"Rare reversals: {cur.fetchone()[0]:,}")
    cur.execute("""
        SELECT
            CASE WHEN insider_switch_rate < 0.1 THEN '0.0-0.1'
                 WHEN insider_switch_rate < 0.2 THEN '0.1-0.2'
                 WHEN insider_switch_rate < 0.3 THEN '0.2-0.3'
                 WHEN insider_switch_rate < 0.5 THEN '0.3-0.5'
                 ELSE '0.5+' END AS bucket,
            COUNT(*)
        FROM trades
        WHERE insider_switch_rate IS NOT NULL
        GROUP BY bucket ORDER BY bucket
    """)
    print("\nSwitch rate distribution:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
