#!/usr/bin/env python3
"""
Migrate option EOD data from theta_cache.db into insiders.db option_prices table.

Reads all opt_eod_daily entries from the cache, parses the JSON, and inserts
structured rows into option_prices. Also populates option_pull_status from
event_done checkpoints.

Safe to re-run — uses INSERT OR IGNORE on the unique constraint.
"""

import json
import os
import sqlite3
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DB = os.path.join(SCRIPT_DIR, "data", "theta_cache.db")
INSIDERS_DB = os.path.normpath(os.path.join(
    os.path.dirname(SCRIPT_DIR), os.pardir,
    "strategies", "insider_catalog", "insiders.db",
))
PRICES_DB = os.path.join(os.path.dirname(INSIDERS_DB), "prices.db")


def migrate_eod_prices():
    """Migrate opt_eod_daily cache entries to option_prices table."""
    cache_conn = sqlite3.connect(CACHE_DB)
    ins_conn = sqlite3.connect(INSIDERS_DB)
    ins_conn.execute("PRAGMA journal_mode=WAL")
    ins_conn.execute("PRAGMA synchronous=NORMAL")
    if os.path.exists(PRICES_DB):
        ins_conn.execute(f"ATTACH DATABASE '{PRICES_DB}' AS prices")

    # Count total
    total = cache_conn.execute(
        "SELECT COUNT(*) FROM cache WHERE cache_key LIKE 'opt_eod_daily|%'"
    ).fetchone()[0]
    print(f"Migrating {total:,} EOD cache entries to option_prices...")

    cursor = cache_conn.execute(
        "SELECT cache_key, response_json FROM cache WHERE cache_key LIKE 'opt_eod_daily|%'"
    )

    inserted = 0
    skipped = 0
    errors = 0
    batch = []
    batch_size = 5000
    t0 = time.monotonic()

    for cache_key, response_json in cursor:
        if not response_json:
            skipped += 1
            continue

        try:
            records = json.loads(response_json)
        except json.JSONDecodeError:
            errors += 1
            continue

        if not records or not isinstance(records, list):
            skipped += 1
            continue

        # Parse cache key: opt_eod_daily|TICKER|EXP|STRIKE|RIGHT|START|END
        parts = cache_key.split("|")
        if len(parts) < 5:
            errors += 1
            continue

        ticker = parts[1]

        for r in records:
            # Extract trade_date from 'created' timestamp
            created = r.get("created", "")
            if not created:
                continue
            trade_date = created[:10]

            try:
                strike = float(r.get("strike", 0))
                right_raw = r.get("right", "")
                right = "C" if right_raw == "CALL" else "P" if right_raw == "PUT" else right_raw
                expiration = r.get("expiration", "")

                batch.append((
                    ticker,
                    expiration,
                    strike,
                    right,
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
                errors += 1
                continue

        if len(batch) >= batch_size:
            ins_conn.executemany(
                """INSERT OR IGNORE INTO option_prices
                   (ticker, expiration, strike, right, trade_date,
                    open, high, low, close, volume, bid, ask,
                    bid_size, ask_size, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                batch,
            )
            ins_conn.commit()
            inserted += len(batch)
            batch = []
            elapsed = time.monotonic() - t0
            rate = inserted / elapsed if elapsed > 0 else 0
            print(f"  {inserted:,} rows inserted ({rate:,.0f}/sec)...", end="\r")

    # Flush remaining
    if batch:
        ins_conn.executemany(
            """INSERT OR IGNORE INTO option_prices
               (ticker, expiration, strike, right, trade_date,
                open, high, low, close, volume, bid, ask,
                bid_size, ask_size, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            batch,
        )
        ins_conn.commit()
        inserted += len(batch)

    elapsed = time.monotonic() - t0
    print(f"\n  Done: {inserted:,} rows inserted, {skipped:,} empty, {errors:,} errors in {elapsed:.1f}s")

    cache_conn.close()
    ins_conn.close()
    return inserted


def migrate_pull_status():
    """Migrate event_done checkpoints to option_pull_status table."""
    cache_conn = sqlite3.connect(CACHE_DB)
    ins_conn = sqlite3.connect(INSIDERS_DB)
    if os.path.exists(PRICES_DB):
        ins_conn.execute(f"ATTACH DATABASE '{PRICES_DB}' AS prices")

    cursor = cache_conn.execute(
        "SELECT cache_key, response_json FROM cache WHERE cache_key LIKE 'event_done|%'"
    )

    batch = []
    for cache_key, response_json in cursor:
        # event_done|TICKER|DATE|TYPE
        parts = cache_key.split("|")
        if len(parts) < 4:
            continue

        ticker = parts[1]
        trade_date = parts[2]
        trade_type = parts[3]

        try:
            data = json.loads(response_json) if response_json else {}
        except json.JSONDecodeError:
            data = {}

        batch.append((
            ticker,
            trade_date,
            trade_type,
            data.get("ok", 0),
            data.get("total", 0) - data.get("ok", 0),
        ))

    ins_conn.executemany(
        """INSERT OR REPLACE INTO option_pull_status
           (ticker, trade_date, trade_type, contracts_found, contracts_empty)
           VALUES (?, ?, ?, ?, ?)""",
        batch,
    )
    ins_conn.commit()
    print(f"  Migrated {len(batch):,} event checkpoints to option_pull_status")

    cache_conn.close()
    ins_conn.close()


def verify():
    """Print verification stats."""
    conn = sqlite3.connect(INSIDERS_DB)
    if os.path.exists(PRICES_DB):
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")

    total = conn.execute("SELECT COUNT(*) FROM option_prices").fetchone()[0]
    tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM option_prices").fetchone()[0]
    date_range = conn.execute(
        "SELECT MIN(trade_date), MAX(trade_date) FROM option_prices"
    ).fetchone()
    pull_status = conn.execute("SELECT COUNT(*) FROM option_pull_status").fetchone()[0]
    by_type = conn.execute(
        "SELECT trade_type, COUNT(*), SUM(contracts_found) FROM option_pull_status GROUP BY trade_type"
    ).fetchall()

    print(f"\n{'='*60}")
    print(f"  option_prices:      {total:,} rows")
    print(f"  tickers:            {tickers:,}")
    print(f"  date range:         {date_range[0]} to {date_range[1]}")
    print(f"  option_pull_status: {pull_status:,} events")
    for trade_type, cnt, contracts in by_type:
        print(f"    {trade_type}: {cnt:,} events, {contracts:,} contracts with data")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    print("=== Migrating theta_cache.db → insiders.db ===\n")
    print("[1/3] Migrating EOD option prices...")
    migrate_eod_prices()
    print("\n[2/3] Migrating pull status checkpoints...")
    migrate_pull_status()
    print("\n[3/3] Verifying...")
    verify()
