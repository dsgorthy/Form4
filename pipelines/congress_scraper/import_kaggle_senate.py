"""One-time import of Kaggle Senate financial disclosure data.

Fills the Senate gap from 2020-12 to 2023-02 where senate_watcher stopped
and Capitol Trades hadn't started yet.

Usage:
    python3 pipelines/congress_scraper/import_kaggle_senate.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"
CSV_PATH = Path.home() / "Downloads" / "kaggle_senate" / "senate_stock_discosures.csv"

SOURCE = "kaggle_senate"

# Only import records in our gap period
GAP_START = "2020-12-01"
GAP_END = "2023-03-01"

TX_MAP = {
    "Purchase": "buy",
    "Sale (Full)": "sell",
    "Sale (Partial)": "sell",
    "Exchange": "exchange",
}

OWNER_MAP = {
    "Self": "Self",
    "Spouse": "Spouse",
    "Joint": "Joint",
    "Child": "Child",
}


def normalize_name(name: str) -> str:
    name = re.sub(
        r"\b(Hon\.?|Senator|Sen\.?|Jr\.?|Sr\.?|III|II|IV)\b",
        "",
        name,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s+", " ", name.strip().lower())


def get_or_create_politician(
    conn: sqlite3.Connection,
    first_name: str,
    last_name: str,
    cache: dict[str, int],
) -> int:
    display = f"{first_name} {last_name}".strip()
    norm = normalize_name(display)

    if norm in cache:
        return cache[norm]

    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'Senate'",
        (norm,),
    ).fetchone()

    if row:
        cache[norm] = row[0]
        return row[0]

    # Try last-name-first format
    norm_rev = normalize_name(f"{last_name}, {first_name}")
    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'Senate'",
        (norm_rev,),
    ).fetchone()

    if row:
        cache[norm] = row[0]
        return row[0]

    # Try fuzzy match on last name
    row = conn.execute(
        "SELECT politician_id, name_normalized FROM politicians WHERE chamber = 'Senate' AND name_normalized LIKE ?",
        (f"%{normalize_name(last_name)}%",),
    ).fetchone()

    if row:
        cache[norm] = row[0]
        return row[0]

    # Create new
    conn.execute(
        "INSERT INTO politicians (name, name_normalized, chamber) VALUES (?, ?, 'Senate')",
        (display, norm),
    )
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    cache[norm] = pid
    return pid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--csv", type=str, default=str(CSV_PATH))
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    with open(csv_path) as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows):,} rows from {csv_path.name}")

    # Filter to gap period
    gap_rows = []
    for r in rows:
        try:
            d = datetime.strptime(r["transaction_date"], "%m/%d/%Y")
            iso = d.strftime("%Y-%m-%d")
            if GAP_START <= iso < GAP_END:
                r["_trade_date"] = iso
                gap_rows.append(r)
        except ValueError:
            continue

    print(f"Records in gap period ({GAP_START} to {GAP_END}): {len(gap_rows):,}")

    if args.dry_run:
        print("Dry run — not writing to DB")
        from collections import Counter

        by_month = Counter(r["_trade_date"][:7] for r in gap_rows)
        for m in sorted(by_month):
            print(f"  {m}: {by_month[m]}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    politician_cache: dict[str, int] = {}
    inserted = 0
    skipped = 0
    errors = 0

    for r in gap_rows:
        ticker = r.get("ticker", "").strip()
        if not ticker or not re.match(r"^[A-Z]{1,5}$", ticker):
            skipped += 1
            continue

        trade_type = TX_MAP.get(r.get("transaction", ""))
        if not trade_type:
            skipped += 1
            continue

        pid = get_or_create_politician(
            conn, r.get("first_name", ""), r.get("last_name", ""), politician_cache
        )

        value_low = int(r["asset_value_low"]) if r.get("asset_value_low") else None
        value_high = int(r["asset_value_high"]) if r.get("asset_value_high") else None
        value_estimate = (
            (value_low + value_high) // 2 if value_low and value_high else None
        )

        owner = OWNER_MAP.get(r.get("owner", ""), r.get("owner"))

        try:
            conn.execute(
                """INSERT OR IGNORE INTO congress_trades
                   (politician_id, ticker, company, asset_type, trade_type,
                    trade_date, value_low, value_high, value_estimate,
                    filing_date, owner, source)
                   VALUES (?, ?, ?, 'stock', ?, ?, ?, ?, ?, NULL, ?, ?)""",
                (
                    pid,
                    ticker,
                    r.get("asset_name", ""),
                    trade_type,
                    r["_trade_date"],
                    value_low,
                    value_high,
                    value_estimate,
                    owner,
                    SOURCE,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                inserted += 1
            else:
                skipped += 1
        except sqlite3.Error as e:
            print(f"  DB error: {e}")
            errors += 1

    conn.commit()
    conn.close()

    print(f"\nDone: {inserted} inserted, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    main()
