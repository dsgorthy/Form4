"""
Import Senate stock transaction data from senate-stock-watcher-data.

Downloads all_transactions.json from GitHub, parses each record,
and inserts into the congress_trades / politicians tables in insiders.db.

Usage:
    python3 pipelines/insider_study/import_senate.py
"""

import json
import re
import sqlite3
import urllib.request
from pathlib import Path
from typing import Optional, Tuple

DATA_URL = (
    "https://raw.githubusercontent.com/"
    "timothycarambat/senate-stock-watcher-data/master/aggregate/all_transactions.json"
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "strategies" / "insider_catalog" / "insiders.db"
SCHEMA_PATH = PROJECT_ROOT / "strategies" / "insider_catalog" / "congress_schema.sql"

# Map raw type strings to normalized trade types
TYPE_MAP = {
    "purchase": "buy",
    "sale (full)": "sell",
    "sale (partial)": "sell",
    "sale": "sell",
    "exchange": "exchange",
}

# Normalize owner strings
OWNER_MAP = {
    "self": "Self",
    "spouse": "Spouse",
    "joint": "Joint",
    "child": "Child",
    "dependent": "Child",
}

SKIP_TICKERS = {"--", "N/A", "n/a", "", None}


def normalize_name(name: str) -> str:
    """Lowercase, strip whitespace, collapse spaces."""
    return re.sub(r"\s+", " ", name.strip().lower())


def parse_amount(amount_str: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Parse amount bands like "$1,001 - $15,000" into (low, high, estimate).
    Returns (None, None, None) if unparseable.
    """
    if not amount_str or amount_str.strip() == "--":
        return None, None, None

    # Remove dollar signs and commas, then split on " - "
    cleaned = amount_str.replace("$", "").replace(",", "").strip()
    parts = re.split(r"\s*-\s*", cleaned)

    try:
        if len(parts) == 2:
            low = int(parts[0].strip())
            high = int(parts[1].strip())
            estimate = (low + high) // 2
            return low, high, estimate
        elif len(parts) == 1 and parts[0].strip():
            val = int(parts[0].strip())
            return val, val, val
    except ValueError:
        pass

    return None, None, None


def parse_date(date_str: str) -> Optional[str]:
    """Convert MM/DD/YYYY to YYYY-MM-DD. Returns None on failure."""
    if not date_str or date_str.strip() in ("--", ""):
        return None
    # Handle both MM/DD/YYYY and YYYY-MM-DD (already correct)
    date_str = date_str.strip()
    if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
        return date_str
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    return None


def normalize_owner(owner_str: str) -> Optional[str]:
    """Map owner field to normalized value."""
    if not owner_str or owner_str.strip() in ("--", ""):
        return None
    key = owner_str.strip().lower()
    return OWNER_MAP.get(key, owner_str.strip())


def map_trade_type(raw_type: str) -> Optional[str]:
    """Map raw transaction type to buy/sell/exchange."""
    if not raw_type:
        return None
    return TYPE_MAP.get(raw_type.strip().lower())


def download_data() -> list:
    """Download and parse the JSON transaction data."""
    print(f"Downloading from {DATA_URL} ...")
    req = urllib.request.Request(DATA_URL, headers={"User-Agent": "trading-framework/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
    data = json.loads(raw)
    print(f"Downloaded {len(data):,} raw transactions")
    return data


def init_db(conn: sqlite3.Connection) -> None:
    """Run the congress schema SQL to create tables/indexes."""
    schema_sql = SCHEMA_PATH.read_text()
    conn.executescript(schema_sql)
    conn.commit()


def get_or_create_politician(conn: sqlite3.Connection, name: str, cache: dict) -> int:
    """Get politician_id, creating the record if needed. Uses in-memory cache."""
    name_norm = normalize_name(name)
    if name_norm in cache:
        return cache[name_norm]

    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'Senate'",
        (name_norm,),
    ).fetchone()

    if row:
        cache[name_norm] = row[0]
        return row[0]

    conn.execute(
        "INSERT OR IGNORE INTO politicians (name, name_normalized, chamber) VALUES (?, ?, 'Senate')",
        (name.strip(), name_norm),
    )
    conn.commit()
    row = conn.execute(
        "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'Senate'",
        (name_norm,),
    ).fetchone()
    cache[name_norm] = row[0]
    return row[0]


def import_transactions(data: list) -> dict:
    """Import transactions into the database. Returns stats dict."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    init_db(conn)

    politician_cache = {}
    stats = {"total": len(data), "inserted": 0, "skipped": 0, "errors": 0}

    for i, txn in enumerate(data):
        ticker = (txn.get("ticker") or "").strip()
        if ticker in SKIP_TICKERS:
            stats["skipped"] += 1
            continue

        senator = txn.get("senator", "")
        if not senator or senator.strip() == "--":
            stats["skipped"] += 1
            continue

        trade_type = map_trade_type(txn.get("type", ""))
        if not trade_type:
            stats["skipped"] += 1
            continue

        trade_date = parse_date(txn.get("transaction_date", ""))
        if not trade_date:
            stats["skipped"] += 1
            continue

        politician_id = get_or_create_politician(conn, senator, politician_cache)
        value_low, value_high, value_estimate = parse_amount(txn.get("amount", ""))
        owner = normalize_owner(txn.get("owner", ""))
        asset_type = (txn.get("asset_type") or "stock").strip()
        company = (txn.get("asset_description") or "").strip() or None
        report_url = (txn.get("ptr_link") or "").strip() or None

        # Check for existing trade with same key (COALESCE-safe NULL dedup)
        existing = conn.execute(
            """SELECT 1 FROM congress_trades
               WHERE politician_id = ? AND ticker = ? AND trade_type = ?
                 AND trade_date = ? AND COALESCE(value_low, -1) = COALESCE(?, -1)""",
            (politician_id, ticker, trade_type, trade_date, value_low),
        ).fetchone()
        if existing:
            stats["skipped"] += 1
            continue

        try:
            conn.execute(
                """INSERT OR IGNORE INTO congress_trades
                   (politician_id, ticker, company, asset_type, trade_type,
                    trade_date, value_low, value_high, value_estimate,
                    owner, report_url, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'senate_watcher')""",
                (
                    politician_id,
                    ticker,
                    company,
                    asset_type,
                    trade_type,
                    trade_date,
                    value_low,
                    value_high,
                    value_estimate,
                    owner,
                    report_url,
                ),
            )
            if conn.total_changes:
                stats["inserted"] += 1
        except sqlite3.Error as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                print(f"  DB error on row {i}: {e}")

        if (i + 1) % 2000 == 0:
            conn.commit()
            print(f"  Processed {i + 1:,} / {stats['total']:,} ...")

    conn.commit()

    # Get final counts
    politician_count = conn.execute("SELECT COUNT(*) FROM politicians WHERE chamber = 'Senate'").fetchone()[0]
    trade_count = conn.execute("SELECT COUNT(*) FROM congress_trades WHERE source = 'senate_watcher'").fetchone()[0]

    conn.close()

    stats["politicians_in_db"] = politician_count
    stats["trades_in_db"] = trade_count
    return stats


def main():
    print(f"Database: {DB_PATH}")
    print(f"Schema:   {SCHEMA_PATH}")
    print()

    data = download_data()
    stats = import_transactions(data)

    print()
    print("=" * 50)
    print(f"Total raw transactions:   {stats['total']:,}")
    print(f"Skipped (no ticker/date): {stats['skipped']:,}")
    print(f"Insert attempts:          {stats['total'] - stats['skipped']:,}")
    print(f"Errors:                   {stats['errors']:,}")
    print()
    print(f"Politicians in DB:        {stats['politicians_in_db']:,}")
    print(f"Senate trades in DB:      {stats['trades_in_db']:,}")
    print("=" * 50)


if __name__ == "__main__":
    main()
