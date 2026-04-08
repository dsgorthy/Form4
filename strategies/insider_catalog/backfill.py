#!/usr/bin/env python3
"""
Backfill the insider catalog SQLite DB from existing EDGAR bulk CSV data.

Steps:
  1. Create DB + schema
  2. Import all buy transactions from edgar_bulk_form4.csv (56K rows)
  3. Import all sell transactions from edgar_bulk_form4_sells.csv (398K rows)
  4. Import 7-day forward returns from results_bulk_7d.csv
  5. Compute per-insider track records and scores

Usage:
  python backfill.py                    # full backfill
  python backfill.py --refresh-scores   # only recompute track records
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import re
import sqlite3
import statistics
from pathlib import Path

try:
    from strategies.insider_catalog.normalize_titles import normalize_title as _normalize_title
except ImportError:
    try:
        from normalize_titles import normalize_title as _normalize_title
    except ImportError:
        def _normalize_title(title: str | None) -> str:
            return ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CATALOG_DIR = Path(__file__).resolve().parent
DB_PATH = CATALOG_DIR / "insiders.db"
RESEARCH_DB = DB_PATH.parent / "research.db"  # derivative_trades, filing_footnotes, nonderiv_holdings
SCHEMA_PATH = CATALOG_DIR / "schema.sql"

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "pipelines" / "insider_study" / "data"
BUYS_CSV = DATA_DIR / "edgar_bulk_form4.csv"
SELLS_CSV = DATA_DIR / "edgar_bulk_form4_sells.csv"
RESULTS_7D_CSV = DATA_DIR / "results_bulk_7d.csv"

# ── Title classification (same logic as edgar_monitor.py) ─────────────────

CSUITE_KEYWORDS = [
    "ceo", "chief exec", "chief executive", "co-ceo",
    "cfo", "chief financial", "chief fin",
    "coo", "chief operating",
    "president", "pres",
    "chairman", "chairwoman", "chair", "cob",
    "evp", "executive vp", "executive vice president",
    "svp", "senior vp", "senior vice president",
]

TITLE_WEIGHT_RULES = [
    (["ceo", "chief exec"],                           3.0),
    (["chairman", "exec chair", "executive chair"],   3.0),
    (["cfo", "chief financial"],                      2.5),
    (["president"],                                   2.5),
    (["10% owner", "10 percent owner", "10pct", "tenpercentowner"], 2.5),
    (["coo", "chief operating"],                      2.0),
    (["svp", "evp", "senior vp", "senior vice",
      "exec vp", "executive vp", "executive vice president"], 1.8),
    (["vp", "vice president"],                        1.5),
    (["director", "board", "dir"],                    1.5),
    (["treasurer", "secretary"],                      1.2),
]


ENTITY_KEYWORDS = ['llc', 'l.l.c', 'l.p.', 'trust', 'fund', 'holdings', 'inc.', 'inc,',
                    'corp.', 'corp,', 'corporation', 'capital', 'partners', 'group',
                    'management', 'investments', 'limited', 'ltd', 'enterprise',
                    'associates', 'advisors', 'family', 'estate', 'foundation', 'ventures']


def _is_entity_name(name_normalized: str) -> bool:
    return any(kw in name_normalized for kw in ENTITY_KEYWORDS)


def is_csuite(title: str) -> bool:
    if not title:
        return False
    t = title.lower()
    return any(kw in t for kw in CSUITE_KEYWORDS)


def get_title_weight(title: str) -> float:
    if not title:
        return 1.0
    t = title.lower()
    for keywords, weight in TITLE_WEIGHT_RULES:
        for kw in keywords:
            if kw in t:
                return weight
    return 1.0


def normalize_ticker(raw: str) -> str | None:
    """
    Normalize a ticker symbol from EDGAR/CSV data.

    Handles:
      - Exchange prefixes: NYSE:, OTCQX:, ASX:, etc.
      - Bloomberg suffixes: "AROC US" → "AROC"
      - Parentheses: "(NYSE:FBC)" → "FBC"
      - Dual-class / multi-ticker: "X AND Y" → "X", "X, Y" → "X", "X/Y" → "X"
      - Invalid values: "N/A" → None
      - Whitespace stripping

    Preserves dots in tickers (e.g. FCE.A, MOG.A).
    Returns None for clearly invalid tickers.
    """
    if not raw:
        return None

    t = raw.strip()

    # Remove surrounding parentheses
    t = t.strip("()")

    # Reject obvious non-tickers
    if t.upper() in ("N/A", "NA", ""):
        return None

    # "NONE" = private/unlisted company — preserve as-is for display purposes
    if t.upper() == "NONE":
        return "NONE"

    # Strip known exchange prefixes: "NYSE:", "OTCQX:", "ASX:", "NASDAQ:", etc.
    # Use explicit list to avoid stripping ticker:suffix patterns like "PAYD:OTC"
    EXCHANGE_PREFIXES = (
        "NYSE", "NASDAQ", "AMEX", "OTCQX", "OTCQB", "OTC", "ASX",
        "TSX", "LSE", "BATS", "ARCA", "CBOE", "NSDQ",
    )
    upper = t.upper()
    for pfx in EXCHANGE_PREFIXES:
        if upper.startswith(pfx + ":"):
            t = t[len(pfx) + 1:].lstrip()
            break

    # Handle "X AND Y" → take first part
    if " AND " in t.upper():
        t = re.split(r'\s+AND\s+', t, flags=re.IGNORECASE)[0].strip()

    # Handle "X, Y" → take first part (but not "X.A")
    if ", " in t:
        t = t.split(",")[0].strip()

    # Handle "X; Y" → take first part
    if ";" in t:
        t = t.split(";")[0].strip()

    # Handle "X/Y" → take first part (preserve dots like FCE.A)
    if "/" in t:
        t = t.split("/")[0].strip()

    # Strip Bloomberg-style suffixes: "AROC US" → "AROC"
    # If two space-separated tokens and last is a short alpha suffix, take first
    parts = t.split()
    if len(parts) == 2 and parts[1].isalpha() and len(parts[1]) <= 3:
        t = parts[0]

    # Strip exchange/market suffixes after colon: "PAYD:OTC", "OV6:GR"
    if ":" in t:
        t = t.split(":")[0].strip()

    t = t.strip().upper()

    # Final validation: reject if empty, contains spaces, or looks non-ticker-like
    if not t or " " in t or len(t) > 10:
        return None

    # Reject if all digits (not a ticker)
    if t.isdigit():
        return None

    return t


def normalize_name(name: str) -> str:
    """Normalize insider name for dedup matching."""
    if not name:
        return ""
    # Lowercase, strip extra whitespace, remove suffixes like Jr., III, etc.
    n = name.lower().strip()
    n = re.sub(r'\s+(jr\.?|sr\.?|iii|ii|iv|v)\s*$', '', n)
    n = re.sub(r'\s+', ' ', n)
    return n


def parse_value(val: str) -> float:
    """Parse value strings like '+$113,720' or '-$5,024,700'."""
    if not val:
        return 0.0
    cleaned = re.sub(r'[,$+]', '', val)
    try:
        return abs(float(cleaned))
    except ValueError:
        return 0.0


def parse_qty(val: str) -> int:
    """Parse quantity strings like '+2,000' or '-90,000'."""
    if not val:
        return 0
    cleaned = re.sub(r'[,+]', '', val)
    try:
        return abs(int(float(cleaned)))
    except ValueError:
        return 0


def parse_price(val: str) -> float:
    """Parse price strings like '$56.86'."""
    if not val:
        return 0.0
    cleaned = re.sub(r'[$,]', '', val)
    try:
        return abs(float(cleaned))
    except ValueError:
        return 0.0


def validate_trade_date(trade_date: str, filing_date: str = None) -> str | None:
    """
    Validate and optionally fix a trade date. Returns corrected date or None if unfixable.

    Rules:
      - Year must be 2000-2030
      - Known fixes: 0024→2024, 0025→2025, 2033→2023
      - Cross-reference filing_date if available
    """
    if not trade_date or len(trade_date) < 10:
        return None

    year_str = trade_date[:4]
    try:
        year = int(year_str)
    except ValueError:
        return None

    # Already valid
    if 2000 <= year <= 2030:
        return trade_date

    # Known fixes
    fix_map = {"0024": "2024", "0025": "2025", "2033": "2023"}
    if year_str in fix_map:
        fixed = fix_map[year_str] + trade_date[4:]
        return fixed

    return None


# ── DB Operations ─────────────────────────────────────────────────────────

def create_db(conn: sqlite3.Connection):
    """Create schema from SQL file."""
    schema_sql = SCHEMA_PATH.read_text()
    conn.executescript(schema_sql)
    logger.info("Schema created/verified")


def migrate_schema(conn: sqlite3.Connection):
    """
    Additive schema migration: add new columns to trades table and create
    new tables (derivative_trades, filing_footnotes, nonderiv_holdings,
    insider_ticker_scores, score_history).

    Idempotent — safe to run multiple times.
    """
    # New columns on trades table
    new_columns = [
        ("trans_code", "TEXT"),
        ("trans_acquired_disp", "TEXT"),
        ("direct_indirect", "TEXT"),
        ("shares_owned_after", "REAL"),
        ("value_owned_after", "REAL"),
        ("nature_of_ownership", "TEXT"),
        ("equity_swap", "INTEGER"),
        ("is_10b5_1", "INTEGER"),
        ("security_title", "TEXT"),
        ("deemed_execution_date", "TEXT"),
        ("trans_form_type", "TEXT"),
        ("rptowner_cik", "TEXT"),
        ("signal_quality", "REAL"),
        ("signal_category", "TEXT"),
        ("is_routine", "INTEGER"),
    ]

    added = 0
    for col_name, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")
            added += 1
        except sqlite3.OperationalError:
            pass  # column already exists

    if added:
        logger.info("Added %d new columns to trades table", added)

    # Create new tables (IF NOT EXISTS in schema.sql handles idempotency)
    # Re-run schema to pick up new table definitions
    schema_sql = SCHEMA_PATH.read_text()
    conn.executescript(schema_sql)

    # Add indexes on new trades columns
    index_stmts = [
        "CREATE INDEX IF NOT EXISTS idx_trades_trans_code ON trades(trans_code)",
        "CREATE INDEX IF NOT EXISTS idx_trades_10b5_1 ON trades(is_10b5_1)",
        "CREATE INDEX IF NOT EXISTS idx_trades_signal_quality ON trades(signal_quality)",
        "CREATE INDEX IF NOT EXISTS idx_trades_signal_category ON trades(signal_category)",
        "CREATE INDEX IF NOT EXISTS idx_trades_is_routine ON trades(is_routine)",
        "CREATE INDEX IF NOT EXISTS idx_trades_rptowner_cik ON trades(rptowner_cik)",
    ]
    for stmt in index_stmts:
        conn.execute(stmt)

    conn.commit()
    logger.info("Schema migration complete")


def get_or_create_insider(conn: sqlite3.Connection, name: str, cik: str = None) -> int:
    """Get existing insider_id or create new insider. Returns insider_id."""
    name_norm = normalize_name(name)
    if not name_norm:
        name_norm = "unknown"

    # Try exact match first
    row = conn.execute(
        "SELECT insider_id FROM insiders WHERE name_normalized = ? AND (cik = ? OR cik IS NULL OR ? IS NULL)",
        (name_norm, cik, cik),
    ).fetchone()

    if row:
        return row[0]

    # Insert new — use RETURNING for PG, lastrowid fallback for SQLite
    cur = conn.execute(
        "INSERT INTO insiders (name, name_normalized, cik) VALUES (?, ?, ?) RETURNING insider_id",
        (name, name_norm, cik),
    )
    insider_id = cur.lastrowid
    if insider_id is None:
        # Fallback: query for the row we just inserted
        row = conn.execute(
            "SELECT insider_id FROM insiders WHERE name_normalized = ? AND COALESCE(cik, '') = COALESCE(?, '')",
            (name_norm, cik),
        ).fetchone()
        insider_id = row[0] if row else None

    # Flag entity insiders on insert
    if insider_id and _is_entity_name(name_norm):
        try:
            conn.execute("UPDATE insiders SET is_entity = 1 WHERE insider_id = ?", (insider_id,))
        except Exception:
            pass  # is_entity column may not exist yet

    return insider_id


def import_trades(conn: sqlite3.Connection, csv_path: Path, trade_type: str):
    """Import trades from EDGAR bulk CSV into the trades table."""
    if not csv_path.exists():
        logger.warning("CSV not found: %s", csv_path)
        return

    logger.info("Importing %s trades from %s...", trade_type, csv_path.name)

    # CSV columns: Filing Date,Trade Date,Ticker,Company Name,Insider Name,Title,
    #              Trade Type,Price,Qty,Owned,DeltaOwn,Value,1d,1w,1m,6m
    inserted = 0
    skipped = 0
    batch = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("Insider Name", "").strip()
            raw_ticker = row.get("Ticker", "").strip()
            ticker = normalize_ticker(raw_ticker)
            if not name or not ticker:
                skipped += 1
                continue

            price = parse_price(row.get("Price", ""))
            qty = parse_qty(row.get("Qty", ""))
            value = parse_value(row.get("Value", ""))
            if price <= 0 or qty <= 0:
                skipped += 1
                continue

            title = row.get("Title", "").strip()

            trade_date = row.get("Trade Date", "").strip()
            filing_date_raw = row.get("Filing Date", "").strip()
            trade_date = validate_trade_date(trade_date, filing_date_raw)
            if not trade_date:
                skipped += 1
                continue

            batch.append({
                "name": name,
                "ticker": ticker,
                "company": row.get("Company Name", "").strip(),
                "title": title,
                "trade_type": trade_type,
                "trade_date": trade_date,
                "filing_date": filing_date_raw,
                "price": price,
                "qty": qty,
                "value": value,
                "is_csuite": 1 if is_csuite(title) else 0,
                "title_weight": get_title_weight(title),
                "normalized_title": _normalize_title(title),
            })

            # Flush in batches of 5000
            if len(batch) >= 5000:
                inserted += _flush_batch(conn, batch)
                batch = []

    if batch:
        inserted += _flush_batch(conn, batch)

    conn.commit()
    logger.info("Imported %d %s trades (%d skipped)", inserted, trade_type, skipped)


def _flush_batch(conn: sqlite3.Connection, batch: list) -> int:
    """Insert a batch of trades, creating insiders as needed."""
    inserted = 0
    for row in batch:
        insider_id = get_or_create_insider(conn, row["name"])
        try:
            conn.execute("""
                INSERT OR IGNORE INTO trades
                    (insider_id, ticker, company, title, trade_type, trade_date,
                     filing_date, price, qty, value, is_csuite, title_weight, source,
                     normalized_title)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'edgar_bulk', ?)
            """, (
                insider_id, row["ticker"], row["company"], row["title"],
                row["trade_type"], row["trade_date"], row["filing_date"],
                row["price"], row["qty"], row["value"],
                row["is_csuite"], row["title_weight"],
                row.get("normalized_title", ""),
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # duplicate
    return inserted


def import_returns(conn: sqlite3.Connection):
    """Import 7-day forward returns from results_bulk_7d.csv and match to trades."""
    if not RESULTS_7D_CSV.exists():
        logger.warning("Results CSV not found: %s", RESULTS_7D_CSV)
        return

    logger.info("Importing 7d forward returns from %s...", RESULTS_7D_CSV.name)

    # results_bulk_7d columns include: ticker, filing_date, entry_price, exit_price,
    # trade_return, spy_return, abnormal_return, insider_names, etc.
    matched = 0
    unmatched = 0

    with open(RESULTS_7D_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("ticker", "").strip().upper()
            filing_date = row.get("filing_date", "").strip()
            insider_names_str = row.get("insider_names", "")
            entry_price = float(row.get("entry_price", 0) or 0)
            exit_price = float(row.get("exit_price", 0) or 0)
            trade_return = float(row.get("trade_return", 0) or 0)
            spy_return = float(row.get("spy_return", 0) or 0)
            abnormal_return = float(row.get("abnormal_return", 0) or 0)

            if not ticker or not filing_date:
                continue

            # Match to trades by ticker + filing_date + insider name
            insider_names = [n.strip() for n in insider_names_str.split(";") if n.strip()]

            for name in insider_names:
                name_norm = normalize_name(name)
                # Find the trade(s)
                trades = conn.execute("""
                    SELECT t.trade_id FROM trades t
                    JOIN insiders i ON t.insider_id = i.insider_id
                    WHERE t.ticker = ? AND t.filing_date = ? AND i.name_normalized = ?
                      AND t.trade_type = 'buy'
                """, (ticker, filing_date, name_norm)).fetchall()

                for (trade_id,) in trades:
                    conn.execute("""
                        INSERT OR REPLACE INTO trade_returns
                            (trade_id, entry_price, exit_price_7d, return_7d,
                             spy_return_7d, abnormal_7d)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        trade_id, entry_price, exit_price,
                        trade_return / 100.0 if abs(trade_return) > 1 else trade_return,
                        spy_return / 100.0 if abs(spy_return) > 1 else spy_return,
                        abnormal_return / 100.0 if abs(abnormal_return) > 1 else abnormal_return,
                    ))
                    matched += 1

                if not trades:
                    unmatched += 1

    conn.commit()
    logger.info("Matched %d trade returns (%d unmatched)", matched, unmatched)


def _window_stats(values: list) -> tuple:
    """Compute (win_rate, avg_return, median_return) from a list of returns."""
    if not values:
        return (None, None, None)
    wr = sum(1 for r in values if r > 0) / len(values)
    avg = statistics.mean(values)
    med = statistics.median(values)
    return (wr, avg, med)


def compute_track_records(conn: sqlite3.Connection):
    """Compute per-insider track records across 7d/30d/90d windows."""
    use_dedup = os.environ.get("INSIDER_DEDUP") == "1"
    if use_dedup:
        logger.info("Computing insider track records (multi-window) [DEDUP MODE ACTIVE]...")
    else:
        logger.info("Computing insider track records (multi-window)...")

    # Build helper expressions for dedup mode
    if use_dedup:
        id_where = "COALESCE(t.effective_insider_id, t.insider_id) = ?"
        id_where_no_alias = "COALESCE(effective_insider_id, insider_id) = ?"
    else:
        id_where = "t.insider_id = ?"
        id_where_no_alias = "insider_id = ?"

    # Get all insiders with at least 1 trade
    if use_dedup:
        insiders = conn.execute("""
            SELECT DISTINCT COALESCE(t.effective_insider_id, i.insider_id) as insider_id, i.name
            FROM insiders i
            JOIN trades t ON i.insider_id = t.insider_id
            WHERE i.is_entity = 0 OR t.effective_insider_id = i.insider_id
        """).fetchall()
    else:
        insiders = conn.execute("""
            SELECT DISTINCT i.insider_id, i.name
            FROM insiders i
            JOIN trades t ON i.insider_id = t.insider_id
        """).fetchall()

    records = []

    for insider_id, name in insiders:
        # Buy-side: pull all three windows, grouped by filing
        buy_returns = conn.execute(f"""
            SELECT tr.return_7d, tr.abnormal_7d,
                   tr.return_30d, tr.abnormal_30d,
                   tr.return_90d, tr.abnormal_90d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {id_where} AND t.trade_type = 'buy'
              AND t.trans_code = 'P'
            GROUP BY t.filing_key
        """, (insider_id,)).fetchall()

        buy_trades = conn.execute(f"""
            SELECT COUNT(*), SUM(total_value), MIN(first_date), MAX(last_date)
            FROM (
                SELECT SUM(value) AS total_value, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date
                FROM trades WHERE {id_where_no_alias} AND trade_type = 'buy' AND trans_code = 'P'
                GROUP BY filing_key
            )
        """, (insider_id,)).fetchone()
        buy_count = buy_trades[0] or 0
        buy_total_value = buy_trades[1] or 0
        buy_first = buy_trades[2]
        buy_last = buy_trades[3]

        # 7d stats
        ret_7d = [r[0] for r in buy_returns if r[0] is not None]
        abn_7d = [r[1] for r in buy_returns if r[1] is not None]
        wr_7d, avg_7d, med_7d = _window_stats(ret_7d)
        avg_abn_7d = statistics.mean(abn_7d) if abn_7d else None

        # 30d stats
        ret_30d = [r[2] for r in buy_returns if r[2] is not None]
        abn_30d = [r[3] for r in buy_returns if r[3] is not None]
        wr_30d, avg_30d, _ = _window_stats(ret_30d)
        avg_abn_30d = statistics.mean(abn_30d) if abn_30d else None

        # 90d stats
        ret_90d = [r[4] for r in buy_returns if r[4] is not None]
        abn_90d = [r[5] for r in buy_returns if r[5] is not None]
        wr_90d, avg_90d, _ = _window_stats(ret_90d)
        avg_abn_90d = statistics.mean(abn_90d) if abn_90d else None

        # Determine best window for this insider
        n_7d = len(ret_7d)
        n_30d = len(ret_30d)
        n_90d = len(ret_90d)
        window_qualities = []
        if wr_7d is not None:
            window_qualities.append(("7d", _score_window(wr_7d, avg_abn_7d or avg_7d, n_7d)))
        if wr_30d is not None:
            window_qualities.append(("30d", _score_window(wr_30d, avg_abn_30d or avg_30d, n_30d)))
        if wr_90d is not None:
            window_qualities.append(("90d", _score_window(wr_90d, avg_abn_90d or avg_90d, n_90d)))
        best_window = max(window_qualities, key=lambda x: x[1])[0] if window_qualities else None

        # Sell-side stats (filing-level)
        sell_trades = conn.execute(f"""
            SELECT COUNT(*), SUM(total_value), MIN(first_date), MAX(last_date)
            FROM (
                SELECT SUM(value) AS total_value, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date
                FROM trades WHERE {id_where_no_alias} AND trade_type = 'sell' AND trans_code = 'S'
                GROUP BY filing_key
            )
        """, (insider_id,)).fetchone()
        sell_count = sell_trades[0] or 0
        sell_total_value = sell_trades[1] or 0
        sell_first = sell_trades[2]
        sell_last = sell_trades[3]

        # Sell-side returns (filing-level, all windows, for sells a "win" is when price drops)
        sell_returns = conn.execute(f"""
            SELECT tr.return_7d, tr.abnormal_7d,
                   tr.return_30d, tr.abnormal_30d,
                   tr.return_90d, tr.abnormal_90d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE {id_where} AND t.trade_type = 'sell' AND t.trans_code = 'S'
            GROUP BY t.filing_key
        """, (insider_id,)).fetchall()

        sell_ret_7d = [r[0] for r in sell_returns if r[0] is not None]
        sell_abn_7d = [r[1] for r in sell_returns if r[1] is not None]
        sell_ret_30d = [r[2] for r in sell_returns if r[2] is not None]
        sell_abn_30d = [r[3] for r in sell_returns if r[3] is not None]
        sell_ret_90d = [r[4] for r in sell_returns if r[4] is not None]
        sell_abn_90d = [r[5] for r in sell_returns if r[5] is not None]

        # For sells: "win" = stock declined (return <= 0)
        sell_wr_7d = (sum(1 for r in sell_ret_7d if r <= 0) / len(sell_ret_7d)) if sell_ret_7d else None
        sell_avg_7d = statistics.mean(sell_ret_7d) if sell_ret_7d else None
        sell_wr_30d = (sum(1 for r in sell_ret_30d if r <= 0) / len(sell_ret_30d)) if sell_ret_30d else None
        sell_avg_30d = statistics.mean(sell_ret_30d) if sell_ret_30d else None
        sell_avg_abn_30d = statistics.mean(sell_abn_30d) if sell_abn_30d else None
        sell_wr_90d = (sum(1 for r in sell_ret_90d if r <= 0) / len(sell_ret_90d)) if sell_ret_90d else None
        sell_avg_90d = statistics.mean(sell_ret_90d) if sell_ret_90d else None
        sell_avg_abn_90d = statistics.mean(sell_abn_90d) if sell_abn_90d else None

        # Primary title/ticker (skip junk titles)
        title_row = conn.execute(f"""
            SELECT COALESCE(normalized_title, title) AS t, COUNT(*) as cnt FROM trades
            WHERE {id_where_no_alias} AND COALESCE(normalized_title, title) != '' AND COALESCE(normalized_title, title) IS NOT NULL
              AND COALESCE(normalized_title, title) NOT IN ('See Remarks', 'Other', 'Unknown', 'See Remark')
            GROUP BY t ORDER BY cnt DESC LIMIT 1
        """, (insider_id,)).fetchone()
        primary_title = title_row[0] if title_row else None

        ticker_row = conn.execute(f"""
            SELECT ticker, COUNT(*) as cnt FROM trades
            WHERE {id_where_no_alias}
            GROUP BY ticker ORDER BY cnt DESC LIMIT 1
        """, (insider_id,)).fetchone()
        primary_ticker = ticker_row[0] if ticker_row else None

        n_tickers = conn.execute(
            f"SELECT COUNT(DISTINCT ticker) FROM trades WHERE {id_where_no_alias}",
            (insider_id,),
        ).fetchone()[0]

        records.append((
            insider_id, buy_count, wr_7d, avg_7d,
            med_7d, avg_abn_7d,
            wr_30d, avg_30d, wr_90d, avg_90d,
            buy_total_value, buy_first, buy_last,
            sell_count, sell_wr_7d, sell_avg_7d, sell_total_value, sell_first, sell_last,
            primary_title, primary_ticker, n_tickers,
            best_window, avg_abn_30d, avg_abn_90d,
            sell_wr_30d, sell_avg_30d, sell_avg_abn_30d,
            sell_wr_90d, sell_avg_90d, sell_avg_abn_90d,
        ))

    # Insert track records
    conn.execute("DELETE FROM insider_track_records")
    conn.executemany("""
        INSERT INTO insider_track_records
            (insider_id, buy_count, buy_win_rate_7d, buy_avg_return_7d,
             buy_median_return_7d, buy_avg_abnormal_7d,
             buy_win_rate_30d, buy_avg_return_30d, buy_win_rate_90d, buy_avg_return_90d,
             buy_total_value, buy_first_date, buy_last_date,
             sell_count, sell_win_rate_7d, sell_avg_return_7d,
             sell_total_value, sell_first_date, sell_last_date,
             primary_title, primary_ticker, n_tickers,
             best_window, buy_avg_abnormal_30d, buy_avg_abnormal_90d,
             sell_win_rate_30d, sell_avg_return_30d, sell_avg_abnormal_30d,
             sell_win_rate_90d, sell_avg_return_90d, sell_avg_abnormal_90d)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    logger.info("Computed track records for %d insiders", len(records))

    # Now compute scores and percentiles
    _compute_scores(conn)

    # Build insider_companies mapping
    _build_insider_companies(conn)


def _score_window(wr: float, avg_abn: float, n: int = 0) -> float:
    """
    Compute a single-window quality score (0 to 1).
    Uses N-adjusted Sharpe-like metric on abnormal returns:
      - Excess win rate above 40% baseline
      - Abnormal return magnitude
      - N-penalty: (1 - 2/N) so small samples are penalized
    """
    if wr is None or avg_abn is None or n < 3:
        return 0.0
    wr_part = max(0, (wr - 0.4)) * 2.5       # 0.4 = 0, 0.8 = 1.0
    ret_part = max(0, min(1.0, avg_abn * 10 + 0.5))  # 0% = 0.5, +5% = 1.0
    n_confidence = max(0, 1.0 - 2.0 / n)     # N=3: 0.33, N=5: 0.6, N=10: 0.8
    return (wr_part * 0.5 + ret_part * 0.5) * n_confidence


def _compute_scores(conn: sqlite3.Connection):
    """
    Compute composite score (0-3) using multi-window evaluation.

    For each insider, evaluates 7d, 30d, and 90d windows. The score uses
    the BEST performing window (highest quality) as the primary signal,
    with a consistency bonus if the insider performs well across multiple windows.

    Score factors:
      - Best-window quality: WR + abnormal return at strongest window (40%)
      - Longer-horizon bonus: 30d/90d alpha is harder to fake (15%)
      - Multi-window consistency: positive across 2+ windows (15%)
      - Trade frequency: log-scaled count (15%)
      - Window breadth: data across all 3 windows (10%)
      - Trade size: log-scaled total $ value (5%)
    """
    logger.info("Computing multi-window scores and percentiles...")

    scoreable = conn.execute("""
        SELECT tr.insider_id, tr.buy_count,
               tr.buy_win_rate_7d, tr.buy_avg_return_7d, tr.buy_avg_abnormal_7d,
               tr.buy_win_rate_30d, tr.buy_avg_return_30d, tr.buy_avg_abnormal_30d,
               tr.buy_win_rate_90d, tr.buy_avg_return_90d, tr.buy_avg_abnormal_90d,
               tr.buy_total_value,
               -- per-window N counts
               (SELECT COUNT(*) FROM trades t2 JOIN trade_returns r2 ON t2.trade_id = r2.trade_id
                WHERE t2.insider_id = tr.insider_id AND t2.trade_type = 'buy' AND r2.abnormal_7d IS NOT NULL) as n7,
               (SELECT COUNT(*) FROM trades t2 JOIN trade_returns r2 ON t2.trade_id = r2.trade_id
                WHERE t2.insider_id = tr.insider_id AND t2.trade_type = 'buy' AND r2.abnormal_30d IS NOT NULL) as n30,
               (SELECT COUNT(*) FROM trades t2 JOIN trade_returns r2 ON t2.trade_id = r2.trade_id
                WHERE t2.insider_id = tr.insider_id AND t2.trade_type = 'buy' AND r2.abnormal_90d IS NOT NULL) as n90
        FROM insider_track_records tr
        WHERE tr.buy_count >= 3
          AND (tr.buy_win_rate_7d IS NOT NULL
               OR tr.buy_win_rate_30d IS NOT NULL
               OR tr.buy_win_rate_90d IS NOT NULL)
    """).fetchall()

    if not scoreable:
        logger.warning("No scoreable insiders found")
        return

    raw_scores = []
    for row in scoreable:
        (insider_id, count,
         wr7, avg7, abn7,
         wr30, avg30, abn30,
         wr90, avg90, abn90,
         total_val,
         n7, n30, n90) = row

        # Per-window quality with N-adjustment and abnormal returns for all windows
        q7  = _score_window(wr7, abn7 if abn7 is not None else avg7, n7)
        q30 = _score_window(wr30, abn30 if abn30 is not None else avg30, n30)
        q90 = _score_window(wr90, abn90 if abn90 is not None else avg90, n90)

        window_scores = [("7d", q7), ("30d", q30), ("90d", q90)]
        best_window, best_quality = max(window_scores, key=lambda x: x[1])

        # Multi-window consistency: how many windows show positive quality?
        positive_windows = sum(1 for _, q in window_scores if q > 0.3)
        consistency = min(1.0, positive_windows / 3.0)

        # Longer-horizon bonus: 30d and 90d alpha is more meaningful
        horizon_bonus = 0.0
        if q30 > 0.3:
            horizon_bonus += 0.3
        if q90 > 0.3:
            horizon_bonus += 0.7
        horizon_bonus = min(1.0, horizon_bonus)

        # Frequency: log-scaled
        freq_score = min(1.0, math.log2(max(1, count)) / 5)

        # Size: log-scaled total $ value
        size_score = min(1.0, math.log10(max(1, total_val or 1)) / 8)

        # Window breadth: bonus for having data across all windows
        windows_with_data = sum(1 for _, q in window_scores if q > 0)
        breadth = windows_with_data / 3.0

        # Composite: weighted sum scaled to 0-3
        raw = (
            best_quality * 0.40 +
            horizon_bonus * 0.15 +
            consistency * 0.15 +
            freq_score * 0.15 +
            size_score * 0.05 +
            breadth * 0.10
        ) * 3.0

        raw_scores.append((insider_id, raw, best_window))

    # Sort by raw score to compute percentiles
    raw_scores.sort(key=lambda x: x[1])
    n = len(raw_scores)

    best_window_counts = {"7d": 0, "30d": 0, "90d": 0}

    for rank, (insider_id, raw, best_window) in enumerate(raw_scores):
        percentile = (rank + 1) / n * 100
        score = min(3.0, max(0.0, raw))

        if percentile >= 93:
            tier = 3
        elif percentile >= 80:
            tier = 2
        elif percentile >= 67:
            tier = 1
        else:
            tier = 0

        if tier >= 2:
            best_window_counts[best_window] = best_window_counts.get(best_window, 0) + 1

        conn.execute("""
            UPDATE insider_track_records
            SET score = ?, score_tier = ?, percentile = ?, best_window = ?
            WHERE insider_id = ?
        """, (round(score, 4), tier, round(percentile, 2), best_window, insider_id))

    conn.commit()

    # Report
    for tier in range(4):
        count = conn.execute(
            "SELECT COUNT(*) FROM insider_track_records WHERE score_tier = ?", (tier,)
        ).fetchone()[0]
        logger.info("  Tier %d: %d insiders", tier, count)

    logger.info("  Best window distribution (tier 2+): %s", best_window_counts)

    top_10 = conn.execute("""
        SELECT i.name, tr.score, tr.score_tier, tr.buy_count,
               tr.buy_win_rate_7d, tr.buy_avg_return_7d,
               tr.buy_win_rate_30d, tr.buy_avg_return_30d,
               tr.buy_win_rate_90d, tr.buy_avg_return_90d,
               tr.primary_title
        FROM insider_track_records tr
        JOIN insiders i ON tr.insider_id = i.insider_id
        WHERE tr.score_tier >= 2
        ORDER BY tr.score DESC
        LIMIT 10
    """).fetchall()

    logger.info("Top 10 proven insiders (multi-window):")
    for row in top_10:
        name, score, tier, count = row[0], row[1], row[2], row[3]
        wr7 = f"{row[4]*100:.0f}%" if row[4] else "-"
        ret7 = f"{row[5]*100:.1f}%" if row[5] else "-"
        ret30 = f"{row[7]*100:.1f}%" if row[7] else "-"
        ret90 = f"{row[9]*100:.1f}%" if row[9] else "-"
        title = row[10] or "?"
        logger.info(
            "  [T%d] %s (%s) — %d trades | 7d WR:%s ret:%s | 30d:%s | 90d:%s | score %.2f",
            tier, name, title, count, wr7, ret7, ret30, ret90, score
        )


def _build_insider_companies(conn: sqlite3.Connection):
    """Build the insider_companies mapping table."""
    logger.info("Building insider <-> company mappings...")

    conn.execute("DELETE FROM insider_companies")
    conn.execute("""
        INSERT INTO insider_companies (insider_id, ticker, company, title, trade_count, total_value, first_trade, last_trade)
        SELECT
            t.insider_id,
            t.ticker,
            MAX(t.company),
            -- most recent title at this company
            (SELECT t2.title FROM trades t2
             WHERE t2.insider_id = t.insider_id AND t2.ticker = t.ticker
             ORDER BY t2.trade_date DESC LIMIT 1),
            COUNT(*),
            SUM(t.value),
            MIN(t.trade_date),
            MAX(t.trade_date)
        FROM trades t
        WHERE t.trans_code IN ('P', 'S')
        GROUP BY t.insider_id, t.ticker
    """)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM insider_companies").fetchone()[0]
    logger.info("Built %d insider-company mappings", count)


def print_summary(conn: sqlite3.Connection):
    """Print DB summary stats."""
    insiders = conn.execute("SELECT COUNT(*) FROM insiders").fetchone()[0]
    trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    buys = conn.execute("SELECT COUNT(*) FROM trades WHERE trade_type = 'buy'").fetchone()[0]
    sells = conn.execute("SELECT COUNT(*) FROM trades WHERE trade_type = 'sell'").fetchone()[0]
    returns = conn.execute("SELECT COUNT(*) FROM trade_returns").fetchone()[0]
    scored = conn.execute("SELECT COUNT(*) FROM insider_track_records WHERE score IS NOT NULL").fetchone()[0]
    tier2plus = conn.execute("SELECT COUNT(*) FROM insider_track_records WHERE score_tier >= 2").fetchone()[0]
    tier3 = conn.execute("SELECT COUNT(*) FROM insider_track_records WHERE score_tier = 3").fetchone()[0]

    print(f"\n{'='*50}")
    print(f"INSIDER CATALOG SUMMARY")
    print(f"{'='*50}")
    print(f"Total insiders:        {insiders:,}")
    print(f"Total trades:          {trades:,}")
    print(f"  Buy transactions:    {buys:,}")
    print(f"  Sell transactions:   {sells:,}")
    print(f"Trades with 7d return: {returns:,}")
    print(f"Scored insiders:       {scored:,}")
    print(f"  Tier 2+ (top 20%):   {tier2plus:,}")
    print(f"  Tier 3 (top 7%):     {tier3:,}")
    print(f"DB size:               {DB_PATH.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"{'='*50}\n")


def main():
    parser = argparse.ArgumentParser(description="Backfill insider catalog DB")
    parser.add_argument("--refresh-scores", action="store_true",
                        help="Only recompute track records and scores (skip import)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    if RESEARCH_DB.exists():
        conn.execute(f"ATTACH DATABASE '{RESEARCH_DB}' AS research")

    create_db(conn)
    migrate_schema(conn)

    if not args.refresh_scores:
        import_trades(conn, BUYS_CSV, "buy")
        import_trades(conn, SELLS_CSV, "sell")
        import_returns(conn)

        # Validate and fix suspect trade prices
        try:
            from strategies.insider_catalog.price_validator import run_validation
        except ImportError:
            from price_validator import run_validation
        run_validation(conn)

        # Clean display names for all insiders
        try:
            from strategies.insider_catalog.name_cleaner import clean_name, ensure_column
        except ImportError:
            from name_cleaner import clean_name, ensure_column
        ensure_column(conn)
        uncleaned = conn.execute(
            "SELECT insider_id, name, COALESCE(is_entity, 0) FROM insiders WHERE display_name IS NULL"
        ).fetchall()
        if uncleaned:
            for insider_id, name, is_entity in uncleaned:
                display = clean_name(name, bool(is_entity))
                conn.execute(
                    "UPDATE insiders SET display_name = ? WHERE insider_id = ?",
                    (display, insider_id),
                )
            conn.commit()
            logger.info("Cleaned display names for %d insiders", len(uncleaned))

    compute_track_records(conn)
    print_summary(conn)

    conn.close()
    logger.info("Done. DB at %s", DB_PATH)


if __name__ == "__main__":
    main()
