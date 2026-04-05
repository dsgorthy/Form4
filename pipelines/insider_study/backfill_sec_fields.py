#!/usr/bin/env python3
"""
Backfill missing SEC Form 4 fields from bulk ZIP files into insiders.db.

Adds new columns to the trades table (SUBMISSION, NONDERIV_TRANS, REPORTINGOWNER
fields), creates derivative_holdings table, and populates filing_footnotes.

Matches SEC bulk data to existing trade rows by:
    (ticker, trade_date, rptowner_cik, trans_code, price, qty)

Memory-safe: processes one ZIP at a time, commits after each.
Idempotent: safe to re-run (uses ALTER TABLE try/except, UPDATE not INSERT).

Usage:
    python3 pipelines/insider_study/backfill_sec_fields.py
    python3 pipelines/insider_study/backfill_sec_fields.py --quarter 2024q3
    python3 pipelines/insider_study/backfill_sec_fields.py --dry-run
"""

from __future__ import annotations

from pipelines.insider_study.db_lock import db_write_lock

import argparse
import csv
import io
import logging
import re
import sqlite3
import sys
import time
import zipfile
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.insider_study.download_sec_bulk import _reformat_date
from strategies.insider_catalog.backfill import (
    DB_PATH,
    normalize_ticker,
    validate_trade_date,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CACHE_DIR = PROJECT_ROOT / "pipelines" / "insider_study" / "data" / "sec_bulk_cache"

_TICKER_RE = re.compile(r"^[A-Z0-9\.]{1,10}$")


# ── Schema migration ────────────────────────────────────────────────────────

NEW_TRADES_COLUMNS = [
    # From SUBMISSION.tsv
    ("issuer_cik", "TEXT"),
    ("period_of_report", "TEXT"),
    ("date_of_orig_sub", "TEXT"),
    ("document_type", "TEXT"),
    ("remarks", "TEXT"),
    ("no_securities_owned", "INTEGER"),
    ("not_subject_sec16", "INTEGER"),
    ("form3_holdings_reported", "INTEGER"),
    ("form4_trans_reported", "INTEGER"),
    ("aff_10b5_1", "INTEGER"),
    ("filed_at_source", "TEXT"),
    # From NONDERIV_TRANS.tsv
    ("trans_timeliness", "TEXT"),
    # From REPORTINGOWNER.tsv
    ("rptowner_relationship", "TEXT"),
    ("rptowner_text", "TEXT"),
    ("rptowner_street1", "TEXT"),
    ("rptowner_street2", "TEXT"),
    ("rptowner_city", "TEXT"),
    ("rptowner_state", "TEXT"),
    ("rptowner_zipcode", "TEXT"),
    ("file_number", "TEXT"),
]


def migrate_schema(conn: sqlite3.Connection):
    """
    Add new columns to trades table and create new tables.
    Idempotent — ALTER TABLE wrapped in try/except, CREATE IF NOT EXISTS.
    """
    added = 0
    for col_name, col_type in NEW_TRADES_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")
            added += 1
        except sqlite3.OperationalError:
            pass  # column already exists

    if added:
        logger.info("Added %d new columns to trades table", added)

    # New indexes on new columns (tolerate DB corruption in existing indexes)
    index_stmts = [
        "CREATE INDEX IF NOT EXISTS idx_trades_issuer_cik ON trades(issuer_cik)",
        "CREATE INDEX IF NOT EXISTS idx_trades_document_type ON trades(document_type)",
        "CREATE INDEX IF NOT EXISTS idx_trades_timeliness ON trades(trans_timeliness)",
        "CREATE INDEX IF NOT EXISTS idx_trades_relationship ON trades(rptowner_relationship)",
    ]
    for stmt in index_stmts:
        try:
            conn.execute(stmt)
        except sqlite3.DatabaseError as e:
            logger.warning("Index creation skipped (DB issue): %s — %s", stmt.split("ON")[0].strip(), e)

    # derivative_holdings table
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derivative_holdings (
                deriv_holding_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                accession           TEXT NOT NULL,
                security_title      TEXT,
                conversion_price    REAL,
                exercise_date       TEXT,
                expiration_date     TEXT,
                underlying_title    TEXT,
                underlying_shares   REAL,
                underlying_value    REAL,
                shares_following    REAL,
                value_following     REAL,
                direct_indirect     TEXT,
                nature_of_ownership TEXT,
                created_at          TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(accession, security_title, direct_indirect, underlying_title)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_deriv_hold_accession "
            "ON derivative_holdings(accession)"
        )
    except sqlite3.DatabaseError as e:
        logger.warning("derivative_holdings table/index: %s", e)

    # Ensure filing_footnotes exists (may already from schema.sql)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filing_footnotes (
                footnote_id_pk  INTEGER PRIMARY KEY AUTOINCREMENT,
                accession       TEXT NOT NULL,
                footnote_id     TEXT NOT NULL,
                footnote_text   TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(accession, footnote_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_footnotes_accession "
            "ON filing_footnotes(accession)"
        )
    except sqlite3.DatabaseError as e:
        logger.warning("filing_footnotes table/index: %s", e)

    try:
        conn.commit()
    except sqlite3.DatabaseError as e:
        logger.warning("Commit during migration: %s", e)

    logger.info("Schema migration complete")


# ── ZIP parsing ──────────────────────────────────────────────────────────────

def read_tsv_from_zip(
    zf: zipfile.ZipFile, name: str
) -> list[dict]:
    """Read all rows from a TSV inside a ZIP as list of dicts."""
    try:
        with zf.open(name) as fb:
            f = io.TextIOWrapper(fb, encoding="utf-8", errors="replace")
            return list(csv.DictReader(f, delimiter="\t"))
    except KeyError:
        logger.warning("TSV not found in ZIP: %s", name)
        return []


def parse_zip_for_backfill(zip_path: Path) -> dict:
    """
    Parse one quarterly ZIP and return structured data for backfill.

    Returns dict with keys:
        submissions: {accession: row_dict}
        owners:      {accession: [row_dict, ...]}
        nonderiv:    [row_dict, ...]   (raw NONDERIV_TRANS rows)
        footnotes:   [row_dict, ...]
        deriv_hold:  [row_dict, ...]
    """
    zip_bytes = zip_path.read_bytes()
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile:
        logger.error("Bad ZIP file: %s", zip_path.name)
        return {
            "submissions": {},
            "owners": {},
            "nonderiv": [],
            "footnotes": [],
            "deriv_hold": [],
        }

    # SUBMISSION.tsv — keyed by accession
    sub_rows = read_tsv_from_zip(zf, "SUBMISSION.tsv")
    submissions = {}
    for row in sub_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        if acc:
            submissions[acc] = row

    # REPORTINGOWNER.tsv — multiple owners per accession
    owner_rows = read_tsv_from_zip(zf, "REPORTINGOWNER.tsv")
    owners_by_acc: dict[str, list[dict]] = {}
    for row in owner_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        if acc:
            owners_by_acc.setdefault(acc, []).append(row)

    # NONDERIV_TRANS.tsv — raw rows (we'll match individually)
    nonderiv = read_tsv_from_zip(zf, "NONDERIV_TRANS.tsv")

    # FOOTNOTES.tsv
    footnotes = read_tsv_from_zip(zf, "FOOTNOTES.tsv")

    # DERIV_HOLDING.tsv
    deriv_hold = read_tsv_from_zip(zf, "DERIV_HOLDING.tsv")

    zf.close()

    return {
        "submissions": submissions,
        "owners": owners_by_acc,
        "nonderiv": nonderiv,
        "footnotes": footnotes,
        "deriv_hold": deriv_hold,
    }


# ── Matching + update logic ─────────────────────────────────────────────────

def _safe_int(val: str) -> int | None:
    """Parse a string to int, returning None for empty/invalid."""
    val = (val or "").strip()
    if not val:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        # Handle "0", "1", "true", etc.
        if val.lower() in ("true", "1"):
            return 1
        if val.lower() in ("false", "0"):
            return 0
        return None


def _safe_float(val: str) -> float | None:
    """Parse a string to float, returning None for empty/invalid."""
    val = (val or "").strip()
    if not val:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _clean_ticker(sub: dict) -> str | None:
    """Extract and clean ticker from a SUBMISSION row."""
    ticker_raw = sub.get("ISSUERTRADINGSYMBOL", "").strip().upper()
    ticker = ticker_raw.strip("\"'()").split(",")[0]
    if ":" in ticker:
        ticker = ticker.split(":")[1]
    ticker = ticker.strip()
    if not ticker or not _TICKER_RE.match(ticker):
        return None
    return ticker


def _build_trade_lookup(conn: sqlite3.Connection, date_min: str, date_max: str) -> dict:
    """
    Load all P/S trades in the date range into an in-memory lookup.

    Returns dict keyed by (ticker, trade_date, trans_code) -> list of
    (trade_id, rptowner_cik, price, qty, accession).

    This avoids per-row queries against a potentially corrupted DB.
    """
    lookup: dict[tuple, list] = {}
    try:
        cursor = conn.execute("""
            SELECT trade_id, ticker, trade_date, trans_code, rptowner_cik,
                   price, qty, accession
            FROM trades
            WHERE trade_date >= ? AND trade_date <= ?
              AND trans_code IN ('P', 'S')
        """, (date_min, date_max))

        for row in cursor:
            trade_id, ticker, trade_date, trans_code, cik, price, qty, acc = row
            key = (ticker, trade_date, trans_code)
            lookup.setdefault(key, []).append({
                "trade_id": trade_id,
                "rptowner_cik": cik or "",
                "price": price or 0.0,
                "qty": qty or 0,
                "accession": acc or "",
            })
    except sqlite3.DatabaseError as e:
        logger.warning("Error loading trade lookup: %s", e)
        # Try loading without date filter as fallback (slower but may avoid corrupt pages)
        try:
            cursor = conn.execute("""
                SELECT trade_id, ticker, trade_date, trans_code, rptowner_cik,
                       price, qty, accession
                FROM trades
                WHERE trans_code IN ('P', 'S')
            """)
            for row in cursor:
                trade_id, ticker, trade_date, trans_code, cik, price, qty, acc = row
                key = (ticker, trade_date, trans_code)
                lookup.setdefault(key, []).append({
                    "trade_id": trade_id,
                    "rptowner_cik": cik or "",
                    "price": price or 0.0,
                    "qty": qty or 0,
                    "accession": acc or "",
                })
        except sqlite3.DatabaseError as e2:
            logger.error("Cannot load trades at all: %s", e2)

    logger.info("  Loaded %d trade keys into memory for matching", len(lookup))
    return lookup


def _find_matching_trade_ids(
    lookup: dict,
    ticker: str,
    trade_date: str,
    trans_code: str,
    rptowner_cik: str,
    price: float,
    qty: float,
    accession: str,
) -> list[int]:
    """
    Find matching trade_ids from the in-memory lookup.

    Match priority:
    1. (ticker, trade_date, trans_code) + rptowner_cik + price/qty tolerance
    2. (ticker, trade_date, trans_code) + accession match
    3. (ticker, trade_date, trans_code) + price/qty tolerance (no CIK required)
    """
    candidates = lookup.get((ticker, trade_date, trans_code), [])
    if not candidates:
        return []

    # Strategy 1: exact CIK + price/qty match
    ids = []
    for c in candidates:
        if c["rptowner_cik"] == rptowner_cik:
            if abs(c["price"] - price) < 0.015 and abs(c["qty"] - qty) < 1.5:
                ids.append(c["trade_id"])
    if ids:
        return ids[:5]

    # Strategy 2: accession match
    if accession:
        ids = [c["trade_id"] for c in candidates if c["accession"] == accession]
        if ids:
            return ids[:5]

    # Strategy 3: price/qty match without CIK
    ids = []
    for c in candidates:
        if abs(c["price"] - price) < 0.015 and abs(c["qty"] - qty) < 1.5:
            if not c["rptowner_cik"] or c["rptowner_cik"] == rptowner_cik:
                ids.append(c["trade_id"])
    return ids[:5]


def update_trades(
    conn: sqlite3.Connection,
    submissions: dict[str, dict],
    owners_by_acc: dict[str, list[dict]],
    nonderiv_rows: list[dict],
    dry_run: bool = False,
    trade_lookup: dict | None = None,
) -> dict:
    """
    Match NONDERIV_TRANS rows to existing trades and UPDATE with new fields.

    Uses in-memory lookup for speed. Match signature:
    (ticker, trade_date, rptowner_cik, trans_code, price, qty)

    If trade_lookup is provided, uses it directly. Otherwise builds one
    from the DB for this quarter's date range.

    Returns stats dict.
    """
    if trade_lookup is not None:
        lookup = trade_lookup
    else:
        # Determine date range for this quarter's data
        dates = []
        for row in nonderiv_rows:
            acc = row.get("ACCESSION_NUMBER", "").strip()
            sub = submissions.get(acc)
            if not sub:
                continue
            filing_date = _reformat_date(sub.get("FILING_DATE", ""))
            td = _reformat_date(row.get("TRANS_DATE", "") or filing_date)
            if td and len(td) >= 10:
                dates.append(td[:10])

        if not dates:
            return {"matched": 0, "updated": 0, "unmatched": 0, "skipped": 0}

        date_min = min(dates)
        date_max = max(dates)
        date_min_adj = f"{int(date_min[:4]) - 1}-01-01"
        date_max_adj = f"{int(date_max[:4]) + 1}-12-31"
        lookup = _build_trade_lookup(conn, date_min_adj, date_max_adj)

    matched = 0
    updated = 0
    unmatched = 0
    skipped = 0

    for row in nonderiv_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        sub = submissions.get(acc)
        if not sub:
            skipped += 1
            continue

        trans_code = row.get("TRANS_CODE", "").strip()
        if trans_code not in ("P", "S"):
            skipped += 1
            continue

        ticker = _clean_ticker(sub)
        if not ticker:
            skipped += 1
            continue

        ticker_norm = normalize_ticker(ticker)
        if not ticker_norm:
            skipped += 1
            continue

        filing_date = _reformat_date(sub.get("FILING_DATE", ""))
        trade_date = _reformat_date(row.get("TRANS_DATE", "") or filing_date)
        trade_date = validate_trade_date(trade_date, filing_date)
        if not trade_date:
            skipped += 1
            continue

        try:
            price = float(row.get("TRANS_PRICEPERSHARE", 0) or 0)
            qty = float(row.get("TRANS_SHARES", 0) or 0)
        except (ValueError, TypeError):
            skipped += 1
            continue

        if price <= 0 or qty <= 0:
            skipped += 1
            continue

        owners = owners_by_acc.get(acc, [])
        if not owners:
            skipped += 1
            continue

        primary_owner = owners[0]
        rptowner_cik = primary_owner.get("RPTOWNERCIK", "").strip()

        # In-memory matching
        trade_ids = _find_matching_trade_ids(
            lookup, ticker_norm, trade_date, trans_code,
            rptowner_cik, price, qty, acc,
        )

        if not trade_ids:
            unmatched += 1
            continue

        matched += len(trade_ids)

        if dry_run:
            continue

        # Build relationship string from all owners' flags
        relationships = []
        for owner in owners:
            rel = owner.get("RPTOWNER_RELATIONSHIP", "").strip()
            if rel:
                relationships.append(rel)
        rptowner_relationship = "; ".join(relationships) if relationships else None
        rptowner_text = primary_owner.get("RPTOWNER_TXT", "").strip() or None

        # Gather all fields to update
        issuer_cik = sub.get("ISSUERCIK", "").strip() or None
        period_of_report = _reformat_date(sub.get("PERIOD_OF_REPORT", "")) or None
        date_of_orig_sub = _reformat_date(sub.get("DATE_OF_ORIG_SUB", "")) or None
        document_type = sub.get("DOCUMENT_TYPE", "").strip() or None
        remarks_val = sub.get("REMARKS", "").strip() or None
        no_securities_owned = _safe_int(sub.get("NO_SECURITIES_OWNED", ""))
        not_subject_sec16 = _safe_int(sub.get("NOT_SUBJECT_SEC16", ""))
        form3_holdings_reported = _safe_int(sub.get("FORM3_HOLDINGS_REPORTED", ""))
        form4_trans_reported = _safe_int(sub.get("FORM4_TRANS_REPORTED", ""))
        aff_10b5_1 = _safe_int(sub.get("AFF10B5ONE", ""))
        trans_timeliness = row.get("TRANS_TIMELINESS", "").strip() or None
        rptowner_street1 = primary_owner.get("RPTOWNER_STREET1", "").strip() or None
        rptowner_street2 = primary_owner.get("RPTOWNER_STREET2", "").strip() or None
        rptowner_city = primary_owner.get("RPTOWNER_CITY", "").strip() or None
        rptowner_state = primary_owner.get("RPTOWNER_STATE", "").strip() or None
        rptowner_zipcode = primary_owner.get("RPTOWNER_ZIPCODE", "").strip() or None
        file_number = primary_owner.get("FILE_NUMBER", "").strip() or None

        for trade_id in trade_ids:
            try:
                conn.execute("""
                    UPDATE trades SET
                        accession = COALESCE(accession, ?),
                        issuer_cik = ?,
                        period_of_report = ?,
                        date_of_orig_sub = ?,
                        document_type = ?,
                        remarks = ?,
                        no_securities_owned = ?,
                        not_subject_sec16 = ?,
                        form3_holdings_reported = ?,
                        form4_trans_reported = ?,
                        aff_10b5_1 = ?,
                        trans_timeliness = ?,
                        rptowner_relationship = ?,
                        rptowner_text = ?,
                        rptowner_street1 = ?,
                        rptowner_street2 = ?,
                        rptowner_city = ?,
                        rptowner_state = ?,
                        rptowner_zipcode = ?,
                        file_number = ?
                    WHERE trade_id = ?
                """, (
                    acc,
                    issuer_cik,
                    period_of_report,
                    date_of_orig_sub,
                    document_type,
                    remarks_val,
                    no_securities_owned,
                    not_subject_sec16,
                    form3_holdings_reported,
                    form4_trans_reported,
                    aff_10b5_1,
                    trans_timeliness,
                    rptowner_relationship,
                    rptowner_text,
                    rptowner_street1,
                    rptowner_street2,
                    rptowner_city,
                    rptowner_state,
                    rptowner_zipcode,
                    file_number,
                    trade_id,
                ))
                updated += 1
            except sqlite3.DatabaseError:
                pass  # corrupted page for this trade_id

    return {
        "matched": matched,
        "updated": updated,
        "unmatched": unmatched,
        "skipped": skipped,
    }


def insert_footnotes(
    conn: sqlite3.Connection,
    footnote_rows: list[dict],
    dry_run: bool = False,
) -> int:
    """Insert filing footnotes (INSERT OR IGNORE for idempotency)."""
    inserted = 0
    for row in footnote_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        fn_id = row.get("FOOTNOTE_ID", "").strip()
        fn_text = row.get("FOOTNOTE_TXT", "").strip()
        if not acc or not fn_id:
            continue
        if dry_run:
            inserted += 1
            continue
        try:
            cur = conn.execute(
                "INSERT OR IGNORE INTO filing_footnotes "
                "(accession, footnote_id, footnote_text) VALUES (?, ?, ?)",
                (acc, fn_id, fn_text),
            )
            if cur.rowcount > 0:
                inserted += 1
        except sqlite3.IntegrityError:
            pass
    return inserted


def insert_derivative_holdings(
    conn: sqlite3.Connection,
    deriv_hold_rows: list[dict],
    submissions: dict[str, dict],
    dry_run: bool = False,
) -> int:
    """Insert derivative holdings (INSERT OR IGNORE for idempotency)."""
    inserted = 0
    for row in deriv_hold_rows:
        acc = row.get("ACCESSION_NUMBER", "").strip()
        if not acc:
            continue
        # Only include if submission exists (validates it's a real filing)
        if acc not in submissions:
            continue

        security_title = row.get("SECURITY_TITLE", "").strip() or None
        conversion_price = _safe_float(row.get("CONV_EXERCISE_PRICE", ""))
        exercise_date = _reformat_date(row.get("EXERCISE_DATE", "")) or None
        expiration_date = _reformat_date(row.get("EXPIRATION_DATE", "")) or None
        underlying_title = row.get("UNDLYNG_SEC_TITLE", "").strip() or None
        underlying_shares = _safe_float(row.get("UNDLYNG_SEC_SHARES", ""))
        underlying_value = _safe_float(row.get("UNDLYNG_SEC_VALUE", ""))
        shares_following = _safe_float(row.get("SHRS_OWND_FOLWNG_TRANS", ""))
        value_following = _safe_float(row.get("VALU_OWND_FOLWNG_TRANS", ""))
        direct_indirect = row.get("DIRECT_INDIRECT_OWNERSHIP", "").strip() or None
        nature_of_ownership = row.get("NATURE_OF_OWNERSHIP", "").strip() or None

        if dry_run:
            inserted += 1
            continue

        try:
            conn.execute("""
                INSERT OR IGNORE INTO derivative_holdings
                    (accession, security_title, conversion_price, exercise_date,
                     expiration_date, underlying_title, underlying_shares,
                     underlying_value, shares_following, value_following,
                     direct_indirect, nature_of_ownership)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                acc, security_title, conversion_price, exercise_date,
                expiration_date, underlying_title, underlying_shares,
                underlying_value, shares_following, value_following,
                direct_indirect, nature_of_ownership,
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    return inserted


# ── Main processing loop ─────────────────────────────────────────────────────

def get_zip_paths(quarter: str | None = None) -> list[Path]:
    """Get sorted list of ZIP paths, optionally filtered to one quarter."""
    zips = sorted(CACHE_DIR.glob("*_form345.zip"))
    if not zips:
        logger.error("No cached ZIPs found in %s", CACHE_DIR)
        return []

    if quarter:
        # Normalize: accept "2024q3", "2024Q3", "2024-Q3"
        q = quarter.lower().replace("-", "").replace("_", "")
        zips = [z for z in zips if z.stem.replace("_form345", "") == q]
        if not zips:
            logger.error("No ZIP found for quarter '%s'", quarter)
            return []

    return zips


def process_all(conn: sqlite3.Connection, quarter: str | None, dry_run: bool):
    """Process all (or one) quarterly ZIPs."""
    zips = get_zip_paths(quarter)
    if not zips:
        return

    logger.info(
        "Processing %d quarterly ZIP%s%s",
        len(zips),
        "s" if len(zips) != 1 else "",
        " (DRY RUN)" if dry_run else "",
    )

    # Pre-load all trades into memory once (avoids re-scanning per quarter)
    logger.info("Loading trade lookup from DB...")
    trade_lookup = _build_trade_lookup(conn, "2000-01-01", "2030-12-31")

    start_time = time.monotonic()
    totals = {
        "matched": 0,
        "updated": 0,
        "unmatched": 0,
        "skipped": 0,
        "footnotes": 0,
        "deriv_holdings": 0,
    }

    for i, zip_path in enumerate(zips):
        quarter_label = zip_path.stem.replace("_form345", "")
        logger.info("[%d/%d] Parsing %s...", i + 1, len(zips), quarter_label)

        data = parse_zip_for_backfill(zip_path)

        # 1. Update existing trades with new fields
        stats = update_trades(
            conn,
            data["submissions"],
            data["owners"],
            data["nonderiv"],
            dry_run=dry_run,
            trade_lookup=trade_lookup,
        )
        totals["matched"] += stats["matched"]
        totals["updated"] += stats["updated"]
        totals["unmatched"] += stats["unmatched"]
        totals["skipped"] += stats["skipped"]

        # 2. Insert footnotes
        n_fn = insert_footnotes(conn, data["footnotes"], dry_run=dry_run)
        totals["footnotes"] += n_fn

        # 3. Insert derivative holdings
        n_dh = insert_derivative_holdings(
            conn, data["deriv_hold"], data["submissions"], dry_run=dry_run,
        )
        totals["deriv_holdings"] += n_dh

        if not dry_run:
            try:
                conn.commit()
            except sqlite3.DatabaseError as e:
                logger.warning("Commit error for %s: %s", quarter_label, e)

        elapsed = time.monotonic() - start_time
        logger.info(
            "  %s: matched %d, updated %d, unmatched %d, "
            "+%d footnotes, +%d deriv holdings (%.0fs elapsed)",
            quarter_label,
            stats["matched"],
            stats["updated"],
            stats["unmatched"],
            n_fn,
            n_dh,
            elapsed,
        )

    elapsed = time.monotonic() - start_time
    logger.info("")
    logger.info("=" * 64)
    logger.info(
        "BACKFILL COMPLETE%s (%.1f min)",
        " — DRY RUN" if dry_run else "",
        elapsed / 60,
    )
    logger.info("=" * 64)
    logger.info("  Trades matched:        %d", totals["matched"])
    logger.info("  Trades updated:        %d", totals["updated"])
    logger.info("  Trades unmatched:      %d", totals["unmatched"])
    logger.info("  Nonderiv rows skipped: %d", totals["skipped"])
    logger.info("  Footnotes inserted:    %d", totals["footnotes"])
    logger.info("  Deriv holdings:        %d", totals["deriv_holdings"])
    logger.info("=" * 64)


def print_verification(conn: sqlite3.Connection):
    """Print verification stats after backfill."""
    logger.info("")
    logger.info("Verification:")

    queries = [
        (
            "issuer_cik coverage",
            "SELECT COUNT(*) FROM trades WHERE issuer_cik IS NOT NULL",
        ),
        (
            "period_of_report coverage",
            "SELECT COUNT(*) FROM trades WHERE period_of_report IS NOT NULL",
        ),
        (
            "document_type breakdown",
            "SELECT document_type, COUNT(*) FROM trades "
            "WHERE document_type IS NOT NULL GROUP BY document_type",
        ),
        (
            "trans_timeliness (late filings)",
            "SELECT trans_timeliness, COUNT(*) FROM trades "
            "WHERE trans_timeliness IS NOT NULL AND trans_timeliness != '' "
            "GROUP BY trans_timeliness",
        ),
        (
            "rptowner_relationship coverage",
            "SELECT COUNT(*) FROM trades WHERE rptowner_relationship IS NOT NULL",
        ),
        (
            "aff_10b5_1 breakdown",
            "SELECT aff_10b5_1, COUNT(*) FROM trades "
            "WHERE aff_10b5_1 IS NOT NULL GROUP BY aff_10b5_1",
        ),
        (
            "filing_footnotes total",
            "SELECT COUNT(*) FROM filing_footnotes",
        ),
        (
            "derivative_holdings total",
            "SELECT COUNT(*) FROM derivative_holdings",
        ),
        (
            "accession coverage",
            "SELECT COUNT(*) as has_acc FROM trades WHERE accession IS NOT NULL "
            "AND accession != ''",
        ),
    ]

    for label, query in queries:
        try:
            rows = conn.execute(query).fetchall()
            logger.info("  %s: %s", label, rows)
        except Exception as e:
            logger.warning("  %s: ERROR %s", label, e)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing SEC Form 4 fields from bulk ZIP files"
    )
    parser.add_argument(
        "--quarter",
        default=None,
        help="Process a single quarter (e.g. 2024q3). Default: all ZIPs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse ZIPs and log matches, but don't write to DB.",
    )
    args = parser.parse_args()

    with db_write_lock(timeout_msg="backfill_sec_fields"):
        _run_backfill(args)


def _run_backfill(args):
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64 MB page cache

    # Run schema migration first
    migrate_schema(conn)

    # Process ZIPs
    process_all(conn, args.quarter, args.dry_run)

    # Print verification
    if not args.dry_run:
        print_verification(conn)

    conn.close()


if __name__ == "__main__":
    main()
