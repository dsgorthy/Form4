#!/usr/bin/env python3
"""
Re-parse all cached SEC quarterly ZIPs to populate new EDGAR fields.

Iterates over cached ZIPs in pipelines/insider_study/data/sec_bulk_cache/,
calls parse_quarter_full() for each, and:
  1. UPDATEs existing trades with new columns (trans_code, direct_indirect, etc.)
  2. INSERTs derivative_trades for F/M/A/G/V/X transactions
  3. INSERTs filing_footnotes from FOOTNOTES.tsv
  4. INSERTs nonderiv_holdings from NONDERIV_HOLDING.tsv

Commits per quarter, logs progress. Idempotent — safe to re-run.

Usage:
    python strategies/insider_catalog/reparse_bulk.py
    python strategies/insider_catalog/reparse_bulk.py --quarters 2024-Q1 2024-Q4
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.insider_study.download_sec_bulk import parse_quarter_full, quarter_range
from strategies.insider_catalog.backfill import (
    DB_PATH,
    get_or_create_insider,
    is_csuite,
    get_title_weight,
    normalize_name,
    normalize_ticker,
    validate_trade_date,
    migrate_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CACHE_DIR = PROJECT_ROOT / "pipelines" / "insider_study" / "data" / "sec_bulk_cache"


def update_existing_trades(conn: sqlite3.Connection, nonderiv_trans: list[dict]) -> dict:
    """
    Update existing trades with new EDGAR fields by matching on
    (accession + insider_name_normalized + ticker + trade_date + value).

    Returns stats dict.
    """
    updated = 0
    not_found = 0

    for t in nonderiv_trans:
        # Only update P/S trades (those already in our DB)
        if t["trans_code"] not in ("P", "S"):
            continue

        acc = t["accession"]
        ticker = normalize_ticker(t["ticker"])
        if not ticker:
            continue

        trade_date = validate_trade_date(t["trade_date"], t["filing_date"])
        if not trade_date:
            continue

        value = t["value"]
        if value <= 0:
            continue

        # Match by accession first (most precise), then fall back to value match
        if acc:
            rows = conn.execute("""
                SELECT trade_id FROM trades
                WHERE accession = ? AND ticker = ? AND trade_date = ?
                LIMIT 5
            """, (acc, ticker, trade_date)).fetchall()
        else:
            rows = []

        if not rows:
            # Fall back: match by (ticker, trade_date, value within 1%)
            name_norm = normalize_name(t["insider_name"])
            rows = conn.execute("""
                SELECT t.trade_id FROM trades t
                JOIN insiders i ON t.insider_id = i.insider_id
                WHERE t.ticker = ? AND t.trade_date = ?
                  AND i.name_normalized = ?
                  AND ABS(t.value - ?) / MAX(t.value, 1) < 0.01
                LIMIT 5
            """, (ticker, trade_date, name_norm, value)).fetchall()

        if not rows:
            not_found += 1
            continue

        for (trade_id,) in rows:
            conn.execute("""
                UPDATE trades SET
                    trans_code = ?,
                    trans_acquired_disp = ?,
                    direct_indirect = ?,
                    shares_owned_after = ?,
                    value_owned_after = ?,
                    nature_of_ownership = ?,
                    equity_swap = ?,
                    is_10b5_1 = ?,
                    security_title = ?,
                    deemed_execution_date = ?,
                    trans_form_type = ?,
                    rptowner_cik = ?
                WHERE trade_id = ?
            """, (
                t["trans_code"],
                t["trans_acquired_disp"],
                t["direct_indirect"],
                t["shares_owned_after"] if t["shares_owned_after"] > 0 else None,
                t["value_owned_after"] if t["value_owned_after"] > 0 else None,
                t["nature_of_ownership"] or None,
                t["equity_swap"],
                t["is_10b5_1"],
                t["security_title"] or None,
                t["deemed_execution_date"] or None,
                t["trans_form_type"] or None,
                t["rptowner_cik"] or None,
                trade_id,
            ))
            updated += 1

    return {"updated": updated, "not_found": not_found}


def insert_derivative_trades(conn: sqlite3.Connection, deriv_trans: list[dict]) -> int:
    """Insert derivative transactions into derivative_trades table."""
    inserted = 0
    for t in deriv_trans:
        ticker = normalize_ticker(t["ticker"])
        if not ticker:
            continue

        trade_date = validate_trade_date(t["trade_date"], t["filing_date"])
        if not trade_date:
            continue

        insider_id = get_or_create_insider(conn, t["insider_name"])
        title = t["title"]

        try:
            conn.execute("""
                INSERT OR IGNORE INTO derivative_trades
                    (insider_id, ticker, company, title, trans_code, trans_acquired_disp,
                     trade_date, filing_date, security_title, exercise_price, expiration_date,
                     trans_shares, trans_price_per_share, trans_total_value,
                     underlying_title, underlying_shares, underlying_value,
                     shares_owned_after, value_owned_after, direct_indirect,
                     nature_of_ownership, equity_swap, is_10b5_1,
                     deemed_execution_date, trans_form_type, rptowner_cik,
                     accession, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'edgar_bulk')
            """, (
                insider_id, ticker, t["company"], title,
                t["trans_code"], t["trans_acquired_disp"],
                trade_date, t["filing_date"],
                t["security_title"] or None,
                t["exercise_price"] if t["exercise_price"] > 0 else None,
                t["expiration_date"] or None,
                t["trans_shares"] if t["trans_shares"] != 0 else None,
                t["trans_price_per_share"] if t["trans_price_per_share"] > 0 else None,
                t["trans_total_value"] if t["trans_total_value"] > 0 else None,
                t["underlying_title"] or None,
                t["underlying_shares"] if t["underlying_shares"] > 0 else None,
                t["underlying_value"] if t["underlying_value"] > 0 else None,
                t["shares_owned_after"] if t["shares_owned_after"] > 0 else None,
                t["value_owned_after"] if t["value_owned_after"] > 0 else None,
                t["direct_indirect"] or None,
                t["nature_of_ownership"] or None,
                t["equity_swap"],
                t["is_10b5_1"],
                t["deemed_execution_date"] or None,
                t["trans_form_type"] or None,
                t["rptowner_cik"] or None,
                t["accession"],
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    return inserted


def insert_footnotes(conn: sqlite3.Connection, footnotes: list[dict]) -> int:
    """Insert filing footnotes."""
    inserted = 0
    for fn in footnotes:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO filing_footnotes
                    (accession, footnote_id, footnote_text)
                VALUES (?, ?, ?)
            """, (fn["accession"], fn["footnote_id"], fn["footnote_text"]))
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    return inserted


def insert_holdings(conn: sqlite3.Connection, holdings: list[dict]) -> int:
    """Insert non-derivative holdings."""
    inserted = 0
    for h in holdings:
        ticker = h["ticker"]
        if not ticker:
            continue

        insider_id = None
        if h["insider_name"]:
            insider_id = get_or_create_insider(conn, h["insider_name"])

        try:
            conn.execute("""
                INSERT OR IGNORE INTO nonderiv_holdings
                    (accession, insider_id, ticker, security_title,
                     shares_owned, value_owned, direct_indirect,
                     nature_of_ownership, trans_form_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                h["accession"], insider_id, ticker,
                h["security_title"] or None,
                h["shares_owned"] if h["shares_owned"] > 0 else None,
                h["value_owned"] if h["value_owned"] > 0 else None,
                h["direct_indirect"] or None,
                h["nature_of_ownership"] or None,
                h["trans_form_type"] or None,
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    return inserted


def reparse_all(conn: sqlite3.Connection, start_q: str = None, end_q: str = None):
    """Re-parse all cached quarterly ZIPs."""
    # Find all cached ZIPs
    zips = sorted(CACHE_DIR.glob("*_form345.zip"))
    if not zips:
        logger.error("No cached ZIPs found in %s", CACHE_DIR)
        return

    # Optionally filter by quarter range
    if start_q and end_q:
        quarters = set()
        for y, q in quarter_range(start_q, end_q):
            quarters.add(f"{y}q{q}")
        zips = [z for z in zips if z.stem.replace("_form345", "") in quarters]

    logger.info("Processing %d cached quarterly ZIPs", len(zips))
    start_time = time.monotonic()

    totals = {
        "trades_updated": 0,
        "trades_not_found": 0,
        "deriv_inserted": 0,
        "footnotes_inserted": 0,
        "holdings_inserted": 0,
    }

    for i, zip_path in enumerate(zips):
        quarter_label = zip_path.stem.replace("_form345", "")
        logger.info("[%d/%d] Parsing %s...", i + 1, len(zips), quarter_label)

        zip_bytes = zip_path.read_bytes()
        data = parse_quarter_full(zip_bytes)

        # 1. Update existing trades with new fields
        stats = update_existing_trades(conn, data["nonderiv_trans"])
        totals["trades_updated"] += stats["updated"]
        totals["trades_not_found"] += stats["not_found"]

        # 2. Insert derivative trades
        n_deriv = insert_derivative_trades(conn, data["deriv_trans"])
        totals["deriv_inserted"] += n_deriv

        # 3. Insert footnotes
        n_fn = insert_footnotes(conn, data["footnotes"])
        totals["footnotes_inserted"] += n_fn

        # 4. Insert holdings
        n_hold = insert_holdings(conn, data["nonderiv_holdings"])
        totals["holdings_inserted"] += n_hold

        conn.commit()

        logger.info(
            "  %s: updated %d trades, +%d deriv, +%d footnotes, +%d holdings "
            "(%.0fs elapsed)",
            quarter_label, stats["updated"], n_deriv, n_fn, n_hold,
            time.monotonic() - start_time,
        )

    elapsed = time.monotonic() - start_time
    logger.info("")
    logger.info("=" * 60)
    logger.info("RE-PARSE COMPLETE (%.1f min)", elapsed / 60)
    logger.info("=" * 60)
    logger.info("  Trades updated:       %d", totals["trades_updated"])
    logger.info("  Trades not matched:   %d", totals["trades_not_found"])
    logger.info("  Derivative trades:    %d", totals["deriv_inserted"])
    logger.info("  Footnotes:            %d", totals["footnotes_inserted"])
    logger.info("  Holdings:             %d", totals["holdings_inserted"])
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Re-parse cached SEC ZIPs for new fields")
    parser.add_argument("--start", default=None, help="Start quarter YYYY-QN (default: all)")
    parser.add_argument("--end", default=None, help="End quarter YYYY-QN (default: all)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Ensure schema is up to date
    migrate_schema(conn)

    reparse_all(conn, args.start, args.end)

    # Print verification queries
    logger.info("")
    logger.info("Verification:")
    for label, query in [
        ("trans_code coverage",
         "SELECT trans_code, COUNT(*) FROM trades WHERE trans_code IS NOT NULL GROUP BY trans_code ORDER BY COUNT(*) DESC"),
        ("10b5-1 coverage",
         "SELECT is_10b5_1, COUNT(*) FROM trades WHERE is_10b5_1 IS NOT NULL GROUP BY is_10b5_1"),
        ("derivative trades",
         "SELECT trans_code, COUNT(*) FROM derivative_trades GROUP BY trans_code ORDER BY COUNT(*) DESC"),
        ("footnotes",
         "SELECT COUNT(*) as n FROM filing_footnotes"),
        ("holdings",
         "SELECT COUNT(*) as n FROM nonderiv_holdings"),
    ]:
        rows = conn.execute(query).fetchall()
        logger.info("  %s: %s", label, rows)

    conn.close()


if __name__ == "__main__":
    main()
