#!/usr/bin/env python3
"""
Incremental EDGAR Form 4 fetcher — designed to run every 5 minutes.

Unlike backfill_live.py which scans an entire date range, this script:
  1. Checks which accessions we already processed (via processed_filings table)
  2. Queries EFTS for today's (and yesterday's) filings
  3. Skips any accession already processed
  4. Only fetches+parses XML for truly new filings
  5. Runs price validation and name cleaning on new inserts

Typical run: <30 seconds when there are 0-20 new filings since last check.

Usage:
  python fetch_latest.py              # fetch today + yesterday
  python fetch_latest.py --days 3     # fetch last 3 days
  python fetch_latest.py --dry-run    # report without inserting
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection
from backfill_live import (
    fetch_all_form4_filings,
    fetch_form4_xml,
    insert_trades,
    parse_form4_xml,
)
from backfill import DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def ensure_processed_table(conn):
    """Create processed_filings table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_filings (
            accession TEXT PRIMARY KEY,
            filing_date TEXT,
            trade_count INTEGER DEFAULT 0,
            processed_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()


def update_last_fetch_time(conn):
    """Record the current time as the last successful fetch run."""
    from datetime import datetime
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO sync_meta (key, value) VALUES ('last_fetch_at', ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (now_str,),
    )
    conn.commit()


def backfill_processed_from_trades(conn):
    """One-time: populate processed_filings from existing trades table."""
    existing = conn.execute("SELECT COUNT(*) FROM processed_filings").fetchone()[0]
    if existing > 0:
        return  # already populated

    logger.info("Backfilling processed_filings from trades table...")
    conn.execute("""
        INSERT OR IGNORE INTO processed_filings (accession, filing_date, trade_count)
        SELECT accession, MIN(filing_date), COUNT(*)
        FROM trades
        WHERE accession IS NOT NULL
        GROUP BY accession
    """)
    cnt = conn.execute("SELECT COUNT(*) FROM processed_filings").fetchone()[0]
    conn.commit()
    logger.info("Backfilled %d accessions into processed_filings", cnt)


def get_known_accessions(conn) -> set:
    """Get all accession numbers we've already processed."""
    rows = conn.execute(
        "SELECT accession FROM processed_filings"
    ).fetchall()
    return {r[0] for r in rows}


def mark_processed(conn, accession: str, filing_date: str, trade_count: int):
    """Mark a filing as processed (even if it had 0 trades)."""
    conn.execute(
        "INSERT OR IGNORE INTO processed_filings (accession, filing_date, trade_count) VALUES (?, ?, ?)",
        (accession, filing_date, trade_count),
    )


class IndicatorComputeError(RuntimeError):
    """Raised when a downstream compute subprocess fails. Caller must
    abort + alert; never silently mark filings as processed."""


def _run_indicators():
    """Run CW indicators + PIT grades as subprocesses after fetch.

    Uses separate processes to avoid SIGBUS from stale memory-mapped files
    in the parent process. Each subprocess gets fresh file handles.
    Called OUTSIDE db_write_lock() so subprocesses can acquire their own locks.

    HARD FAIL POLICY (post-April-2026 outage): any subprocess failure raises
    IndicatorComputeError. Previously these were `logger.warning()` and the
    parent script would mark filings as 'processed' → permanent feature gap.
    """
    t0 = time.monotonic()
    script_dir = Path(__file__).resolve().parents[2] / "pipelines" / "insider_study"
    python = "/opt/homebrew/bin/python3" if Path("/opt/homebrew/bin/python3").exists() else sys.executable
    repo_root = str(Path(__file__).resolve().parents[2])

    failures: list[str] = []

    def _run(label: str, args: list[str], timeout: int):
        nonlocal failures
        sub_t0 = time.monotonic()
        try:
            result = subprocess.run(
                args, capture_output=True, text=True, timeout=timeout, cwd=repo_root,
            )
        except subprocess.TimeoutExpired:
            failures.append(f"{label}: TIMEOUT after {timeout}s")
            logger.error("%s timed out after %ds", label, timeout)
            return
        except Exception as e:
            failures.append(f"{label}: subprocess raised {type(e).__name__}: {e}")
            logger.exception("%s subprocess raised", label)
            return
        if result.returncode == 0:
            logger.info("%s computed (%.1fs)", label, time.monotonic() - sub_t0)
        else:
            tail = result.stderr[-500:] if result.stderr else ""
            failures.append(f"{label}: exit {result.returncode}: {tail!r}")
            logger.error("%s failed (exit %d): %s", label, result.returncode, tail)

    _run("CW indicators",
         [python, str(script_dir / "compute_cw_indicators.py")],
         timeout=300)
    _run("PIT grades",
         [python, str(script_dir / "backfill_pit_grades.py")],
         timeout=120)

    if failures:
        # Append to logs/alerts.ndjson before raising.
        with __import__("contextlib").suppress(Exception):
            from framework.alerts.log import alert
            alert.critical(
                "fetch_latest._run_indicators",
                "\n".join(f"  • {f}" for f in failures),
                failure_count=len(failures),
            )
        raise IndicatorComputeError(
            f"{len(failures)} indicator/PIT subprocess(es) failed: {failures}"
        )


def run_fetch(days: int = 2, dry_run: bool = False) -> dict:
    """
    Fetch new Form 4 filings since `days` ago.
    Returns stats dict.
    """
    today = date.today()
    start_date = (today - timedelta(days=days)).isoformat()
    end_date = today.isoformat()

    stats = _run_fetch_inner(start_date, end_date, dry_run)

    if not dry_run and stats.get("inserted", 0) > 0:
        _run_indicators()

    return stats


def _run_fetch_inner(start_date: str, end_date: str, dry_run: bool) -> dict:
    conn = get_connection()

    ensure_processed_table(conn)
    backfill_processed_from_trades(conn)

    # Get all accessions we've ever processed
    known = get_known_accessions(conn)
    logger.info("Known processed accessions: %d", len(known))

    # Fetch filing metadata from EFTS
    t0 = time.monotonic()
    filings = fetch_all_form4_filings(start_date, end_date)

    # Filter to only new filings
    new_filings = [f for f in filings if f["accession"] not in known]
    logger.info("EFTS filings: %d total, %d new", len(filings), len(new_filings))

    if not new_filings:
        elapsed = time.monotonic() - t0
        logger.info("No new filings. Done in %.1fs", elapsed)
        update_last_fetch_time(conn)
        conn.close()
        return {"new": 0, "inserted": 0, "elapsed": elapsed}

    # Process only new filings
    total_inserted = 0
    total_parsed = 0
    xml_failures = 0
    buys = 0
    sells = 0

    for i, filing in enumerate(new_filings):
        xml, filed_at = fetch_form4_xml(filing["cik"], filing["accession"])
        if xml is None:
            xml_failures += 1
            # Still mark as processed to avoid retrying bad XMLs every run
            if not dry_run:
                mark_processed(conn, filing["accession"], filing["filing_date"], 0)
            continue

        trades = parse_form4_xml(
            xml, filing["cik"], filing["filing_date"], filing["company"]
        )
        total_parsed += len(trades)
        for t in trades:
            if t["trade_type"] == "buy":
                buys += 1
            else:
                sells += 1

        if not dry_run:
            inserted = insert_trades(conn, trades, filing["accession"], filed_at=filed_at) if trades else 0
            total_inserted += inserted
            mark_processed(conn, filing["accession"], filing["filing_date"], len(trades))

        if (i + 1) % 50 == 0:
            conn.commit()
            logger.info("  %d/%d new filings processed...", i + 1, len(new_filings))

    if not dry_run:
        conn.commit()

    # Post-processing for new inserts
    if not dry_run and total_inserted > 0:
        # Price validation
        try:
            from price_validator import run_validation
            run_validation(conn)
        except Exception as e:
            logger.warning("Price validation error: %s", e)

        # Name cleaning
        try:
            from name_cleaner import clean_name, ensure_column
            ensure_column(conn)
            new_insiders = conn.execute(
                "SELECT insider_id, name, COALESCE(is_entity, 0) FROM insiders WHERE display_name IS NULL"
            ).fetchall()
            if new_insiders:
                for insider_id, name, is_entity in new_insiders:
                    display = clean_name(name, bool(is_entity))
                    conn.execute(
                        "UPDATE insiders SET display_name = ? WHERE insider_id = ?",
                        (display, insider_id),
                    )
                conn.commit()
                logger.info("Cleaned %d new insider names", len(new_insiders))
        except Exception as e:
            logger.warning("Name cleaning error: %s", e)

    elapsed = time.monotonic() - t0
    stats = {
        "new": len(new_filings),
        "parsed": total_parsed,
        "inserted": total_inserted,
        "buys": buys,
        "sells": sells,
        "xml_failures": xml_failures,
        "elapsed": elapsed,
    }

    logger.info(
        "Done: %d new filings → %d trades (%d buys, %d sells) in %.1fs",
        len(new_filings), total_inserted, buys, sells, elapsed,
    )

    update_last_fetch_time(conn)
    conn.close()
    return stats


def main():
    parser = argparse.ArgumentParser(description="Incremental EDGAR Form 4 fetcher")
    parser.add_argument("--days", type=int, default=2, help="Look back N days (default: 2)")
    parser.add_argument("--dry-run", action="store_true", help="Report without inserting")
    args = parser.parse_args()

    run_fetch(days=args.days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
