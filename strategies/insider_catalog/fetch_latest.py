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


# A filing that could not be FETCHED is not a filing with no trades. Attempts
# are capped so a genuinely dead accession cannot be retried forever, but the
# cap is generous: everything under it is a transient EDGAR error and EDGAR
# rate-limits aggressively.
MAX_FETCH_ATTEMPTS = 6

# Ceiling on how many previously-failed filings one run re-drives. The normal
# path only looks at the last `--days` days, so without this sweep a filing
# that failed for two days would fall out of the window and never be seen
# again. Two requests each, ~10/s at EDGAR's limit.
RETRY_SWEEP_LIMIT = 200


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
    # WHAT THIS TABLE MEANS CHANGED ON 2026-08-26.
    #
    # It used to record only "we have seen this accession", and the fetcher
    # wrote a row for a filing whose XML DOWNLOAD FAILED with the comment
    # "Still mark as processed to avoid retrying bad XMLs every run". Since
    # get_known_accessions reads the whole table, that filing was then never
    # requested again. EDGAR rate-limits hard, so this converted every
    # transient 403/429/timeout into permanent data loss.
    #
    # The rate was 0.0% every month through 2026-02 and then 21.5% in April,
    # 25.5% in July. Of 24 sampled zero-trade rows, 14 held real
    # non-derivative transactions that are simply absent from `trades`.
    #
    # `status` is the distinction that was missing:
    #   ok        parsed, produced trades
    #   empty     parsed, genuinely has no non-derivative transactions
    #   failed    we never got an answer — RETRY
    #   abandoned failed MAX_FETCH_ATTEMPTS times; kept for audit, not retried
    for ddl in (
        "ALTER TABLE processed_filings ADD COLUMN IF NOT EXISTS status TEXT",
        "ALTER TABLE processed_filings ADD COLUMN IF NOT EXISTS attempts INTEGER DEFAULT 0",
        "ALTER TABLE processed_filings ADD COLUMN IF NOT EXISTS last_error TEXT",
        "ALTER TABLE processed_filings ADD COLUMN IF NOT EXISTS last_attempt_at TEXT",
        # Needed to rebuild the EDGAR request on a retry. The table only ever
        # held the accession, so a failed filing could not be re-fetched even
        # if we had wanted to.
        "ALTER TABLE processed_filings ADD COLUMN IF NOT EXISTS cik TEXT",
        "ALTER TABLE processed_filings ADD COLUMN IF NOT EXISTS company TEXT",
        "CREATE INDEX IF NOT EXISTS idx_processed_retry ON processed_filings (status, attempts)",
    ):
        try:
            conn.execute(ddl)
        except Exception:  # pragma: no cover - older engines / already applied
            pass
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
    """Accessions we are DONE with — i.e. must not fetch again.

    A row whose status is `failed` and which still has attempts left is
    deliberately NOT in this set. That is the whole fix: the old version
    returned every row in the table, so one bad download retired a filing
    permanently.

    NULL status is every row written before 2026-08-26. Those are treated as
    done, because re-driving 948k of them through EDGAR one at a time is the
    wrong tool — the bulk SEC datasets cover that ground far faster.
    """
    rows = conn.execute(
        """SELECT accession FROM processed_filings
            WHERE status IS NULL
               OR status IN ('ok', 'empty', 'abandoned')
               OR attempts >= ?""",
        (MAX_FETCH_ATTEMPTS,),
    ).fetchall()
    return {r[0] for r in rows}


def get_retryable(conn, limit: int) -> list:
    """Filings we failed to fetch and have not given up on, oldest first."""
    return conn.execute(
        """SELECT accession, filing_date, attempts, cik, company
             FROM processed_filings
            WHERE status = 'failed' AND attempts < ? AND cik IS NOT NULL
            ORDER BY filing_date DESC, accession
            LIMIT ?""",
        (MAX_FETCH_ATTEMPTS, limit),
    ).fetchall()


def mark_processed(conn, accession: str, filing_date: str, trade_count: int):
    """Record a filing we actually READ. Never call this for a failed fetch.

    An upsert, not INSERT OR IGNORE: a filing that failed earlier already has
    a row, and the old statement would silently keep the failure and drop the
    successful retry on the floor.
    """
    status = "ok" if trade_count > 0 else "empty"
    conn.execute(
        """INSERT INTO processed_filings
               (accession, filing_date, trade_count, status, attempts,
                last_error, last_attempt_at)
           VALUES (?, ?, ?, ?, 1, NULL, datetime('now'))
           ON CONFLICT (accession) DO UPDATE SET
               trade_count     = excluded.trade_count,
               status          = excluded.status,
               attempts        = processed_filings.attempts + 1,
               last_error      = NULL,
               last_attempt_at = excluded.last_attempt_at""",
        (accession, filing_date, trade_count, status),
    )


def mark_attempt_failed(conn, accession: str, filing_date: str, error: str,
                        cik: str = None, company: str = None):
    """Record a fetch that produced NO ANSWER. The filing stays in the queue.

    This is the row the old code wrote as `processed` with trade_count = 0,
    which is why ~12% of filings vanished. Once attempts reach the cap the
    status becomes `abandoned` so it stops being retried, but it is still
    visible — an abandoned row is a reconciliation finding, not a silent one.
    """
    conn.execute(
        """INSERT INTO processed_filings
               (accession, filing_date, trade_count, status, attempts,
                last_error, last_attempt_at, cik, company)
           VALUES (?, ?, 0, 'failed', 1, ?, datetime('now'), ?, ?)
           ON CONFLICT (accession) DO UPDATE SET
               attempts        = processed_filings.attempts + 1,
               status          = CASE
                                     WHEN processed_filings.attempts + 1 >= ?
                                     THEN 'abandoned' ELSE 'failed' END,
               last_error      = excluded.last_error,
               last_attempt_at = excluded.last_attempt_at,
               cik             = COALESCE(excluded.cik, processed_filings.cik),
               company         = COALESCE(excluded.company, processed_filings.company)""",
        (accession, filing_date, str(error)[:300], cik, company, MAX_FETCH_ATTEMPTS),
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

    # --since limits the compute window to recent trades only — we just
    # ingested filings from the last `days` days, the heavy SMA200/dip_3mo
    # recompute over 1.5M+ historical trades isn't needed for incremental
    # updates. Morning refresh-features at 06:00 PT does the broader scan
    # with a 30-day window. Without --since this hit a 300s timeout every
    # 5-min cycle and pager-stormed alerts.ndjson.
    from datetime import timedelta as _td, date as _d
    since_str = (_d.today() - _td(days=7)).isoformat()
    _run("CW indicators",
         [python, str(script_dir / "compute_cw_indicators.py"), "--since", since_str],
         timeout=600)
    _run("PIT grades",
         [python, str(script_dir / "backfill_pit_grades.py"), "--since", since_str],
         timeout=180)

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


def _process_one(conn, filing: dict, dry_run: bool):
    """Fetch, parse and insert ONE filing.

    Returns (inserted, outcome, parsed, buys, sells) where outcome is
    'ok' | 'empty' | 'failed'. Shared by the live pass and the retry sweep so
    the two cannot drift on what counts as a success.
    """
    acc, fdate = filing["accession"], filing["filing_date"]
    xml, filed_at = fetch_form4_xml(filing["cik"], acc)
    if xml is None:
        # THE BUG THIS FUNCTION EXISTS TO PREVENT: this used to call
        # mark_processed(..., 0), which retired the filing forever.
        if not dry_run:
            mark_attempt_failed(conn, acc, fdate, "xml unavailable from EDGAR",
                                cik=filing.get("cik"), company=filing.get("company"))
        return 0, "failed", 0, 0, 0

    trades = parse_form4_xml(xml, filing["cik"], fdate, filing.get("company"))
    buys = sum(1 for t in trades if t["trade_type"] == "buy")
    sells = len(trades) - buys
    if dry_run:
        return 0, ("ok" if trades else "empty"), len(trades), buys, sells

    inserted = insert_trades(conn, trades, acc, filed_at=filed_at) if trades else 0
    mark_processed(conn, acc, fdate, len(trades))
    return inserted, ("ok" if trades else "empty"), len(trades), buys, sells


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
        inserted, outcome, t_parsed, b, sl = _process_one(conn, filing, dry_run)
        if outcome == "failed":
            xml_failures += 1
        total_inserted += inserted
        total_parsed += t_parsed
        buys += b
        sells += sl

        if (i + 1) % 50 == 0:
            conn.commit()
            logger.info("  %d/%d new filings processed...", i + 1, len(new_filings))

    if not dry_run:
        conn.commit()

    # Re-drive filings an earlier run could not read. Without this the fix is
    # only half a fix: the normal path asks EFTS for the last `--days` days,
    # so anything that failed for longer than that window would never be
    # offered again no matter how retryable we marked it.
    retried = recovered = 0
    try:
        for acc, fdate, attempts, cik, company in get_retryable(conn, RETRY_SWEEP_LIMIT):
            retried += 1
            inserted, outcome, _, _, _ = _process_one(
                conn, {"accession": acc, "filing_date": fdate,
                       "cik": cik, "company": company}, dry_run)
            if outcome != "failed":
                recovered += 1
                total_inserted += inserted
        if retried and not dry_run:
            conn.commit()
    except Exception as exc:            # never let recovery break the live path
        logger.warning("retry sweep aborted after %d: %s", retried, exc)
    if retried:
        logger.info("Retry sweep: %d re-driven, %d recovered", retried, recovered)

    if xml_failures:
        logger.warning(
            "%d filing(s) could not be fetched this run and remain QUEUED "
            "(they are not counted as processed)", xml_failures)

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

    # Freshness contract: trades.filing_date is now current.
    # Write only on non-zero ingest — a 0-row run during off-hours is
    # expected and shouldn't refresh the timestamp (the contract should
    # legitimately catch a multi-day filing drought).
    if not dry_run and total_inserted > 0:
        from framework.contracts.freshness_writer import write_freshness
        write_freshness(
            conn,
            table="trades",
            column="filing_date",
            n_rows_affected=total_inserted,
            populated_by="strategies/insider_catalog/fetch_latest.py",
        )
        conn.commit()

    update_last_fetch_time(conn)
    conn.close()
    return stats


def main():
    parser = argparse.ArgumentParser(description="Incremental EDGAR Form 4 fetcher")
    parser.add_argument("--days", type=int, default=2, help="Look back N days (default: 2)")
    parser.add_argument("--dry-run", action="store_true", help="Report without inserting")
    args = parser.parse_args()

    from framework.observability import pipeline_run

    with pipeline_run(
        "insider_fetch",
        log_path="/Users/derekg/trading-framework/logs/insider-fetch.log",
    ) as prun:
        stats = run_fetch(days=args.days, dry_run=args.dry_run)
        prun.set_rows_written(int(stats.get("inserted", 0) or 0))
        prun.set_metadata({"args": {"days": args.days, "dry_run": args.dry_run},
                           "stats": stats})


if __name__ == "__main__":
    main()
