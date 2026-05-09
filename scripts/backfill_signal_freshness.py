#!/usr/bin/env python3
"""One-shot: seed signal_freshness with current MAX timestamps from source tables.

Run ONCE after Phase 2 P0 deployment, before strategies restart. After this,
the next compute pipeline run overwrites with accurate values via
write_freshness().

Why we need this:
- Phase 1 (2026-05-02) created the signal_freshness schema but the paired
  pipeline wires were never implemented, so the table is empty.
- Phase 2 P0 (this deploy) adds the writes — but they only fire on the
  next compute run.
- Without seeding, the runner's new meta-check (`assert_freshness_system_healthy`)
  raises FreshnessSystemBrokenError immediately on first scan after deploy.

This script reads the same fallback logic the OLD freshness.py used
(MAX(filing_date) where column NOT NULL, etc.) and writes one
signal_freshness row per contracted column. Treat as a TIME MACHINE: we're
saying "as of now, this is the latest the data was effectively current to."
The next nightly compute run replaces these with real timestamps.

Usage:
    ssh derekg@100.78.9.66 'python3 /Users/derekg/trading-framework/scripts/backfill_signal_freshness.py'

    # Dry run (show what would be written without writing):
    python3 scripts/backfill_signal_freshness.py --dry-run

Idempotent — re-running just inserts more rows with newer timestamps.
The freshness lookup uses MAX(last_computed_at) so older rows don't matter.
"""
from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection
from framework.contracts.freshness import FreshnessRegistry


def _max_filing_date(conn, column: str) -> datetime | None:
    """For trades.* columns: MAX(filing_date) WHERE column IS NOT NULL."""
    try:
        row = conn.execute(
            f"SELECT MAX(filing_date) FROM trades WHERE {column} IS NOT NULL"
        ).fetchone()
    except Exception as e:
        print(f"  ! query failed: {e}")
        return None
    if not row or not row[0]:
        return None
    d = row[0]
    if isinstance(d, str):
        return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
    if isinstance(d, datetime):
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def _max_score_as_of(conn) -> datetime | None:
    try:
        row = conn.execute(
            "SELECT MAX(as_of_date) FROM insider_ticker_scores"
        ).fetchone()
    except Exception:
        return None
    if not row or not row[0]:
        return None
    d = row[0]
    if isinstance(d, str):
        return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
    if isinstance(d, datetime):
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def _max_price_date(conn) -> datetime | None:
    try:
        row = conn.execute(
            "SELECT MAX(date) FROM prices.daily_prices WHERE ticker = 'SPY'"
        ).fetchone()
    except Exception:
        return None
    if not row or not row[0]:
        return None
    d = row[0]
    if isinstance(d, str):
        return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
    if isinstance(d, datetime):
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def _max_filing_date_unfiltered(conn) -> datetime | None:
    """For trades.filing_date contract: MAX of the column itself."""
    try:
        row = conn.execute("SELECT MAX(filing_date) FROM trades").fetchone()
    except Exception:
        return None
    if not row or not row[0]:
        return None
    d = row[0]
    if isinstance(d, str):
        return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
    if isinstance(d, datetime):
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def _row_count(conn, table: str, column: str) -> int:
    """Best-effort row count for n_rows_affected."""
    try:
        if "." in table:
            schema, table_name = table.split(".", 1)
            qualified = f"{schema}.{table_name}"
        else:
            qualified = table
        row = conn.execute(
            f"SELECT COUNT(*) FROM {qualified} WHERE {column} IS NOT NULL"
        ).fetchone()
        return int(row[0]) if row and row[0] else 0
    except Exception:
        return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be written without writing")
    args = p.parse_args()

    registry = FreshnessRegistry.get()
    contracts = registry.all()
    conn = get_connection()

    run_id = str(uuid.uuid4())
    print(f"Backfilling signal_freshness for {len(contracts)} contract(s)")
    print(f"Run ID: {run_id}")
    print(f"Mode: {'DRY-RUN' if args.dry_run else 'WRITE'}")
    print("─" * 80)

    written = 0
    skipped = 0

    for c in contracts:
        # Pick the right MAX-timestamp source per contract
        if c.table == "insider_ticker_scores" and c.column == "blended_score":
            ts = _max_score_as_of(conn)
        elif c.table.endswith("daily_prices") and c.column == "date":
            ts = _max_price_date(conn)
        elif c.table == "trades" and c.column == "filing_date":
            ts = _max_filing_date_unfiltered(conn)
        elif c.table == "trades":
            ts = _max_filing_date(conn, c.column)
        else:
            print(f"  ! {c.table}.{c.column}: no seed strategy, skipping")
            skipped += 1
            continue

        if ts is None:
            print(f"  ! {c.table}.{c.column}: no data found, skipping")
            skipped += 1
            continue

        n_rows = _row_count(conn, c.table, c.column)

        age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
        marker = "STALE" if age_h > c.max_staleness_hours else "ok"
        print(f"  {marker:>6}  {c.table}.{c.column:<30s}  age={age_h:6.1f}h  rows={n_rows:>10,}  ts={ts.isoformat()}")

        if not args.dry_run:
            if "." in c.table:
                source, table_name = c.table.split(".", 1)
            else:
                source, table_name = "public", c.table

            conn.execute(
                """
                INSERT INTO signal_freshness
                    (source, table_name, column_name, last_computed_at,
                     n_rows_affected, run_id, populated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (source, table_name, c.column, ts, n_rows, run_id,
                 "scripts/backfill_signal_freshness.py"),
            )
            written += 1

    if not args.dry_run:
        conn.commit()

    conn.close()

    print("─" * 80)
    print(f"Wrote: {written}  Skipped: {skipped}")
    if args.dry_run:
        print("(dry-run — no rows actually written)")


if __name__ == "__main__":
    main()
