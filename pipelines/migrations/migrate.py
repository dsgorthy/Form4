#!/usr/bin/env python3
"""Idempotent PG migration runner with ledger.

The 2026-04-07 SQLite→PG migration was non-atomic — several scripts kept
reading from the old SQLite cache. This runner ensures every schema change
goes through the same audited gate:

  1. Each `*.sql` in this directory is a versioned migration.
  2. PG `schema_migrations` table records which ones have been applied,
     when, by whom, the git SHA at apply time, and the file's sha256.
  3. The runner refuses to apply if a previously-applied migration's
     checksum differs from what's now on disk (detects retroactive edits).
  4. New migrations are applied in lexical order. Each runs inside a
     transaction; failure rolls back and aborts the whole run.

Naming convention for new migrations:
    YYYY-WNN_NNN_short_slug.sql      (e.g. 2026-W19_002_signal_freshness.sql)

Usage:
    python3 pipelines/migrations/migrate.py --plan      # show what would run
    python3 pipelines/migrations/migrate.py             # apply
    python3 pipelines/migrations/migrate.py --status    # show ledger
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent
LEDGER_BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version       text PRIMARY KEY,
    applied_at    timestamptz NOT NULL DEFAULT NOW(),
    applied_by    text,
    git_sha       text,
    checksum      text NOT NULL,
    duration_ms   integer
);
"""


def _git_sha() -> Optional[str]:
    """Best-effort current commit SHA at repo root."""
    try:
        import subprocess
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=Path(__file__).resolve().parents[2],
        )
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def _user() -> str:
    return f"{os.environ.get('USER', '?')}@{os.uname().nodename}"


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _list_migration_files() -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    return files


def _ensure_ledger(conn) -> None:
    cur = conn.cursor()
    cur.execute(LEDGER_BOOTSTRAP_SQL)
    conn.commit()


def _load_ledger(conn) -> dict[str, dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM schema_migrations ORDER BY version")
    return {row["version"]: dict(row) for row in cur.fetchall()}


def _apply_one(conn, path: Path, version: str, checksum: str) -> None:
    cur = conn.cursor()
    sql = path.read_text()
    t0 = datetime.now(timezone.utc)
    try:
        cur.execute(sql)
        elapsed_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        cur.execute(
            """INSERT INTO schema_migrations
               (version, applied_at, applied_by, git_sha, checksum, duration_ms)
               VALUES (%s, NOW(), %s, %s, %s, %s)""",
            (version, _user(), _git_sha(), checksum, elapsed_ms),
        )
        conn.commit()
        logger.info("applied %s in %dms", version, elapsed_ms)
    except Exception:
        conn.rollback()
        raise


def _detect_drift(disk_files: list[Path], ledger: dict[str, dict]) -> list[str]:
    """Find migrations that exist in the ledger with a checksum that no
    longer matches what's on disk. This indicates someone edited a frozen
    migration after it was applied — never safe."""
    issues: list[str] = []
    for path in disk_files:
        version = path.stem
        if version not in ledger:
            continue
        cs = _checksum(path)
        if cs != ledger[version]["checksum"]:
            issues.append(
                f"{version}: disk checksum {cs[:16]}... differs from "
                f"applied {ledger[version]['checksum'][:16]}..."
            )
    return issues


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--plan", action="store_true",
                   help="Show pending migrations without applying")
    p.add_argument("--status", action="store_true",
                   help="Show ledger contents")
    p.add_argument("--db-url", default=None,
                   help="PG DSN (default: dbname=form4 via local socket)")
    p.add_argument("--allow-drift", action="store_true",
                   help="(DANGEROUS) skip the disk-vs-ledger checksum check")
    args = p.parse_args()

    pg_dsn = args.db_url or "dbname=form4"
    conn = psycopg2.connect(pg_dsn, connect_timeout=10)
    _ensure_ledger(conn)
    ledger = _load_ledger(conn)
    disk = _list_migration_files()

    if args.status:
        print(f"Ledger has {len(ledger)} applied migration(s):")
        for v, row in sorted(ledger.items()):
            print(f"  {v:50s}  applied {row['applied_at']:%Y-%m-%d %H:%M}  by {row['applied_by']}")
        return

    drift = _detect_drift(disk, ledger)
    if drift and not args.allow_drift:
        print("ERROR: checksum drift detected (a previously-applied migration was edited):", file=sys.stderr)
        for issue in drift:
            print(f"  • {issue}", file=sys.stderr)
        print("\nRefusing to apply. Either:", file=sys.stderr)
        print("  - Revert the edits to the original on-disk version, or", file=sys.stderr)
        print("  - Author a NEW migration with the corrected schema, or", file=sys.stderr)
        print("  - Pass --allow-drift (NOT RECOMMENDED) to proceed anyway.", file=sys.stderr)
        sys.exit(2)

    pending: list[Path] = []
    for path in disk:
        if path.stem not in ledger:
            pending.append(path)

    if not pending:
        print(f"No pending migrations. Ledger up to date ({len(ledger)} applied).")
        return

    print(f"Pending migrations ({len(pending)}):")
    for path in pending:
        cs = _checksum(path)
        print(f"  • {path.stem}  ({path.stat().st_size:,} bytes, sha256={cs[:16]}...)")

    if args.plan:
        print("\n--plan: not applying.")
        return

    print()
    for path in pending:
        version = path.stem
        cs = _checksum(path)
        try:
            _apply_one(conn, path, version, cs)
        except Exception as e:
            print(f"\nERROR applying {version}: {e}", file=sys.stderr)
            print("Aborting; remaining pending migrations not applied.", file=sys.stderr)
            sys.exit(3)

    print(f"\nDone. Applied {len(pending)} migration(s).")


if __name__ == "__main__":
    main()
