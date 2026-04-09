#!/usr/bin/env python3
"""Database sync between environments.

Handles:
  - Snapshotting prod → sandbox (full or selective tables)
  - Transferring tested changes from sandbox → prod
  - Pre-migration backups
  - Schema comparison between environments

Usage:
    # Snapshot prod → sandbox (all read-only tables)
    python3 pipelines/db_sync.py snapshot --to sandbox

    # Snapshot specific tables only
    python3 pipelines/db_sync.py snapshot --to sandbox --tables trades,daily_prices

    # Transfer a table from sandbox → prod (after testing)
    python3 pipelines/db_sync.py transfer --from sandbox --to prod --table strategy_portfolio

    # Backup prod before migration
    python3 pipelines/db_sync.py backup --env prod

    # Compare schemas between environments
    python3 pipelines/db_sync.py compare

    # Initialize sandbox DB from prod
    python3 pipelines/db_sync.py init-sandbox
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent

# Environment database paths
ENV_PATHS = {
    "prod": {
        "insiders": BASE / "strategies" / "insider_catalog" / "insiders.db",
        "prices": BASE / "strategies" / "insider_catalog" / "prices.db",
        "research": BASE / "strategies" / "insider_catalog" / "research.db",
        "notifications": BASE / "api" / "notifications.db",
    },
    "sandbox": {
        "insiders": BASE / "strategies" / "insider_catalog" / "sandbox" / "insiders.db",
        "prices": BASE / "strategies" / "insider_catalog" / "sandbox" / "prices.db",
        "research": BASE / "strategies" / "insider_catalog" / "sandbox" / "research.db",
        "notifications": BASE / "strategies" / "insider_catalog" / "sandbox" / "notifications.db",
    },
}

BACKUP_DIR = BASE / "data" / "db-backups"

# Tables that are safe to snapshot (populated by pipelines, not user actions)
READ_ONLY_TABLES = [
    "trades", "insiders", "insider_track_records", "insider_ticker_scores",
    "score_history", "trade_signals", "trade_returns",
    "derivative_trades", "nonderiv_holdings",
]

# Tables that are per-environment (user/strategy state)
WRITE_TABLES = [
    "strategy_portfolio", "portfolios",
]


def get_db_path(env: str, db: str = "insiders") -> Path:
    return ENV_PATHS[env][db]


def ensure_sandbox_dir():
    sandbox_dir = BASE / "strategies" / "insider_catalog" / "sandbox"
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    return sandbox_dir


def snapshot(to_env: str, tables: list[str] | None = None):
    """Copy prod databases to target environment."""
    if to_env == "prod":
        logger.error("Cannot snapshot TO prod. Use 'transfer' for targeted table moves.")
        return

    ensure_sandbox_dir()

    # Full DB copy for prices and research (read-only, no user data)
    for db_name in ["prices", "research"]:
        src = ENV_PATHS["prod"][db_name]
        dst = ENV_PATHS[to_env][db_name]
        if src.exists():
            logger.info(f"Copying {db_name}.db ({src.stat().st_size / 1e9:.1f} GB)...")
            shutil.copy2(src, dst)
            logger.info(f"  → {dst}")

    # For insiders.db: selective table copy if specified, otherwise full copy
    src_db = ENV_PATHS["prod"]["insiders"]
    dst_db = ENV_PATHS[to_env]["insiders"]

    if tables:
        # Selective: copy only specified tables via PG
        logger.info(f"Selective snapshot: {', '.join(tables)}")

        src_conn = get_connection(readonly=True)

        for table in tables:
            logger.info(f"  Copying table: {table}")
            # Verify table exists
            check = src_conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = ?", (table,)
            ).fetchone()
            if not check:
                logger.warning(f"  Table {table} not found in source")
                continue

            # Count rows
            row_count = src_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  → {row_count:,} rows (table exists in PG, no file copy needed)")

        src_conn.close()
        logger.info("Note: with PostgreSQL, selective snapshot is a no-op (single DB)")
    else:
        # Full copy
        logger.info(f"Full snapshot: insiders.db ({src_db.stat().st_size / 1e9:.1f} GB)...")
        shutil.copy2(src_db, dst_db)

    # Copy notifications.db
    src_notif = ENV_PATHS["prod"]["notifications"]
    dst_notif = ENV_PATHS[to_env]["notifications"]
    if src_notif.exists():
        shutil.copy2(src_notif, dst_notif)

    logger.info(f"Snapshot complete: prod → {to_env}")


def transfer(from_env: str, to_env: str, table: str):
    """Transfer a specific table from one environment to another."""
    src_db = get_db_path(from_env)
    dst_db = get_db_path(to_env)

    if to_env == "prod":
        # Safety: backup prod before transfer
        backup_env("prod")
        logger.warning(f"Transferring {table} from {from_env} → PROD")

    conn = get_connection(readonly=True)

    # Verify table exists
    check = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name = ?", (table,)
    ).fetchone()
    if not check:
        logger.error(f"Table {table} not found")
        conn.close()
        return

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.close()

    logger.info(f"Table {table}: {row_count:,} rows (PG: single database, transfer is a no-op)")
    logger.info("Note: with PostgreSQL, prod and sandbox share the same database. "
                "Use schema-based separation or pg_dump for environment isolation.")


def backup_env(env: str):
    """Create a timestamped backup using pg_dump."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = BACKUP_DIR / f"{env}_{ts}.sql.gz"

    db_url = os.environ.get("DATABASE_URL", "postgresql:///form4")
    cmd = f"pg_dump '{db_url}' | gzip > '{backup_file}'"
    logger.info(f"Running pg_dump backup...")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        size_mb = backup_file.stat().st_size / 1e6
        logger.info(f"  Backed up to {backup_file} ({size_mb:.0f} MB)")
    else:
        logger.error(f"  pg_dump failed: {result.stderr}")
        return

    # Prune old backups (keep last 5 per environment)
    all_backups = sorted(BACKUP_DIR.glob(f"{env}_*.sql.gz"))
    for old in all_backups[:-5]:
        old.unlink()
        logger.info(f"  Pruned old backup: {old.name}")


def compare_schemas():
    """List all tables and their column counts in the PostgreSQL database."""
    conn = get_connection(readonly=True)

    tables = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """).fetchall()

    print(f"{'Table':>30} | {'Columns':>7} | {'Rows':>10}")
    print("-" * 55)
    for row in tables:
        table_name = row[0]
        col_count = conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = ?",
            (table_name,)
        ).fetchone()[0]
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except Exception:
            row_count = -1
        print(f"{table_name:>30} | {col_count:>7} | {row_count:>10,}")

    conn.close()


def init_sandbox():
    """Initialize sandbox databases from a full prod snapshot."""
    ensure_sandbox_dir()
    logger.info("Initializing sandbox from prod snapshot...")
    snapshot("sandbox")
    logger.info("Sandbox initialized. You can now run migrations on sandbox independently.")


def main():
    parser = argparse.ArgumentParser(description="Database sync between environments")
    sub = parser.add_subparsers(dest="command")

    snap = sub.add_parser("snapshot", help="Copy prod DB to target environment")
    snap.add_argument("--to", required=True, choices=["sandbox"])
    snap.add_argument("--tables", help="Comma-separated table names (default: all)")

    xfer = sub.add_parser("transfer", help="Transfer a table between environments")
    xfer.add_argument("--from", dest="from_env", required=True, choices=["sandbox", "prod"])
    xfer.add_argument("--to", dest="to_env", required=True, choices=["sandbox", "prod"])
    xfer.add_argument("--table", required=True)

    bak = sub.add_parser("backup", help="Backup an environment's databases")
    bak.add_argument("--env", required=True, choices=["sandbox", "prod"])

    sub.add_parser("compare", help="Compare schemas between prod and sandbox")
    sub.add_parser("init-sandbox", help="Initialize sandbox from prod")

    args = parser.parse_args()

    if args.command == "snapshot":
        tables = args.tables.split(",") if args.tables else None
        snapshot(args.to, tables)
    elif args.command == "transfer":
        transfer(args.from_env, args.to_env, args.table)
    elif args.command == "backup":
        backup_env(args.env)
    elif args.command == "compare":
        compare_schemas()
    elif args.command == "init-sandbox":
        init_sandbox()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
