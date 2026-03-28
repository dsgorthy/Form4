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
import shutil
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

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
        # Selective: copy only specified tables
        logger.info(f"Selective snapshot: {', '.join(tables)}")
        if not dst_db.exists():
            logger.error(f"Destination DB doesn't exist. Run 'init-sandbox' first.")
            return

        src_conn = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
        dst_conn = sqlite3.connect(str(dst_db))
        dst_conn.execute("PRAGMA journal_mode=wal")

        for table in tables:
            logger.info(f"  Copying table: {table}")
            # Get schema
            schema = src_conn.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not schema:
                logger.warning(f"  Table {table} not found in source")
                continue

            # Drop and recreate
            dst_conn.execute(f"DROP TABLE IF EXISTS {table}")
            dst_conn.execute(schema[0])

            # Copy data in chunks
            rows = src_conn.execute(f"SELECT * FROM {table}").fetchall()
            if rows:
                cols = [d[0] for d in src_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]
                placeholders = ",".join("?" * len(cols))
                dst_conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
                logger.info(f"  → {len(rows):,} rows")

            dst_conn.commit()

        src_conn.close()
        dst_conn.close()
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

    src_conn = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
    dst_conn = sqlite3.connect(str(dst_db))
    dst_conn.execute("PRAGMA journal_mode=wal")

    # Get source schema and data
    schema = src_conn.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not schema:
        logger.error(f"Table {table} not found in {from_env}")
        return

    rows = src_conn.execute(f"SELECT * FROM {table}").fetchall()
    cols = [d[0] for d in src_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]

    # Replace table in destination
    dst_conn.execute(f"DROP TABLE IF EXISTS {table}")
    dst_conn.execute(schema[0])
    if rows:
        placeholders = ",".join("?" * len(cols))
        dst_conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    dst_conn.commit()
    src_conn.close()
    dst_conn.close()

    logger.info(f"Transferred {table}: {len(rows):,} rows from {from_env} → {to_env}")


def backup_env(env: str):
    """Create a timestamped backup of an environment's databases."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_subdir = BACKUP_DIR / f"{env}_{ts}"
    backup_subdir.mkdir()

    for db_name, path in ENV_PATHS[env].items():
        if path.exists():
            dst = backup_subdir / f"{db_name}.db"
            shutil.copy2(path, dst)
            logger.info(f"  Backed up {db_name}.db ({path.stat().st_size / 1e6:.0f} MB)")

    logger.info(f"Backup: {backup_subdir}")

    # Prune old backups (keep last 5 per environment)
    all_backups = sorted(BACKUP_DIR.glob(f"{env}_*"))
    for old in all_backups[:-5]:
        shutil.rmtree(old)
        logger.info(f"  Pruned old backup: {old.name}")


def compare_schemas():
    """Compare table schemas between prod and sandbox."""
    prod_db = get_db_path("prod")
    sandbox_db = get_db_path("sandbox")

    if not sandbox_db.exists():
        logger.error("Sandbox DB doesn't exist. Run 'init-sandbox' first.")
        return

    prod_conn = sqlite3.connect(f"file:{prod_db}?mode=ro", uri=True)
    sandbox_conn = sqlite3.connect(f"file:{sandbox_db}?mode=ro", uri=True)

    prod_tables = {r[0]: r[1] for r in prod_conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()}

    sandbox_tables = {r[0]: r[1] for r in sandbox_conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()}

    all_tables = sorted(set(prod_tables) | set(sandbox_tables))

    print(f"{'Table':>30} | {'Prod':>5} | {'Sandbox':>7} | Status")
    print("-" * 65)
    for t in all_tables:
        in_prod = t in prod_tables
        in_sandbox = t in sandbox_tables
        if in_prod and in_sandbox:
            if prod_tables[t] == sandbox_tables[t]:
                status = "OK"
            else:
                status = "SCHEMA DIFF"
        elif in_prod:
            status = "MISSING in sandbox"
        else:
            status = "EXTRA in sandbox"
        print(f"{t:>30} | {'YES' if in_prod else '':>5} | {'YES' if in_sandbox else '':>7} | {status}")

    prod_conn.close()
    sandbox_conn.close()


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
