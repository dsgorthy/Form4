#!/usr/bin/env python3
"""
Migrate SQLite databases to PostgreSQL.

Reads all three SQLite DBs (insiders.db, prices.db, research.db) + notifications.db
and migrates schema + data into a single PostgreSQL database with schemas:
  - public:        tables from insiders.db
  - prices:        tables from prices.db
  - research:      tables from research.db
  - notifications: tables from notifications.db

Usage:
    python3 scripts/migrate_to_pg.py                    # full migration
    python3 scripts/migrate_to_pg.py --schema-only      # just create tables
    python3 scripts/migrate_to_pg.py --data-only        # just migrate data (tables must exist)
    python3 scripts/migrate_to_pg.py --tables trades,insiders  # migrate specific tables
    python3 scripts/migrate_to_pg.py --verify           # verify row counts match
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql:///form4")

# SQLite database paths
_ROOT = Path(__file__).resolve().parents[1]
INSIDERS_DB = _ROOT / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = _ROOT / "strategies" / "insider_catalog" / "prices.db"
RESEARCH_DB = _ROOT / "strategies" / "insider_catalog" / "research.db"
NOTIFICATIONS_DB = _ROOT / "api" / "notifications.db"

# Skip these SQLite-internal tables
SKIP_TABLES = {"sqlite_sequence", "lost_and_found", "lost_and_found_0"}

# Map SQLite DB → PG schema
DB_SCHEMA_MAP = {
    "insiders": "public",
    "prices": "prices",
    "research": "research",
    "notifications": "notifications",
}

BATCH_SIZE = 50_000

# PostgreSQL reserved words that need quoting when used as column names
PG_RESERVED = {
    'right', 'left', 'order', 'group', 'user', 'table', 'column',
    'check', 'default', 'offset', 'limit', 'primary', 'key', 'index',
    'references', 'constraint', 'foreign', 'all', 'select', 'from',
    'where', 'and', 'or', 'not', 'in', 'between', 'like', 'is', 'null',
    'true', 'false', 'as', 'on', 'join', 'cross', 'natural', 'using',
    'grant', 'end', 'do', 'to', 'then', 'when', 'case', 'else',
}


def _quote_reserved_in_columns(sql: str) -> str:
    """Quote known reserved-word column names in CREATE TABLE / INDEX statements.

    Only handles the specific columns that actually conflict (right, order, etc.)
    rather than trying to generically rewrite all SQL.
    """
    # Quote 'right' when used as a column name (followed by type or in column list)
    # In CREATE TABLE: "    right           TEXT"
    result = re.sub(
        r'(\s)right(\s+TEXT\b)', r'\1"right"\2', sql, flags=re.IGNORECASE
    )
    # In UNIQUE/index column lists: (ticker, expiration, strike, right, ...)
    result = re.sub(
        r',\s*right\s*([,\)])', r', "right"\1', result, flags=re.IGNORECASE
    )
    result = re.sub(
        r'\(\s*right\s*,', '("right",', result, flags=re.IGNORECASE
    )
    return result


def _strip_fk_references(sql: str) -> tuple[str, list[str]]:
    """Remove REFERENCES clauses from CREATE TABLE and collect them for later.

    Returns (cleaned_sql, list_of_fk_descriptions).
    """
    fk_refs = []
    # Match REFERENCES table(col) clauses
    result = re.sub(
        r'\s+REFERENCES\s+\w+\(\w+\)',
        '', sql, flags=re.IGNORECASE,
    )
    return result, fk_refs


def translate_create_table(sql: str, pg_schema: str) -> str:
    """Convert a SQLite CREATE TABLE statement to PostgreSQL syntax."""
    result = sql

    # Strip FK REFERENCES (we'll skip FK enforcement — data integrity comes from app logic)
    result, _ = _strip_fk_references(result)

    # INTEGER PRIMARY KEY AUTOINCREMENT → BIGSERIAL PRIMARY KEY
    result = re.sub(
        r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT',
        'BIGSERIAL PRIMARY KEY',
        result, flags=re.IGNORECASE,
    )

    # Bare INTEGER → BIGINT (SQLite INTEGER is 64-bit, PG INTEGER is 32-bit)
    result = re.sub(r'\bINTEGER\b', 'BIGINT', result, flags=re.IGNORECASE)

    # DEFAULT (datetime('now')) → DEFAULT NOW()
    result = re.sub(r"DEFAULT\s*\(datetime\('now'\)\)", "DEFAULT NOW()", result, flags=re.IGNORECASE)
    result = re.sub(r"DEFAULT\s+datetime\('now'\)", "DEFAULT NOW()", result, flags=re.IGNORECASE)

    # date('now', '-N days') → CURRENT_DATE + INTERVAL
    result = re.sub(
        r"date\('now',\s*'(-?\d+)\s+(days?|months?)'\)",
        lambda m: f"CURRENT_DATE + INTERVAL '{m.group(1)} {m.group(2)}'",
        result, flags=re.IGNORECASE,
    )
    result = re.sub(r"date\('now'\)", "CURRENT_DATE", result, flags=re.IGNORECASE)

    # REAL → DOUBLE PRECISION
    result = re.sub(r'\bREAL\b', 'DOUBLE PRECISION', result, flags=re.IGNORECASE)

    # DEFAULT "string" → DEFAULT 'string' (PG uses double quotes for identifiers)
    result = re.sub(
        r'''DEFAULT\s+"([^"]*)"''',
        r"DEFAULT '\1'",
        result,
    )

    # Remove square bracket quoting
    result = re.sub(r'\[(\w+)\]', r'"\1"', result)

    # Quote reserved-word column names
    result = _quote_reserved_in_columns(result)

    # Handle schema-qualified table name
    if pg_schema != "public":
        result = re.sub(
            r'CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?:"?(\w+)"?)',
            lambda m: f'CREATE TABLE {m.group(1) or ""}{pg_schema}.{m.group(2)}',
            result, count=1, flags=re.IGNORECASE,
        )

    return result


def translate_create_index(sql: str, pg_schema: str, table_map: set[str]) -> str:
    """Convert a SQLite CREATE INDEX statement to PostgreSQL."""
    result = sql

    # Remove square bracket quoting — replace with double-quote quoting
    result = re.sub(r'\[(\w+)\]', r'"\1"', result)

    # Quote reserved words in column lists
    result = _quote_reserved_in_columns(result)

    # Schema-qualify the table reference in ON clause
    if pg_schema != "public":
        result = re.sub(
            r'\bON\s+(\w+)',
            lambda m: f'ON {pg_schema}.{m.group(1)}' if m.group(1) in table_map else m.group(0),
            result, count=1, flags=re.IGNORECASE,
        )

    return result


def translate_create_view(sql: str, pg_schema: str) -> str:
    """Convert a SQLite CREATE VIEW statement to PostgreSQL."""
    result = sql

    result = re.sub(
        r'CREATE\s+VIEW\s+IF\s+NOT\s+EXISTS',
        'CREATE OR REPLACE VIEW',
        result, flags=re.IGNORECASE,
    )

    result = re.sub(
        r"date\('now',\s*'(-?\d+)\s+(days?|months?)'\)",
        lambda m: f"CURRENT_DATE + INTERVAL '{m.group(1)} {m.group(2)}'",
        result, flags=re.IGNORECASE,
    )
    result = re.sub(r"date\('now'\)", "CURRENT_DATE", result, flags=re.IGNORECASE)

    return result


def get_sqlite_tables(sqlite_conn: sqlite3.Connection) -> list[str]:
    """Get all user tables from a SQLite database."""
    rows = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return [r[0] for r in rows if r[0] not in SKIP_TABLES]


def get_sqlite_schema(sqlite_conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    """Get all CREATE statements from SQLite.

    Returns list of (type, name, sql) tuples.
    """
    rows = sqlite_conn.execute(
        "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name"
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows if r[1] not in SKIP_TABLES and 'sqlite_' not in r[1]]


def create_schema(pg_conn, sqlite_path: Path, pg_schema: str) -> None:
    """Create PG tables/indexes/views from a SQLite database's schema."""
    sqlite_conn = sqlite3.connect(str(sqlite_path))
    schema_items = get_sqlite_schema(sqlite_conn)

    tables = {name for typ, name, _ in schema_items if typ == "table"}
    cur = pg_conn.cursor()

    # Create schema if not public
    if pg_schema != "public":
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {pg_schema}")

    # Create tables first — commit each individually so one failure doesn't undo others
    for typ, name, sql in schema_items:
        if typ != "table":
            continue
        try:
            pg_sql = translate_create_table(sql, pg_schema)
            cur.execute(pg_sql)
            pg_conn.commit()
            logger.info("  Created table: %s.%s", pg_schema, name)
        except psycopg2.Error as e:
            pg_conn.rollback()
            if "already exists" in str(e):
                logger.info("  Table %s.%s already exists, skipping", pg_schema, name)
            else:
                logger.error("  Failed to create table %s.%s: %s\n  SQL: %s", pg_schema, name, e, pg_sql[:200])

    # Create indexes
    for typ, name, sql in schema_items:
        if typ != "index":
            continue
        try:
            pg_sql = translate_create_index(sql, pg_schema, tables)
            cur.execute(pg_sql)
            pg_conn.commit()
        except psycopg2.Error as e:
            pg_conn.rollback()
            if "already exists" not in str(e):
                logger.warning("  Index %s failed: %s", name, str(e)[:100])

    # Create views
    for typ, name, sql in schema_items:
        if typ != "view":
            continue
        try:
            pg_sql = translate_create_view(sql, pg_schema)
            cur.execute(pg_sql)
            pg_conn.commit()
            logger.info("  Created view: %s.%s", pg_schema, name)
        except psycopg2.Error as e:
            pg_conn.rollback()
            logger.warning("  View %s failed: %s", name, str(e)[:100])
    sqlite_conn.close()


def migrate_table_data(
    pg_conn,
    sqlite_conn: sqlite3.Connection,
    table: str,
    pg_schema: str,
) -> int:
    """Migrate data from one SQLite table to PostgreSQL.

    Returns the number of rows migrated.
    """
    # Get column info from SQLite
    col_info = sqlite_conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    columns = [c[1] for c in col_info]

    # Skip generated columns (they can't be inserted into)
    # Check for GENERATED ALWAYS in the CREATE TABLE SQL
    create_sql = sqlite_conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if create_sql:
        gen_cols = set()
        for col in columns:
            # Check if column is GENERATED ALWAYS
            pattern = re.compile(
                rf'\b{re.escape(col)}\b[^,]*GENERATED\s+ALWAYS', re.IGNORECASE
            )
            if pattern.search(create_sql[0]):
                gen_cols.add(col)
        columns = [c for c in columns if c not in gen_cols]

    if not columns:
        return 0

    # Remove square brackets from column names
    clean_columns = [c.strip('[]') for c in columns]

    # Build INSERT statement
    qualified_table = f"{pg_schema}.{table}" if pg_schema != "public" else table
    col_list = ", ".join(f'"{c}"' for c in clean_columns)
    placeholders = ", ".join(["%s"] * len(clean_columns))
    insert_sql = f"INSERT INTO {qualified_table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    # Get PG column types for data coercion (SQLite allows text in REAL columns, PG doesn't)
    pg_cur_meta = pg_conn.cursor()
    pg_cur_meta.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (pg_schema, table))
    pg_types = {row[0]: row[1] for row in pg_cur_meta.fetchall()}

    # Build coercion map: column index → type that needs coercion
    numeric_types = {'double precision', 'real', 'numeric', 'integer', 'bigint', 'smallint'}
    coerce_indices = []
    for i, col in enumerate(clean_columns):
        pg_type = pg_types.get(col, '')
        if pg_type in numeric_types:
            coerce_indices.append(i)

    def _coerce_row(row: tuple) -> tuple:
        """Coerce non-numeric values to None for numeric PG columns."""
        if not coerce_indices:
            return row
        row_list = list(row)
        for idx in coerce_indices:
            val = row_list[idx]
            if val is not None and isinstance(val, str):
                try:
                    float(val)
                except (ValueError, TypeError):
                    row_list[idx] = None
        return tuple(row_list)

    # Count source rows
    total = sqlite_conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    if total == 0:
        return 0

    # Stream data in batches
    cursor = sqlite_conn.execute(
        f"SELECT {', '.join(f'[{c}]' for c in columns)} FROM [{table}]"
    )

    pg_cur = pg_conn.cursor()
    migrated = 0
    batch = []

    for row in cursor:
        batch.append(_coerce_row(tuple(row)))
        if len(batch) >= BATCH_SIZE:
            psycopg2.extras.execute_values(
                pg_cur,
                f"INSERT INTO {qualified_table} ({col_list}) VALUES %s ON CONFLICT DO NOTHING",
                batch,
                page_size=BATCH_SIZE,
            )
            migrated += len(batch)
            batch = []
            if migrated % 200_000 == 0:
                logger.info("    %s: %d/%d rows (%.0f%%)", table, migrated, total, migrated / total * 100)

    if batch:
        psycopg2.extras.execute_values(
            pg_cur,
            f"INSERT INTO {qualified_table} ({col_list}) VALUES %s ON CONFLICT DO NOTHING",
            batch,
            page_size=BATCH_SIZE,
        )
        migrated += len(batch)

    pg_conn.commit()
    return migrated


def reset_sequences(pg_conn, pg_schema: str, tables: list[str]) -> None:
    """Reset SERIAL sequences to max(id) for all tables."""
    cur = pg_conn.cursor()
    for table in tables:
        qualified = f"{pg_schema}.{table}" if pg_schema != "public" else table
        # Find SERIAL columns (sequences named table_column_seq)
        try:
            cur.execute(f"""
                SELECT column_name, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                  AND column_default LIKE 'nextval%%'
            """, (pg_schema, table))
            for col_name, col_default in cur.fetchall():
                seq_match = re.search(r"nextval\('([^']+)'", col_default)
                if seq_match:
                    seq_name = seq_match.group(1)
                    cur.execute(f'SELECT MAX("{col_name}") FROM {qualified}')
                    max_val = cur.fetchone()[0]
                    if max_val is not None:
                        cur.execute(f"SELECT setval('{seq_name}', {max_val})")
                        logger.info("  Reset sequence %s to %d", seq_name, max_val)
        except psycopg2.Error as e:
            pg_conn.rollback()
            logger.warning("  Sequence reset failed for %s.%s: %s", pg_schema, table, e)
    pg_conn.commit()


def verify_counts(pg_conn, sqlite_conn: sqlite3.Connection, tables: list[str], pg_schema: str) -> bool:
    """Verify row counts match between SQLite and PostgreSQL."""
    all_match = True
    pg_cur = pg_conn.cursor()
    for table in tables:
        sqlite_count = sqlite_conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        qualified = f"{pg_schema}.{table}" if pg_schema != "public" else table
        try:
            pg_cur.execute(f"SELECT COUNT(*) FROM {qualified}")
            pg_count = pg_cur.fetchone()[0]
        except psycopg2.Error:
            pg_conn.rollback()
            pg_count = 0

        match = "OK" if pg_count >= sqlite_count else "MISMATCH"
        if pg_count < sqlite_count:
            all_match = False
        logger.info("  %s.%-30s SQLite: %10d  PG: %10d  %s",
                     pg_schema, table, sqlite_count, pg_count, match)
    return all_match


def migrate_db(pg_conn, sqlite_path: Path, pg_schema: str,
               schema_only: bool = False, data_only: bool = False,
               only_tables: set[str] | None = None) -> None:
    """Migrate one SQLite database to PostgreSQL."""
    if not sqlite_path.exists():
        logger.warning("SQLite DB not found: %s — skipping", sqlite_path)
        return

    logger.info("=== Migrating %s → %s schema ===", sqlite_path.name, pg_schema)
    sqlite_conn = sqlite3.connect(str(sqlite_path))

    if not data_only:
        logger.info("Creating schema...")
        create_schema(pg_conn, sqlite_path, pg_schema)

    if not schema_only:
        tables = get_sqlite_tables(sqlite_conn)
        if only_tables:
            tables = [t for t in tables if t in only_tables]

        logger.info("Migrating data for %d tables...", len(tables))
        for table in tables:
            t0 = time.time()
            count = migrate_table_data(pg_conn, sqlite_conn, table, pg_schema)
            elapsed = time.time() - t0
            if count > 0:
                logger.info("  %s.%-30s %10d rows  (%.1fs, %.0f rows/s)",
                            pg_schema, table, count, elapsed,
                            count / elapsed if elapsed > 0 else 0)

        logger.info("Resetting sequences...")
        reset_sequences(pg_conn, pg_schema, tables)

        logger.info("Running ANALYZE...")
        pg_conn.autocommit = True
        cur = pg_conn.cursor()
        for table in tables:
            qualified = f"{pg_schema}.{table}" if pg_schema != "public" else table
            cur.execute(f"ANALYZE {qualified}")
        pg_conn.autocommit = False

        logger.info("Verifying counts...")
        verify_counts(pg_conn, sqlite_conn, tables, pg_schema)

    sqlite_conn.close()


def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite to PostgreSQL")
    parser.add_argument("--schema-only", action="store_true", help="Only create schema")
    parser.add_argument("--data-only", action="store_true", help="Only migrate data")
    parser.add_argument("--tables", help="Comma-separated list of tables to migrate")
    parser.add_argument("--verify", action="store_true", help="Only verify row counts")
    parser.add_argument("--db", choices=["insiders", "prices", "research", "notifications", "all"],
                        default="all", help="Which database to migrate (default: all)")
    args = parser.parse_args()

    only_tables = set(args.tables.split(",")) if args.tables else None

    pg_conn = psycopg2.connect(DATABASE_URL)

    # Create schemas
    cur = pg_conn.cursor()
    for schema in ["prices", "research", "notifications"]:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    pg_conn.commit()

    db_map = {
        "insiders": (INSIDERS_DB, "public"),
        "prices": (PRICES_DB, "prices"),
        "research": (RESEARCH_DB, "research"),
        "notifications": (NOTIFICATIONS_DB, "notifications"),
    }

    if args.verify:
        for name, (path, schema) in db_map.items():
            if args.db != "all" and args.db != name:
                continue
            if not path.exists():
                continue
            sqlite_conn = sqlite3.connect(str(path))
            tables = get_sqlite_tables(sqlite_conn)
            if only_tables:
                tables = [t for t in tables if t in only_tables]
            logger.info("=== Verifying %s ===", name)
            verify_counts(pg_conn, sqlite_conn, tables, schema)
            sqlite_conn.close()
        pg_conn.close()
        return

    t_start = time.time()

    for name, (path, schema) in db_map.items():
        if args.db != "all" and args.db != name:
            continue
        migrate_db(pg_conn, path, schema,
                   schema_only=args.schema_only,
                   data_only=args.data_only,
                   only_tables=only_tables)

    elapsed = time.time() - t_start
    logger.info("=== Migration complete in %.1f minutes ===", elapsed / 60)

    pg_conn.close()


if __name__ == "__main__":
    main()
