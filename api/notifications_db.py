from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from config.database import ConnectionWrapper, get_connection as _get_connection


SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications.notification_preferences (
    user_id TEXT PRIMARY KEY,
    email_enabled INTEGER NOT NULL DEFAULT 1,
    in_app_enabled INTEGER NOT NULL DEFAULT 1,
    email_frequency TEXT NOT NULL DEFAULT 'daily',
    high_value_filing INTEGER NOT NULL DEFAULT 1,
    cluster_formation INTEGER NOT NULL DEFAULT 1,
    activity_spike INTEGER NOT NULL DEFAULT 0,
    congress_convergence INTEGER NOT NULL DEFAULT 1,
    watchlist_activity INTEGER NOT NULL DEFAULT 1,
    min_trade_value DOUBLE PRECISION NOT NULL DEFAULT 100000,
    min_insider_tier INTEGER NOT NULL DEFAULT 2,
    portfolio_alert INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT NOW(),
    updated_at TEXT NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications.watchlist (
    user_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    added_at TEXT NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, ticker)
);

CREATE TABLE IF NOT EXISTS notifications.notifications (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    ticker TEXT,
    dedup_key TEXT NOT NULL,
    is_read INTEGER NOT NULL DEFAULT 0,
    emailed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, dedup_key)
);
CREATE INDEX IF NOT EXISTS idx_notifications_user_read
    ON notifications.notifications (user_id, is_read, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_user_created
    ON notifications.notifications (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS notifications.scan_watermarks (
    event_type TEXT PRIMARY KEY,
    last_processed_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications.user_profiles (
    user_id TEXT PRIMARY KEY,
    user_type TEXT,
    primary_use_case TEXT,
    experience_level TEXT,
    referral_source TEXT,
    onboarding_skipped INTEGER DEFAULT 0,
    created_at TEXT DEFAULT NOW(),
    updated_at TEXT DEFAULT NOW()
);
"""


def init_db() -> None:
    """Ensure notifications tables exist (idempotent)."""
    conn = _get_connection(readonly=False)
    try:
        cur = conn._conn.cursor()
        for stmt in SCHEMA.split(';'):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()
    finally:
        conn.close()


def _add_column_if_missing(conn, table: str, column: str, typedef: str) -> None:
    """Add a column to a table if it doesn't already exist."""
    cur = conn._conn.cursor()
    # Parse schema.table if present
    parts = table.split('.')
    schema = parts[0] if len(parts) > 1 else 'notifications'
    tbl = parts[-1]

    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, tbl, column))
    if not cur.fetchone():
        cur.execute(f'ALTER TABLE {table} ADD COLUMN {column} {typedef}')
        conn.commit()


def get_connection() -> ConnectionWrapper:
    """Return a new read-write connection for notifications."""
    conn = _get_connection(readonly=False)
    return conn


@contextmanager
def get_notifications_db() -> Generator[ConnectionWrapper, None, None]:
    """Context manager that yields a read-write connection and closes it on exit."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
