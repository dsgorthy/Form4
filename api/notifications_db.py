from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

DB_PATH = Path(os.getenv(
    "NOTIFICATIONS_DB_PATH",
    str(Path(__file__).resolve().parent / "notifications.db"),
))

SCHEMA = """
CREATE TABLE IF NOT EXISTS notification_preferences (
    user_id TEXT PRIMARY KEY,
    email_enabled INTEGER NOT NULL DEFAULT 1,
    in_app_enabled INTEGER NOT NULL DEFAULT 1,
    email_frequency TEXT NOT NULL DEFAULT 'daily',  -- 'realtime' | 'daily'
    high_value_filing INTEGER NOT NULL DEFAULT 1,
    cluster_formation INTEGER NOT NULL DEFAULT 1,
    activity_spike INTEGER NOT NULL DEFAULT 0,  -- opt-in: noisy category
    congress_convergence INTEGER NOT NULL DEFAULT 1,
    watchlist_activity INTEGER NOT NULL DEFAULT 1,
    min_trade_value REAL NOT NULL DEFAULT 100000,
    min_insider_tier INTEGER NOT NULL DEFAULT 2,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS watchlist (
    user_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    added_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, ticker)
);

CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    ticker TEXT,
    dedup_key TEXT NOT NULL,
    is_read INTEGER NOT NULL DEFAULT 0,
    emailed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_id, dedup_key)
);
CREATE INDEX IF NOT EXISTS idx_notifications_user_read
    ON notifications (user_id, is_read, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_user_created
    ON notifications (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS scan_watermarks (
    event_type TEXT PRIMARY KEY,
    last_processed_date TEXT NOT NULL
);
"""


def init_db() -> None:
    """Create notifications database and tables if they don't exist."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.executescript(SCHEMA)
        # Additive migrations — safe to re-run
        _add_column_if_missing(conn, "notification_preferences", "portfolio_alert", "INTEGER NOT NULL DEFAULT 1")
    finally:
        conn.close()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, typedef: str) -> None:
    """Add a column to a table if it doesn't already exist."""
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {typedef}")
        conn.commit()


def get_connection() -> sqlite3.Connection:
    """Return a new read-write SQLite connection for notifications."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=wal")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_notifications_db() -> Generator[sqlite3.Connection, None, None]:
    """Context manager that yields a read-write connection and closes it on exit."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
