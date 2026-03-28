from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

DB_PATH = Path(os.getenv(
    "INSIDERS_DB_PATH",
    str(Path(__file__).resolve().parent / ".." / "strategies" / "insider_catalog" / "insiders.db"),
))

# Derive prices/research DB paths from same directory as insiders.db
PRICES_DB_PATH = Path(os.getenv(
    "PRICES_DB_PATH",
    str(DB_PATH.parent / "prices.db"),
))

RESEARCH_DB_PATH = Path(os.getenv(
    "RESEARCH_DB_PATH",
    str(DB_PATH.parent / "research.db"),
))


def get_connection() -> sqlite3.Connection:
    """Return a new read-only SQLite connection.

    Each call creates a fresh connection for thread safety.
    Uses row_factory = sqlite3.Row for dict-like access.
    """
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=wal")
    conn.execute("PRAGMA query_only=ON")
    # Attach prices DB so daily_prices/option_prices queries work transparently
    if PRICES_DB_PATH.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB_PATH}?mode=ro' AS prices")
    return conn


@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Context manager that yields a read-only connection and closes it on exit."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
