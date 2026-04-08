from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from config.database import ConnectionWrapper, get_db as _get_db, get_connection as _get_connection


def get_connection() -> ConnectionWrapper:
    """Return a new read-only database connection.

    Each call creates a fresh connection for thread safety.
    Returns a ConnectionWrapper that supports row["column"] access.
    """
    return _get_connection(readonly=True)


@contextmanager
def get_db() -> Generator[ConnectionWrapper, None, None]:
    """Context manager that yields a read-only pooled connection."""
    with _get_db(readonly=True) as conn:
        yield conn
