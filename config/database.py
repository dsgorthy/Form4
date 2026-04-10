"""
PostgreSQL connection layer for the trading framework.

Replaces sqlite3 connections with psycopg2, providing a compatibility
layer that translates SQLite SQL patterns to PostgreSQL automatically.

Usage:
    from config.database import get_connection, get_db

    # For scripts (individual connection):
    conn = get_connection()          # read-write
    conn = get_connection(readonly=True)  # read-only

    # For API (pooled, context manager):
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM trades WHERE ticker = ?", ("AAPL",)).fetchall()
        # ? params auto-translated to %s
        # rows support row["column"] and row[0] access
"""

from __future__ import annotations

import logging
import os
import re
import threading
from contextlib import contextmanager
from typing import Any, Generator, Iterator, Optional, Sequence

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql:///form4")

# ── Connection pool (lazy init) ─────────────────────────────────────────────

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Get or create the shared connection pool."""
    global _pool
    if _pool is None or _pool.closed:
        with _pool_lock:
            if _pool is None or _pool.closed:
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=20,
                    dsn=DATABASE_URL,
                    # TCP keepalives prevent Postgres/kernel from silently
                    # closing idle connections that the pool then hands out dead.
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
    return _pool


def close_pool() -> None:
    """Close the shared connection pool."""
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        _pool = None


# ── SQL translation ─────────────────────────────────────────────────────────

# Pre-compiled patterns for performance
_RE_PARAM = re.compile(r'\?')
_RE_INSERT_OR_IGNORE = re.compile(
    r'INSERT\s+OR\s+IGNORE\s+INTO', re.IGNORECASE
)
_RE_INSERT_OR_REPLACE = re.compile(
    r'INSERT\s+OR\s+REPLACE\s+INTO', re.IGNORECASE
)
_RE_PRAGMA = re.compile(r'^\s*PRAGMA\s+', re.IGNORECASE)
_RE_ATTACH = re.compile(r'^\s*ATTACH\s+DATABASE\s+', re.IGNORECASE)
_RE_DATETIME_NOW = re.compile(r"datetime\('now'\)", re.IGNORECASE)
# strftime('%X', col) → EXTRACT(X FROM col::date) or to_char
_RE_STRFTIME_MONTH = re.compile(r"strftime\('%m',\s*(\w+)\)", re.IGNORECASE)
_RE_STRFTIME_YEAR = re.compile(r"strftime\('%Y',\s*(\w+)\)", re.IGNORECASE)
_RE_STRFTIME_DAY = re.compile(r"strftime\('%d',\s*(\w+)\)", re.IGNORECASE)
_RE_DATE_NOW = re.compile(r"date\('now'\)", re.IGNORECASE)
_RE_DATE_NOW_OFFSET = re.compile(
    r"date\('now',\s*'(-?\d+)\s+(day|days|month|months)'\)", re.IGNORECASE
)
_RE_DATE_PARAM_OFFSET = re.compile(
    r"date\(\?,\s*'(-?\d+)\s+(day|days|month|months)'\)", re.IGNORECASE
)
# Dynamic offset via concatenation: date(?, '-' || ? || ' days') or date(?, '+' || ? || ' days')
_RE_DATE_PARAM_DYNAMIC_OFFSET = re.compile(
    r"date\(\?,\s*'([+-])'\s*\|\|\s*\?\s*\|\|\s*'\s*(day|days)'\)", re.IGNORECASE
)
_RE_GROUP_CONCAT = re.compile(
    r"GROUP_CONCAT\(([^,)]+),\s*'([^']*)'\)", re.IGNORECASE
)
_RE_GROUP_CONCAT_SINGLE = re.compile(
    r"GROUP_CONCAT\(([^,)]+)\)", re.IGNORECASE
)
_RE_IFNULL = re.compile(r'IFNULL\(', re.IGNORECASE)
_RE_JULIANDAY_DIFF = re.compile(
    r"julianday\(([^)]+)\)\s*-\s*julianday\(([^)]+)\)", re.IGNORECASE
)
_RE_AUTOINCREMENT = re.compile(
    r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT', re.IGNORECASE
)


def translate_sql(sql: str) -> tuple[str, bool]:
    """Translate SQLite SQL to PostgreSQL.

    Returns (translated_sql, is_noop) where is_noop=True means
    the statement should be skipped (PRAGMA, ATTACH, etc.).
    """
    stripped = sql.strip()

    # PRAGMA table_info(table) → query information_schema (needed by ensure_columns patterns)
    pragma_ti = re.match(r"PRAGMA\s+table_info\((\w+)\)", stripped, re.IGNORECASE)
    if pragma_ti:
        table = pragma_ti.group(1)
        return (
            f"SELECT ordinal_position AS cid, column_name AS name, data_type AS type, "
            f"CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull, "
            f"column_default AS dflt_value, 0 AS pk "
            f"FROM information_schema.columns WHERE table_name = '{table}' "
            f"ORDER BY ordinal_position"
        ), False

    # Skip other PRAGMAs and ATTACH
    if _RE_PRAGMA.match(stripped):
        return sql, True
    if _RE_ATTACH.match(stripped):
        return sql, True

    result = sql

    # SQLite date/time functions → PG equivalents (BEFORE ? → %s so date(?, ...) is matched)
    # Cast to TEXT since date columns are stored as TEXT in this schema
    result = _RE_DATETIME_NOW.sub("NOW()::text", result)
    result = _RE_DATE_NOW_OFFSET.sub(
        lambda m: f"(CURRENT_DATE + INTERVAL '{m.group(1)} {m.group(2)}')::text",
        result,
    )
    result = _RE_DATE_PARAM_OFFSET.sub(
        lambda m: f"(?::date + INTERVAL '{m.group(1)} {m.group(2)}')::text",
        result,
    )
    # Dynamic offset: date(?, '-' || ? || ' days') → (?::date - ? * interval '1 day')::text
    result = _RE_DATE_PARAM_DYNAMIC_OFFSET.sub(
        lambda m: f"(?::date {m.group(1)} ? * interval '1 day')::text",
        result,
    )
    result = _RE_DATE_NOW.sub('CURRENT_DATE::text', result)

    # Parameter placeholders: ? → %s
    result = _RE_PARAM.sub('%s', result)

    # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    if _RE_INSERT_OR_IGNORE.search(result):
        result = _RE_INSERT_OR_IGNORE.sub('INSERT INTO', result)
        # Append ON CONFLICT DO NOTHING before any RETURNING
        if 'RETURNING' in result.upper():
            result = re.sub(
                r'(\s+RETURNING\s+)',
                r' ON CONFLICT DO NOTHING\1',
                result, flags=re.IGNORECASE,
            )
        else:
            result = result.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'

    # INSERT OR REPLACE → INSERT ... ON CONFLICT DO NOTHING
    # WARNING: This drops updates! Use explicit ON CONFLICT DO UPDATE for upserts.
    if _RE_INSERT_OR_REPLACE.search(result):
        logger.warning("INSERT OR REPLACE translated to DO NOTHING — data may not update. "
                        "Use explicit ON CONFLICT DO UPDATE instead. SQL: %s", sql[:120])
        result = _RE_INSERT_OR_REPLACE.sub('INSERT INTO', result)
        if 'RETURNING' in result.upper():
            result = re.sub(
                r'(\s+RETURNING\s+)',
                r' ON CONFLICT DO NOTHING\1',
                result, flags=re.IGNORECASE,
            )
        else:
            result = result.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'

    # strftime('%m'/''%Y'/'%d', col) → EXTRACT(MONTH/YEAR/DAY FROM col::date)
    result = _RE_STRFTIME_MONTH.sub(r"EXTRACT(MONTH FROM \1::date)", result)
    result = _RE_STRFTIME_YEAR.sub(r"EXTRACT(YEAR FROM \1::date)", result)
    result = _RE_STRFTIME_DAY.sub(r"EXTRACT(DAY FROM \1::date)", result)

    # julianday(a) - julianday(b) → EXTRACT(EPOCH FROM (a::timestamp - b::timestamp)) / 86400
    result = _RE_JULIANDAY_DIFF.sub(
        r"EXTRACT(EPOCH FROM (\1::timestamp - \2::timestamp)) / 86400.0",
        result,
    )

    # GROUP_CONCAT → STRING_AGG
    result = _RE_GROUP_CONCAT.sub(r"STRING_AGG(\1::text, '\2')", result)
    result = _RE_GROUP_CONCAT_SINGLE.sub(r"STRING_AGG(\1::text, ',')", result)

    # IFNULL → COALESCE
    result = _RE_IFNULL.sub('COALESCE(', result)

    # SQLite scalar MIN/MAX(a, b) → PG LEAST/GREATEST(a, b)
    # Match 2-arg MAX/MIN where args can contain nested function calls like COUNT(*)
    def _max_to_greatest(m):
        full = m.group(0)
        inner = full[4:-1]  # strip MAX( and )
        # Find the comma that splits the two args (not inside nested parens)
        depth = 0
        for i, c in enumerate(inner):
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            elif c == ',' and depth == 0:
                a, b = inner[:i].strip(), inner[i+1:].strip()
                return f'GREATEST({a}, {b})'
        return full  # single-arg, don't convert

    def _min_to_least(m):
        full = m.group(0)
        inner = full[4:-1]
        depth = 0
        for i, c in enumerate(inner):
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            elif c == ',' and depth == 0:
                a, b = inner[:i].strip(), inner[i+1:].strip()
                return f'LEAST({a}, {b})'
        return full

    # Match MAX(...) or MIN(...) — the function handles single vs multi-arg
    result = re.sub(r'\bMAX\([^)]*(?:\([^)]*\)[^)]*)*\)', _max_to_greatest, result)
    result = re.sub(r'\bMIN\([^)]*(?:\([^)]*\)[^)]*)*\)', _min_to_least, result)

    return result, False


# ── Row wrapper ──────────────────────────────────────────────────────────────

class Row:
    """Row that supports both row["column"] and row[0] integer index access.

    Compatible with sqlite3.Row: supports dict(), keys(), len(), iteration.
    Automatically converts Decimal to float for SQLite compat.
    """
    __slots__ = ('_data', '_keys')

    def __init__(self, data: dict):
        from decimal import Decimal
        # Coerce Decimal → float (PG returns Decimal for numeric/ROUND results,
        # but all existing Python code expects float)
        self._data = {
            k: float(v) if isinstance(v, Decimal) else v
            for k, v in data.items()
        }
        self._keys = list(data.keys())

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            if isinstance(key, int):
                return self._data[self._keys[key]]
            vals = [self._data[k] for k in self._keys[key]]
            return vals
        return self._data[key]

    def __contains__(self, key):
        return key in self._data

    def __len__(self):
        return len(self._keys)

    def __iter__(self):
        return iter(self._data.values())

    def __repr__(self):
        return f"Row({self._data!r})"

    def keys(self):
        return self._keys

    def values(self):
        return list(self._data.values())

    def items(self):
        return list(self._data.items())

    def get(self, key, default=None):
        return self._data.get(key, default)


# ── Cursor wrapper ───────────────────────────────────────────────────────────

class CursorWrapper:
    """Wraps a psycopg2 cursor with SQL translation and Row results."""

    def __init__(self, cursor):
        self._cursor = cursor
        self._lastrowid = None
        self._description = None

    @property
    def lastrowid(self):
        return self._lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def execute(self, sql: str, params: Any = None) -> 'CursorWrapper':
        translated, is_noop = translate_sql(sql)
        if is_noop:
            return self

        if params is not None:
            # Convert list params to tuple (psycopg2 requires tuples)
            if isinstance(params, list):
                params = tuple(params)
            elif not isinstance(params, tuple):
                params = (params,)

        try:
            self._cursor.execute(translated, params)
        except psycopg2.Error:
            logger.debug("SQL translation:\n  Original: %s\n  Translated: %s", sql, translated)
            # Rollback the failed transaction so the connection is usable again
            try:
                self._cursor.connection.rollback()
            except Exception:
                pass
            raise

        # Capture lastrowid from RETURNING
        if self._cursor.description and 'RETURNING' in sql.upper():
            row = self._cursor.fetchone()
            if row:
                self._lastrowid = list(row.values())[0] if isinstance(row, dict) else row[0]

        return self

    def executemany(self, sql: str, params_list: Sequence) -> 'CursorWrapper':
        translated, is_noop = translate_sql(sql)
        if is_noop:
            return self

        # Convert each params entry to tuple
        converted = []
        for p in params_list:
            if isinstance(p, list):
                converted.append(tuple(p))
            elif isinstance(p, tuple):
                converted.append(p)
            else:
                converted.append((p,))

        try:
            self._cursor.executemany(translated, converted)
        except psycopg2.Error:
            logger.debug("SQL (executemany):\n  Original: %s\n  Translated: %s", sql, translated)
            try:
                self._cursor.connection.rollback()
            except Exception:
                pass
            raise
        return self

    def executescript(self, sql: str) -> 'CursorWrapper':
        """Execute multiple SQL statements separated by semicolons."""
        for stmt in sql.split(';'):
            stmt = stmt.strip()
            if stmt:
                self.execute(stmt)
        return self

    def fetchone(self) -> Optional[Row]:
        if self._cursor.description is None:
            return None
        row = self._cursor.fetchone()
        if row is None:
            return None
        return Row(dict(row)) if isinstance(row, dict) else Row(
            {desc[0]: val for desc, val in zip(self._cursor.description, row)}
        )

    def fetchall(self) -> list[Row]:
        if self._cursor.description is None:
            return []
        rows = self._cursor.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return [Row(dict(r)) for r in rows]
        return [
            Row({desc[0]: val for desc, val in zip(self._cursor.description, r)})
            for r in rows
        ]

    def fetchmany(self, size: int = None) -> list[Row]:
        if self._cursor.description is None:
            return []
        rows = self._cursor.fetchmany(size)
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return [Row(dict(r)) for r in rows]
        return [
            Row({desc[0]: val for desc, val in zip(self._cursor.description, r)})
            for r in rows
        ]

    def __iter__(self) -> Iterator[Row]:
        """Iterate over results row by row (for cursor-based streaming)."""
        if self._cursor.description is None:
            return
        for row in self._cursor:
            if isinstance(row, dict):
                yield Row(dict(row))
            else:
                yield Row({desc[0]: val for desc, val in zip(self._cursor.description, row)})

    def close(self):
        self._cursor.close()


# ── Connection wrapper ───────────────────────────────────────────────────────

class ConnectionWrapper:
    """Wraps a psycopg2 connection with sqlite3-compatible interface."""

    def __init__(self, conn, from_pool: bool = False):
        self._conn = conn
        self._from_pool = from_pool
        self._closed = False
        # Set autocommit off by default (match SQLite behavior)
        self._conn.autocommit = False

    # Ignored attribute — sqlite3.Row factory
    @property
    def row_factory(self):
        return None

    @row_factory.setter
    def row_factory(self, value):
        pass  # No-op: we always return Row objects

    def cursor(self) -> CursorWrapper:
        return CursorWrapper(self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor))

    def execute(self, sql: str, params: Any = None) -> CursorWrapper:
        cur = self.cursor()
        return cur.execute(sql, params)

    def executemany(self, sql: str, params_list: Sequence) -> CursorWrapper:
        cur = self.cursor()
        return cur.executemany(sql, params_list)

    def executescript(self, sql: str) -> CursorWrapper:
        cur = self.cursor()
        return cur.executescript(sql)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._from_pool:
            try:
                self._conn.rollback()  # Clean up any uncommitted transaction
            except Exception:
                pass
            try:
                get_pool().putconn(self._conn)
            except Exception:
                self._conn.close()
        else:
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        self.close()

    def __del__(self):
        if not self._closed:
            self.close()


# ── Public API ───────────────────────────────────────────────────────────────

def get_connection(readonly: bool = False) -> ConnectionWrapper:
    """Get a new database connection (not pooled).

    Use this for pipeline scripts and batch jobs.
    For the API layer, use get_db() which uses the connection pool.
    """
    conn = psycopg2.connect(DATABASE_URL)
    wrapper = ConnectionWrapper(conn, from_pool=False)
    if readonly:
        conn.set_session(readonly=True)
    # Set search path so prices.* and research.* tables are accessible
    conn.cursor().execute("SET search_path TO public, prices, research, notifications")
    conn.commit()
    return wrapper


@contextmanager
def get_db(readonly: bool = True) -> Generator[ConnectionWrapper, None, None]:
    """Context manager for pooled API connections.

    Replaces api/db.py's get_db(). Returns connection to pool on exit.
    Validates connection is alive before returning — retries up to 5 times
    to handle the case where multiple connections in the pool are stale.
    """
    pool = get_pool()
    raw_conn = None
    last_err: Optional[Exception] = None

    # Loop until we get a working connection. The pool may have multiple
    # dead connections after a Postgres restart or long idle period.
    for attempt in range(5):
        candidate = pool.getconn()
        try:
            cur = candidate.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            candidate.commit()
            raw_conn = candidate
            break
        except Exception as e:
            last_err = e
            # Dead connection — close and discard, then try another
            try:
                pool.putconn(candidate, close=True)
            except Exception:
                pass

    if raw_conn is None:
        raise RuntimeError(
            f"Could not get a working DB connection after 5 attempts: {last_err}"
        )

    try:
        if readonly:
            raw_conn.set_session(readonly=True)
        raw_conn.cursor().execute("SET search_path TO public, prices, research, notifications")
        raw_conn.commit()
        wrapper = ConnectionWrapper(raw_conn, from_pool=True)
        yield wrapper
    except Exception:
        try:
            raw_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            pool.putconn(raw_conn)
        except Exception:
            pass
