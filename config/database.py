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
import time
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

#: Postgres `max_connections` on Studio. Not a guess — `SHOW max_connections`.
PG_MAX_CONNECTIONS = 100

#: Connections kept for everything that is not the API: Dagster's daemon and
#: run workers, the launchd jobs, the nightly backfills, and a human with psql.
#: 17 were in use box-wide at a quiet moment on 2026-10-02.
NON_API_CONNECTION_RESERVE = 36

#: Uvicorn worker processes, from the CMD in deploy/Dockerfile.api. This pool is
#: module state, so each worker holds its own — the API's ceiling is
#: workers x maxconn, which is the arithmetic that nearly shipped at 128/100.
API_WORKERS = 4

#: Per-worker pool ceiling, derived rather than typed so the three numbers
#: above cannot drift apart silently.
API_POOL_MAX_PER_WORKER = (PG_MAX_CONNECTIONS - NON_API_CONNECTION_RESERVE) // API_WORKERS


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Get or create the shared connection pool."""
    global _pool
    if _pool is None or _pool.closed:
        with _pool_lock:
            if _pool is None or _pool.closed:
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2,
                    # PER WORKER PROCESS, AND THERE ARE FOUR OF THEM.
                    # deploy/Dockerfile.api runs `uvicorn --workers 4`, and this
                    # pool is module state, so the API's real ceiling is
                    # 4 x maxconn. At maxconn=32 that is 128 connections against
                    # Postgres's max_connections=100 — the API would exhaust the
                    # server and take Dagster, the strategy runners and psql
                    # down with it.
                    #
                    # The budget: 100 total, less ~36 reserved for everything
                    # that is not the API (17 in use box-wide at a normal
                    # moment, plus headroom for a backfill and a human). That
                    # leaves 64 across 4 workers.
                    #
                    # Raising this is NOT the answer to a burst — _checkout
                    # waits rather than failing. test_pool_fits_postgres_budget
                    # fails the build if this, the worker count, or the reserve
                    # stop adding up.
                    maxconn=API_POOL_MAX_PER_WORKER,
                    dsn=DATABASE_URL,
                    # TCP keepalives prevent Postgres/kernel from silently
                    # closing idle connections that the pool then hands out dead.
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
    return _pool


#: How long a request will wait for a free pooled connection before giving up.
#: Matches the 5s `connect_timeout` the per-request implementation used, so the
#: worst case a caller can see is unchanged by pooling.
POOL_WAIT_SECONDS = 5.0


def _checkout(deadline_s: float = POOL_WAIT_SECONDS):
    """Take a connection from the pool, WAITING rather than failing on a burst.

    `psycopg2.pool.ThreadedConnectionPool.getconn` does not block — it raises
    `PoolError: connection pool exhausted` the moment `maxconn` are checked out.
    Measured on Studio against the real database before this existed: 40
    concurrent threads against maxconn=20 turned **593 of 1,000 requests into
    errors**, and in production each one is a 500 the frontend renders as a 502.

    A read API should turn a burst into LATENCY, not into errors. Queries here
    are sub-50ms, so 20 connections serve several hundred requests a second;
    waiting briefly is strictly better than failing, and the deadline keeps a
    genuinely stuck pool from hanging a worker forever.

    `PoolError` still propagates once the deadline passes — a pool that cannot
    produce a connection in five seconds is a real problem and must be loud.
    """
    pool = get_pool()
    start = time.monotonic()
    delay = 0.005
    while True:
        try:
            return pool.getconn()
        except psycopg2.pool.PoolError:
            if time.monotonic() - start >= deadline_s:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 0.05)


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
# Composite formats: 'YYYY-MM' (year-month) and 'YYYY-MM-DD' (full date).
_RE_STRFTIME_YM = re.compile(r"strftime\('%Y-%m',\s*(\w+)\)", re.IGNORECASE)
_RE_STRFTIME_YMD = re.compile(r"strftime\('%Y-%m-%d',\s*(\w+)\)", re.IGNORECASE)
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
# SELECT name FROM sqlite_master WHERE type='table' AND name='X' → information_schema lookup
_RE_SQLITE_MASTER_TABLE = re.compile(
    r"SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*'table'\s+AND\s+name\s*=\s*'([^']+)'",
    re.IGNORECASE,
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

    # SELECT name FROM sqlite_master WHERE type='table' AND name='X' → information_schema
    sm = _RE_SQLITE_MASTER_TABLE.match(stripped)
    if sm:
        table = sm.group(1)
        return (
            f"SELECT table_name AS name FROM information_schema.tables "
            f"WHERE table_name = '{table}'"
        ), False

    result = sql

    # SQLite date/time functions → PG equivalents (BEFORE ? → %s so date(?, ...) is matched)
    # Cast to TEXT since date columns are stored as TEXT in this schema.
    #
    # CRITICAL: `(CURRENT_DATE + INTERVAL '-N days')` yields a TIMESTAMP, and
    # `::text` formats it as 'YYYY-MM-DD HH:MM:SS'. Comparing 'YYYY-MM-DD'
    # (the column) >= 'YYYY-MM-DD HH:MM:SS' is a STRING comparison that
    # evaluates FALSE when the date parts are equal (the shorter string is
    # the prefix), causing an off-by-one in lookback windows. Always cast
    # the intermediate result to ::date first, then ::text, so the final
    # value is exactly 'YYYY-MM-DD'.
    result = _RE_DATETIME_NOW.sub("NOW()::text", result)
    result = _RE_DATE_NOW_OFFSET.sub(
        lambda m: f"((CURRENT_DATE + INTERVAL '{m.group(1)} {m.group(2)}')::date)::text",
        result,
    )
    result = _RE_DATE_PARAM_OFFSET.sub(
        lambda m: f"((?::date + INTERVAL '{m.group(1)} {m.group(2)}')::date)::text",
        result,
    )
    # Dynamic offset: date(?, '-' || ? || ' days') → ((?::date - ? * interval '1 day')::date)::text
    result = _RE_DATE_PARAM_DYNAMIC_OFFSET.sub(
        lambda m: f"((?::date {m.group(1)} ? * interval '1 day')::date)::text",
        result,
    )
    result = _RE_DATE_NOW.sub('CURRENT_DATE::text', result)

    # Strip SQL comments BEFORE the ? → %s substitution. A literal ?
    # inside a comment ("-- did it pass?") would otherwise be treated as
    # a parameter placeholder and crash psycopg2 with IndexError when the
    # caller's params don't have a corresponding value. Affects line
    # comments (--) and block comments (/* ... */).
    result = re.sub(r'--[^\n]*', '', result)
    result = re.sub(r'/\*.*?\*/', '', result, flags=re.DOTALL)

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

    # Composite strftime formats first — order matters so simpler regexes
    # below don't accidentally consume part of these.
    result = _RE_STRFTIME_YMD.sub(r"TO_CHAR(\1::date, 'YYYY-MM-DD')", result)
    result = _RE_STRFTIME_YM.sub(r"TO_CHAR(\1::date, 'YYYY-MM')", result)
    # strftime('%m'/''%Y'/'%d', col) → EXTRACT(MONTH/YEAR/DAY FROM col::date)
    result = _RE_STRFTIME_MONTH.sub(r"EXTRACT(MONTH FROM \1::date)", result)
    result = _RE_STRFTIME_YEAR.sub(r"EXTRACT(YEAR FROM \1::date)", result)
    result = _RE_STRFTIME_DAY.sub(r"EXTRACT(DAY FROM \1::date)", result)

    # julianday(a) - julianday(b) → EXTRACT(EPOCH FROM (a::timestamp - b::timestamp)) / 86400
    result = _RE_JULIANDAY_DIFF.sub(
        r"EXTRACT(EPOCH FROM (\1::timestamp - \2::timestamp)) / 86400.0",
        result,
    )

    # INTEGER PRIMARY KEY AUTOINCREMENT → BIGSERIAL PRIMARY KEY
    # SQLite-isms inside CREATE TABLE statements (notably compute_signals.py
    # bootstrapping trade_signals).
    result = _RE_AUTOINCREMENT.sub('BIGSERIAL PRIMARY KEY', result)

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
        """Execute multiple SQL statements separated by semicolons.

        Strips comments BEFORE splitting on ';' so a literal ';' inside a
        comment doesn't shred a single statement into two halves
        (PG then chokes on the comment-text-as-SQL fragment; SQLite tolerated it).
        """
        # Strip line comments (-- to end of line) and block comments (/* ... */).
        sql_no_comments = re.sub(r'--[^\n]*', '', sql)
        sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_no_comments, flags=re.DOTALL)
        for stmt in sql_no_comments.split(';'):
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
    """Context manager for API database connections. POOLED, max 20.

    ## Why this is pooled, on the third attempt

    The API runs in a Colima VM and reaches Postgres over TCP through Lima's
    user-network relay (`DATABASE_URL=postgresql://host.docker.internal/form4`),
    because the host's Postgres socket in /tmp is not mounted into the VM. So
    every connection is a new TCP handshake through a userspace relay, not a
    local socket open.

    A connection per request survives ordinary traffic and does not survive a
    crawl. On 2026-10-02, with a scraper at **21 requests/second across 6,610
    distinct IPs** (one request per IP, to defeat rate limiting), the API was
    opening roughly 50 connections a second and logged **1,708**
    `connection to server at "host.docker.internal" (192.168.5.2), port 5432
    failed: timeout expired` in thirty minutes. 10% of all origin requests
    returned 502.

    Postgres was never involved in that failure: 17 of 100 connections, and
    3ms to connect when asked directly. TIME_WAIT was 2,247 of 16,384 — 14%,
    not exhaustion. **What saturated was the relay**, plus the API container's
    1024-fd soft limit (`could not look up local user ID 100: Too many open
    files`, 28 times — psycopg2 unable to read /etc/passwd for want of an fd).

    Pooling takes steady-state connection churn from ~50/second to ~0 and caps
    concurrent connections at 20. `minconn=2, maxconn=20` in `get_pool()`; at
    21 req/s with sub-50ms queries the needed concurrency is 1-2, so 20 is
    headroom, and an exhausted pool raises `PoolError` immediately rather than
    hanging for the 5-second connect timeout.

    ## The mistake this must not repeat

    I pooled this on 2026-09-30 and broke production within the hour.
    `ConnectionWrapper.close()` ALREADY returns a pooled connection via
    `get_pool().putconn()` when its `from_pool` flag is set, and it is reached
    from both `__exit__` and `__del__`. My version called `putconn` in a
    `finally` as well, so every connection was returned TWICE, landed in the
    free list twice, and was handed to two concurrent requests at once:

        psycopg2.OperationalError: connection pointer is NULL
        psycopg2.InterfaceError: cursor already closed

    The rule that version lacked: **ONE object owns the connection's lifecycle,
    and a test pins it** — not a docstring claiming it, which is what I wrote
    last time. The owner is the `ConnectionWrapper`; the test is
    `tests/unit/test_pooled_connection_has_one_owner.py`, which counts getconn
    against putconn through normal exit, an exception, and a double close.

    ## What a pooled connection needs that a fresh one did not

    Session state is inherited from the previous holder, so `readonly` and
    `search_path` are set on every checkout, and a `rollback()` precedes
    `set_session` because it refuses to run inside a transaction. See the
    comments in the body.

    Historical note, since it misled the 09-30 diagnosis: the urgency then came
    from `grep '127.0.0.1.5432'`, which in netstat output also matches ephemeral
    ports 54320-54329. Anchored on the state column with the foreign port
    pinned, idle steady state is ~483 TIME_WAIT sockets to Postgres. The number
    that justifies pooling is the 1,708 connect timeouts above, measured under
    load — not a port count.
    """
    raw_conn = _checkout()

    # THE WRAPPER OWNS THE RETURN. It is constructed before anything that can
    # raise, so there is exactly one object responsible for this connection from
    # here on, and `finally` below is the single place it is released.
    #
    # Do NOT add a `get_pool().putconn()` anywhere in this function. That is the
    # 2026-09-30 regression, verbatim: `ConnectionWrapper.close()` already
    # returns the connection when `from_pool` is set and is reached from
    # `__exit__` AND `__del__`, so a second putconn here puts the same
    # connection in the free list twice and two concurrent requests get it.
    # `tests/unit/test_pooled_connection_has_one_owner.py` fails the build if a
    # second release path reappears.
    wrapper = ConnectionWrapper(raw_conn, from_pool=True)
    try:
        # A POOLED CONNECTION CARRIES ITS PREVIOUS USER'S SESSION STATE, which a
        # fresh connection never did. Both of these have to be set on every
        # checkout rather than once at construction:
        #   - rollback() first, because set_session() refuses to run inside a
        #     transaction and we cannot assume how the last holder left it
        #   - readonly EXPLICITLY in both directions. The old code only ever set
        #     readonly=True and relied on a new connection defaulting to
        #     read-write; on a reused connection that leaves a writer holding a
        #     read-only session. Every one of the 72 get_db() call sites is
        #     readonly today, so this is a trap for the next caller, not a live
        #     bug — which is exactly when it is cheap to close.
        raw_conn.rollback()
        raw_conn.set_session(readonly=readonly)
        cur = raw_conn.cursor()
        cur.execute("SET search_path TO public, prices, research, notifications")
        cur.close()
        raw_conn.commit()
        yield wrapper
    except Exception:
        try:
            wrapper.rollback()
        except Exception:
            pass
        raise
    finally:
        wrapper.close()
