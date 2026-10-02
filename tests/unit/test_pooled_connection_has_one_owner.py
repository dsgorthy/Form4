"""A pooled connection is returned exactly once, by exactly one owner.

## Why this file exists

`get_db` was pooled on 2026-09-30 and broke production within the hour.
`ConnectionWrapper.close()` already returns a pooled connection via
`get_pool().putconn()` when its `from_pool` flag is set, and it is reached from
both `__exit__` and `__del__`. That version called `putconn` in a `finally` as
well, so every connection was returned TWICE, landed in the pool's free list
twice, and was handed to two concurrent requests at once:

    psycopg2.OperationalError: connection pointer is NULL
    psycopg2.InterfaceError: cursor already closed

The change was reverted the same day with a docstring saying "ONE thing must
own the connection's lifecycle, and a test has to pin that — not a docstring
claiming it, which is what I wrote." This is that test.

It counts `getconn` against `putconn` instead of reading the source, because
the defect was not a visible second call to `putconn` — it was a second
RELEASE PATH, and the two were in different files.

## Why the connection is pooled at all

2026-10-02: a scraper at 21 req/s across 6,610 distinct IPs had the API opening
~50 connections/second through Lima's user-network relay, which logged 1,708
`host.docker.internal:5432 ... timeout expired` in 30 minutes and returned 502
on 10% of origin requests. Postgres itself sat at 17 of 100 connections.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("psycopg2")

import psycopg2  # noqa: E402

import config.database as db  # noqa: E402


class FakeRaw:
    """A psycopg2 connection, as much as this code touches."""

    def __init__(self, ident: int):
        self.ident = ident
        self.autocommit = False
        self.closed = 0
        self.session_readonly = None
        self.statements: list[str] = []
        self.rollbacks = 0
        self.commits = 0
        # A connection handed back by a pool may still be inside a transaction.
        # psycopg2's putconn rolls back, and so does ConnectionWrapper.close(),
        # but get_db must not DEPEND on either having happened — model the
        # pessimistic case so the guard is actually exercised.
        self.in_transaction = True

    def cursor(self, *a, **k):
        conn = self

        class Cur:
            def execute(self, sql, params=None):
                conn.statements.append(sql)
                conn.in_transaction = True
                return self

            def close(self):
                pass

            def fetchone(self):
                return None

        return Cur()

    def set_session(self, readonly=None, **k):
        # psycopg2 raises here, verbatim: "set_session cannot be used inside a
        # transaction". This is why get_db must rollback() first.
        if self.in_transaction:
            raise psycopg2.ProgrammingError(
                "set_session cannot be used inside a transaction"
            )
        self.session_readonly = readonly

    def rollback(self):
        self.rollbacks += 1
        self.in_transaction = False

    def commit(self):
        self.commits += 1
        self.in_transaction = False

    def close(self):
        self.closed = 1


class CountingPool:
    """Counts checkouts and returns, and refuses a double return the way a
    real pool's free list would make fatal."""

    def __init__(self):
        self.out: dict[int, FakeRaw] = {}
        self.getconn_calls = 0
        self.putconn_calls = 0
        self.double_returns: list[int] = []
        self._next = 0
        self.closed = False

    def getconn(self):
        self._next += 1
        c = FakeRaw(self._next)
        self.out[c.ident] = c
        self.getconn_calls += 1
        return c

    def putconn(self, conn):
        self.putconn_calls += 1
        if conn.ident not in self.out:
            # This is the production failure: the connection is already back in
            # the free list and is about to be handed to a second request.
            self.double_returns.append(conn.ident)
            return
        del self.out[conn.ident]


@pytest.fixture
def pool(monkeypatch):
    p = CountingPool()
    monkeypatch.setattr(db, "get_pool", lambda: p)
    return p


def test_a_normal_request_checks_out_one_and_returns_one(pool):
    with db.get_db() as conn:
        assert conn is not None
    assert (pool.getconn_calls, pool.putconn_calls) == (1, 1)
    assert pool.double_returns == []
    assert pool.out == {}, "the connection was not returned to the pool"


def test_a_raising_request_still_returns_its_connection_exactly_once(pool):
    """A leak here is worse than a double return: the pool drains to zero and
    every later request fails with PoolError."""
    with pytest.raises(ValueError):
        with db.get_db() as conn:
            raise ValueError("boom")
    assert (pool.getconn_calls, pool.putconn_calls) == (1, 1)
    assert pool.double_returns == []
    assert pool.out == {}


def test_closing_the_wrapper_inside_the_block_does_not_double_return(pool):
    """`__exit__` and `__del__` both call close(), and a caller may close it
    too. close() is idempotent via its _closed flag; this pins that, because
    the double return is the exact production defect."""
    with db.get_db() as conn:
        conn.close()
        conn.close()
    assert pool.putconn_calls == 1, (
        f"the connection was returned {pool.putconn_calls} times; the pool's "
        f"free list now holds it more than once and two requests will share it"
    )
    assert pool.double_returns == []


def test_many_sequential_requests_never_leak_and_never_double_return(pool):
    for _ in range(50):
        with db.get_db():
            pass
    assert pool.getconn_calls == 50
    assert pool.putconn_calls == 50
    assert pool.double_returns == []
    assert pool.out == {}, f"{len(pool.out)} connections leaked out of 50"


def test_get_db_never_calls_putconn_itself(pool, monkeypatch):
    """The owner is the wrapper. If get_db also releases, the counts above can
    still balance in a single-threaded test while being fatal concurrently —
    so assert the ownership directly, not just the arithmetic."""
    seen: list[str] = []
    real_close = db.ConnectionWrapper.close

    def tracking_close(self):
        seen.append("wrapper.close")
        return real_close(self)

    monkeypatch.setattr(db.ConnectionWrapper, "close", tracking_close)

    def forbidden(_conn):
        seen.append("pool.putconn-direct")
        raise AssertionError(
            "get_db returned the connection to the pool itself. The wrapper "
            "owns that; a second release path is the 2026-09-30 regression."
        )

    # Any putconn that does NOT arrive via wrapper.close() is a second owner.
    orig_putconn = pool.putconn

    def guarded(conn):
        if seen and seen[-1] == "wrapper.close":
            return orig_putconn(conn)
        return forbidden(conn)

    pool.putconn = guarded
    with db.get_db():
        pass
    assert "pool.putconn-direct" not in seen


# ── what a pooled connection needs that a fresh one did not ─────────────────

def _spy_raws(monkeypatch):
    """Capture the raw connections get_db hands to the wrapper."""
    raws: list[FakeRaw] = []
    orig = db.ConnectionWrapper.__init__

    def spy(self, conn, from_pool=False):
        raws.append(conn)
        return orig(self, conn, from_pool=from_pool)

    monkeypatch.setattr(db.ConnectionWrapper, "__init__", spy)
    return raws


def test_session_state_is_reset_on_every_checkout(pool, monkeypatch):
    """A pooled connection inherits the last holder's session.

    The pre-pool code only ever set readonly=True and relied on a NEW
    connection defaulting to read-write. On a reused connection that leaves a
    writer holding a read-only session, which fails at COMMIT — far from the
    cause.
    """
    raws = _spy_raws(monkeypatch)
    for ro in (True, False, True):
        with db.get_db(readonly=ro):
            pass
    assert [r.session_readonly for r in raws] == [True, False, True], (
        "set_session was not called with the requested mode on every checkout"
    )


def test_readonly_is_passed_through_in_both_directions(pool, monkeypatch):
    raws = _spy_raws(monkeypatch)
    with db.get_db(readonly=True):
        pass
    assert raws[-1].session_readonly is True
    with db.get_db(readonly=False):
        pass
    assert raws[-1].session_readonly is False, (
        "readonly=False did not reach set_session, so a writer gets a "
        "read-only session from the pool"
    )


def test_the_wrapper_is_told_it_came_from_the_pool(pool, monkeypatch):
    """`from_pool=False` on a pooled connection means close() calls
    `conn.close()` instead of returning it — the pool drains to empty and
    every later request raises PoolError."""
    flags: list[bool] = []
    orig = db.ConnectionWrapper.__init__

    def spy(self, conn, from_pool=False):
        flags.append(from_pool)
        return orig(self, conn, from_pool=from_pool)

    monkeypatch.setattr(db.ConnectionWrapper, "__init__", spy)
    with db.get_db():
        pass
    assert flags == [True], (
        "get_db built the wrapper with from_pool=False, so the connection is "
        "hard-closed instead of returned and the pool bleeds out"
    )


def test_search_path_is_set_on_every_checkout(pool, monkeypatch):
    """The schemas are not on the role's default search_path, and a pooled
    connection may arrive with whatever the last holder set."""
    raws = _spy_raws(monkeypatch)
    for _ in range(3):
        with db.get_db():
            pass
    assert len(raws) == 3
    for r in raws:
        joined = " ".join(r.statements)
        assert "SET search_path TO" in joined, (
            "no search_path was set on this checkout; queries will fail to "
            "resolve prices./research./notifications. tables"
        )
        for schema in ("public", "prices", "research", "notifications"):
            assert schema in joined, f"{schema} missing from the search_path"


def test_rollback_precedes_set_session(pool, monkeypatch):
    """psycopg2 refuses set_session inside a transaction, and we cannot assume
    how the previous holder left the connection."""
    raws = _spy_raws(monkeypatch)
    with db.get_db():
        pass
    (raw,) = raws
    assert raw.rollbacks >= 1, (
        "no rollback before set_session; a connection returned mid-transaction "
        "makes set_session raise and the request 500s"
    )
    assert raw.session_readonly is True, "set_session never ran"


def test_the_pool_is_not_bypassed_by_a_fresh_connect(pool, monkeypatch):
    """If get_db goes back to psycopg2.connect, every guard above is moot and
    the 50-connections-per-second behaviour returns silently."""
    def exploded(*a, **k):
        raise AssertionError(
            "get_db called psycopg2.connect directly instead of taking a "
            "connection from the pool"
        )

    monkeypatch.setattr(db.psycopg2, "connect", exploded)
    with db.get_db():
        pass
    assert pool.getconn_calls == 1


# ── a burst must become latency, not errors ─────────────────────────────────

def test_an_exhausted_pool_waits_instead_of_failing(monkeypatch):
    """`ThreadedConnectionPool.getconn` RAISES when maxconn are checked out.

    Measured on Studio against the real database before `_checkout` existed:
    40 concurrent threads against maxconn=20 turned 593 of 1,000 requests into
    `PoolError: connection pool exhausted`, and in production each of those is
    a 500 that the frontend renders as a 502. A read API should turn a burst
    into latency.
    """
    calls = {"n": 0}

    class Flaky:
        closed = False

        def getconn(self):
            calls["n"] += 1
            if calls["n"] < 4:                 # busy for the first few tries
                raise psycopg2.pool.PoolError("connection pool exhausted")
            return FakeRaw(99)

        def putconn(self, conn):
            pass

    monkeypatch.setattr(db, "get_pool", lambda: Flaky())
    monkeypatch.setattr(db.time, "sleep", lambda s: None)   # no real waiting
    with db.get_db() as conn:
        assert conn is not None
    assert calls["n"] == 4, "the checkout did not retry a busy pool"


def test_a_pool_that_never_frees_up_still_raises(monkeypatch):
    """Waiting forever would hang a worker. Past the deadline it must be loud."""
    class Always:
        closed = False

        def getconn(self):
            raise psycopg2.pool.PoolError("connection pool exhausted")

        def putconn(self, conn):
            pass

    monkeypatch.setattr(db, "get_pool", lambda: Always())
    monkeypatch.setattr(db.time, "sleep", lambda s: None)
    clock = {"t": 0.0}
    monkeypatch.setattr(db.time, "monotonic", lambda: clock["t"])

    def advancing():
        clock["t"] += 1.0
        return clock["t"]

    monkeypatch.setattr(db.time, "monotonic", advancing)
    with pytest.raises(psycopg2.pool.PoolError):
        with db.get_db():
            pass


def test_the_wait_budget_matches_the_old_connect_timeout():
    """Pooling must not make the worst case a caller sees any worse than the
    5s connect_timeout the per-request implementation used."""
    assert db.POOL_WAIT_SECONDS == 5.0


# ── the pool must fit inside Postgres ───────────────────────────────────────

def test_pool_fits_postgres_budget():
    """THE ARITHMETIC THAT NEARLY SHIPPED AT 128 AGAINST 100.

    `_pool` is module state and `deploy/Dockerfile.api` runs uvicorn with
    `--workers 4`, so the API's real connection ceiling is workers x maxconn,
    not maxconn. At the 32 I first wrote, four workers would have opened 128
    connections against Postgres's max_connections=100 — exhausting the server
    and taking Dagster, the strategy runners and psql down with the API.

    Worse, it would not have looked like an API bug.
    """
    total = db.API_WORKERS * db.API_POOL_MAX_PER_WORKER
    assert total + db.NON_API_CONNECTION_RESERVE <= db.PG_MAX_CONNECTIONS, (
        f"{db.API_WORKERS} workers x {db.API_POOL_MAX_PER_WORKER} connections "
        f"= {total}, plus {db.NON_API_CONNECTION_RESERVE} reserved for Dagster "
        f"and friends, exceeds max_connections={db.PG_MAX_CONNECTIONS}"
    )
    assert db.API_POOL_MAX_PER_WORKER >= 8, (
        f"maxconn={db.API_POOL_MAX_PER_WORKER} per worker is too small to "
        f"absorb a burst; raise PG_MAX_CONNECTIONS (and Postgres) or lower the "
        f"worker count instead of starving the pool"
    )


def test_the_declared_worker_count_matches_the_dockerfile():
    """API_WORKERS is the input to the budget above, so it has to be the real
    number. A `--workers` bump with this left behind silently doubles the
    API's connection ceiling."""
    dockerfile = (ROOT / "deploy" / "Dockerfile.api").read_text()
    m = re.search(r'"--workers",\s*"(\d+)"', dockerfile)
    assert m, (
        "could not find --workers in deploy/Dockerfile.api; if the CMD changed "
        "shape, update API_WORKERS in config/database.py to match and fix this "
        "test to read the new form"
    )
    assert int(m.group(1)) == db.API_WORKERS, (
        f"Dockerfile.api runs {m.group(1)} uvicorn workers but "
        f"config.database.API_WORKERS says {db.API_WORKERS}; the pool budget "
        f"is computed from the wrong number"
    )


def test_postgres_max_connections_is_what_we_think_it_is():
    """Pinned so the budget is checkable by eye. If Postgres is reconfigured,
    change this deliberately — `psql -c 'SHOW max_connections'`."""
    assert db.PG_MAX_CONNECTIONS == 100


def test_the_reserve_actually_reserves_something():
    """A reserve of zero passes the arithmetic and starves everything else.

    100 total with 0 reserved gives the API all 100 and Dagster none — the API
    would be inside its budget and the box would still fall over, which is the
    failure the budget exists to prevent. 17 connections were in use box-wide
    at a quiet moment; a backfill and a human with psql need room on top.
    """
    assert db.NON_API_CONNECTION_RESERVE >= 25, (
        f"only {db.NON_API_CONNECTION_RESERVE} connections are reserved for "
        f"Dagster, the launchd jobs, the backfills and psql; 17 were in use "
        f"box-wide at a quiet moment, so this leaves no room for a backfill"
    )


def test_the_pool_is_actually_built_with_the_derived_ceiling(monkeypatch):
    """Assert what reaches psycopg2, not just what the constants say.

    Checking the constants alone lets someone hardcode `maxconn=32` next to a
    correct-looking derivation and ship 128 connections against 100 with a
    green suite. That mutation survived the first version of this file.
    """
    captured: dict = {}

    class FakePool:
        closed = False

        def __init__(self, *a, **kw):
            captured.update(kw)

    monkeypatch.setattr(db.psycopg2.pool, "ThreadedConnectionPool", FakePool)
    monkeypatch.setattr(db, "_pool", None)
    db.get_pool()
    monkeypatch.setattr(db, "_pool", None)      # don't leave a fake installed

    assert captured.get("maxconn") == db.API_POOL_MAX_PER_WORKER, (
        f"the pool was built with maxconn={captured.get('maxconn')} but the "
        f"budget derives {db.API_POOL_MAX_PER_WORKER} per worker; "
        f"{db.API_WORKERS} workers would open "
        f"{db.API_WORKERS * (captured.get('maxconn') or 0)} against "
        f"max_connections={db.PG_MAX_CONNECTIONS}"
    )
    assert captured.get("minconn", 0) >= 1
    assert captured.get("keepalives") == 1, (
        "keepalives are off, so the pool will hand out connections the kernel "
        "has silently dropped"
    )
