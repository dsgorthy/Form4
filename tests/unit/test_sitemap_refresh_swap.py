"""The sitemap-quality refresh builds first and swaps second, and the swap can
never be half of a deadlock.

Until 2026-11-09 it built and swapped each table in turn inside ONE
transaction, so the ACCESS EXCLUSIVE lock from the insiders swap was held while
the companies table was built -- every sitemap request waited out a full
aggregate over `trades`, and one holding companies while waiting on insiders
deadlocked the refresh. It died that way on 11-08 and 11-09 at 03:00
("deadlock detected"), the tables went stale, and Dagster paged both mornings.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

psycopg2 = pytest.importorskip("psycopg2")
import psycopg2.errors  # noqa: E402

import pipelines.insider_study.refresh_sitemap_quality as refresh_mod  # noqa: E402
from config.database import translate_sql  # noqa: E402


class FakeConn:
    def __init__(self, refuse_locks: int = 0, error=psycopg2.errors.LockNotAvailable):
        self.log: list[str] = []
        self.refuse_locks = refuse_locks
        self.error = error

    def cursor(self):
        conn = self

        class Cur:
            last = None

            def execute(self, sql, params=None):
                conn.log.append(" ".join(sql.split()))
                if sql.startswith("LOCK TABLE") and conn.refuse_locks:
                    conn.refuse_locks -= 1
                    raise conn.error("could not obtain lock")
                self.last = sql

            def fetchone(self):
                return (1234,) if "COUNT(*)" in self.last else (1,)

        return Cur()

    def commit(self):
        self.log.append("COMMIT")

    def rollback(self):
        self.log.append("ROLLBACK")

    def close(self):
        self.log.append("CLOSE")


@pytest.fixture
def run(monkeypatch):
    monkeypatch.setattr(refresh_mod.time, "sleep", lambda s: None)

    def go(conn, **kw):
        monkeypatch.setattr(refresh_mod, "get_connection", lambda: conn)
        return refresh_mod.refresh(**kw)
    return go


def _first(log, prefix):
    return next(i for i, s in enumerate(log) if s.startswith(prefix))


def test_both_tables_are_built_and_committed_before_any_live_lock(run):
    conn = FakeConn()
    run(conn)
    log = conn.log
    builds = [i for i, s in enumerate(log) if s.startswith("CREATE TABLE")]
    assert len(builds) == 2
    lock = _first(log, "LOCK TABLE")
    assert max(builds) < log.index("COMMIT") < lock, (
        "a live table is locked while a replacement is still being built")
    assert _first(log, "ALTER TABLE") > lock


def test_the_swap_takes_both_tables_at_once_and_never_queues(run):
    conn = FakeConn()
    run(conn)
    (lock,) = [s for s in conn.log if s.startswith("LOCK TABLE")]
    assert "sitemap_quality_insiders" in lock and "sitemap_quality_companies" in lock
    assert lock.endswith("IN ACCESS EXCLUSIVE MODE NOWAIT")
    sql, noop = translate_sql(lock)
    assert not noop and " ".join(sql.split()) == lock, "the SQL compat layer rewrote the LOCK"


@pytest.mark.parametrize("err", [psycopg2.errors.LockNotAvailable, psycopg2.errors.DeadlockDetected])
def test_a_reader_in_the_way_is_retried_not_fatal(run, err):
    conn = FakeConn(refuse_locks=3, error=err)
    counts = run(conn)
    assert counts == {"sitemap_quality_insiders": 1234, "sitemap_quality_companies": 1234}
    assert sum(s.startswith("LOCK TABLE") for s in conn.log) == 4
    assert conn.log.count("ROLLBACK") == 3
    assert conn.log[-2:] == ["COMMIT", "CLOSE"]
    assert sum(s.startswith("COMMENT ON TABLE") for s in conn.log) == 2


def test_a_swap_that_never_gets_its_locks_still_fails_loudly(run):
    conn = FakeConn(refuse_locks=10**6)
    with pytest.raises(psycopg2.errors.LockNotAvailable):
        run(conn)
    assert sum(s.startswith("LOCK TABLE") for s in conn.log) == refresh_mod.SWAP_ATTEMPTS
    assert not any(s.startswith("ALTER TABLE") for s in conn.log)


def test_dry_run_builds_and_rolls_back_without_touching_live_tables(run):
    conn = FakeConn()
    run(conn, dry_run=True)
    assert "COMMIT" not in conn.log and "ROLLBACK" in conn.log
    assert not any(s.startswith(("LOCK TABLE", "ALTER TABLE")) for s in conn.log)
