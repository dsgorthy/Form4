"""Unit test for the max_daily_buys guardrail's UNION fallback.

The pre-2026-05-17 implementation counted today's orders only from
`order_audit`. That table is wrapped in try/except in cw_runner's order
write path, so a silent write failure (or the order_audit table being
brand-new like it was after 2026-05-02) would drop the count to 0 and
make the daily cap unenforceable.

The hardened version takes MAX(order_audit, strategy_portfolio entries today)
on the buy side, so a writer failure can't disable the cap.

Tests use a stub conn that pattern-matches SQL — the real production SQL
uses PG-specific `CURRENT_DATE::text` casts that sqlite3 can't parse.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework.risk.guardrails import validate_entry_order, _count_orders_today


# ── Fake conn ────────────────────────────────────────────────────────────


class _FakeRow:
    def __init__(self, n):
        self._n = n
    def __getitem__(self, k):
        return self._n
    def keys(self):
        return ["n"]


class _FakeCursor:
    def __init__(self, n):
        self._n = n
    def fetchone(self):
        return _FakeRow(self._n)


class FakeConn:
    """Stub connection that returns hard-coded COUNT(*) results based on
    which table is named in the SQL."""

    def __init__(self, *, audit_count: int = 0, portfolio_count: int = 0,
                 audit_raises: bool = False, portfolio_raises: bool = False):
        self.audit_count = audit_count
        self.portfolio_count = portfolio_count
        self.audit_raises = audit_raises
        self.portfolio_raises = portfolio_raises
        self.queries: list[str] = []

    def execute(self, sql, params=()):
        self.queries.append(sql)
        if "order_audit" in sql:
            if self.audit_raises:
                raise RuntimeError("audit query failed")
            return _FakeCursor(self.audit_count)
        if "strategy_portfolio" in sql:
            if self.portfolio_raises:
                raise RuntimeError("portfolio query failed")
            return _FakeCursor(self.portfolio_count)
        raise AssertionError(f"unexpected SQL: {sql[:120]}")


# ── Counter mechanics ────────────────────────────────────────────────────


def test_audit_only_when_portfolio_empty():
    conn = FakeConn(audit_count=2, portfolio_count=0)
    assert _count_orders_today(conn, "qm", "buy") == 2


def test_portfolio_floor_used_when_audit_empty():
    """The whole point of the hardening: portfolio has 3 entries today,
    audit has 0. Guardrail must still see 3."""
    conn = FakeConn(audit_count=0, portfolio_count=3)
    assert _count_orders_today(conn, "qm", "buy") == 3


def test_max_of_audit_and_portfolio():
    """If both have rows, return MAX. Doesn't double-count."""
    conn = FakeConn(audit_count=1, portfolio_count=2)
    assert _count_orders_today(conn, "qm", "buy") == 2
    conn = FakeConn(audit_count=4, portfolio_count=2)
    assert _count_orders_today(conn, "qm", "buy") == 4


def test_sell_side_does_not_query_portfolio():
    """Sells don't create strategy_portfolio rows — only audit applies."""
    conn = FakeConn(audit_count=1, portfolio_count=99)
    assert _count_orders_today(conn, "qm", "sell") == 1
    # And the SQL must never have referenced strategy_portfolio
    assert all("strategy_portfolio" not in q for q in conn.queries)


def test_audit_exception_falls_back_to_portfolio():
    """If the audit query raises (e.g. missing table on fresh DB), the
    cap must still fire based on portfolio."""
    conn = FakeConn(audit_raises=True, portfolio_count=5)
    assert _count_orders_today(conn, "qm", "buy") == 5


def test_both_exceptions_return_zero():
    """Belt and suspenders: if everything fails, return 0 not crash."""
    conn = FakeConn(audit_raises=True, portfolio_raises=True)
    assert _count_orders_today(conn, "qm", "buy") == 0


# ── validate_entry_order end-to-end ──────────────────────────────────────


def test_validate_blocks_when_portfolio_at_cap_and_audit_empty():
    """The bug the hardening fixes: 5 portfolio entries today, audit empty
    because the write silently failed. With max_daily_buys=5, a 6th must
    be blocked even though order_audit alone returns 0."""
    conn = FakeConn(audit_count=0, portfolio_count=5)
    ok, reason = validate_entry_order(
        conn, strategy="qm", side="buy",
        qty=100, dollar_amount=10_000, current_price=100.0, equity=100_000,
        guardrails_cfg={"max_daily_buys": 5},
    )
    assert not ok
    assert "already 5 buy orders today" in reason


def test_validate_allows_when_under_cap():
    conn = FakeConn(audit_count=0, portfolio_count=2)
    ok, reason = validate_entry_order(
        conn, strategy="qm", side="buy",
        qty=100, dollar_amount=10_000, current_price=100.0, equity=100_000,
        guardrails_cfg={"max_daily_buys": 5},
    )
    assert ok, f"unexpected block: {reason}"


def test_validate_uses_audit_when_higher_than_portfolio():
    """If audit has more than portfolio, audit wins (covers cases where
    multiple order attempts are recorded against fewer successful entries)."""
    conn = FakeConn(audit_count=3, portfolio_count=1)
    ok, reason = validate_entry_order(
        conn, strategy="qm", side="buy",
        qty=100, dollar_amount=10_000, current_price=100.0, equity=100_000,
        guardrails_cfg={"max_daily_buys": 3},
    )
    assert not ok
    assert "already 3 buy orders today" in reason
