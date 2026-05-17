"""Unit test for compute_consecutive_sells window fix.

Regression guard: the function previously truncated its INPUT query by
`trade_date >= MIN_DATE`, so daily refresh with `--since 30d` only saw
trades in the last 30 days, collapsing the prior-sell count to ~0.

Post-fix behavior: load full history, count correctly, only emit
UPDATEs for trades whose trade_date >= MIN_DATE.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE trades (
            trade_id INTEGER PRIMARY KEY,
            insider_id INTEGER,
            ticker TEXT,
            trade_type TEXT,
            trade_date TEXT,
            consecutive_sells_before INTEGER
        )
    """)
    return db


def _insert(db, trade_id, insider_id, ticker, ttype, date):
    db.execute(
        "INSERT INTO trades(trade_id, insider_id, ticker, trade_type, trade_date) VALUES (?,?,?,?,?)",
        (trade_id, insider_id, ticker, ttype, date),
    )


def test_count_uses_full_history_even_when_since_truncates_window(conn):
    """5 historical sells in 2020, one recent buy in 2026.
    With --since 2026-01-01 the recent buy must still report csb=5,
    not csb=0 (the bug)."""
    # Old sells (pre-window) — should still be counted
    for tid, date in enumerate(
        ["2020-01-15", "2020-02-15", "2020-03-15", "2020-04-15", "2020-05-15"], start=1
    ):
        _insert(conn, tid, 100, "ACME", "sell", date)
    # Recent buy (in window)
    _insert(conn, 100, 100, "ACME", "buy", "2026-05-13")
    conn.commit()

    # Import here so the SINCE-mutation happens before module reads it
    import pipelines.insider_study.compute_cw_indicators as ci
    ci.MIN_DATE = "2026-01-01"
    ci.compute_consecutive_sells(conn)

    csb = conn.execute(
        "SELECT consecutive_sells_before FROM trades WHERE trade_id = 100"
    ).fetchone()[0]
    assert csb == 5, f"expected csb=5 across full history, got {csb}"


def test_updates_only_within_since_window(conn):
    """An old buy in 2018 must NOT be touched when --since 2026-01-01.
    Its consecutive_sells_before stays whatever it was (NULL here)."""
    _insert(conn, 1, 100, "ACME", "sell", "2017-01-15")
    _insert(conn, 2, 100, "ACME", "sell", "2017-02-15")
    _insert(conn, 3, 100, "ACME", "buy",  "2018-03-15")  # old buy — skip update
    _insert(conn, 4, 100, "ACME", "buy",  "2026-05-13")  # recent buy — update
    conn.commit()

    import pipelines.insider_study.compute_cw_indicators as ci
    ci.MIN_DATE = "2026-01-01"
    ci.compute_consecutive_sells(conn)

    old_csb = conn.execute(
        "SELECT consecutive_sells_before FROM trades WHERE trade_id = 3"
    ).fetchone()[0]
    new_csb = conn.execute(
        "SELECT consecutive_sells_before FROM trades WHERE trade_id = 4"
    ).fetchone()[0]
    assert old_csb is None, "old buy should not be updated"
    assert new_csb == 0, "recent buy preceded by old buy (not sell) → csb=0"


def test_chain_of_sells_terminated_by_buy(conn):
    """Sells must be 'immediately' before the buy — any interim buy resets the streak."""
    _insert(conn, 1, 100, "ACME", "sell", "2026-01-01")
    _insert(conn, 2, 100, "ACME", "sell", "2026-01-02")
    _insert(conn, 3, 100, "ACME", "buy",  "2026-01-03")  # resets streak
    _insert(conn, 4, 100, "ACME", "sell", "2026-01-04")
    _insert(conn, 5, 100, "ACME", "sell", "2026-01-05")
    _insert(conn, 6, 100, "ACME", "buy",  "2026-01-06")
    conn.commit()

    import pipelines.insider_study.compute_cw_indicators as ci
    ci.MIN_DATE = "2026-01-01"
    ci.compute_consecutive_sells(conn)

    csb6 = conn.execute(
        "SELECT consecutive_sells_before FROM trades WHERE trade_id = 6"
    ).fetchone()[0]
    csb3 = conn.execute(
        "SELECT consecutive_sells_before FROM trades WHERE trade_id = 3"
    ).fetchone()[0]
    assert csb6 == 2, f"buy after 2 sells: expected 2, got {csb6}"
    assert csb3 == 2, f"first buy after 2 sells: expected 2, got {csb3}"
