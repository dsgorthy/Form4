"""Unit tests for compute_switch_rate (is_rare_reversal recurring writer).

Regression guard for the 8-week silent outage where `is_rare_reversal` had
no recurring writer (old SQLite-only script was orphaned by the PG migration).

The new PG-native module must:
  - Walk the FULL prior-event history per insider for correct counting
  - Only UPDATE trades whose filing_date >= --since
  - Set is_rare_reversal=1 only when 5+ consecutive same-direction events
    are followed by an opposite-direction event
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


class _ConnProxy:
    """Wraps a sqlite3.Connection so the SUT's conn.close() is a no-op
    (we need the connection alive for assertions after main() returns)."""
    def __init__(self, db):
        self._db = db
    def __getattr__(self, name):
        if name == "close":
            return lambda: None
        return getattr(self._db, name)


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE trades (
            trade_id INTEGER PRIMARY KEY,
            insider_id INTEGER,
            trade_type TEXT,
            filing_date TEXT,
            filing_key TEXT,
            insider_switch_rate REAL,
            is_rare_reversal INTEGER DEFAULT 0
        )
    """)
    db.execute("""
        CREATE TABLE signal_freshness (
            source TEXT, table_name TEXT, column_name TEXT,
            last_computed_at TEXT, n_rows_affected INTEGER,
            populated_by TEXT
        )
    """)
    return _ConnProxy(db)


def _insert(db, tid, iid, ttype, date, fkey=None):
    db.execute(
        "INSERT INTO trades(trade_id, insider_id, trade_type, filing_date, filing_key) VALUES (?,?,?,?,?)",
        (tid, iid, ttype, date, fkey or f"f{tid}"),
    )


def _run(conn, since):
    """Helper: invoke compute_switch_rate.main() with stubbed argv + conn."""
    import importlib
    sys.modules.pop("pipelines.insider_study.compute_switch_rate", None)
    mod = importlib.import_module("pipelines.insider_study.compute_switch_rate")

    # Patch get_connection to return our in-memory db (mod._flush uses ?).
    mod.get_connection = lambda: conn  # type: ignore
    # Patch argv
    old_argv = sys.argv
    sys.argv = ["compute_switch_rate", "--since", since]
    try:
        mod.main()
    finally:
        sys.argv = old_argv


def test_rare_reversal_5_sells_then_buy(conn):
    """5+ consecutive sells, then a buy → is_rare_reversal=1 on the buy."""
    for i, date in enumerate(
        ["2026-01-01", "2026-01-08", "2026-01-15", "2026-01-22", "2026-01-29"], start=1
    ):
        _insert(conn, i, 100, "sell", date)
    _insert(conn, 6, 100, "buy", "2026-02-05")  # reversal
    conn.commit()

    _run(conn, since="2026-02-01")

    rare = conn.execute(
        "SELECT is_rare_reversal FROM trades WHERE trade_id = 6"
    ).fetchone()[0]
    assert rare == 1


def test_only_4_sells_then_buy_is_not_rare(conn):
    """Only 4 prior sells → does NOT qualify (threshold is 5)."""
    for i, date in enumerate(
        ["2026-01-01", "2026-01-08", "2026-01-15", "2026-01-22"], start=1
    ):
        _insert(conn, i, 100, "sell", date)
    _insert(conn, 5, 100, "buy", "2026-02-05")
    conn.commit()

    _run(conn, since="2026-02-01")

    rare = conn.execute(
        "SELECT is_rare_reversal FROM trades WHERE trade_id = 5"
    ).fetchone()[0]
    assert rare == 0


def test_full_history_used_when_since_truncates_window(conn):
    """5 sells happened in 2020; recent buy in 2026. Counter must look back."""
    for i, date in enumerate(
        ["2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01", "2020-05-01"], start=1
    ):
        _insert(conn, i, 100, "sell", date)
    _insert(conn, 6, 100, "buy", "2026-02-05")
    conn.commit()

    _run(conn, since="2026-02-01")

    rare = conn.execute(
        "SELECT is_rare_reversal FROM trades WHERE trade_id = 6"
    ).fetchone()[0]
    assert rare == 1


def test_old_trades_not_updated(conn):
    """Trade in 2018 is OUT of the --since 2026 window → stays NULL."""
    _insert(conn, 1, 100, "sell", "2018-01-01")
    _insert(conn, 2, 100, "buy",  "2018-02-01")  # old buy — must not be updated
    _insert(conn, 3, 100, "sell", "2026-01-15")
    _insert(conn, 4, 100, "buy",  "2026-02-05")
    conn.commit()

    _run(conn, since="2026-02-01")

    old = conn.execute(
        "SELECT is_rare_reversal FROM trades WHERE trade_id = 2"
    ).fetchone()[0]
    new = conn.execute(
        "SELECT is_rare_reversal FROM trades WHERE trade_id = 4"
    ).fetchone()[0]
    # Default at column-level is 0; we just want to confirm we didn't overwrite
    # to something different. Critical assertion is on the new row.
    assert old == 0  # default, untouched
    assert new == 0  # only 1 prior sell — not enough for rare


def test_lot_split_dedup(conn):
    """Same filing_key counts as one event (lot splits)."""
    # 5 events worth of sells, but 4 of them share a filing_key → only 2 events
    _insert(conn, 1, 100, "sell", "2026-01-01", fkey="A")
    _insert(conn, 2, 100, "sell", "2026-01-01", fkey="A")
    _insert(conn, 3, 100, "sell", "2026-01-01", fkey="A")
    _insert(conn, 4, 100, "sell", "2026-01-02", fkey="B")
    _insert(conn, 5, 100, "sell", "2026-01-03", fkey="C")
    _insert(conn, 6, 100, "buy",  "2026-02-05", fkey="D")
    conn.commit()

    _run(conn, since="2026-02-01")

    rare = conn.execute(
        "SELECT is_rare_reversal FROM trades WHERE trade_id = 6"
    ).fetchone()[0]
    # Only 3 prior EVENTS (A, B, C) — below 5-event threshold
    assert rare == 0
