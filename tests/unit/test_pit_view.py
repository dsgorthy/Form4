"""Unit tests for PITDataView.

These tests use an in-memory SQLite database with a stub schema that mirrors
the parts of PG `form4` that the view reads. We exercise WHERE-clause
correctness (no row past clock.as_of_date is returned) and clock-tape
recording. A separate integration test (`test_pit_engine_contamination.py`)
exercises the same paths against the real PG schema.
"""
from __future__ import annotations

import sqlite3
import pytest

from framework.pit import (
    InsiderScore,
    LookaheadError,
    PITClock,
    PITDataView,
    TradeEvent,
)


# ── Fixture: in-memory DB with the columns PITDataView reads ────────────

@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE trades (
            trade_id INTEGER PRIMARY KEY,
            insider_id INTEGER,
            ticker TEXT,
            trade_date TEXT,
            filing_date TEXT,
            trade_type TEXT,
            title TEXT,
            is_csuite INTEGER,
            consecutive_sells_before INTEGER,
            dip_1mo REAL,
            dip_3mo REAL,
            above_sma50 INTEGER,
            above_sma200 INTEGER,
            is_largest_ever INTEGER,
            is_rare_reversal INTEGER,
            is_10b5_1 INTEGER,
            is_recurring INTEGER,
            is_tax_sale INTEGER,
            cohen_routine INTEGER,
            pit_grade TEXT,
            career_grade TEXT,
            pit_blended_score REAL,
            company TEXT
        )
    """)
    db.execute("""
        CREATE TABLE insider_ticker_scores (
            insider_id INTEGER,
            ticker TEXT,
            as_of_date TEXT,
            blended_score REAL,
            career_blended_score REAL,
            career_grade TEXT,
            ticker_trade_count INTEGER,
            global_trade_count INTEGER,
            sufficient_data INTEGER
        )
    """)
    db.execute("""
        CREATE TABLE trade_returns (
            trade_id INTEGER PRIMARY KEY,
            abnormal_7d REAL,
            abnormal_30d REAL,
            abnormal_90d REAL
        )
    """)
    # Mimic the PG schema-qualified path
    db.execute("ATTACH DATABASE ':memory:' AS prices")
    db.execute("""
        CREATE TABLE prices.daily_prices (
            ticker TEXT, date TEXT, close REAL
        )
    """)
    # PG-style date::text cast — SQLite has no `::` operator. Wrap the
    # connection to strip the cast before forwarding.
    class _SqlShimConn:
        def __init__(self, inner):
            self._inner = inner
        def execute(self, sql, *args, **kw):
            sql = sql.replace("::text", "")
            return self._inner.execute(sql, *args, **kw)
        def commit(self):
            return self._inner.commit()
    yield _SqlShimConn(db)
    db.close()


def _insert_trade(conn, trade_id, insider_id, ticker, trade_date, filing_date,
                  trade_type="buy", pit_grade="A", career_grade="A"):
    conn.execute(
        "INSERT INTO trades (trade_id, insider_id, ticker, trade_date, filing_date, "
        "trade_type, pit_grade, career_grade) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (trade_id, insider_id, ticker, trade_date, filing_date, trade_type,
         pit_grade, career_grade),
    )
    conn.commit()


def _insert_score(conn, insider_id, ticker, as_of_date,
                  blended=2.0, career_blended=2.2, career_grade="A"):
    conn.execute(
        "INSERT INTO insider_ticker_scores (insider_id, ticker, as_of_date, "
        "blended_score, career_blended_score, career_grade, ticker_trade_count, "
        "global_trade_count, sufficient_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (insider_id, ticker, as_of_date, blended, career_blended, career_grade,
         5, 50, 1),
    )
    conn.commit()


def _insert_price(conn, ticker, date, close):
    conn.execute(
        "INSERT INTO prices.daily_prices (ticker, date, close) VALUES (?, ?, ?)",
        (ticker, date, close),
    )
    conn.commit()


# ── events_filed_on ─────────────────────────────────────────────────────

def test_events_filed_on_returns_only_that_date(conn):
    _insert_trade(conn, 1, 100, "AAPL", "2024-03-10", "2024-03-12")
    _insert_trade(conn, 2, 100, "MSFT", "2024-03-15", "2024-03-15")
    _insert_trade(conn, 3, 100, "GOOG", "2024-03-15", "2024-03-17")

    view = PITDataView(PITClock("2024-03-20"), conn)
    events = view.events_filed_on("2024-03-15")
    assert len(events) == 1
    assert events[0].ticker == "MSFT"


def test_events_filed_on_rejects_future_date(conn):
    view = PITDataView(PITClock("2024-03-15"), conn)
    with pytest.raises(ValueError):
        view.events_filed_on("2024-03-16")


def test_events_filed_on_records_to_tape(conn):
    _insert_trade(conn, 1, 100, "MSFT", "2024-03-15", "2024-03-15")
    clock = PITClock("2024-03-20")
    view = PITDataView(clock, conn)
    view.events_filed_on("2024-03-15")
    # Tape has 1 entry — the trade's filing_date
    assert len(clock.tape) == 1
    assert clock.tape.entries[0][1] == "2024-03-15"


# ── get_prior_trades ────────────────────────────────────────────────────

def test_get_prior_trades_filters_by_filing_date(conn):
    # Trade filed before as_of → visible
    _insert_trade(conn, 1, 100, "AAPL", "2024-03-01", "2024-03-03")
    # Trade with transaction in past but filed AFTER as_of → INVISIBLE
    # (this is the bitemporal case — late filing)
    _insert_trade(conn, 2, 100, "MSFT", "2024-02-01", "2024-04-01")

    clock = PITClock("2024-03-15")
    view = PITDataView(clock, conn)
    priors = view.get_prior_trades(insider_id=100)
    assert len(priors) == 1
    assert priors[0].trade_id == 1
    assert priors[0].ticker == "AAPL"


def test_get_prior_trades_ticker_filter(conn):
    _insert_trade(conn, 1, 100, "AAPL", "2024-03-01", "2024-03-03")
    _insert_trade(conn, 2, 100, "MSFT", "2024-03-01", "2024-03-03")

    view = PITDataView(PITClock("2024-03-15"), conn)
    aapl = view.get_prior_trades(insider_id=100, ticker="AAPL")
    assert len(aapl) == 1
    assert aapl[0].ticker == "AAPL"


def test_get_prior_trades_returns_immutable_events(conn):
    _insert_trade(conn, 1, 100, "AAPL", "2024-03-01", "2024-03-03")
    view = PITDataView(PITClock("2024-03-15"), conn)
    events = view.get_prior_trades(insider_id=100)
    assert isinstance(events[0], TradeEvent)
    # TradeEvent is frozen — mutation raises
    with pytest.raises(Exception):
        events[0].trade_id = 999


# ── get_insider_score ───────────────────────────────────────────────────

def test_get_insider_score_picks_most_recent_before_as_of(conn):
    _insert_score(conn, 100, "AAPL", "2024-01-01", blended=1.0, career_blended=1.2)
    _insert_score(conn, 100, "AAPL", "2024-02-15", blended=1.5, career_blended=1.6)
    _insert_score(conn, 100, "AAPL", "2024-05-01", blended=2.5, career_blended=2.8)

    view = PITDataView(PITClock("2024-03-15"), conn)
    s = view.get_insider_score(100, "AAPL")
    assert s.as_of_date == "2024-02-15"
    assert s.blended_score == 1.5
    assert s.career_blended_score == 1.6


def test_get_insider_score_missing_returns_empty(conn):
    view = PITDataView(PITClock("2024-03-15"), conn)
    s = view.get_insider_score(999, "NOPE")
    assert s.sufficient_data is False
    assert s.blended_score is None
    assert s.as_of_date == "2024-03-15"  # falls back to clock


# ── observable_returns ──────────────────────────────────────────────────

def _insert_return(conn, trade_id, insider_id, ticker, trade_date, filing_date,
                   abnormal_7d=0.05, abnormal_30d=0.10, abnormal_90d=0.20):
    _insert_trade(conn, trade_id, insider_id, ticker, trade_date, filing_date)
    conn.execute(
        "INSERT INTO trade_returns (trade_id, abnormal_7d, abnormal_30d, abnormal_90d) "
        "VALUES (?, ?, ?, ?)",
        (trade_id, abnormal_7d, abnormal_30d, abnormal_90d),
    )
    conn.commit()


def test_observable_returns_filters_by_lag(conn):
    # 7d return needs ≥10 day lag to be observable.
    # Trade on 2024-03-01 → return endpoint 2024-03-08, observable by 2024-03-11.
    # Clock at 2024-03-15 — trade_date 2024-03-01 < cutoff (2024-03-05). OK.
    # Trade on 2024-03-10 → endpoint 2024-03-17 — NOT observable at as_of=2024-03-15.
    # cutoff = 2024-03-05. trade_date 2024-03-10 > 2024-03-05 → excluded.
    _insert_return(conn, 1, 100, "AAPL", "2024-03-01", "2024-03-01", abnormal_7d=0.05)
    _insert_return(conn, 2, 100, "AAPL", "2024-03-10", "2024-03-10", abnormal_7d=0.08)

    view = PITDataView(PITClock("2024-03-15"), conn)
    obs = view.observable_returns(100, "AAPL", window="7d")
    assert len(obs) == 1
    assert obs[0] == ("2024-03-01", 0.05)


def test_observable_returns_filters_by_filing_date(conn):
    # Trade old enough by trade_date but filed late — should be EXCLUDED.
    _insert_return(conn, 1, 100, "AAPL", "2024-01-01", "2024-04-15",
                   abnormal_7d=0.99)
    view = PITDataView(PITClock("2024-03-15"), conn)
    obs = view.observable_returns(100, "AAPL", window="7d")
    assert obs == []


def test_observable_returns_invalid_window(conn):
    view = PITDataView(PITClock("2024-03-15"), conn)
    with pytest.raises(ValueError):
        view.observable_returns(100, "AAPL", window="bad")


# ── get_close ────────────────────────────────────────────────────────────

def test_get_close_returns_most_recent(conn):
    _insert_price(conn, "AAPL", "2024-03-10", 175.0)
    _insert_price(conn, "AAPL", "2024-03-11", 176.0)
    _insert_price(conn, "AAPL", "2024-03-12", 177.0)

    view = PITDataView(PITClock("2024-03-15"), conn)
    res = view.get_close("AAPL")
    assert res is not None
    date, close = res
    assert date == "2024-03-12"
    assert close == 177.0


def test_get_close_respects_lookback_window(conn):
    _insert_price(conn, "AAPL", "2024-02-01", 100.0)
    view = PITDataView(PITClock("2024-03-15"), conn)
    # Default lookback 5d — Feb 1 is way past, returns None.
    assert view.get_close("AAPL", on_or_before="2024-03-10") is None
    # Longer lookback finds it.
    res = view.get_close("AAPL", on_or_before="2024-03-10", lookback_days=60)
    assert res == ("2024-02-01", 100.0)


def test_get_close_rejects_future_target(conn):
    view = PITDataView(PITClock("2024-03-15"), conn)
    with pytest.raises(ValueError):
        view.get_close("AAPL", on_or_before="2024-03-16")
