"""Contamination property test.

Builds a small in-memory DB with KNOWN GOOD (knowledge_date ≤ as_of) and
KNOWN BAD (knowledge_date > as_of) rows mixed in. Iterates every accessor
on `PITDataView` with several as_of_dates and asserts:

  1. No KNOWN BAD row's identifier ever surfaces in returned data.
  2. The clock's read tape contains only knowledge_dates ≤ as_of.
  3. Direct attempts to ask for future dates raise (not silently return None).

This is the "you can't say lookahead in this API" guarantee. If a future
contributor adds an accessor that bypasses the bitemporal WHERE clause,
this test catches it on the next CI run.

Property test in spirit even though we use parametrize for the as_of grid —
the structure (good + bad data, run accessors, check no bad data leaks)
is property-style.
"""
from __future__ import annotations

import sqlite3
from typing import Set

import pytest

from framework.pit import LookaheadError, PITClock, PITDataView


# ── Test data IDs ────────────────────────────────────────────────────────
# We use ID ranges to make assertions readable.
GOOD_TRADES = [(i, f"T{i:03d}") for i in range(1, 11)]      # trade_ids 1-10
BAD_TRADES = [(i, f"T{i:03d}") for i in range(101, 106)]    # trade_ids 101-105
ALL_BAD_TRADE_IDS: Set[int] = {i for i, _ in BAD_TRADES}


@pytest.fixture
def populated_conn():
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
            company TEXT,
            is_duplicate INTEGER DEFAULT 0
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
    db.execute("ATTACH DATABASE ':memory:' AS prices")
    db.execute("""
        CREATE TABLE prices.daily_prices (ticker TEXT, date TEXT, close REAL)
    """)

    # ── Populate ───────────────────────────────────────────────────────
    # GOOD trades: filed on or before 2024-03-15
    for trade_id, ticker in GOOD_TRADES:
        db.execute(
            "INSERT INTO trades (trade_id, insider_id, ticker, trade_date, "
            "filing_date, trade_type) VALUES (?, ?, ?, ?, ?, 'buy')",
            (trade_id, 100, ticker, f"2024-01-{trade_id:02d}",
             f"2024-01-{trade_id:02d}"),
        )
        db.execute(
            "INSERT INTO trade_returns (trade_id, abnormal_7d, abnormal_30d, abnormal_90d) "
            "VALUES (?, 0.05, 0.10, 0.20)",
            (trade_id,),
        )

    # BAD trades: filed AFTER 2024-03-15 (late filing — knowledge_date in future)
    for trade_id, ticker in BAD_TRADES:
        db.execute(
            "INSERT INTO trades (trade_id, insider_id, ticker, trade_date, "
            "filing_date, trade_type) VALUES (?, ?, ?, ?, ?, 'buy')",
            (trade_id, 100, ticker, "2024-01-15", "2024-04-15"),  # filed Apr 15
        )
        db.execute(
            "INSERT INTO trade_returns (trade_id, abnormal_7d, abnormal_30d, abnormal_90d) "
            "VALUES (?, 999.0, 999.0, 999.0)",
            (trade_id,),
        )

    # GOOD scores
    db.execute(
        "INSERT INTO insider_ticker_scores (insider_id, ticker, as_of_date, "
        "blended_score, career_blended_score, career_grade, ticker_trade_count, "
        "global_trade_count, sufficient_data) VALUES (100, 'AAPL', '2024-02-01', "
        "1.5, 1.7, 'B', 5, 50, 1)"
    )
    # BAD score: future as_of_date
    db.execute(
        "INSERT INTO insider_ticker_scores (insider_id, ticker, as_of_date, "
        "blended_score, career_blended_score, career_grade, ticker_trade_count, "
        "global_trade_count, sufficient_data) VALUES (100, 'AAPL', '2024-05-01', "
        "999.0, 999.0, 'X', 99, 99, 1)"
    )

    # GOOD prices
    db.execute("INSERT INTO prices.daily_prices VALUES ('AAPL', '2024-03-12', 175.0)")
    # BAD price: future date
    db.execute("INSERT INTO prices.daily_prices VALUES ('AAPL', '2024-04-01', 999.0)")

    db.commit()

    class _Shim:
        def __init__(self, inner):
            self._inner = inner
        def execute(self, sql, *args, **kw):
            return self._inner.execute(sql.replace("::text", ""), *args, **kw)
        def commit(self):
            return self._inner.commit()

    yield _Shim(db)
    db.close()


# ── The contamination guarantees ────────────────────────────────────────

@pytest.mark.parametrize("as_of", ["2024-03-15", "2024-03-31", "2024-04-14"])
def test_no_late_filed_trade_surfaces_in_get_prior_trades(populated_conn, as_of):
    view = PITDataView(PITClock(as_of), populated_conn)
    priors = view.get_prior_trades(insider_id=100)
    surfaced_ids = {p.trade_id for p in priors}
    leaked = surfaced_ids & ALL_BAD_TRADE_IDS
    assert leaked == set(), (
        f"as_of={as_of} leaked late-filed trade_ids {leaked} — bitemporal "
        "filter must exclude them"
    )


@pytest.mark.parametrize("as_of", ["2024-03-15", "2024-03-31", "2024-04-14"])
def test_no_late_filed_return_surfaces_in_observable_returns(populated_conn, as_of):
    view = PITDataView(PITClock(as_of), populated_conn)
    obs = view.observable_returns(insider_id=100, ticker="AAPL", window="7d")
    # BAD returns have value 999.0 — assert they never show up
    for trade_date, val in obs:
        assert val != 999.0, (
            f"as_of={as_of} leaked a BAD return ({val}); the bitemporal "
            "filter in observable_returns failed"
        )


@pytest.mark.parametrize("as_of", ["2024-03-15", "2024-03-31", "2024-04-14"])
def test_future_score_never_picked_in_get_insider_score(populated_conn, as_of):
    view = PITDataView(PITClock(as_of), populated_conn)
    s = view.get_insider_score(100, "AAPL")
    # Either no row (None) or the GOOD row (blended_score 1.5)
    assert s.blended_score in (None, 1.5), (
        f"as_of={as_of} picked a future score (got {s.blended_score})"
    )


@pytest.mark.parametrize("as_of", ["2024-03-15", "2024-03-31"])
def test_future_price_never_surfaces_in_get_close(populated_conn, as_of):
    view = PITDataView(PITClock(as_of), populated_conn)
    res = view.get_close("AAPL", lookback_days=60)
    if res is not None:
        date, close = res
        assert close != 999.0, (
            f"as_of={as_of} leaked future price ({date}, {close})"
        )


def test_read_tape_max_knowledge_never_exceeds_as_of(populated_conn):
    clock = PITClock("2024-03-15")
    view = PITDataView(clock, populated_conn)
    # Exercise every accessor that has data behind it
    view.get_prior_trades(insider_id=100)
    view.observable_returns(100, "AAPL", "7d")
    view.observable_returns(100, "AAPL", "30d")
    view.get_insider_score(100, "AAPL")
    view.get_close("AAPL", lookback_days=60)

    max_kd = clock.tape.max_knowledge_date()
    assert max_kd is not None, "tape should have entries"
    assert max_kd <= clock.as_of_date, (
        f"tape recorded knowledge_date={max_kd} > as_of={clock.as_of_date}"
    )


def test_explicit_future_query_raises(populated_conn):
    view = PITDataView(PITClock("2024-03-15"), populated_conn)
    with pytest.raises(ValueError):
        view.events_filed_on("2024-04-01")
    with pytest.raises(ValueError):
        view.get_close("AAPL", on_or_before="2024-04-01")


# ── Direct clock violation surfaces ────────────────────────────────────

def test_lookahead_error_message_is_actionable():
    clock = PITClock("2024-03-15")
    with pytest.raises(LookaheadError) as exc:
        clock.assert_known("2024-04-01", source="trades.filing_date")
    msg = str(exc.value)
    # Error should explain WHAT failed and HOW to fix it.
    assert "trades.filing_date" in msg
    assert "2024-04-01" in msg
    assert "2024-03-15" in msg
    assert "WHERE" in msg or "fix" in msg.lower()  # actionable hint
