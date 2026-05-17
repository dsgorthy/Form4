"""Unit tests for pit_scoring.upsert_score.

Regression guard for the role-change conviction-drift bug (Paul Gu / UPST,
2026-05-14): when a scoring row has `sufficient_data=0`, the writer must
emit `blended_score=NULL`, not a degenerate 0.0. Downstream readers (cw_runner,
backfill_pit_grades) interpret 0.0 as a grade-D signal.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog"))
from pit_scoring import ScoringResult, upsert_score  # noqa: E402


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE insider_ticker_scores (
            insider_id INTEGER, ticker TEXT, as_of_date TEXT,
            ticker_trade_count INTEGER, ticker_win_rate_7d REAL,
            ticker_avg_abnormal_7d REAL, ticker_score REAL,
            global_trade_count INTEGER, global_win_rate_7d REAL,
            global_avg_abnormal_7d REAL, global_score REAL,
            blended_score REAL, role_at_ticker TEXT, role_weight REAL,
            is_primary_company INTEGER, sufficient_data INTEGER,
            PRIMARY KEY (insider_id, ticker, as_of_date)
        )
    """)
    db.execute("""
        CREATE TABLE score_history (
            insider_id INTEGER, ticker TEXT, as_of_date TEXT,
            trigger_trade_id INTEGER,
            blended_score REAL, global_score REAL, ticker_score REAL,
            trade_count INTEGER
        )
    """)
    return db


def _result(sufficient: int, blended: float = 0.0) -> ScoringResult:
    return ScoringResult(
        insider_id=14206, ticker="UPST", as_of_date="2026-05-14",
        blended_score=blended,
        ticker_score=0.0, global_score=0.0,
        ticker_weight=0.0, global_weight=1.0,
        ticker_win_rate_7d=None, ticker_avg_abnormal_7d=None,
        global_win_rate_7d=None, global_avg_abnormal_7d=None,
        ticker_trade_count=0, global_trade_count=0,
        n_observable=0,
        score_7d=0.0, score_30d=0.0, score_90d=0.0,
        grade=None,
        role_weight=1.10, role_at_ticker="Chief Executive Officer",
        is_primary_company=1, sufficient_data=sufficient,
        method="bayesian_v2",
    )


def test_insufficient_data_writes_null_blended_score(conn):
    """sufficient_data=0 row → blended_score is NULL in the DB."""
    upsert_score(conn, _result(sufficient=0, blended=0.0), trigger_trade_id=1741574)
    row = conn.execute(
        "SELECT blended_score, sufficient_data FROM insider_ticker_scores"
    ).fetchone()
    assert row[1] == 0, "sufficient_data should still be 0 in the row"
    assert row[0] is None, "blended_score must be NULL when sufficient_data=0"


def test_insufficient_data_writes_null_in_score_history(conn):
    """score_history should also see NULL — same fix applies to both writes."""
    upsert_score(conn, _result(sufficient=0), trigger_trade_id=1741574)
    row = conn.execute(
        "SELECT blended_score FROM score_history"
    ).fetchone()
    assert row[0] is None


def test_sufficient_data_writes_blended_score(conn):
    """sufficient_data=1 row preserves blended_score exactly."""
    upsert_score(conn, _result(sufficient=1, blended=1.2543), trigger_trade_id=1741574)
    row = conn.execute(
        "SELECT blended_score, sufficient_data FROM insider_ticker_scores"
    ).fetchone()
    assert row[1] == 1
    assert row[0] == pytest.approx(1.2543)


def test_dict_input_respects_sufficient_data(conn):
    """upsert_score accepts a dict; same NULL contract."""
    d = {
        "insider_id": 14206, "ticker": "UPST", "as_of_date": "2026-05-14",
        "ticker_trade_count": 0, "ticker_win_rate_7d": None,
        "ticker_avg_abnormal_7d": None, "ticker_score": 0.0,
        "global_trade_count": 0, "global_win_rate_7d": None,
        "global_avg_abnormal_7d": None, "global_score": 0.0,
        "blended_score": 0.0,
        "role_at_ticker": "CEO", "role_weight": 1.10,
        "is_primary_company": 1, "sufficient_data": 0,
    }
    upsert_score(conn, d, trigger_trade_id=99)
    row = conn.execute("SELECT blended_score FROM insider_ticker_scores").fetchone()
    assert row[0] is None
