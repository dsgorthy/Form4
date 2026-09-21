"""The V2 walk-forward (build_pit_scores) must apply the same three rules as
the career scorer: a filing cannot enter its own grade, a filing is one
observation however many lots it filled in, and only decisions are graded.

All three were fixed in pit_scoring._get_returns (2026-08-22 lots,
2026-08-25 population, 2026-08-30 self-grade) and none reached this copy —
which is the copy that produces `pit_grade`, the input conviction reads.
Measured before the fix on the strategy books: four of A-List's nine
late-filed positions and two of Breakout's were admitted on grades the strict
scorer does not give.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
SRC = REPO / "strategies" / "insider_catalog" / "build_pit_scores.py"

from strategies.insider_catalog.build_pit_scores import RunningAggregates  # noqa: E402


def _agg_with(*filings):
    """filings: (trade_date, filing_date, filing_key, ab7) for insider 1, ticker X."""
    agg = RunningAggregates()
    added = []
    for td, fd, key, ab7 in filings:
        added.append(agg.add_trade(1, "X", td, ab7, ab7, ab7, "CEO",
                                   filing_date=fd, filing_key=key))
    return agg, added


def test_a_late_filing_does_not_enter_its_own_score():
    """Executed 2020-09-17, filed 2020-11-27 (71 days later, ELYS). Every
    maturity cutoff is cleared, so only the filing_date guard keeps it out."""
    agg, _ = _agg_with(("2020-09-17", "2020-11-27", "acc-1", 1.22))
    for window in ("7d", "30d"):
        assert agg.get_observable_returns(1, "X", "2020-11-27", window) == [], (
            f"the {window} window let a filing grade itself"
        )
        assert agg.get_observable_returns(1, None, "2020-11-27", window) == []


def test_a_prior_filing_still_counts():
    agg, _ = _agg_with(("2020-09-11", "2020-09-16", "acc-0", 0.18),
                       ("2020-09-17", "2020-11-27", "acc-1", 1.22))
    obs = agg.get_observable_returns(1, "X", "2020-11-27", "7d")
    assert obs == [("2020-09-11", 0.18)]


def test_same_day_siblings_are_excluded():
    """Two filings published in the same session cannot inform each other."""
    agg, _ = _agg_with(("2020-09-01", "2020-11-27", "acc-a", 0.5),
                       ("2020-09-02", "2020-11-27", "acc-b", 0.7))
    assert agg.get_observable_returns(1, "X", "2020-11-27", "7d") == []


def test_lots_of_one_filing_are_one_observation():
    agg, added = _agg_with(("2020-09-17", "2020-09-18", "acc-1", 0.10),
                           ("2020-09-17", "2020-09-18", "acc-1", 0.10),
                           ("2020-09-17", "2020-09-18", "acc-1", 0.10))
    assert added == [True, False, False]
    assert len(agg.get_observable_returns(1, "X", "2021-01-01", "7d")) == 1
    assert agg.ticker_counts[1]["X"] == 1


def test_the_maturity_guard_survives():
    """Strict filing guard does not replace the observable-return lag."""
    agg, _ = _agg_with(("2020-11-20", "2020-11-21", "acc-1", 0.3))
    # 7d lag is 10 days: on 2020-11-25 the return is not observable yet.
    assert agg.get_observable_returns(1, "X", "2020-11-25", "7d") == []
    assert agg.get_observable_returns(1, "X", "2020-12-05", "7d") == [("2020-11-20", 0.3)]


def test_population_is_signal_class_not_trade_type():
    src = SRC.read_text()
    body = src[src.index("def build_walkforward_scores"):]
    # Judge the SQL, not the prose: strip comment lines first.
    body = "\n".join(l for l in body.splitlines() if not l.lstrip().startswith("#"))
    assert "trade_type = 'buy'" not in body, (
        "the walk-forward gates on trade_type again; 184k grants and 221k "
        "exercises carry trade_type='buy'"
    )
    assert "signal_class IN" in body and "MEANINGFUL_BUY_CLASSES" in src
    for predicate in ("superseded_by IS NULL", "is_derivative = 0", "is_duplicate"):
        assert predicate in body, f"hygiene predicate missing: {predicate}"


def test_clear_preserves_career_columns():
    """--clear used to DELETE every row, erasing four hours of
    compute_career_grades work whenever it ran second."""
    src = SRC.read_text()
    main = src[src.index("def main"):]
    assert not re.search(r"DELETE FROM insider_ticker_scores\b", main), (
        "--clear deletes insider_ticker_scores rows again; career_blended_score "
        "and career_grade live on those rows and belong to another job"
    )
    assert "blended_score = NULL" in main and "sufficient_data = 0" in main
