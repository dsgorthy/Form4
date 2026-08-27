""""Routine" gets one definition, and it lives in api/programmatic.py.

Four overlapping notions existed before this: cohen_routine (academic, kept
separate on purpose), is_recurring (buys only, cadence only),
is_distribution_program (sells only, a raw count, living inside the Stocktwits
generator), and is_routine — a column NOTHING ever wrote while
compute_trade_grade deducted five points for it.

These tests keep the thresholds in one place and the predicate honest.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from api.programmatic import (
    MAX_CV_INTERVAL,
    MAX_CV_VALUE,
    MIN_FILINGS,
    coefficient_of_variation,
    is_programmatic,
    score_sequence,
)

REPO = Path(__file__).resolve().parents[2]


def _seq(days, values):
    return [(date(2026, 1, 1) + __import__("datetime").timedelta(days=d), v)
            for d, v in zip(days, values)]


# ── the definition needs BOTH regularities ─────────────────────────────────

def test_regular_cadence_and_regular_size_is_a_programme():
    """CRWD's CEO: $3.67M, $3.61M, $3.71M, $3.65M, one a week."""
    s = score_sequence(_seq([0, 7, 14, 21], [3.67e6, 3.61e6, 3.71e6, 3.65e6]))
    assert s["is_programmatic"] == 1


def test_regular_cadence_but_wild_sizes_is_not():
    """Buying every quarter in wildly different amounts is ordinary."""
    s = score_sequence(_seq([0, 90, 180, 270], [10_000, 900_000, 25_000, 1_400_000]))
    assert s["is_programmatic"] == 0, "cadence alone must not be enough"


def test_regular_sizes_but_random_timing_is_not():
    s = score_sequence(_seq([0, 5, 200, 640], [500_000, 505_000, 495_000, 500_000]))
    assert s["is_programmatic"] == 0, "size alone must not be enough"


# ── refuse to judge what cannot be judged ──────────────────────────────────

@pytest.mark.parametrize("n", [0, 1, 2])
def test_too_few_filings_is_never_a_programme(n: int):
    s = score_sequence(_seq(list(range(n)), [100_000] * n))
    assert s["is_programmatic"] == 0
    assert s["cv_interval"] is None and s["cv_value"] is None, (
        "a sequence too short to judge must report None, not 0 — a 0 CV reads "
        "as 'perfectly regular', which is the opposite of what we know"
    )


def test_cv_is_none_not_zero_when_undefined():
    assert coefficient_of_variation([]) is None
    assert coefficient_of_variation([5.0]) is None
    assert coefficient_of_variation([0.0, 0.0]) is None   # mean 0


# ── the predicate and the scorer must agree ────────────────────────────────

def test_predicate_matches_the_scorer():
    for days, vals in ((( 0, 7, 14, 21), (1e6, 1.01e6, 0.99e6, 1e6)),
                       (( 0, 90, 400, 900), (1e6, 2e5, 9e5, 3e4))):
        s = score_sequence(_seq(list(days), list(vals)))
        assert bool(s["is_programmatic"]) == is_programmatic(
            s["cv_interval"], s["cv_value"], s["n_filings"]), (
            "is_programmatic() and score_sequence() disagree — the predicate "
            "exists so stored columns and live sequences give one answer")


# ── nobody re-types the thresholds ─────────────────────────────────────────

def test_thresholds_are_not_duplicated_anywhere():
    """Four definitions became four definitions by being typed at call sites."""
    offenders = []
    for path in list((REPO / "pipelines").rglob("*.py")) + \
                list((REPO / "api").rglob("*.py")) + \
                list((REPO / "strategies").rglob("*.py")):
        if path.name in ("programmatic.py", "compute_programmatic.py"):
            continue
        if "archive" in str(path):
            continue
        src = path.read_text(encoding="utf-8", errors="ignore")
        if "MAX_CV_INTERVAL" in src or "MAX_CV_VALUE" in src:
            if "from api.programmatic import" not in src:
                offenders.append(str(path.relative_to(REPO)))
    assert not offenders, (
        f"these re-declare the programmatic thresholds instead of importing "
        f"them: {offenders}")


def test_cohen_routine_is_deliberately_not_folded_in():
    """cohen_routine answers a different question and is a cited measure."""
    doc = (REPO / "api" / "programmatic.py").read_text()
    assert "cohen_routine" in doc and "KEPT SEPARATE" in doc, (
        "api/programmatic.py must say why cohen_routine is not consolidated, "
        "or someone will helpfully merge it")


def test_the_thresholds_are_sane():
    assert MIN_FILINGS >= 3, "two points cannot establish regularity"
    assert 0 < MAX_CV_INTERVAL <= 1.0
    assert 0 < MAX_CV_VALUE <= 1.0
