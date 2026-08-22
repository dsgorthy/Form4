"""The weekend cohort post must not be able to flatter itself.

It publishes a performance number to the public, computed by us, about a rule
we chose. Every degree of freedom in that sentence is a way to cheat, so the
ones that matter are pinned here rather than left to good intentions:

  * the lookback window is COMPUTED, not chosen;
  * every member has completed its full hold before being measured;
  * nothing anywhere filters on the outcome;
  * when the median and the mean disagree, both are published.

The last one is the easiest lie available. On 2026-08-22 the cohort's median
was +5.1% and its mean +0.2% — more than half the names worked, but the misses
were bigger than the hits. Reporting only the median would have been true and
misleading, which is the combination worth guarding against.
"""
from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pytest

from pipelines.generate_weekend_posts import (
    CLUSTER_MIN,
    CLUSTER_MIN_VALUE,
    COHORT_SPAN_DAYS,
    HOLD_DAYS,
    cohort_window,
    render_cohort,
    trading_week,
)

SOURCE = Path(__file__).resolve().parents[2] / "pipelines" / "generate_weekend_posts.py"


def _members(pcts: list[float]) -> list[dict]:
    return [{"ticker": f"T{i}", "pct": p, "n_ins": 2, "v": 1e6}
            for i, p in enumerate(pcts)]


# ── the window ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("anchor", [
    date(2026, 8, 22), date(2026, 1, 3), date(2026, 12, 31), date(2027, 2, 28),
])
def test_every_cohort_member_has_completed_its_hold(anchor):
    """No name may be measured mid-flight.

    The first version pooled a month up to the present, so a cluster filed
    yesterday was averaged in beside one held a full 30 days and the result was
    called a 30-day return.
    """
    lo, hi = cohort_window(anchor)
    last_maturity = date.fromisoformat(hi) + timedelta(days=HOLD_DAYS)
    assert last_maturity <= anchor, (
        f"cohort window ends {hi}, whose last member matures {last_maturity} — "
        f"after the {anchor} anchor. That name has not had its {HOLD_DAYS} days."
    )


def test_the_window_is_a_full_span(anchor=date(2026, 8, 22)):
    lo, hi = cohort_window(anchor)
    assert (date.fromisoformat(hi) - date.fromisoformat(lo)).days == COHORT_SPAN_DAYS - 1


def test_the_window_moves_with_the_anchor_and_nothing_else():
    """Same shape every week — no discretion in where it lands."""
    a, b = cohort_window(date(2026, 8, 22)), cohort_window(date(2026, 8, 29))
    assert (date.fromisoformat(b[0]) - date.fromisoformat(a[0])).days == 7
    assert (date.fromisoformat(b[1]) - date.fromisoformat(a[1])).days == 7


def test_trading_week_on_a_weekend_is_the_week_that_just_closed():
    assert trading_week(date(2026, 8, 22)) == ("2026-08-17", "2026-08-21")  # Sat
    assert trading_week(date(2026, 8, 23)) == ("2026-08-17", "2026-08-21")  # Sun


# ── the honesty guards ────────────────────────────────────────────────────

def test_nothing_filters_on_the_outcome():
    """A cohort is only worth publishing if membership is decided before the
    result is known. Selecting on `pct` anywhere would make it a highlight
    reel with a statistics header."""
    src = SOURCE.read_text()
    offenders = [
        line.strip() for line in src.splitlines()
        if re.search(r"""(pct|p\d)\s*[><]=?\s*[-\d]""", line)
        and not line.lstrip().startswith(("#", "*", '"'))
        # Counting how many were up is a report, not a filter.
        and "up = sum" not in line
        and "for p in pcts if p > 0" not in line
    ]
    assert not offenders, (
        f"the cohort appears to be filtered on its own outcome: {offenders}"
    )


def test_the_mean_is_published_when_it_disagrees_with_the_median():
    """The 2026-08-22 shape: most names up, but the losers larger."""
    body = render_cohort(_members([8, 7, 6, 5, 4, 3, -40]), "2026-06-25",
                         "2026-07-22", spy=4.4)
    assert body and "average" in body, body
    assert "misses were bigger than the hits" in body, body


def test_the_mean_is_omitted_when_it_agrees():
    """Two near-identical numbers side by side is noise, not disclosure."""
    body = render_cohort(_members([3, 4, 5, 6, 7]), "2026-06-25", "2026-07-22", spy=2.0)
    assert body and "average" not in body, body


def test_a_losing_cohort_still_publishes():
    """The first negative week is worth more to the account than any positive
    one, and it must not require a human decision to ship."""
    body = render_cohort(_members([-3, -5, -8, -12, -20]), "2026-06-25",
                         "2026-07-22", spy=1.0)
    assert body is not None
    assert "0 up, 5 down" in body, body
    assert "worst $T4 -20.0%" in body, body


def test_the_post_never_claims_these_were_our_calls():
    """We did not post about these companies at the time. Implying otherwise is
    the one unrecoverable mistake an account like this can make."""
    body = render_cohort(_members([5, 3, -2, 8, -6]), "2026-06-25", "2026-07-22", spy=2.0)
    lowered = body.lower()
    for claim in ("we called", "we flagged", "our picks", "we said", "we told"):
        assert claim not in lowered, f"post implies authorship: {claim!r}"
    assert "not a stock pick list" in lowered


def test_a_thin_cohort_is_withheld():
    """Three names is not a sample and must not be dressed as one."""
    assert render_cohort(_members([5, 3, -2]), "2026-06-25", "2026-07-22", spy=1.0) is None


def test_members_with_no_price_are_excluded_not_zeroed():
    """A missing price must drop the row, never count as a flat outcome —
    ~9% of filing tickers have no coverage at all."""
    members = _members([5, 3, -2, 8, 6]) + [{"ticker": "EOS", "pct": None}]
    body = render_cohort(members, "2026-06-25", "2026-07-22", spy=2.0)
    assert "5 companies" in body, body
    assert "EOS" not in body


# ── the thresholds ────────────────────────────────────────────────────────

def test_thresholds_are_constants():
    """If a threshold can be computed at runtime it can be tuned to the answer."""
    assert isinstance(HOLD_DAYS, int) and HOLD_DAYS == 30
    assert isinstance(COHORT_SPAN_DAYS, int) and COHORT_SPAN_DAYS == 28
    assert isinstance(CLUSTER_MIN, int) and CLUSTER_MIN == 2
    assert isinstance(CLUSTER_MIN_VALUE, int) and CLUSTER_MIN_VALUE == 250_000


def test_the_dollar_floor_is_documented_with_its_cost():
    """The floor was adopted knowing it made that week's number worse. That
    fact is the evidence it was not fitted, so it stays written down."""
    src = SOURCE.read_text()
    assert "+7.1% median to" in src and "+0.7%" in src, (
        "the note recording what the $250K floor cost the 2026-08-22 post is "
        "gone. Without it, a future reader cannot tell the threshold was not "
        "chosen to flatter the result."
    )
