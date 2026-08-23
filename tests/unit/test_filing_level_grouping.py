"""One Form 4 is one decision, however many tranches it filled in.

THE BUG, THREE TIMES

A Form 4 reports a single purchase as however many execution lots the broker
filled, each its own row with its own returns. Anything that counts rows counts
the ladder. This has now been found in four places:

  1. generate_stocktwits_posts.py — caught first, and its header documents it:
     "Ungrouped it posted as $534K; it is $1,014,594."
  2. The trade detail panel — Benjamin Wood's one May purchase rendered as
     "5 earlier buys".
  3. pit_scoring._get_returns — the SCORER. Five lots became five
     observations, and total_weight is what decides how hard the Beta prior
     shrinks a thin record. Wood scored A+ (2.8163) on a single purchase;
     grouped, the same purchase is a B (1.6778). Across a 250-row sample of
     2026 grades, 21% moved, almost all downward.
  4. compute_cw_indicators.is_largest_ever — compared tranches, so "the
     largest purchase they have ever made" meant "a bigger slice than any
     previous slice". Wood's August filing ($1,014,594 across two lots) was
     published as his largest when his May filing ($1,025,900 across five) was
     bigger. 26.6% of flags flip once filings are compared.

The grouping key is COALESCE(filing_key, accession, trade_date) — the same one
/insiders/{id}/trades uses, so every surface agrees on what counts as one
trade.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCORER = REPO / "strategies" / "insider_catalog" / "pit_scoring.py"
INDICATORS = REPO / "pipelines" / "insider_study" / "compute_cw_indicators.py"
PORTFOLIO = REPO / "api" / "routers" / "portfolio.py"
STOCKTWITS = REPO / "pipelines" / "generate_stocktwits_posts.py"

#: Every place that aggregates an insider's trades must collapse lots first.
GROUPERS = {
    "pit_scoring (the scorer)": SCORER,
    "compute_cw_indicators (is_largest_ever)": INDICATORS,
    "portfolio trade detail": PORTFOLIO,
    "stocktwits generator": STOCKTWITS,
}


@pytest.mark.parametrize("name,path", sorted(GROUPERS.items()))
def test_every_aggregator_groups_by_filing(name, path):
    """A count of rows is a count of tranches, not of decisions."""
    src = path.read_text()
    assert re.search(r"filing_key", src), (
        f"{name} no longer references filing_key. Aggregating an insider's "
        "trades without collapsing execution lots counts the ladder — see "
        "this module's docstring for the four places it has already happened."
    )


def test_the_scorer_groups_its_observations():
    """The one where it changes a published letter grade."""
    src = SCORER.read_text()
    m = re.search(r"def _get_returns.*?return \[", src, re.S)
    assert m, "_get_returns not found in pit_scoring"
    body = m.group(0)
    assert "GROUP BY" in body, (
        "pit_scoring._get_returns is not grouping. Each execution lot would "
        "count as an independent observation, inflating total_weight — the "
        "term that decides how hard the Beta prior shrinks a thin record. Wood "
        "went A+ on one purchase filled in five tranches."
    )
    assert "filing_key" in body, "the grouping key must be the filing"


def test_largest_ever_compares_filings():
    """"Their largest purchase" must mean the purchase, not the tranche."""
    src = INDICATORS.read_text()
    m = re.search(r"def compute_purchase_size_metrics.*?(?=\ndef )", src, re.S)
    assert m, "compute_purchase_size_metrics not found"
    body = m.group(0)
    assert "filing_key" in body, (
        "is_largest_ever is comparing raw rows again. That published "
        "'the largest purchase they have ever made' about a filing that was "
        "not — 26.6% of flags are wrong when lots are compared."
    )


def test_career_grades_cover_buys_and_sells():
    """The grade describes the person, so it belongs on both sides.

    It is still COMPUTED from buys only — that is the side the separation was
    validated on — but a reader should not see a rating on someone's purchase
    and a blank on the same person's sale.
    """
    src = (REPO / "pipelines" / "insider_study" / "compute_career_grades.py").read_text()
    assert "trans_code IN ('P', 'S')" in src, (
        "compute_career_grades is buy-only again. 395,168 legacy sells carry a "
        "grade and sells after May 2026 carry none, so the corpus shows a "
        "rating on old sells and a blank on new ones."
    )
    # The SCORE still comes from buys.
    assert "trade_type = 'buy'" in SCORER.read_text(), (
        "the scorer is now ingesting sells. A sell 'wins' by falling, so "
        "mixing the two produces a number that means nothing either way."
    )


def test_the_scorer_says_it_is_the_production_scorer():
    """It claimed "not yet wired into production" while writing every grade.

    Asserted POSITIVELY rather than as the absence of that phrase. The first
    version checked the phrase was gone and passed against a wrapped line, and
    the second passed against the historical note explaining the fix — an
    absence test cannot tell a claim from a description of a claim.
    """
    src = SCORER.read_text()
    m = re.search(r"class BayesianScorerV3.*?\"\"\"(.*?)\"\"\"", src, re.S)
    assert m, "BayesianScorerV3 docstring not found"
    doc = " ".join(m.group(1).split())
    assert "THIS IS THE PRODUCTION SCORER" in doc, (
        "BayesianScorerV3's docstring must state plainly that it is in "
        "production. It produces every career_grade the product publishes, and "
        "for months it told the reader the opposite."
    )
