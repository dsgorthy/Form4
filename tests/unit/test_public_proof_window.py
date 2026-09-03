"""One window of analysis is public. Alpha is not.

WHY THIS CHANGED

The doctrine was "volume is public, outcomes are not", and taken literally it
left an anonymous visitor with a name, a filing count and some dates -- what a
free SEC scraper gives them. Nothing on the page said we had done any work.

Worse, `best_career_grade` was popped alongside the raw scores, so
<InsiderGradeBadge grade={best_career_grade}> rendered EMPTY. The single glyph
that says we have a view on this person was hidden from exactly the visitor we
were trying to convert -- on the pages that carry the most traffic (filing 57
visitors, insider 25, company 24 over 90 days, against /pricing's 2).

WHAT IS PUBLIC NOW

  the rating           a grade is a conclusion, not a score
  30d buy accuracy     proof that we compute something
  30d buy avg move     "
  30d scored filings   the DENOMINATOR -- an accuracy without one is the exact
                       defect that showed a rate over 154 lots under a header
                       reading 19

WHAT IS STILL PRO: all three windows, both sides, per-ticker grades, sell
patterns, best window, and every numeric score and percentile.

AND ALPHA, DELIBERATELY. Accuracy and average move are facts about what
happened. Alpha reads as a claim about skill, and three separate experiments in
September 2026 found our grades do not predict forward returns. Publishing
history is honest; publishing it in a frame that implies forecast is not.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from api.public_fields import (  # noqa: E402
    PUBLIC_FILING_STAT_FIELDS,
    PUBLIC_VOLUME_FIELDS,
)


def test_alpha_is_never_public():
    """The one field whose framing we cannot stand behind."""
    leaked = [f for f in PUBLIC_FILING_STAT_FIELDS if "abnormal" in f or "alpha" in f]
    assert not leaked, (
        f"{leaked} would publish alpha to anonymous visitors. Accuracy and "
        "average move are facts about what happened; alpha reads as a claim "
        "about skill, and our grades do not predict forward returns."
    )


def test_only_one_window_and_only_buys():
    """Proof, not product. The depth is what Pro buys."""
    assert all("30d" in f for f in PUBLIC_FILING_STAT_FIELDS), (
        "more than one window is public; the public set is a teaser, not the "
        "block"
    )
    assert all(f.startswith("buy_") for f in PUBLIC_FILING_STAT_FIELDS), (
        "sell-side stats are public. Sells are scored on different terms and "
        "are the more easily misread half."
    )


def test_the_denominator_is_published_with_the_rate():
    """An accuracy without its denominator is how a rate over 154 lots ended
    up under a header reading 19."""
    has_rate = any("win_rate" in f for f in PUBLIC_FILING_STAT_FIELDS)
    has_n = any("scored_filings" in f for f in PUBLIC_FILING_STAT_FIELDS)
    assert not has_rate or has_n, (
        "a win rate is public without the count it was computed over"
    )


def test_the_rating_survives_gating():
    """A grade is a conclusion, not a score. It was popped with the scores and
    left the badge blank on the highest-traffic pages."""
    src = (REPO / "api/routers/insiders.py").read_text(encoding="utf-8")
    block = src[src.index('result["gated"] = True') - 2500:
                src.index('result["gated"] = True')]
    popped = block[block.index("for f in ("):block.index("result.pop")]
    assert "best_career_grade" not in popped, (
        "best_career_grade is being stripped again, so InsiderGradeBadge "
        "renders empty for anonymous visitors"
    )
    assert "percentile" in popped and "score" in popped, (
        "the numeric scores should still be Pro -- a grade is a claim, a "
        "percentile invites arithmetic"
    )


def test_volume_and_analysis_sets_do_not_overlap():
    assert not (set(PUBLIC_VOLUME_FIELDS) & set(PUBLIC_FILING_STAT_FIELDS)), (
        "a field is in both allowlists; they answer different questions and "
        "are gated by different rules"
    )


# ── the API making it public is not the same as the page showing it ────────

PAGE = (Path(__file__).resolve().parents[2]
        / "frontend" / "src" / "app" / "insider" / "[id]" / "page.tsx"
       ).read_text(encoding="utf-8")


def _gated_block() -> str:
    """The branch an anonymous visitor actually renders."""
    i = PAGE.index("{isGated && (")
    j = PAGE.index("{tr && !isGated &&", i)
    return PAGE[i:j]


def test_the_gated_track_record_renders_the_public_window():
    """WHAT WENT WRONG, 2026-09-03.

    The allowlist shipped, the endpoint served
    buy_win_rate_30d=0.2857 to anonymous visitors, and every test here
    passed — while the page kept drawing nine placeholder bars, because the
    gated block never read filing_stats at all. It was reported as working.

    An allowlist is a permission, not a rendering. This asserts the page
    actually consumes the fields the allowlist opens.
    """
    block = _gated_block()
    assert "filing_stats" in block, (
        "the gated Track Record block does not read filing_stats, so the "
        "public 30-day window is allowed through the API and then thrown away "
        "— the reader sees placeholder bars"
    )
    for f in ("buy_win_rate_30d", "buy_avg_return_30d", "buy_scored_filings_30d"):
        assert f in block, f"the gated block does not render {f}"


def test_alpha_is_withheld_in_every_window_on_the_page():
    """Alpha's row must stay bars. It is the number that most invites reading
    history as forecast, and our grades do not predict forward returns."""
    block = _gated_block()
    i = block.index('"Alpha vs SPY"')
    row = block[i:i + 120]
    assert "null" in row, (
        "the Alpha vs SPY row is no longer pinned to null in the gated table; "
        "alpha stays Pro in all three windows by design"
    )


def test_the_denominator_is_rendered_beside_the_rate():
    """A rate without its count is how a figure over 154 lots once appeared
    under a header reading 19."""
    block = _gated_block()
    assert "n30" in block and "scored" in block, (
        "the scored-filing count is not rendered next to the public rate"
    )
