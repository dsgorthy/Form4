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


def test_the_whole_buy_grid_is_public():
    """REVERSED 2026-09-03, deliberately, and the reason is recorded.

    This file previously asserted the opposite twice: alpha is never public,
    and only one window ships. Both were written to protect a distinction that
    does not exist on the page they were defending.

    /insiders/{id}/trades serves return_7d/30d/90d AND abnormal_7d/30d/90d on
    every filing row, ungated. The 3x3 aggregate is arithmetic over numbers
    already printed below the table. Withholding it hid nothing from anyone
    with a calculator and made the block read as broken to everyone else --
    which is how it was reported, twice.

    If the trades endpoint ever starts gating per-filing returns, this test is
    the thing to revisit: the argument here rests entirely on those being
    public.
    """
    for metric in ("win_rate", "avg_return", "avg_abnormal", "scored_filings"):
        for window in ("7d", "30d", "90d"):
            f = f"buy_{metric}_{window}"
            assert f in PUBLIC_FILING_STAT_FIELDS, (
                f"{f} is not public. The gated table renders a 3x3 grid and a "
                "missing field there is a grey bar the reader cannot "
                "distinguish from a data problem."
            )


def test_the_whole_track_record_is_public():
    """REVERSED 2026-09-03 — the second reversal in this file, deliberately.

    This test previously asserted the SELL SIDE STAYS PRO, on the reasoning
    that sell figures rest on our decision-sell classification and are
    therefore the work rather than the arithmetic.

    The line moved because it was drawn through the wrong thing. Two facts
    settled it:

      - /insiders/{id}/trades serves return AND abnormal for 7/30/90 on every
        filing row, ungated. Sells appear in that table like buys do, so the
        sell aggregates were arithmetic over published numbers too.
      - 2 of 179 visitors ever reached /pricing. The wall was not converting;
        it was making the pages that carry the organic traffic prove nothing.

    So the product sells a CAPABILITY — alerts when a followed insider files,
    saved screens, the strategy books, the digest — and every number on the
    profile is public. Those capabilities are gated at their own endpoints.

    WHAT WOULD REVERSE THIS AGAIN: per-filing returns being gated on the
    trades endpoint. The argument here rests entirely on them being public.
    """
    for side in ("buy", "sell"):
        for metric in ("win_rate", "avg_return", "avg_abnormal", "scored_filings"):
            for window in ("7d", "30d", "90d"):
                f = f"{side}_{metric}_{window}"
                assert f in PUBLIC_FILING_STAT_FIELDS, (
                    f"{f} is not public. The whole track record is published; "
                    "Pro is alerts and screening."
                )


def test_the_scores_are_still_stripped_and_not_as_a_paywall():
    """score / percentile / best_pit_score go because they are not a published
    scale — api/ratings.py is explicit that pit_grade and conviction must never
    render as a user-facing rating. Different reason from 'pay us'."""
    router = (REPO / "api" / "routers" / "insiders.py").read_text(encoding="utf-8")
    block = router[router.index('result["gated"] = True') - 900:]
    block = block[:block.index('result["gated"] = True') + 40]
    for f in ("score", "percentile", "best_pit_score"):
        assert f'"{f}"' in block, f"{f} is no longer stripped from the payload"


def test_gating_still_builds_from_an_allowlist():
    """The allowlists now cover the whole published set, which makes them look
    redundant. They are not: iterating an allowlist is what stops a column
    added to a future SELECT from publishing itself."""
    router = (REPO / "api" / "routers" / "insiders.py").read_text(encoding="utf-8")
    assert "for k in PUBLIC_VOLUME_FIELDS if k in tr_full" in router
    assert "for k in PUBLIC_FILING_STAT_FIELDS" in router


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


def test_the_placeholder_table_is_gone():
    """It drew nine grey bars for numbers that are now public. Rendering
    placeholders over published data is worse than rendering nothing."""
    assert "aria-hidden" not in PAGE or "h-3.5 w-12 rounded bg-" not in PAGE, (
        "the gated placeholder bars are back on the insider page"
    )


def test_anonymous_visitors_get_the_real_track_record():
    """The full block used to be guarded by `tr && !isGated`."""
    assert "{tr && !isGated" not in PAGE, (
        "the full track record is gated again; the whole record is public and "
        "Pro is alerts and screening"
    )


def test_the_cta_sells_a_capability_not_the_numbers():
    """It sat under a table of grey bars promising the figures behind them.
    Those figures are printed on the page now, so promising them reads as a
    lie."""
    i = PAGE.index("<FollowCta")
    cta = PAGE[i:i + 400]
    assert "Alerts" in cta, "the CTA no longer leads with alerts"
    for banned in ("Win rate, average move", "alpha across"):
        assert banned not in cta, (
            f"the CTA offers {banned!r}, which is rendered above it for free"
        )


def test_the_denominator_is_rendered_beside_the_rate():
    """A rate without its count is how a figure over 154 lots once appeared
    under a header reading 19."""
    # Two places carry it now: the verdict block states it in prose, and the
    # full track record derives its basis from the per-window scored counts.
    verdict = (REPO / "frontend" / "src" / "components" / "insider-verdict.tsx"
               ).read_text(encoding="utf-8")
    assert "scored purchases" in verdict, (
        "the verdict no longer states how many filings it is built from"
    )
    assert "buy_scored_filings" in PAGE and "buyBasis" in PAGE, (
        "the track record block no longer derives its denominator from the "
        "per-window scored counts"
    )
