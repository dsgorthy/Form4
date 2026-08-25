"""Risk figures must be measured daily, never from a sampled series.

WHAT WENT WRONG

The homepage hero advertised "Worst drop 9.5% peak to trough". It computed that
from /portfolio/overlay, which returns a WEEKLY series — 178 points across 3.5
years. A weekly series cannot see a drawdown that opens and closes inside a
week, and misses every intra-week trough.

The daily-marked figure for the same book is 23.7%. So the single most
prominent risk number on the site understated by 2.5x, and told every visitor
the ride had been smooth.

This is the same defect as the API returning trade-row drawdown (11.3% against
a lived 21.5% on Dip Buys) — a coarse sample of an equity curve is not a
drawdown, whatever the sampling interval. Both are now sourced from the daily
walk in _blended_inner.

The bar Derek set: concentration is fine to publish, smoothness is not.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HOME = REPO / "frontend/src/app/page.tsx"
PORTFOLIO_VIEW = REPO / "frontend/src/components/portfolio-view.tsx"
PERF = REPO / "frontend/src/app/performance/page.tsx"


def _code(path: Path) -> str:
    return "\n".join(
        l for l in path.read_text().splitlines()
        if not l.strip().startswith(("//", "*", "/*"))
    )


def test_the_homepage_does_not_derive_drawdown_from_the_overlay():
    """The overlay is weekly. Deriving a drawdown from it understates by 2.5x."""
    code = _code(HOME)
    assert not re.search(r"dd:\s*maxDrawdown\(", code), (
        "the homepage computes its drawdown tile from the overlay series "
        "again. That series is weekly; the number it produces is not a "
        "drawdown, it is a lower bound nobody can distinguish from one."
    )
    assert "max_drawdown_daily" in code, (
        "the homepage no longer reads the daily-marked drawdown from the API"
    )


def test_no_surface_falls_back_to_a_coarser_drawdown():
    """A fallback silently substitutes an understated figure. A missing value
    must render as a dash, which is visibly missing."""
    for path in (HOME, PORTFOLIO_VIEW):
        code = _code(path)
        assert not re.search(r"max_drawdown_daily\s*\?\?\s*", code), (
            f"{path.name} falls back from the daily drawdown to a coarser one"
        )


def test_the_year_by_year_table_is_published():
    """The counterweight to a single compounded figure. One book made most of
    its return in one year; the CAGR alone cannot show that."""
    code = _code(PERF)
    assert "annual_returns" in code, (
        "/performance no longer publishes year-by-year returns, so a "
        "concentrated book reads as a steady one"
    )


def test_nothing_claims_returns_are_steady_or_smooth():
    """Concentration is fine to publish. Implying evenness is not."""
    banned = re.compile(
        r"(steady|smooth|predictable|month after month|consistent)\s+"
        r"(returns?|gains?|profits?|growth|performance)", re.I)
    for path in (HOME, PERF, REPO / "frontend/src/app/research/methodology/page.tsx"):
        hits = banned.findall(_code(path))
        assert not hits, f"{path.name} implies smooth returns: {hits}"
