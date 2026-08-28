"""A sweep that ignores transaction costs recommends the most turnover.

WHAT IT WOULD HAVE COST

simulate_strategy_portfolio models no transaction costs anywhere -- the
quality_notrend yaml says so in its own comments. Swept gross over 2016-2026,
A-List's hold period reads:

    hold_days=10   430 trades   +1167%
    hold_days=21   275 trades    +779%
    hold_days=42   161 trades    +516%   <- shipped

which says, unambiguously, shorten the hold. But hold=10 does 2.7x the trades,
and a round trip is charged on every one of them. Once it is:

    hold    @0%     @0.5%   @1%     @2%
      10    26.9%   18.8%   11.1%   -2.8%
      42    18.6%   15.7%   12.9%   +7.5%

The ranking INVERTS between 0.5% and 1%, and at 2% the "winner" is the only
config that loses money. Insider strategies trade small caps where a 1% round
trip is optimistic, so acting on the gross table would have tripled turnover to
destroy the book.

THE PROPERTIES

  1. Cost rises with trade count, so turnover is penalised at all.
  2. The A-List hold case specifically inverts, because that is the decision
     this exists to get right.
  3. A config that cannot survive its own costs reports None, not a negative
     number dressed up as a result.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "scripts" / "strategy_sweep.py"


def _mod():
    spec = importlib.util.spec_from_file_location("sweep", SRC)
    m = importlib.util.module_from_spec(spec)
    argv, sys.argv = sys.argv, ["x"]
    try:
        spec.loader.exec_module(m)
    except SystemExit:
        pass
    finally:
        sys.argv = argv
    return m


YEARS, SLOTS = 10.66, 3


def test_more_turnover_costs_more():
    ca = _mod().cost_adjusted
    few = ca(6.16, 161, SLOTS, YEARS)["cagr_at_10bp_pct"]
    many = ca(6.16, 430, SLOTS, YEARS)["cagr_at_10bp_pct"]
    assert many < few, (
        "the same gross growth reached with 430 trades must net LESS than with "
        "161; otherwise the sweep is blind to turnover"
    )


def test_the_a_list_hold_ranking_inverts_under_cost():
    """The exact decision. Gross says 10; net at 1% says 42."""
    ca = _mod().cost_adjusted
    h10 = ca(12.67, 430, SLOTS, YEARS)     # +1167%
    h42 = ca(6.16, 161, SLOTS, YEARS)      # +516%

    assert h10["cagr_at_5bp_pct"] > h42["cagr_at_5bp_pct"], (
        "at 0.5% the high-turnover config should still lead -- if not, the "
        "model is charging far too much"
    )
    assert h42["cagr_at_10bp_pct"] > h10["cagr_at_10bp_pct"], (
        "at 1% round-trip the shipped hold_days=42 must beat hold_days=10. "
        "This inversion is the entire reason the adjustment exists."
    )
    assert h42["cagr_at_20bp_pct"] > h10["cagr_at_20bp_pct"], (
        "at 2% the gap should widen, not close"
    )


def test_high_turnover_at_flat_growth_is_a_large_loss():
    """Flat gross growth plus 1,000 round trips is a rout, and should be
    reported as one. (Not None: with many trades per slot the per-trade factor
    stays near 1.0, so the compounding is perfectly well defined -- it is just
    very bad. An earlier version of this test asserted None here and was simply
    wrong about the arithmetic.)"""
    ca = _mod().cost_adjusted
    out = ca(1.0, 1000, SLOTS, YEARS)
    assert out["cagr_at_20bp_pct"] < -20


def test_the_undefined_case_reports_none():
    """None is reserved for where compounding genuinely breaks down: so few
    trades per slot that a single round trip exceeds the whole per-trade gross
    factor. A negative base raised to a fractional power is not a number."""
    ca = _mod().cost_adjusted
    out = ca(0.01, 3, SLOTS, YEARS)      # 99% loss over one trade per slot
    assert out["cagr_at_20bp_pct"] is None


def test_degenerate_inputs_do_not_raise():
    ca = _mod().cost_adjusted
    for growth, n in ((0.0, 10), (-1.0, 10), (2.0, 0)):
        ca(growth, n, SLOTS, YEARS)      # must not raise


def test_the_cost_model_documents_that_it_overstates_drag():
    """It charges a full round trip against a fully committed slot, but A-List
    sits ~40% in cash. A reader must not take @1% as a point estimate."""
    src = SRC.read_text(encoding="utf-8")
    body = src[src.index("def cost_adjusted"):]
    body = body[:body.index("\n    out = {}")]
    assert "OVERSTATES" in body or "overstates" in body, (
        "the docstring no longer warns that the model is a floor rather than a "
        "point estimate"
    )
