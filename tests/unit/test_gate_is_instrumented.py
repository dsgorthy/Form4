"""The paywall gate must emit an impression event.

WHY

PostHog was already capturing pageviews, autocapture, session replay and four
funnel events -- signed_up, onboarding_complete, checkout_started,
upgrade_complete. What it could not answer was the only question that matters
for revenue.

`checkout_started` fires ONLY on /pricing. Measured 2026-09-03 over 90 days:

    all visitors            179
    reached /pricing          2      (1.1%)
    checkout_started ever      3     from ONE person, last 2026-07-19
    upgrade_complete ever      0

So the funnel showed arrivals and zero purchases with NOTHING in between. Ten
components link to /pricing, including pro-gate, so the links exist -- but with
no event on the gate itself there was no way to distinguish "gates never
render" from "gates render and are ignored", and those need opposite fixes: one
is placement, the other is the offer.

THE PROPERTIES

  1. gate_shown fires when a gate actually blurs something.
  2. gate_cta_clicked fires on the way out, so the ratio is measurable.
  3. THE HOOK IS CALLED UNCONDITIONALLY. A first attempt put useEffect after
     `if (cleared) return` and `if (compact) return`, which violates the rules
     of hooks -- call order changes between renders as auth resolves. The
     condition belongs inside the effect, not around the hook.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GATE = REPO / "frontend/src/components/pro-gate.tsx"


def test_the_gate_emits_an_impression():
    src = GATE.read_text(encoding="utf-8")
    assert "gate_shown" in src, (
        "pro-gate no longer reports impressions. Without it the funnel cannot "
        "distinguish a gate that never renders from one that is ignored."
    )


def test_the_cta_click_is_captured():
    src = GATE.read_text(encoding="utf-8")
    assert "gate_cta_clicked" in src, (
        "the CTA click is not captured, so gate_shown has no denominator"
    )


def test_the_hook_precedes_every_early_return():
    """Rules of hooks. This component has three conditional returns before the
    CTA is even computed, and the effect must sit above all of them."""
    lines = GATE.read_text(encoding="utf-8").splitlines()
    body = next(i for i, l in enumerate(lines) if "export function ProGate" in l)

    effect = next((i for i, l in enumerate(lines)
                   if i > body and "useEffect(" in l), None)
    assert effect is not None, "the impression effect is gone"

    first_return = next((i for i, l in enumerate(lines)
                         if i > body and re.match(r"\s{2}return[\s(<]", l)), None)
    if first_return is not None:
        assert effect < first_return, (
            f"useEffect (line {effect+1}) is called AFTER an early return "
            f"(line {first_return+1}). React requires an unconditional call "
            "order; as auth resolves this component takes different return "
            "paths and the hook would sometimes not run."
        )


def test_the_impression_is_conditioned_inside_the_effect():
    """Called unconditionally, but must not report an impression for a gate
    that cleared -- that would count every Pro user as seeing a paywall."""
    src = GATE.read_text(encoding="utf-8")
    eff = src[src.index("useEffect("):]
    eff = eff[:eff.index("}, [")]
    assert "return" in eff and ("isPro" in eff or "cleared" in eff), (
        "the effect does not check whether the gate actually gated. Every "
        "Pro user rendering this component would be logged as a paywall "
        "impression."
    )
    assert "isLoaded" in eff, (
        "the effect does not wait for auth to resolve, so a signed-in Pro "
        "user is counted as gated during the first render"
    )
