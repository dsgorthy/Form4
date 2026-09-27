"""A page that gates something must offer a way to learn what is behind it.

`compact` mode is a blurred span with `pointer-events-none` and no CTA, which is
right for a table cell — fifty call-to-actions in a fifty-row table is not a
design. But /explore renders ONLY compact gates, so there was no path from the
blur to the offer anywhere on the page.

Measured 2026-09-26 on the only real user the product had: about twenty
encounters with the blur across four days and ZERO visits to /pricing, ever. The
gate was not unpersuasive; there was nothing to click.

Also pins two things that keep the fix honest:
  - the notice must not be a wall. It hides nothing and blocks nothing.
  - `gate_shown` must fire once per page, not once per row. It fired per mounted
    gate, so one /explore pageview emitted 50-75 events and that one user
    produced 453 of them — the funnel read as enormous exposure with zero
    conversion when the truth was about twenty visits.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
GATE = REPO / "frontend" / "src" / "components" / "pro-gate.tsx"
#: Components that render a compact gate, and therefore owe the page a notice.
COMPACT_CALLERS = [
    "frontend/src/components/trades-table.tsx",
    "frontend/src/components/insider-roster.tsx",
]


def test_the_notice_exists_and_links_to_the_offer():
    src = GATE.read_text()
    assert "export function ProGateNotice" in src
    body = src[src.index("export function ProGateNotice"):]
    body = body[:body.index("export function ProGate(")]
    assert 'href="/pricing"' in body, "the notice does not link to the offer"
    assert "gate_cta_clicked" in body, (
        "the notice's link is not instrumented, so we still cannot tell whether "
        "the path from blur to offer is taken"
    )


def test_the_notice_is_not_a_wall():
    """It must not blur, hide, or intercept anything. It is one line of text.

    Scans the RETURNED JSX only. Scanning the whole function matched the word
    "blur" in its own docstring, which explains what the blur covers — the same
    comment-versus-code confusion that made the first version of this file fail
    on prose.
    """
    src = GATE.read_text()
    body = src[src.index("export function ProGateNotice"):]
    body = body[:body.index("export function ProGate(")]
    # ...and stop at the function's own closing brace. Slicing to the next
    # `export function` swallowed ProGate's doc comment, which says "Inline
    # blurred overlay" — prose about a different component.
    body = body[body.index("return ("):]
    body = body[:body.index("\n}\n")]
    for forbidden in ("blur", "pointer-events-none", "absolute inset-0",
                      "select-none", "fixed"):
        assert forbidden not in body, (
            f"ProGateNotice contains {forbidden!r} — it is becoming a wall. It "
            "exists to name what is already gated, not to gate more."
        )


def test_the_notice_does_not_re_derive_who_is_gated():
    """Two copies of 'cleared' is how a notice advertises Pro to a subscriber."""
    src = GATE.read_text()
    body = src[src.index("export function ProGateNotice"):]
    body = body[:body.index("export function ProGate(")]
    assert "useGateState(" in body
    assert "isPro(" not in body, "the notice re-implements the cleared test"


@pytest.mark.parametrize("rel", COMPACT_CALLERS)
def test_every_compact_gate_caller_renders_one_notice(rel):
    src = (REPO / rel).read_text()
    if "<ProGate compact" not in src:
        pytest.skip(f"{rel} no longer renders a compact gate")
    assert "<ProGateNotice" in src, (
        f"{rel} blurs cells with a compact gate but never tells the reader what "
        "is behind the blur or where to find out"
    )
    assert src.count("<ProGateNotice") == 1, (
        f"{rel} renders the notice more than once — it belongs once per gated "
        "region, not once per row"
    )


def test_impressions_are_counted_once_per_page():
    src = GATE.read_text()
    assert "firstImpressionOnThisPage" in src, (
        "gate_shown is no longer deduped; a fifty-row table will report fifty "
        "impressions for one pageview"
    )
    effect = src[src.index("useEffect(() => {"):]
    effect = effect[:effect.index("posthog?.capture?.(\"gate_shown\"")]
    assert "firstImpressionOnThisPage(" in effect, (
        "the dedupe is not guarding the gate_shown capture"
    )


def test_the_table_shows_one_unblurred_proof_row():
    """A wall over every row asserts the numbers exist; one visible row proves
    it. insider-roster documents this pattern; trades-table had no proof at all,
    so the free tier showed a company's filings with every return blurred."""
    src = (REPO / "frontend/src/components/trades-table.tsx").read_text()
    assert "isProofRow" in src
    assert re.search(r"offset === 0 && rowIdx === 0", src), (
        "the proof row is not pinned to the first row of the first page — one "
        "that follows the reader through pagination is just an ungated column"
    )
