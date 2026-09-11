"""The follow CTA must not wait on Clerk before it renders.

MEASURED 2026-09-10, over the window since the CTA's instrumentation went live:

    people whose CTA rendered       15    median 97.9s on page
    people whose CTA never did      81    median 17.2s on page

The component opened with `if (!isLoaded || pro) return null`, defended in a
comment as "flashing an upsell at a paying subscriber for a beat is worse than
showing the CTA a beat late". The median search visitor leaves in 17 seconds and
Clerk does not resolve in that window, so for 84% of them the CTA never mounted
and the product never made its cheapest ask. The only people who saw it were the
ones who stayed 5.7x longer — the ones least in need of persuading. In the same
window `follow_cta_clicked` fired ZERO times, out of 19 impressions.

The flicker being avoided costs a subscriber a fraction of a second, and there
were zero active subscriptions at the time, so it protected nobody.

Two further traps this pins down:

  - `follow_cta_shown` fires on MOUNT and always did. It never meant anyone saw
    anything. Reading it as "saw the CTA" produced a confident, wrong diagnosis
    ("they don't scroll") that survived a week. `follow_cta_viewed` is the
    viewport event; the two must stay distinct so the old series keeps one
    meaning across its whole history.
  - The capture hook must stay above every early return, or hook order changes
    between renders as Clerk resolves.
"""
import re
from pathlib import Path

import pytest

CTA = Path(__file__).resolve().parents[2] / "frontend" / "src" / "components" / "follow-cta.tsx"


@pytest.fixture(scope="module")
def src() -> str:
    return CTA.read_text()


def _body(src: str) -> str:
    """The component body, past the props docblock."""
    return src[src.index("}) {"):]


def test_render_is_not_gated_on_clerk_loading(src):
    body = _body(src)
    m = re.search(r"^\s*if \(([^)]*)\) return null;", body, re.M)
    assert m, "no early return found — did the component change shape?"
    cond = m.group(1)
    assert "isLoaded" not in cond, (
        f"the CTA early-returns on `{cond}`, so it renders nothing until Clerk "
        "resolves. Search visitors leave in a median of 17s and never see it. "
        "Gate on `pro` alone and let the anonymous shape render first."
    )
    assert "pro" in cond, (
        f"early return is `{cond}`; it must still unmount for subscribers"
    )


def test_viewport_event_is_separate_from_the_mount_event(src):
    """Redefining follow_cta_shown would silently rewrite history.

    The series runs from 2026-09-03 with mount semantics. Changing what it means
    makes any before/after comparison across that boundary meaningless, which is
    the exact failure that produced the wrong diagnosis in the first place.
    """
    assert "follow_cta_shown" in src, "the mount event was removed"
    assert "follow_cta_viewed" in src, "no viewport event — add one, don't redefine the other"
    assert "IntersectionObserver" in src, (
        "follow_cta_viewed must come from an IntersectionObserver; anything else "
        "is another mount event wearing a viewport name"
    )


def test_capture_hook_stays_above_every_early_return(src):
    body = _body(src)
    first_effect = body.find("useEffect(")
    early_return = body.find("return null;")
    assert first_effect != -1 and early_return != -1
    assert first_effect < early_return, (
        "a useEffect sits below the early return; hook order will change between "
        "renders as Clerk resolves"
    )


def test_click_event_still_exists(src):
    """It has never fired. That is a finding, not a reason to delete it.

    Zero clicks on 19 impressions is consistent with any click-through rate
    below ~10% — at that sample it is not evidence the offer is broken, only
    that nothing can yet be concluded. The event has to survive for the question
    to become answerable once impressions rise.
    """
    assert "follow_cta_clicked" in src, "the click event was removed"
