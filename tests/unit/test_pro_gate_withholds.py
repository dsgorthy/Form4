"""A Pro gate must withhold content, not blur it.

WHAT WAS WRONG

`GatedSection` on /research/methodology rendered its children into the DOM and
applied `blur-sm select-none pointer-events-none`. Every one of those is a
presentation concern. The full Pro methodology — strategy theses, stop levels,
win rates, position sizing — was in the served HTML for anonymous users and
readable with view-source, curl, or any crawler.

Verified live on 2026-08-23: an anonymous request to /research/methodology
returned the gated prose in full while displaying "Portfolio strategies are
Pro-only" over the top of it.

WHAT THIS PINS

The gate renders a skeleton when the viewer is not entitled, and `children`
only when they are. The blurred silhouette stays — it converts — but it must
carry no information.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PAGE = REPO / "frontend/src/app/research/methodology/page.tsx"


def _gate_body() -> str:
    src = PAGE.read_text()
    start = src.index("function GatedSection(")
    return src[start:src.index("\nexport default", start)]


def test_the_gate_does_not_render_children_when_locked():
    body = _gate_body()
    # The entitled path returns children and returns early.
    assert "if (visible) return <>{children}</>;" in body, (
        "the gate no longer short-circuits for entitled viewers"
    )
    # After that early return, `children` must not appear again.
    locked = body.split("if (visible) return <>{children}</>;", 1)[1]
    assert "{children}" not in locked, (
        "GatedSection renders {children} on the LOCKED path — Pro content is "
        "in the DOM for anonymous users again. A CSS blur is not access "
        "control; it was readable with view-source for months."
    )


def test_blur_is_not_applied_to_real_content():
    """Specifically bans the exact construction that shipped."""
    body = _gate_body()
    assert not re.search(r'blur-[\w\[\]-]*"?\s*>\s*\{children\}', body), (
        "children are wrapped in a blur again"
    )


def test_a_skeleton_stands_in_for_the_withheld_content():
    """The gate should still look like there is something behind it."""
    src = PAGE.read_text()
    assert "GateSkeleton" in src, (
        "the locked state renders nothing at all; the blurred silhouette was "
        "doing conversion work and should be replaced, not dropped"
    )
    assert "aria-hidden" in src, "the placeholder is exposed to screen readers"
