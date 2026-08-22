"""A page must not go blank because the API said no.

WHAT HAPPENED

Both fetchers end with `if (!res.ok) throw new Error(...)`. There was no
error.tsx anywhere in the app, so a throw inside a server component rendered
nothing at all — nav, footer, and a void between them.

/congress shipped that way. It called `fetchAPI` — which never attaches the
Clerk token — against `/congress/*` endpoints carrying `Depends(require_pro)`.
Every request arrived anonymous and was refused, for Pro subscribers exactly as
much as for signed-out visitors, and the unguarded `await Promise.all` took the
page down. Measured 2026-08-22: the live page returned 161 characters of
visible text. It is linked in the main nav.

WHAT THESE PIN

  1. An error boundary exists, so the next unguarded throw degrades into a
     message instead of a blank screen.
  2. No page fetches a Pro-gated endpoint with the unauthenticated fetcher —
     that combination cannot ever succeed.
  3. Pro-gated fetches are wrapped, so refusal renders an upgrade prompt.

Static analysis rather than a browser: the failure is a missing token and a
missing try, both visible in the source, and neither reachable from a unit test
that renders React.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
APP = REPO / "frontend" / "src" / "app"
ROUTERS = REPO / "api" / "routers"


def _pages() -> list[Path]:
    return [p for p in APP.rglob("page.tsx") if "node_modules" not in p.parts]


def _pro_gated_prefixes() -> set[str]:
    """Router prefixes whose endpoints sit behind require_pro/require_pro_plus.

    Read from the routers rather than hardcoded, so gating a new router is
    picked up here without anyone remembering to update a list.
    """
    gated = set()
    for path in ROUTERS.glob("*.py"):
        src = path.read_text()
        if "require_pro" not in src:
            continue
        m = re.search(r'APIRouter\(\s*prefix\s*=\s*"([^"]+)"', src)
        if m:
            gated.add(m.group(1).replace("/api/v1", ""))
    return gated


def test_an_error_boundary_exists():
    """Without one, every unhandled throw is a blank page."""
    assert (APP / "error.tsx").exists(), (
        "frontend/src/app/error.tsx is gone. Both fetchers throw on a non-2xx "
        "response, and with no boundary that renders an empty document — which "
        "is how /congress was broken for every user for an unknown period."
    )


def test_the_error_boundary_surfaces_the_digest():
    """The frontend log identifies a failure only by digest; without it on
    screen, a user report cannot be matched to a log line."""
    src = (APP / "error.tsx").read_text()
    assert "digest" in src, "the error page must show error.digest"
    assert "reset" in src, "the error page must offer a retry"


def test_pro_gated_endpoints_are_never_called_with_the_anonymous_fetcher():
    """fetchAPI attaches no Authorization header, so pairing it with a
    require_pro endpoint is a guaranteed 403 — for Pro subscribers too."""
    gated = _pro_gated_prefixes()
    assert gated, "parsed no Pro-gated routers — has the gating moved?"

    offenders = []
    for page in _pages():
        src = page.read_text()
        for line in src.splitlines():
            if "fetchAPI<" not in line or "fetchAPIAuth<" in line:
                continue
            for prefix in gated:
                if f'"{prefix}' in line or f"`{prefix}" in line:
                    offenders.append(
                        f"{page.relative_to(APP)}: {line.strip()[:88]}"
                    )
    assert not offenders, (
        "these pages call a Pro-gated endpoint with the unauthenticated "
        "fetcher, which can only ever 403:\n  " + "\n  ".join(offenders)
        + "\n\nUse fetchAPIAuth and wrap it — see app/congress/page.tsx."
    )


@pytest.mark.parametrize(
    "page_rel", ["congress/page.tsx", "clusters/page.tsx"]
)
def test_pages_that_fetch_gated_data_handle_refusal(page_rel):
    """A 403 on a Pro feature is an expected state, not an exception. It should
    render the upgrade prompt the reader was always meant to see."""
    src = (APP / page_rel).read_text()
    if "fetchAPIAuth" not in src:
        pytest.skip(f"{page_rel} does not fetch")
    assert "try {" in src and "catch" in src, (
        f"{page_rel} awaits a fetch with no try/catch. Both fetchers throw on "
        "a non-2xx, and an unguarded throw in a server component blanks the "
        "page."
    )


def test_congress_specifically_is_authenticated_and_guarded():
    """The page this whole file exists because of."""
    src = (APP / "congress" / "page.tsx").read_text()
    assert "fetchAPIAuth" in src, "congress must send the Clerk token"
    assert re.search(r"\bfetchAPI<", src) is None, (
        "congress is calling the unauthenticated fetchAPI again"
    )
    assert "UpgradePrompt" in src, (
        "a free user hitting congress should get the upgrade prompt, not an "
        "empty page"
    )


def test_the_onboarding_choice_reaches_the_portfolio():
    """Onboarding ends with router.push(`/portfolio?strategy=...`). The
    component ignored the param, so the one question onboarding asks was
    discarded the moment it was answered."""
    view = (REPO / "frontend" / "src" / "components" / "portfolio-view.tsx").read_text()
    assert "useSearchParams" in view, (
        "portfolio-view no longer reads ?strategy=, so onboarding's selection "
        "is being thrown away again"
    )
    page = (APP / "portfolio" / "page.tsx").read_text()
    assert "Suspense" in page, (
        "useSearchParams needs a Suspense boundary around PortfolioView"
    )
