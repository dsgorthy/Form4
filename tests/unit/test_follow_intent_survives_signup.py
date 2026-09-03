"""The promise a landing page makes must survive account creation.

WHAT WENT WRONG

FollowCta told anonymous visitors "get alerted the next time Erez Chimovits
files" and linked to a bare `/sign-up`:

    const href = isSignedIn ? "/pricing" : "/sign-up";

No redirect, no entity, no pending action. So the journey was: land from
Google, read the promise, click, create an account, get pushed to /onboarding
by onboarding-guard -- and the insider is gone. They asked for one specific
thing and received a questionnaire.

Whatever the sign-up rate, the retention value of those accounts was near zero,
because the reason each was created was never acted on.

Entity pages are where this matters: over 90 days, filing pages drew 57
visitors, insider pages 25 and company pages 24 -- 106 against the homepage's
69 -- while /pricing drew 2.

THE PROPERTIES

  1. The CTA carries a stable identifier, not just a display name.
  2. It carries a return path, so the visitor comes back to the page that made
     the promise.
  3. Something completes the follow on return.
  4. Onboarding does not stand in front of it.
  5. A signed-in FREE account is not asked for money on an entity page. They
     already have following; Pro is pitched later, by email, once there is a
     relationship.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CTA = REPO / "frontend/src/components/follow-cta.tsx"
PENDING = REPO / "frontend/src/components/pending-follow.tsx"
GUARD = REPO / "frontend/src/components/onboarding-guard.tsx"
INSIDER = REPO / "frontend/src/app/insider/[id]/page.tsx"
FILING = REPO / "frontend/src/app/filing/[id]/page.tsx"


def test_the_cta_carries_the_follow_target_through_signup():
    src = CTA.read_text(encoding="utf-8")
    assert "follow=" in src, (
        "the sign-up link no longer carries the follow target, so the promise "
        "is discarded at account creation"
    )
    assert "next=" in src, (
        "no return path: the visitor lands somewhere generic instead of back "
        "on the page that made the promise"
    )


def test_entity_pages_pass_a_stable_identifier():
    """`entity` is a display string. A follow needs an id."""
    assert 'kind: "insider"' in INSIDER.read_text(encoding="utf-8"), \
        "the insider page passes no insider_id, so the follow cannot be made"
    assert 'kind: "ticker"' in FILING.read_text(encoding="utf-8"), \
        "the filing page passes no ticker, so the follow cannot be made"


def test_something_completes_the_follow_on_return():
    src = PENDING.read_text(encoding="utf-8")
    assert "watchlist" in src and "POST" in src, \
        "nothing actually performs the follow when the visitor returns"
    assert "follow_completed" in src, (
        "the completion is not instrumented, so we cannot tell a kept promise "
        "from a broken one"
    )


def test_the_parameter_is_stripped_after_use():
    """A refresh or a shared link must not re-follow or re-congratulate."""
    src = PENDING.read_text(encoding="utf-8")
    assert 'delete("follow")' in src


def test_onboarding_yields_to_a_pending_follow():
    src = GUARD.read_text(encoding="utf-8")
    assert '"follow"' in src or "'follow'" in src, (
        "onboarding-guard still redirects during a pending follow, which "
        "discards the only thing the visitor asked for"
    )


def test_the_impression_hook_precedes_every_return():
    """Rules of hooks: this component returns null before the CTA is built."""
    lines = CTA.read_text(encoding="utf-8").splitlines()
    body = next(i for i, l in enumerate(lines) if "export function FollowCta" in l)
    effect = next(i for i, l in enumerate(lines) if i > body and "useEffect(" in l)
    first_return = next(i for i, l in enumerate(lines)
                        if i > body and re.match(r"\s{2}(if .*)?return[\s(;<]", l))
    assert effect < first_return, (
        f"useEffect (line {effect+1}) is called after a return (line "
        f"{first_return+1}); hook order would change as Clerk resolves"
    )


def test_signed_in_free_accounts_are_not_sent_to_pricing_when_they_can_follow():
    """They already have following. Selling them what they have is noise, and
    the relationship is too thin to pitch Pro on an entity page."""
    src = CTA.read_text(encoding="utf-8")
    href = src[src.index("const href ="):]
    href = href[:href.index(";")]
    assert "token" in href, (
        "the signed-in branch ignores the follow target and sends every free "
        "account to /pricing"
    )
