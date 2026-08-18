"""Following a company or insider must survive the trial expiring.

Watchlists and alerts were `require_pro`, so the 7-day trial took them with it
when it lapsed — the one feature that brings a visitor back was the one that
expired. Measured 2026-08-18: 47,000 entity page views a week, every one ending
in "follow this company and hear about the next filing", against six accounts
and one watchlist.

These tests pin the boundary in both directions, because the failure mode is
someone re-gating it for a plausible-sounding reason:

    free account  -> CAN follow, CANNOT see the analytical layer
    anonymous     -> can do neither
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="API deps live in the Docker image")

from fastapi import HTTPException  # noqa: E402

from api.auth import ANONYMOUS, UserContext  # noqa: E402
from api.gating import require_auth, require_pro, require_pro_plus  # noqa: E402
from api.routers.notifications import (  # noqa: E402
    WATCHLIST_LIMIT_FREE,
    WATCHLIST_LIMIT_PRO,
)

FREE = UserContext(user_id="user_free", tier="free")
TRIAL = UserContext(user_id="user_trial", tier="trial", trial_days_left=5)
GRACE = UserContext(user_id="user_grace", tier="grace", grace_days_left=3)
PRO = UserContext(user_id="user_pro", tier="pro")


# ── following is free, after auth ───────────────────────────────────────────

@pytest.mark.parametrize("user", [FREE, TRIAL, GRACE, PRO], ids=lambda u: u.tier)
def test_every_signed_in_tier_can_follow(user):
    assert require_auth(user) is user


def test_anonymous_cannot_follow():
    """401 rather than 403: there is nothing to buy, they just aren't signed in."""
    with pytest.raises(HTTPException) as exc:
        require_auth(ANONYMOUS)
    assert exc.value.status_code == 401


def test_a_user_id_is_what_require_auth_actually_needs():
    """A watchlist has to hang off something. Tier is irrelevant here."""
    with pytest.raises(HTTPException):
        require_auth(UserContext(user_id=None, tier="pro"))


# ── but the analytical layer is still the paid product ──────────────────────

def test_free_account_still_cannot_reach_the_analysis():
    """The whole point of giving the alert away is that Pro keeps selling."""
    with pytest.raises(HTTPException) as exc:
        require_pro(FREE)
    assert exc.value.status_code == 403


def test_grace_is_not_pro():
    """has_full_feed is true for grace; is_pro is not. Don't conflate them."""
    assert GRACE.has_full_feed
    assert not GRACE.is_pro
    with pytest.raises(HTTPException):
        require_pro(GRACE)


def test_trial_is_pro_while_it_lasts():
    assert require_pro(TRIAL) is TRIAL


def test_research_tools_stay_pro_plus():
    with pytest.raises(HTTPException):
        require_pro_plus(PRO)


# ── the free cap is a cost guard, not the product boundary ──────────────────

def test_free_cap_is_lower_than_pro_but_usable():
    assert 0 < WATCHLIST_LIMIT_FREE < WATCHLIST_LIMIT_PRO
    # Ten names is a real watchlist. A cap of one or two would make the free
    # tier a demo, which defeats the reason it exists.
    assert WATCHLIST_LIMIT_FREE >= 10


def test_following_creates_the_preferences_row_that_makes_alerts_fire():
    """The link that makes the whole thing work, and the easiest one to lose.

    notification_scanner picks users with `watchlist_activity = 1` out of
    notification_preferences. A follow that does not create that row produces a
    watchlist entry which never alerts — silently, because nothing errors.
    """
    import inspect

    from api.routers import notifications

    src = inspect.getsource(notifications.add_to_watchlist)
    assert "_ensure_preferences" in src, (
        "add_to_watchlist must ensure a preferences row exists, or the follow "
        "never produces an alert"
    )


def test_alerts_are_on_by_default():
    """Defaults have to be opt-out, not opt-in, or a follow does nothing."""
    from api import notifications_db

    schema = notifications_db.SCHEMA
    assert "watchlist_activity INTEGER NOT NULL DEFAULT 1" in schema
    assert "email_enabled INTEGER NOT NULL DEFAULT 1" in schema


def test_notification_routes_do_not_require_pro():
    """Guards against the whole change being quietly reverted.

    Inspects the router rather than a copy of the rule, so adding a new
    notification route with the wrong dependency fails here too.
    """
    from api.routers import notifications

    paid = {"require_pro", "require_pro_plus"}
    offenders = []
    for route in notifications.router.routes:
        dependant = getattr(route, "dependant", None)
        deps = dependant.dependencies if dependant else []
        names = {getattr(d.call, "__name__", "") for d in deps}
        if names & paid:
            offenders.append(f"{route.path} {sorted(names & paid)}")

    assert not offenders, (
        f"notification routes must not require a subscription: {offenders}"
    )
