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

def test_free_cap_is_usable_and_pro_is_unlimited():
    # Ten names is a real watchlist. A cap of one or two would make the free
    # tier a demo, which defeats the reason it exists.
    assert WATCHLIST_LIMIT_FREE >= 10
    # None means unlimited, from 2026-08-24. A number here -- any number --
    # means someone reintroduced a Pro ceiling.
    assert WATCHLIST_LIMIT_PRO is None


def test_neither_enforcement_site_compares_against_an_unlimited_cap():
    """`count >= None` raises TypeError, and it raises inside the follow
    handler, so a Pro user would get a 500 on their FIRST follow rather than
    an unlimited watchlist. Both call sites must guard on None."""
    import ast, inspect
    from api.routers import notifications as N

    src = inspect.getsource(N)
    tree = ast.parse(src)
    sites = [n for n in ast.walk(tree)
             if isinstance(n, ast.Compare)
             and ast.unparse(n).startswith("count >= limit")]
    assert len(sites) == 2, f"expected 2 cap checks, found {len(sites)}"
    for cmp_node in sites:
        parent = next(n for n in ast.walk(tree)
                      if isinstance(n, ast.If) and cmp_node in ast.walk(n.test))
        guard = ast.unparse(parent.test)
        assert "is not None" in guard, (
            f"cap check is unguarded and will TypeError for Pro: {guard}")


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


# ── the event is free, the judgment is paid ─────────────────────────────────

def test_the_alert_line_is_an_allowlist():
    """A new preference field is Pro until someone deliberately frees it."""
    from api.gating import FREE_ALERT_FIELDS, PRO_ALERT_FIELDS

    assert not (FREE_ALERT_FIELDS & PRO_ALERT_FIELDS), "a field cannot be both"
    # The whole point: quality filtering is not free.
    assert "min_insider_tier" in PRO_ALERT_FIELDS
    # ...but being told about a company you follow is.
    assert "watchlist_activity" in FREE_ALERT_FIELDS
    # A dollar threshold is a fact about the filing, not a view about it, and
    # narrowing your own alerts costs us less mail rather than more.
    assert "min_trade_value" in FREE_ALERT_FIELDS


def test_every_computed_alert_type_is_pro():
    from api.gating import PRO_ALERT_EVENTS

    for algorithmic in ("cluster_formation", "activity_spike",
                        "congress_convergence", "portfolio_alert"):
        assert algorithmic in PRO_ALERT_EVENTS


def test_free_account_cannot_enable_a_pro_alert():
    """Guards the write path. Checked by reading the route, not re-stating it."""
    import inspect

    from api.routers import notifications

    src = inspect.getsource(notifications.update_preferences)
    assert "PRO_ALERT_FIELDS" in src
    assert "is_pro" in src


def test_turning_a_pro_alert_off_is_always_allowed():
    """A lapsed subscriber must be able to silence what they can no longer get."""
    import inspect

    from api.routers import notifications

    src = inspect.getsource(notifications.update_preferences)
    assert "(False, 0, None)" in src, (
        "the check must exempt falsy values, or a lapsed user is trapped with "
        "alerts they cannot disable"
    )


# ── the pricing page is marketing copy for a real gate ──────────────────────

PRICING = (
    __import__("pathlib").Path(__file__).resolve().parents[2]
    / "frontend/src/app/pricing/page.tsx"
)


def test_pricing_copy_matches_the_actual_follow_limits():
    """The gate and the claim about the gate must agree.

    This exact drift has happened before on this page -- FREE advertised
    "Congress trades" while the endpoint required Pro+ -- and the fix was to
    move the code, not the copy. Same rule here: if this fails, decide which
    of the two is wrong before editing either.
    """
    copy = PRICING.read_text()
    free_block = copy[copy.index("FREE_FEATURES"):copy.index("PRO_FEATURES")]
    pro_block = copy[copy.index("PRO_FEATURES"):copy.index("PRO_PLUS_FEATURES")]

    # Strip comments -- they cite the numbers in order to explain them, and
    # matching the explanation instead of the copy is how this test would
    # pass against a page that says the wrong thing.
    def strip(b):
        return "\n".join(l for l in b.splitlines()
                         if not l.strip().startswith("//"))

    free_block, pro_block = strip(free_block), strip(pro_block)

    assert str(WATCHLIST_LIMIT_FREE) in free_block, (
        f"free tier enforces {WATCHLIST_LIMIT_FREE} but the page does not "
        f"say so")

    if WATCHLIST_LIMIT_PRO is None:
        assert "unlimited" in pro_block.lower(), (
            "Pro follows are unlimited and that is a selling point the page "
            "does not mention")
    else:
        assert str(WATCHLIST_LIMIT_PRO) in pro_block, (
            f"Pro is capped at {WATCHLIST_LIMIT_PRO} and the page must say so")
