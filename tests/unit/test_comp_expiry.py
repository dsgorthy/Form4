"""Comped-Pro expiry — `public_metadata.pro_until` on the Clerk user.

Clerk is the system of record for tier; there is no subscriptions table.
`pro_until` is written by hand when comping an account (see
scripts/comp_user.py). Stripe never writes it, so paid subscribers must be
unaffected by every one of these paths.
"""
from datetime import datetime, timedelta, timezone

import pytest

from api.comp import comp_lapsed


def _days_out(n: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=n)).strftime("%Y-%m-%d")


# ── paid subscribers carry no pro_until and must never expire ──────────

@pytest.mark.parametrize("meta", [
    {},
    {"tier": "pro"},
    {"tier": "pro", "stripe_customer_id": "cus_abc"},
    {"tier": "pro_plus"},
    {"tier": "pro", "pro_until": None},
    {"tier": "pro", "pro_until": ""},
])
def test_no_pro_until_never_lapses(meta):
    assert comp_lapsed(meta) is False


# ── the comp window itself ────────────────────────────────────────────

def test_future_date_is_live():
    assert comp_lapsed({"tier": "pro", "pro_until": _days_out(30)}) is False


def test_past_date_has_lapsed():
    assert comp_lapsed({"tier": "pro", "pro_until": _days_out(-1)}) is True


def test_bare_date_runs_through_end_of_that_day():
    """A comp dated today is still live at 00:01 on that day, not expired."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert comp_lapsed({"tier": "pro", "pro_until": today}) is False


def test_full_iso_timestamp_accepted():
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    assert comp_lapsed({"pro_until": past}) is True
    assert comp_lapsed({"pro_until": future}) is False


def test_z_suffix_and_naive_timestamps_accepted():
    past = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    naive = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")
    assert comp_lapsed({"pro_until": past}) is True
    assert comp_lapsed({"pro_until": naive}) is True


# ── a typo must not revoke access we promised ─────────────────────────

@pytest.mark.parametrize("bad", ["not-a-date", "11/11/2026", "2026-13-45", 12345, [], {}])
def test_unparseable_leaves_access_in_place(bad):
    assert comp_lapsed({"tier": "pro", "pro_until": bad}) is False


# ── the two comped July signups, end to end ───────────────────────────

COMPED = {"tier": "pro", "pro_until": "2026-11-11", "comp_reason": "july-signup-outreach"}


def test_july_comp_shape_is_live_before_expiry_and_free_after(monkeypatch):
    import api.comp as comp

    class _FrozenClock(datetime):
        _now = None

        @classmethod
        def now(cls, tz=None):
            return cls._now

    monkeypatch.setattr(comp, "datetime", _FrozenClock)

    _FrozenClock._now = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
    assert comp_lapsed(COMPED) is False, "comp should be live the day before it ends"

    _FrozenClock._now = datetime(2026, 11, 11, 23, 0, tzinfo=timezone.utc)
    assert comp_lapsed(COMPED) is False, "comp runs through the end of its last day"

    _FrozenClock._now = datetime(2026, 11, 12, 0, 30, tzinfo=timezone.utc)
    assert comp_lapsed(COMPED) is True, "comp should be gone the next morning"


def test_trial_emails_skips_live_comp_and_resumes_after_lapse():
    """A comped user shouldn't get win-back mail; a lapsed one is fair game."""
    live = {"tier": "pro", "pro_until": _days_out(30)}
    lapsed = {"tier": "pro", "pro_until": _days_out(-30)}

    def would_skip(meta):
        return meta.get("tier") in ("pro", "pro_plus") and not comp_lapsed(meta)

    assert would_skip(live) is True
    assert would_skip(lapsed) is False
    assert would_skip({"tier": "pro"}) is True      # paying customer
    assert would_skip({}) is False                   # free user
