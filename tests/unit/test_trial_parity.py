"""The trial is computed in two languages. They have to agree.

HOW THE TRIAL WORKS

There is no grant and no record. `api/auth.py` derives the tier from the Clerk
account's `created_at` on every request: <= TRIAL_DAYS old is `trial`, up to
TRIAL_DAYS + GRACE_DAYS is `grace`, older is `free`. Nothing is written, so
nothing can be extended, revoked, or re-granted, and changing TRIAL_DAYS
silently rewrites the terms of every trial already running.

`frontend/src/lib/subscription.ts` recomputes the same thing so the banner can
render without a round-trip. Two implementations of one rule is the drift this
file exists to catch — and they already disagreed when it was written:

    server:  max(1, int(TRIAL_DAYS - age + 0.5))     # rounds
    client:  Math.max(1, Math.ceil(TRIAL_DAYS - age)) # ceils

Half a day into the trial the banner says 7 and the server says 6. Nobody is
harmed by that one, but it is the same shape as a disagreement about whether
the trial has ENDED, which decides whether a paying-adjacent user gets a 403.

These tests pin the constants and the boundaries. They do not pin the rounding,
because the two are genuinely different functions — that is recorded as a known
divergence below rather than asserted away.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TS = REPO / "frontend" / "src" / "lib" / "subscription.ts"
PY = REPO / "api" / "auth.py"


def _ts_const(name: str) -> int:
    m = re.search(rf"const {name}\s*=\s*(\d+)\s*;", TS.read_text())
    assert m, f"{name} not found in subscription.ts"
    return int(m.group(1))


def _py_const(name: str) -> int:
    m = re.search(rf"^{name}\s*=\s*(\d+)", PY.read_text(), re.M)
    assert m, f"{name} not found in auth.py"
    return int(m.group(1))


@pytest.mark.parametrize("name", ["TRIAL_DAYS", "GRACE_DAYS"])
def test_the_two_implementations_use_the_same_window(name):
    """A mismatch means the banner and the gate disagree about who is entitled
    to what — the client would offer access the server refuses, or the reverse."""
    assert _ts_const(name) == _py_const(name), (
        f"{name} is {_ts_const(name)} in subscription.ts and "
        f"{_py_const(name)} in auth.py. The banner and the API would disagree "
        "about whether a user still has access."
    )


def test_the_window_is_what_the_product_promises():
    """Onboarding, the banner and the email ladder all say seven days. The
    email ladder in pipelines/trial_emails.py is built around day 3, 5, 7, 14 —
    changing this without changing those sends '2 days left' at the wrong time."""
    assert _py_const("TRIAL_DAYS") == 7
    assert _py_const("GRACE_DAYS") == 7


def test_the_trial_is_still_derived_from_account_age():
    """Documents the mechanism, and fails loudly if it changes.

    If the trial ever becomes an explicit grant — a `trial_ends_at` written
    once at signup, which is the recommended direction — this test should be
    rewritten rather than deleted, because the thing worth pinning is that ONE
    mechanism decides it, not that the mechanism is this one.
    """
    src = PY.read_text()
    assert "_created_at" in src, (
        "auth.py no longer derives the trial from Clerk's created_at. If the "
        "grant is now explicit, update this test to pin the new field — and "
        "check subscription.ts reads the same one."
    )
    ts = TS.read_text()
    assert "createdAt" in ts, (
        "subscription.ts no longer reads createdAt while auth.py still does — "
        "the two are now computing the trial from different inputs."
    )


def test_a_missing_creation_date_is_not_a_trial():
    """_fetch_clerk_metadata returns {} on any failure, so `_created_at` can be
    absent. That must fall through to the stored tier, never grant Pro."""
    src = PY.read_text()
    m = re.search(r"created_at = metadata\.get\(\"_created_at\"\)\s*\n\s*if created_at:", src)
    assert m, (
        "the trial branch is no longer guarded by `if created_at:`. Without "
        "that guard, a failed Clerk lookup would decide entitlement."
    )
