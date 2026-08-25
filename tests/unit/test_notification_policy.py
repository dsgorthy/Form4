"""Email is scarce; the in-app feed is not. That distinction is the design.

WHY THIS EXISTS

`notifications.emailed` fused two different things into one row: the fact
that something happened and is worth showing (the feed) and the decision to
interrupt someone about it (the email). Every fix to one broke the other --
DAILY_CAP throttled CREATION to protect the inbox, starving the feed, while
the inbox received nothing at all for five months.

The tiers are set from measured attention, over all 6,920 notifications ever
created:

    high_value_filing        68 rows   57.4% read
    cluster_formation       293 rows   24.9% read
    activity_spike        6,378 rows   12.8% read   <- 92% of everything
    congress_convergence    175 rows   10.3% read

Read rate is inversely proportional to volume. That is the whole argument.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from api.notification_policy import (
    DEFAULT_TIER,
    MAX_EMAILS_PER_USER_PER_DAY,
    TIERS,
    Tier,
    emailable_types,
    is_direct,
    may_email,
    tier_of,
)

REPO = Path(__file__).resolve().parents[2]
SCANNER = REPO / "pipelines/notification_scanner.py"


# ── the policy itself ───────────────────────────────────────────────────────

def test_the_noisiest_types_never_email():
    """activity_spike and congress_convergence are 95% of all volume and
    ~12% of all reads. They are feed material, not mail."""
    for t in ("activity_spike", "congress_convergence"):
        assert not may_email(t), f"{t} can still generate email"


def test_the_product_promise_emails_directly():
    for t in ("portfolio_alert", "watchlist_activity"):
        assert is_direct(t), f"{t} should be a DIRECT tier"


def test_an_unknown_event_type_cannot_email():
    """A new detector is noisy before it is tuned. Earn the email."""
    assert DEFAULT_TIER is Tier.FEED_ONLY
    assert not may_email("some_detector_added_next_month")


def test_emailable_types_is_derived_not_typed():
    assert set(emailable_types()) == {t for t in TIERS if may_email(t)}
    assert "activity_spike" not in emailable_types()


def test_the_daily_email_cap_is_small():
    assert 1 <= MAX_EMAILS_PER_USER_PER_DAY <= 6, (
        "a cap this product would not notice is not a cap")


# ── the scanner honours it ──────────────────────────────────────────────────

def _fn(name: str) -> str:
    tree = ast.parse(SCANNER.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == name)
    body = fn.body[1:] if (fn.body and isinstance(fn.body[0], ast.Expr)
                           and isinstance(fn.body[0].value, ast.Constant)) else fn.body
    return "\n".join(ast.unparse(n) for n in body)


def test_realtime_email_is_gated_on_the_tier():
    body = _fn("_try_send_realtime")
    assert "is_direct" in body, (
        "realtime sends are not tier-gated, so a user on `realtime` gets one "
        "email per activity_spike -- eighty a day")


def test_every_realtime_call_says_what_it_is_sending():
    """An un-typed call defaults to sending, which defeats the gate."""
    tree = ast.parse(SCANNER.read_text())
    bad = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_try_send_realtime":
            if len(n.args) < 5 and not any(k.arg == "event_type" for k in n.keywords):
                bad.append(ast.unparse(n)[:80])
    assert not bad, f"_try_send_realtime called without an event_type: {bad}"


def test_the_digest_only_selects_emailable_types():
    """Assert on the WHERE clause, not on the identifier appearing somewhere.

    The first version checked `"emailable_types" in body`, which stayed true
    when the filter was deleted from the query because the placeholder-building
    line above it still mentioned the function. It passed against the bug.
    """
    body = _fn("send_daily_digests")
    assert "event_type IN" in body, (
        "the digest query does not filter by event_type, so FEED_ONLY types "
        "reach inboxes")
    assert "emailable_types()" in body, (
        "the type list must come from the policy, not be typed out")


def test_the_digest_skips_what_the_user_already_read():
    """Email exists for the ABSENT user. Mailing someone what they read an
    hour ago is the purest form of the spam this is meant to prevent."""
    body = _fn("send_daily_digests")
    assert "is_read = 0" in body


def test_the_send_cap_is_at_the_delivery_layer():
    body = _fn("send_daily_digests")
    assert "MAX_EMAILS_PER_USER_PER_DAY" in body
    # and it must not be the creation-layer cap wearing a new name
    assert "DAILY_CAP" not in body


def test_the_cap_counts_sends_not_notifications():
    """One digest covering forty notifications is ONE email. Counting rows
    would gate the user out permanently after a single send."""
    body = _fn("_emails_sent_today")
    assert "emailed_at" in body, "the cap must count by send time"
    assert "COUNT(DISTINCT" in body


def test_delivery_time_is_recorded():
    body = _fn("send_daily_digests")
    assert "emailed_at = NOW()" in body, (
        "nothing stamps emailed_at, so the cap can never see a send")


def test_the_schema_declares_emailed_at():
    """A fresh database must match the migrated one."""
    schema = (REPO / "api/notifications_db.py").read_text()
    assert "emailed_at" in schema


def test_noisy_scanners_run_last():
    """DAILY_CAP is shared across event types, so whichever scanner runs
    first spends the budget. activity_spike alone would consume it."""
    src = SCANNER.read_text()
    order = []
    for ev in ("portfolio_alert", "watchlist_activity", "high_value_filing",
               "congress_convergence", "cluster_formation", "activity_spike"):
        marker = f'results["{ev}"] ='
        assert marker in src, f"{ev} is not dispatched in _scan"
        order.append((src.index(marker), ev))
    order.sort()
    ran = [ev for _, ev in order]
    assert ran[-1] == "activity_spike", (
        f"activity_spike must run last or it eats the shared daily budget "
        f"before anything anyone reads gets a chance: {ran}")
    assert ran.index("high_value_filing") < ran.index("activity_spike")


# ── deliverability and the law ──────────────────────────────────────────────

def test_the_digest_carries_an_unsubscribe_link():
    """Commercial email must have a working unsubscribe (CAN-SPAM). The
    token machinery existed for months; nothing ever passed it a URL."""
    body = _fn("send_daily_digests")
    assert "unsubscribe_url" in body, "the digest is sent with no unsubscribe"


def test_one_click_unsubscribe_headers_are_sent():
    """Gmail and Yahoo have required RFC 8058 of bulk senders since Feb 2024.
    Without it mail is throttled or spam-foldered whatever the content."""
    email_src = (REPO / "api/email.py").read_text()
    assert "List-Unsubscribe" in email_src
    assert "List-Unsubscribe-Post" in email_src
    assert "One-Click" in email_src


def test_the_unsubscribe_endpoint_accepts_post():
    """The header promises a POST target that acts without confirmation.
    Advertising it against a GET-only route gives the user an Unsubscribe
    button that silently does nothing."""
    src = (REPO / "api/routers/notifications.py").read_text()
    i = src.index("def unsubscribe(")
    decorator = src[max(0, i - 300):i]
    assert "POST" in decorator, (
        "unsubscribe is not reachable by POST, but the mail claims it is")


def test_the_architecture_doc_matches_the_code():
    """The doc states the tiers and caps as fact. If they drift, the doc is
    misinformation about a system people will trust it to describe."""
    doc = (REPO / "docs/notification_architecture.md").read_text()
    import pipelines.notification_scanner as NS

    # Name AND value together. Asserting the bare number is useless: a "4" or
    # a "5" appears in a dozen unrelated places in this document, so the
    # first version of this test passed happily when the cap was changed.
    for name, value in (("MAX_EMAILS_PER_USER_PER_DAY", MAX_EMAILS_PER_USER_PER_DAY),
                        ("EMAIL_TTL_DAYS", NS.EMAIL_TTL_DAYS),
                        ("BACKPRESSURE_THRESHOLD", NS.BACKPRESSURE_THRESHOLD)):
        assert f"{name} = {value}" in doc, (
            f"the doc does not state `{name} = {value}`; it has drifted from "
            f"the code it describes")
    for event_type, tier in TIERS.items():
        # every type named, with its tier somewhere in the same table row
        assert event_type in doc, f"{event_type} is not described in the doc"
    for t in ("activity_spike", "congress_convergence"):
        i = doc.index(t)
        assert "FEED_ONLY" in doc[i:i + 300], (
            f"the doc does not show {t} as FEED_ONLY")


# ── the realtime path is capped too ─────────────────────────────────────────

def test_the_realtime_path_respects_the_same_daily_cap():
    """DIRECT is uncapped otherwise, and DIRECT includes watchlist_activity.

    Pro follows became unlimited on 2026-08-24 and there are ~308 meaningful
    filings a day across ~68 tickers, so a heavy follower on `realtime` would
    have received dozens of separate emails a day. The digest was capped; this
    path was not.
    """
    body = _fn("_try_send_realtime")
    assert "MAX_EMAILS_PER_USER_PER_DAY" in body, (
        "realtime sends are uncapped")


def test_going_over_the_cap_keeps_the_notification_in_the_feed():
    """Withhold the email, not the notification."""
    import pipelines.notification_scanner as NS
    body = _fn("_try_send_realtime")
    # the cap path returns without touching the row's state
    i = body.index("MAX_EMAILS_PER_USER_PER_DAY")
    window = body[i:i + 400]
    assert "return" in window
    assert "DELETE" not in window and "emailed = 1" not in window


def test_a_realtime_send_is_stamped():
    """Unstamped, the cap can never see a realtime send and would only ever
    count digests -- so the ceiling would not bind on the one path that can
    actually flood someone."""
    body = _fn("_try_send_realtime")
    assert "emailed_at = NOW()" in body


def test_every_realtime_call_passes_the_notification_id():
    tree = ast.parse(SCANNER.read_text())
    bad = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_try_send_realtime":
            if len(n.args) < 6 and not any(k.arg == "notification_id" for k in n.keywords):
                bad.append(ast.unparse(n)[:80])
    assert not bad, f"cannot stamp what it sent: {bad}"


def test_the_cap_check_cannot_abort_the_scan():
    """_try_send_realtime exists so one bad delivery does not silence every
    other subscriber. A cap check is a DB query and must sit inside the
    guard, not in front of it."""
    tree = ast.parse(SCANNER.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "_try_send_realtime")
    tries = [n for n in fn.body if isinstance(n, ast.Try)]
    assert tries, "_try_send_realtime has no top-level try"
    guarded = ast.unparse(tries[0])
    assert "MAX_EMAILS_PER_USER_PER_DAY" in guarded, (
        "the cap check is outside the try block")
