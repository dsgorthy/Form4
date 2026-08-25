"""The email queue must be safe to leave broken.

On 2026-08-24 there were 6,890 unsent notifications, the oldest from March,
92% of them activity_spike -- one user had 5,114. When the underlying Clerk
credential bug was fixed, the natural next step was "schedule the digest",
and that would have emailed five-month-old alerts and then marked the entire
backlog delivered.

The fix is not a better threshold. It is that the queue cannot accumulate:

  1. relevance at write time  -- don't create what nobody wants
  2. the queue expires        -- stale items are never emailed, ever
  3. the digest is bounded    -- and says what it left out
  4. backpressure is loud     -- a growing queue is a config error

Each is tested here, because each one alone still permits the failure.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCANNER = REPO / "pipelines/notification_scanner.py"
EMAIL = REPO / "api/email.py"


def _spike_code() -> str:
    """scan_activity_spikes with its docstring and comments removed.

    The docstring explaining this fix names `is_routine` in order to say why
    it was wrong. Scanning the raw source matches the explanation and passes
    against a detector that still gates on it -- which is precisely the
    mistake this file exists to stop being repeated.
    """
    tree = ast.parse(SCANNER.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "scan_activity_spikes")
    body = fn.body[1:] if (fn.body and isinstance(fn.body[0], ast.Expr)
                           and isinstance(fn.body[0].value, ast.Constant)) else fn.body
    return "\n".join(ast.unparse(n) for n in body)


# ── 1. relevance at write time ──────────────────────────────────────────────

def test_the_spike_detector_has_absolute_floors():
    """A ratio with a near-zero denominator is not a signal.

    Live examples included "ACHR sell at 989.7x baseline", which is a small
    number divided by a smaller one.
    """
    import pipelines.notification_scanner as NS
    assert NS.SPIKE_MIN_VALUE >= 100_000, "a dollar floor, not a token one"
    assert NS.SPIKE_MIN_INSIDERS >= 2, "one person is not a spike"
    body = _spike_code()
    for name in ("SPIKE_MIN_VALUE", "SPIKE_MIN_INSIDERS", "SPIKE_MIN_RATIO"):
        assert name in body, f"{name} is defined but not applied"


def test_the_spike_detector_uses_signal_class_not_is_routine():
    """`is_routine` is barely populated; signal_class is the definition."""
    body = _spike_code()
    assert "is_routine" not in body, (
        "activity_spike still gates on is_routine, which is why it fired 159 "
        "times a day")
    assert "MEANINGFUL_CLASSES" in body or "{meaningful}" in body


def test_the_meaningful_list_is_not_typed_out():
    """Built from the constant, never hardcoded -- the same rule every other
    surface follows."""
    src = SCANNER.read_text()
    code = "\n".join(l for l in src.splitlines()
                     if not l.strip().startswith("#"))
    assert "'discretionary_buy', 'discretionary_sell'" not in code, (
        "the meaningful class list is hardcoded; derive it from "
        "MEANINGFUL_CLASSES")


def test_the_spike_path_honours_the_users_own_threshold():
    """Only high_value_filing consulted min_trade_value. Someone who set it
    to $1M was still sent $40k spikes."""
    assert "min_trade_value" in _spike_code()


# ── 2. the queue expires ────────────────────────────────────────────────────

def test_there_is_a_ttl_and_it_is_short():
    import pipelines.notification_scanner as NS
    assert 1 <= NS.EMAIL_TTL_DAYS <= 7, (
        f"a {NS.EMAIL_TTL_DAYS}-day TTL is long enough to re-create the "
        f"problem it exists to prevent")


def test_expired_is_distinct_from_sent():
    """"We chose not to email this" and "we emailed this" are different
    facts, and the difference is what makes the backlog auditable."""
    import pipelines.notification_scanner as NS
    assert NS.EMAIL_EXPIRED not in (NS.EMAIL_SENT, NS.EMAIL_PENDING)


def test_the_digest_expires_before_it_composes():
    """Order matters. Expiring afterwards would email the stale items once
    and only then age them out -- which is the exact failure."""
    tree = ast.parse(SCANNER.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "send_daily_digests")
    body = ast.unparse(fn)
    assert "expire_stale_notifications" in body
    assert body.index("expire_stale_notifications") < body.index("_get_user_email"), (
        "the digest reads addresses before expiring the queue")


def test_expiry_uses_the_database_clock():
    """created_at is TEXT written by NOW(), so it carries the server offset.
    Comparing it to a string built from a Python UTC datetime is wrong by
    that offset."""
    src = SCANNER.read_text()
    fn = src[src.index("def expire_stale_notifications"):]
    fn = fn[:fn.index("\ndef ", 1)]
    assert "NOW()" in fn, "expiry must compare against the DB clock"


# ── 3. the digest is bounded and honest ─────────────────────────────────────

def test_the_digest_is_bounded():
    import pipelines.notification_scanner as NS
    assert 1 <= NS.DIGEST_MAX_ITEMS <= 30


def test_the_digest_reports_what_it_omitted():
    from api.email import build_digest_email
    html = build_digest_email([{"title": "t", "body": "b"}], overflow=41)
    assert "41 more" in html, "overflow is dropped silently"
    assert "42 new alerts" in html, "the header undercounts the real total"


def test_nothing_is_marked_sent_before_the_send_succeeds():
    """The old digest marked on the way in, so a Resend 5xx lost the lot."""
    src = SCANNER.read_text()
    fn = src[src.index("def send_daily_digests"):]
    fn = fn[:fn.index("\ndef ", 1)]
    assert fn.index("send_email(") < fn.index("SET emailed"), (
        "notifications are marked emailed before the send is confirmed")


# ── 4. backpressure is loud ─────────────────────────────────────────────────

def test_backpressure_is_reported_at_error():
    src = SCANNER.read_text()
    fn = src[src.index("def report_backpressure"):]
    fn = fn[:fn.index("\ndef ", 1)]
    assert "logger.error" in fn, (
        "a queue growing past the threshold must be an ERROR -- it is a "
        "configuration fault, and an INFO line is what let 5,114 accumulate")


def test_the_digest_checks_backpressure():
    tree = ast.parse(SCANNER.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "send_daily_digests")
    assert "report_backpressure" in ast.unparse(fn)


# ── scheduling ──────────────────────────────────────────────────────────────

def test_the_digest_is_scheduled_in_dagster_not_launchd():
    """Scheduling lives in Dagster. A new plist is not the answer."""
    defs = (REPO / "dataplane/dagster_project/definitions.py").read_text()
    assert "form4_alerts_schedule" in defs
    assert "form4_alerts_job" in defs
    assets = (REPO / "dataplane/dagster_project/assets/form4_pipeline.py").read_text()
    assert '"--digest"' in assets, (
        "the digest asset must actually pass --digest; without it "
        "send_daily_digests never runs, which is why no email ever went out")
    plists = list(REPO.glob("com.openclaw.*digest*.plist"))
    assert not plists, f"digest scheduled via launchd: {plists}"
