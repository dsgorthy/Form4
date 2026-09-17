"""The account email sequence talks to a free account, not a lapsing trial.

Until 2026-09-17 it was six emails -- "your trial starts now", value, "2 days
left on your trial", "your trial has ended", "your grace period has ended",
win-back -- sent every six hours to everyone who signed up, because every
account was a trial by age. 41 went out. Accounts are free now; these pin
what the four remaining emails say, when they go, and to whom.
"""
from __future__ import annotations

import importlib.util
import re
from datetime import datetime, timedelta
from pathlib import Path

from api import email_templates as t

_SPEC = importlib.util.spec_from_file_location(
    "trial_emails", Path(__file__).resolve().parents[2] / "pipelines" / "trial_emails.py",
)
pipe = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pipe)

BANNED = ("your trial", "trial has ended", "grace period", "Pro access expires", "days left",
          "What you'll lose", "hard gate", "!")


def _text(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html)


def test_the_sequence_is_four_emails_and_none_is_a_countdown():
    assert [n for n, _ in t.EMAIL_SEQUENCE] == ["welcome", "your_week", "pro_once", "win_back"]
    assert dict(t.EMAIL_SEQUENCE) == {"welcome": 0, "your_week": 3, "pro_once": 10, "win_back": 30}
    for gone in ("urgency_email", "trial_ended_email", "hard_gate_email", "value_email"):
        assert not hasattr(t, gone), f"{gone} is back"


def test_every_email_reads_as_a_free_account_and_never_as_a_trial():
    rendered = [
        t.welcome_email(["NVDA", "Tim Cook"], "A-List Buys"),
        t.welcome_email([], None),
        t.your_week_email([], following=True),
        t.your_week_email([{"ticker": "RWT", "insider_name": "Dashiell I. Robinson", "trade_type": "buy",
                            "value": 252000, "filing_date": "2026-09-16"}], following=False),
        t.pro_once_email(),
        t.win_back_email([], following=False),
    ]
    for subject, html in rendered:
        body = _text(html)
        for b in BANNED:
            assert b.lower() not in (subject + " " + body).lower(), f"{b!r} in {subject!r}"


def test_welcome_says_free_and_names_who_they_follow():
    subject, html = t.welcome_email(["NVDA", "Tim Cook"], "A-List Buys")
    body = _text(html)
    assert "free, and it stays free" in body
    assert "NVDA, Tim Cook" in body
    assert "A-List Buys" in body
    assert "/feed" in html
    subject, html = t.welcome_email([], None)
    assert "not following anyone yet" in _text(html)
    assert "/explore" in html


def test_pro_is_mentioned_once_and_only_as_a_choice():
    subject, html = t.pro_once_email()
    body = _text(html)
    assert "7-day free trial" in body and "nothing is charged until it ends" in body
    assert "only time we will bring Pro up" in body
    assert "/pricing" in html
    # the welcome mentions Pro exists, in one sentence, without a link to it
    _, welcome = t.welcome_email([], None)
    assert "/pricing" not in welcome


def test_win_back_goes_only_to_an_account_that_has_gone_quiet():
    now = datetime(2026, 9, 17, 12, 0, 0)
    ms = lambda d: int((now - timedelta(days=d)).timestamp() * 1000)  # noqa: E731
    assert pipe.win_back_due(ms(15), now)
    assert pipe.win_back_due(None, now)
    assert not pipe.win_back_due(ms(3), now)
    assert not pipe.win_back_due(ms(13), now)


def test_paying_and_comped_accounts_get_nothing():
    assert pipe.is_paying({"tier": "pro"})
    assert pipe.is_paying({"tier": "pro_plus"})
    future = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")
    past = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    assert pipe.is_paying({"tier": "pro", "pro_until": future})
    assert not pipe.is_paying({"tier": "pro", "pro_until": past})
    assert not pipe.is_paying({})


def test_windows_are_a_day_wide_with_a_days_drift():
    assert pipe.in_window(0.0, 0) and pipe.in_window(1.4, 0)
    assert not pipe.in_window(2.0, 0)
    assert pipe.in_window(3.2, 3) and not pipe.in_window(2.4, 3)


def test_followed_filings_use_the_one_definition_of_meaningful():
    src = Path(pipe.__file__).read_text(encoding="utf-8")
    assert "MEANINGFUL_CLASSES" in src
    assert "trans_code IN ('P', 'S')" not in src, "the old signals query typed its own definition"
