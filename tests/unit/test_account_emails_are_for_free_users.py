"""The account email sequence talks to a free account, not a lapsing trial.

Until 2026-09-17 it was six emails -- "your trial starts now", value, "2 days
left on your trial", "your trial has ended", "your grace period has ended",
win-back -- sent every six hours to everyone who signed up, because every
account was a trial by age. 41 went out. Accounts are free now; these pin
what the emails say, when they go, and to whom.
"""
from __future__ import annotations

import importlib.util
import re
from datetime import datetime, timedelta
from pathlib import Path

from api import email_templates as t
from api import pricing

REPO = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "trial_emails", REPO / "pipelines" / "trial_emails.py",
)
pipe = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pipe)

BANNED = ("your trial", "trial has ended", "grace period", "Pro access expires", "days left",
          "What you'll lose", "hard gate", "!")


def _text(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html)


def test_the_sequence_is_five_emails_and_none_is_a_countdown():
    # The day-1 founder note joined 2026-11-19: one plain-text question from Derek.
    assert [n for n, _ in t.EMAIL_SEQUENCE] == ["welcome", "founder_note", "your_week", "pro_once", "win_back"]
    assert dict(t.EMAIL_SEQUENCE) == {"welcome": 0, "founder_note": 1, "your_week": 3, "pro_once": 10,
                                      "win_back": 30}
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


def test_the_pro_email_lists_what_you_get_and_ends_on_one_offer():
    """Rewritten 2026-11-23 (Derek: "sounds super AI, no clear CTA"): the
    benefits as a list, the price and trial terms, one button to /pricing."""
    subject, html = t.pro_once_email()
    body = _text(html)
    assert subject == t.PRO_EMAIL_SUBJECT
    for title, line in t.PRO_BENEFITS:
        assert title in body and line in body
    assert t.pro_terms() in body
    assert f"the first {pricing.PRO_TRIAL_DAYS} days are free" in body
    assert "won't be charged until the trial ends" in body
    assert f'href="{t.APP_URL}/pricing"' in html
    assert f"Try Pro free for {pricing.PRO_TRIAL_DAYS} days" in body
    # the welcome mentions Pro exists, in one sentence, without a link to it
    _, welcome = t.welcome_email([], None)
    assert "/pricing" not in welcome


def test_the_pro_email_copy_follows_dereks_rules():
    """Nothing that reads as AI: plain sentences, no em-dash joins, no
    "Unlock", no exclamation marks. The layout's footer is not copy."""
    copy = [t.PRO_EMAIL_SUBJECT, t.pro_terms()] + [s for pair in t.PRO_BENEFITS for s in pair]
    for s in copy:
        for bad in ("—", "&mdash;", " -- ", "Unlock", "!"):
            assert bad not in s, f"{bad!r} in {s!r}"


def test_every_pro_benefit_the_email_sells_is_a_real_gate():
    """If a feature moves to Free, this fails instead of the email going on
    selling it."""
    from api.notification_policy import is_direct
    from api.public_fields import PRO_ALERT_EVENTS

    # "Get an email when one of our strategies buys or sells": Pro only, sent at once.
    assert "portfolio_alert" in PRO_ALERT_EVENTS and is_direct("portfolio_alert")
    # Clusters, spikes and Congress convergence are Pro-only alerts.
    for event in ("cluster_formation", "activity_spike", "congress_convergence"):
        assert event in PRO_ALERT_EVENTS, event
    # "Free accounts stop at 10 of each", and Pro has no cap.
    src = (REPO / "api/routers/notifications.py").read_text()
    free = int(re.search(r"^WATCHLIST_LIMIT_FREE = (\d+)", src, re.M).group(1))
    assert re.search(r"^WATCHLIST_LIMIT_PRO = None", src, re.M)
    assert f"stop at {free} of each" in dict(t.PRO_BENEFITS)["Unlimited follows"]
    # The pricing page sells the same things as Pro.
    page = (REPO / "frontend/src/app/pricing/page.tsx").read_text()
    pro = page[page.index("const PRO_FEATURES"):page.index("const PRO_PLUS_FEATURES")]
    for feature in ("Follow unlimited", "Insider grades & full track records", "Filter any feed by grade",
                    "Real-time portfolio", "Trade entry/exit alerts", "Screener & leaderboard",
                    "Clusters, spikes & sell-cessation", "Congress trades"):
        assert feature in pro, f"the pricing page no longer sells {feature!r} as Pro, and this email still does"


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


def test_the_email_fallback_uses_the_same_foreign_price_rule_as_the_posts():
    """One definition: pipelines.generate_stocktwits_posts.price_is_foreign."""
    src = Path(pipe.__file__).read_text(encoding="utf-8")
    assert "from pipelines.generate_stocktwits_posts import price_is_foreign" in src
    assert src.count("return _drop_foreign_priced(conn") == 2, "both filing queries must pass through the guard"


class _Conn:
    def __init__(self, closes): self.closes = closes
    def execute(self, sql, params):
        rows = [{"ticker": t, "close": c} for t, c in self.closes.items() if t in params[0]]
        class R:
            def __init__(s, rows): s.rows = rows
            def fetchall(s): return s.rows
        return R(rows)


def test_the_umc_filing_is_dropped_from_an_email_and_a_domestic_one_kept():
    rows = [
        {"ticker": "UMC", "insider_name": "Chitung Liu", "trade_type": "sell", "value": 228_799_600, "qty": 1_600_000},
        {"ticker": "STX", "insider_name": "William D. Mosley", "trade_type": "sell", "value": 77_045_215, "qty": 100_000},
    ]
    kept = pipe._drop_foreign_priced(_Conn({"UMC": 22.54, "STX": 770.45}), rows)
    assert [r["ticker"] for r in kept] == ["STX"]
