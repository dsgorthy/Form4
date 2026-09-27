"""A watchlist filing under the user's floor lands in the FEED but not the INBOX.

`min_trade_value` was consulted by `high_value_filing` and later by
`activity_spike` — whose comment reads "the user's own floor, which this path
ignored completely... someone who set min_trade_value to $1M was still being sent
$40k spikes". Nobody came back for `scan_watchlist_activity`. Measured 2026-09-26
on the only real user the product had: emailed "Watchlist: LUCK — President and
CFO bought $2,119" against a $100,000 floor, 47x under it, unopened.

The line is drawn at the EMAIL, not the notification, because $100,000 is the
schema default and five of six users carry exactly it — suppressing the feed row
would silence real activity on a deliberately-followed ticker on the strength of
a number the user never picked. Feed pulled and generous, inbox pushed and
scarce.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCANNER = REPO / "pipelines" / "notification_scanner.py"


def _fn(name: str) -> str:
    src = SCANNER.read_text()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == name)
    return ast.get_source_segment(src, fn)


def _decide(pref, value):
    import importlib.util
    spec = importlib.util.spec_from_file_location("_ns_probe", SCANNER)
    # Importing the module pulls DB and mail config; the predicate is pure, so
    # exec just its source instead.
    ns: dict = {}
    exec(_fn("_watchlist_email_worth_sending"), ns)
    return ns["_watchlist_email_worth_sending"](pref, value)


FLOOR = {"min_trade_value": 100_000, "watchlist_all_filings": 0}


@pytest.mark.parametrize("value,expected", [
    (2_119, False),        # the alert that was actually sent
    (99_999.99, False),
    (100_000, True),       # at the floor, not under it
    (401_958, True),
    (26_393_723, True),
])
def test_the_floor_decides_whether_the_email_is_sent(value, expected):
    assert _decide(FLOOR, value) is expected


def test_all_filings_opt_out_still_emails_everything():
    assert _decide({"min_trade_value": 100_000, "watchlist_all_filings": 1}, 2_119)


def test_no_floor_emails_everything():
    assert _decide({"min_trade_value": 0, "watchlist_all_filings": 0}, 1)
    assert _decide({"min_trade_value": None, "watchlist_all_filings": 0}, 1)


def test_unknown_value_fails_open():
    """A filing reported in shares has no dollar value to compare. This module's
    stated preference is one extra email over silencing someone who pays."""
    assert _decide(FLOOR, None) is True


def test_the_notification_itself_is_never_suppressed():
    """The predicate must gate the EMAIL only. If it ever moves above
    `_insert_notification`, a followed ticker goes quiet in the feed too."""
    body = _fn("scan_watchlist_activity")
    insert_at = body.index("_insert_notification(")
    gate_at = body.index("_watchlist_email_worth_sending(")
    assert gate_at > insert_at, (
        "the floor is being checked before the notification is inserted, so a "
        "sub-threshold filing would vanish from the feed as well as the inbox"
    )
    assert "_try_send_realtime" in body[gate_at:gate_at + 400], (
        "the floor check is no longer guarding the email send"
    )


def test_preferences_are_not_refetched_per_notification():
    """They arrive with the subscriptions. The old shape queried
    notification_preferences once per notification inserted."""
    body = _fn("scan_watchlist_activity")
    assert "prefs_by_user" in body
    assert "SELECT email_enabled, email_frequency FROM" not in body, (
        "the per-notification preference query is back"
    )
