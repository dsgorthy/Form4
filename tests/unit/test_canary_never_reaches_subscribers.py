"""A canary position is delivered to the canary user and to nobody else.

scripts/alert_canary.py proves the alert path end to end: it commits a real
execution_source='alert' row to strategy_portfolio, runs scan_portfolio_alerts
with the audience pinned to its own test user, and deletes the row. The
scheduled scanner runs every 5 minutes in a separate process, and both fire at
07:00. On 2026-11-14 the scheduled scan landed while the canary row existed,
and two real subscribers (one of them outside the company) were emailed
"Outsized Buys: bought SPY at $1.00". With a paying Pro customer now subscribed
to portfolio alerts, the next overlap would have reached them too.

These run the real scan_portfolio_alerts against stub connections, with a
canary row and a real row in the book and both a paying subscriber and the
canary user in the audience. Only Clerk, the database and delivery are stubbed.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import pipelines.notification_scanner as ns
from api.public_fields import ACTIVE_STRATEGIES

REPO = Path(__file__).resolve().parents[2]
CANARY_SCRIPT = REPO / "scripts/alert_canary.py"

PAYING = {"user_id": "user_paying", "email_enabled": 1, "email_frequency": "realtime",
          "min_trade_value": 100000, "min_insider_tier": 2}
CANARY = {"user_id": ns.CANARY_USER, "email_enabled": 0, "email_frequency": "daily",
          "min_trade_value": 100000, "min_insider_tier": 2}
TODAY = "2026-11-17"


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Book:
    """strategy_portfolio as the scanner queries it: entries, then exits."""

    def __init__(self, entries, exits):
        self.entries, self.exits = entries, exits

    def execute(self, sql, params=()):
        return _Result(self.exits if "status = 'closed'" in sql else self.entries)


class _Notifs:
    def execute(self, sql, params=()):
        return _Result([])

    def commit(self):
        pass


def _entry(ticker, price, reasoning):
    return {"id": 1, "strategy": ACTIVE_STRATEGIES[0], "ticker": ticker, "insider_name": "Canary",
            "entry_date": TODAY, "entry_price": price, "signal_quality": None,
            "position_size": 0.0, "entry_reasoning": reasoning}


def _exit(ticker, reasoning):
    return {"id": 2, "strategy": ACTIVE_STRATEGIES[0], "ticker": ticker, "insider_name": "X",
            "exit_date": TODAY, "exit_price": 12.0, "entry_price": 10.0, "pnl_pct": 0.2,
            "pnl_dollar": 2.0, "exit_reason": "time", "entry_reasoning": reasoning}


@pytest.fixture
def delivered(monkeypatch):
    """Run the scan; return (user_id, title) for every notification written."""
    out: list[tuple[str, str]] = []
    sent: list[str] = []

    def insert(nconn, user_id, event_type, title, body, ticker, dedup):
        out.append((user_id, title))
        return len(out)

    monkeypatch.setattr(ns, "_get_subscribed_users", lambda nconn, ev: [dict(PAYING), dict(CANARY)])
    monkeypatch.setattr(ns, "_insert_notification", insert)
    monkeypatch.setattr(ns, "_try_send_realtime",
                        lambda nconn, user, title, body, ev, nid: sent.append(user["user_id"]))
    monkeypatch.setattr(ns, "_get_watermark", lambda nconn, ev: TODAY)
    monkeypatch.setattr(ns, "_set_watermark", lambda nconn, ev, d: None)

    def run(entries, exits=()):
        ns.scan_portfolio_alerts(_Book(list(entries), list(exits)), _Notifs(), TODAY)
        return out, sent

    return run


def test_the_canary_row_reaches_only_the_canary_user(delivered):
    out, sent = delivered([_entry("SPY", 1.0, "canary:1763132400")])
    assert out, "the canary row produced no notification at all, so the canary would report red"
    assert {u for u, _ in out} == {ns.CANARY_USER}, (
        f"a canary position was delivered to {sorted({u for u, _ in out})}; "
        "on 2026-11-14 this emailed real subscribers 'bought SPY at $1.00'"
    )
    assert "user_paying" not in sent


def test_a_real_position_still_reaches_the_paying_subscriber(delivered):
    out, sent = delivered([_entry("EAF", 10.59, "A+ insider, 2.6% of ADV, above SMA50")])
    assert ("user_paying" in {u for u, _ in out}), "the guard must not swallow real alerts"
    assert "user_paying" in sent


def test_a_real_row_with_no_reasoning_is_a_real_alert(delivered):
    out, _ = delivered([_entry("EAF", 10.59, None)])
    assert "user_paying" in {u for u, _ in out}


def test_a_closed_canary_row_is_kept_from_subscribers_too(delivered):
    out, _ = delivered([], [_exit("SPY", "canary:1763132400"), _exit("TISI", "rule exit")])
    users_by_ticker = {}
    for user_id, title in out:
        users_by_ticker.setdefault("SPY" if "SPY" in title else "TISI", set()).add(user_id)
    assert users_by_ticker.get("SPY", set()) <= {ns.CANARY_USER}
    assert "user_paying" in users_by_ticker["TISI"]


def test_the_canary_script_uses_the_marker_and_user_the_scanner_guards():
    """The guard keys on two strings the canary script also types. Drift in
    either one silently reopens the leak, so pin them together."""
    src = CANARY_SCRIPT.read_text()
    user = re.search(r'^CANARY_USER\s*=\s*"([^"]+)"', src, flags=re.M)
    assert user and user.group(1) == ns.CANARY_USER
    marker = re.search(r'marker\s*=\s*f"([^{"]+)\{', src)
    assert marker and marker.group(1) == ns.CANARY_MARKER_PREFIX
    assert re.search(r"entry_reasoning[^)]*\)\s*\n?\s*VALUES", src, flags=re.S), (
        "the canary must write its marker into entry_reasoning, which is what the scanner reads"
    )
