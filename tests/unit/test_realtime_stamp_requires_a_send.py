"""A notification is SENT when Resend accepted it, and at no other moment.

WHAT THE DATA SAID (Studio, read-only, 2026-09-11)

Of the 119 rows with `emailed = 1`, 18 carry an `emailed_at` within two
seconds of their own `created_at`. That signature belongs to the realtime
path -- the digest stamps one timestamp across a whole batch, minutes or days
after creation. All 18 are DIRECT (10 portfolio_alert, 8 watchlist_activity)
and every one belongs to a user on `email_frequency = 'daily'`. The single
`realtime` account has no DIRECT rows at all, so the realtime path has never
once sent anything -- yet it had stamped eighteen rows as delivered.

HOW

`_maybe_send_realtime_email` returned None on all five of its exits:
email disabled, not on realtime, no address from Clerk, Resend refused,
Resend accepted. `_try_send_realtime` called it and then stamped
`emailed = SENT, emailed_at = NOW()` unconditionally. A daily user's
portfolio_alert therefore went: created -> tier gate passes (DIRECT) -> cap
passes -> "not realtime, return" -> STAMPED. From then on the digest's
`emailed = PENDING` gate never saw it. Not sent here, not sent there.

Every portfolio_alert ever stamped -- the product's own strategy alerts --
is one of these 18. Nobody received one.

AND IT COMPOUNDS

The 4/day cap counts `COUNT(DISTINCT emailed_at)`, and each realtime stamp
is its own timestamp. Four phantom stamps on 2026-08-26 held one user's
2026-08-27 digest with 46 eligible items behind it; four more held it again
on 08-29 with 82. Four each on 2026-09-10 17:00 held BOTH remaining daily
users' digests on 09-11 (10 and 16 items). Marked-sent-never-sent is the
2026-08-24 outage shape, one row at a time, and this time it was also
silencing the path that worked.
"""
from __future__ import annotations

import pytest

import pipelines.notification_scanner as ns

NID = 4242


class _Conn:
    """Records every statement; answers the cap query with zero sends."""

    def __init__(self):
        self.sql: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.sql.append((sql, tuple(params)))
        return self

    def fetchone(self):
        return {"n": 0}

    def commit(self):
        pass

    @property
    def stamps(self) -> list[tuple[str, tuple]]:
        return [(s, p) for s, p in self.sql if "SET emailed" in s]


@pytest.fixture
def conn(monkeypatch):
    monkeypatch.setattr(ns, "_MAIL_BLOCKED", None)
    monkeypatch.setattr(ns, "_get_user_email", lambda uid: "x@example.com")
    monkeypatch.setattr(ns, "build_notification_email", lambda t, b: "<p></p>")
    monkeypatch.setattr(ns, "_email_cache", {})
    return _Conn()


def _realtime(uid="u_rt"):
    return {"user_id": uid, "email_enabled": 1, "email_frequency": "realtime"}


def test_a_refused_send_is_not_stamped(conn, monkeypatch):
    """send_email returns False on a Resend 5xx, a bad key, a timeout. That
    is a delivery that did not happen and the row must still say so."""
    monkeypatch.setattr(ns, "send_email", lambda *a, **k: False)
    ns._try_send_realtime(conn, _realtime(), "t", "b", "portfolio_alert", NID)
    assert conn.stamps == [], (
        "a refused send was recorded as delivered -- the digest will never "
        "retry it and the cap will count it")


def test_a_daily_users_direct_notification_is_not_stamped(conn, monkeypatch):
    """The 18 rows. A daily user's portfolio_alert reaches this path, is not
    sent because they are not on realtime, and must be left PENDING so the
    digest can carry it."""
    calls = []
    monkeypatch.setattr(ns, "send_email", lambda *a, **k: calls.append(a) or True)
    user = {"user_id": "u_daily", "email_enabled": 1, "email_frequency": "daily"}
    ns._try_send_realtime(conn, user, "t", "b", "portfolio_alert", NID)
    assert calls == [], "a daily user was emailed in realtime"
    assert conn.stamps == [], (
        "a daily user's DIRECT notification was stamped SENT without a send; "
        "it is now invisible to the digest")


@pytest.mark.parametrize("user, address", [
    ({"user_id": "u_off", "email_enabled": 0, "email_frequency": "realtime"},
     "x@example.com"),
    ({"user_id": "u_noaddr", "email_enabled": 1, "email_frequency": "realtime"},
     None),
], ids=["email_disabled", "no_address"])
def test_the_other_silent_exits_are_not_stamped(conn, monkeypatch, user, address):
    """Same defect, the remaining two exits: email switched off, and Clerk
    returning no address (which is what the 2026-08-24 outage looked like
    from here -- every lookup 404'd against the test instance)."""
    monkeypatch.setattr(ns, "_get_user_email", lambda uid: address)
    monkeypatch.setattr(ns, "send_email", lambda *a, **k: True)
    ns._try_send_realtime(conn, user, "t", "b", "watchlist_activity", NID)
    assert conn.stamps == []


def test_an_accepted_send_is_stamped_with_a_time(conn, monkeypatch):
    """The fix must not be bought by never stamping. The cap reads
    emailed_at, and a realtime send it cannot see is a realtime send it
    cannot limit."""
    monkeypatch.setattr(ns, "send_email", lambda *a, **k: True)
    ns._try_send_realtime(conn, _realtime(), "t", "b", "portfolio_alert", NID)
    assert len(conn.stamps) == 1, conn.stamps
    sql, params = conn.stamps[0]
    assert f"emailed = {ns.EMAIL_SENT}" in sql
    assert "emailed_at = NOW()" in sql
    assert params == (NID,), "stamped a row other than the one delivered"


def test_the_send_result_is_the_only_thing_that_says_sent():
    """Structural: the sender reports the outcome, the guard stamps on it.
    Returning None from the sender is how all five exits looked alike."""
    import inspect
    sig = inspect.signature(ns._maybe_send_realtime_email)
    # A string under `from __future__ import annotations`.
    assert sig.return_annotation in (bool, "bool"), (
        "_maybe_send_realtime_email must report whether it sent")
    src = inspect.getsource(ns._try_send_realtime)
    assert "if not _maybe_send_realtime_email(" in src, (
        "the stamp is not conditioned on the send result")
