"""Every email a customer gets can be measured, and can be left.

Until 2026-11-14 no email link carried a utm tag, so nothing could say whether
an email ever brought anyone back; the realtime alert and the whole account
sequence went out without the one-click unsubscribe header the digest carried;
and the welcome had a +/-1.5 day window, so one outage lost it for good.
"""
from __future__ import annotations

import ast
import importlib.util
import time
from pathlib import Path

from api.email import tag_links

REPO = Path(__file__).resolve().parents[2]

_SPEC = importlib.util.spec_from_file_location("trial_emails", REPO / "pipelines" / "trial_emails.py")
pipe = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pipe)

UTM = "utm_source=email&utm_medium=email&utm_campaign=welcome"


def test_form4_links_are_tagged():
    out = tag_links('<a href="https://form4.app/feed">x</a>', "welcome")
    assert f'href="https://form4.app/feed?{UTM}"' in out


def test_an_existing_query_and_a_fragment_survive():
    out = tag_links('<a href="https://form4.app/insider/x?tab=buys#top">x</a>', "welcome")
    assert f'href="https://form4.app/insider/x?tab=buys&{UTM}#top"' in out


def test_unsubscribe_external_and_already_tagged_links_are_left_alone():
    html = ('<a href="https://form4.app/api/v1/notifications/unsubscribe?user_id=u&token=t">u</a>'
            '<a href="https://www.sec.gov/cgi-bin/browse-edgar">sec</a>'
            '<a href="https://form4.app/feed?utm_source=x">t</a>')
    assert tag_links(html, "welcome") == html


def test_the_welcome_is_owed_for_three_days_not_a_window():
    assert pipe.welcome_due(0.0) and pipe.welcome_due(0.01) and pipe.welcome_due(2.9)
    assert not pipe.welcome_due(3.2)
    # the other emails keep their windows
    assert not pipe.in_window(2.0, 0)


class _Conn:
    def execute(self, sql, params=()):
        class _R:
            def fetchone(self_inner):
                return None
        return _R()

    def commit(self):
        pass


def test_a_welcome_missed_by_an_outage_still_goes_with_unsubscribe_and_tags(monkeypatch):
    sent = []
    monkeypatch.setattr(pipe, "_build_email", lambda name, user, conn: (f"subject {name}", "<p>hi</p>"))
    monkeypatch.setattr(pipe, "send_email", lambda to, subject, html, **kw: sent.append((subject, kw)) or True)
    created_ms = (time.time() - 2.5 * 86400) * 1000
    user = {"id": "user_1", "created_at": created_ms, "public_metadata": {},
            "email_addresses": [{"id": "e1", "email_address": "a@example.com"}], "primary_email_address_id": "e1"}
    pipe.process_user(user, _Conn())
    names = [s for s, _ in sent]
    assert "subject welcome" in names, "a welcome missed during an outage was dropped"
    kw = dict(sent)["subject welcome"]
    assert kw.get("campaign") == "welcome"
    assert "unsubscribe" in (kw.get("unsubscribe_url") or "")


def _send_calls(path: Path):
    tree = ast.parse(path.read_text())
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)
            and getattr(n.func, "id", getattr(n.func, "attr", None)) == "send_email"]


def test_every_customer_email_names_a_campaign_and_carries_unsubscribe():
    for rel in ("pipelines/notification_scanner.py", "pipelines/trial_emails.py"):
        calls = _send_calls(REPO / rel)
        assert calls, f"no send_email in {rel}"
        for c in calls:
            kws = {k.arg for k in c.keywords}
            assert "campaign" in kws, f"{rel}:{c.lineno} sends without a campaign, so its visits cannot be measured"
            assert "unsubscribe_url" in kws, f"{rel}:{c.lineno} sends without the one-click unsubscribe header"
