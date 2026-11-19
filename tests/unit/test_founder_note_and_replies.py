"""The founder note asks one question; support_replies.py hears the answer.

Added 2026-11-19. Every signup skips the onboarding questions and PostHog
records clicks, not intent, so a plain-text note from Derek asks each new
account what they came to do. It is sent from and replied to
support@form4.app, a Google Group delivering to the mailbox ceowatcher_reader
already reads; pipelines/support_replies.py reads it read-only every 15
minutes, stores each reply, records the numbered answer and pushes Derek.

These pin: what the note may say, when it goes and to whom, that nobody gets
two emails in one run, who the backfill reaches, how a reply is parsed, that
the push never carries the reply's text, and that the reader cannot change
the mailbox.
"""
from __future__ import annotations

import importlib.util
import inspect
import re
import time
from datetime import datetime, timedelta
from email import message_from_string
from pathlib import Path

import api.email as api_email
from api import email_templates as t

REPO = Path(__file__).resolve().parents[2]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, REPO / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


pipe = _load("trial_emails_fn", "pipelines/trial_emails.py")
replies = _load("support_replies_fn", "pipelines/support_replies.py")


# ─── the note ────────────────────────────────────────────────────────────────

def test_the_note_is_plain_text_from_a_person_and_follows_the_copy_rules():
    for kwargs in ({}, {"signed_up": datetime(2026, 10, 13)}, {"first_name": "Cara"}):
        subject, body = t.founder_note_email(**kwargs)
        assert subject == t.FOUNDER_NOTE_SUBJECT
        assert "<" not in body and "http" not in body, "the note is plain text with no links"
        assert "!" not in subject, "the subject line stays plain"
        for gone in ("your trial", "trial has ended", "grace period", "days left"):
            assert gone not in body.lower()
        assert "—" not in body, "no em-dash joins"
        assert "unlock" not in body.lower()
        for n, (_, label) in t.FOUNDER_NOTE_OPTIONS.items():
            assert f"{n}. {label}" in body
        assert "pro" not in re.sub(r"[^a-z ]", " ", body.lower()).split(), "the note never pitches Pro"


def test_the_backfill_does_not_pretend_they_joined_yesterday():
    _, fresh = t.founder_note_email()
    _, late = t.founder_note_email(signed_up=datetime(2026, 10, 13))
    assert "when you signed up" in fresh and "signed up for Form4 in" not in fresh
    assert "You signed up for Form4 in October." in late
    assert t.founder_note_email(first_name="Cara")[1].startswith("Hi Cara,")
    assert fresh.startswith("Hi there,")


def test_the_note_goes_on_day_one_and_is_owed_through_day_four():
    assert not pipe.founder_note_due(0.9)
    assert pipe.founder_note_due(1.0) and pipe.founder_note_due(2.5) and pipe.founder_note_due(4.0)
    assert not pipe.founder_note_due(4.1)
    assert pipe.is_due("founder_note", 1, 1.2) and not pipe.is_due("founder_note", 1, 0.2)
    assert pipe.is_due("welcome", 0, 0.1)


def test_internal_accounts_are_recognised():
    for addr in ("derek.gorthy@gmail.com", "derek.gorthy+test2@gmail.com",
                 "derek+test_nov14@sidequestgroup.com", "derek@sidequestgroup.com", "alerts@form4.app"):
        assert pipe.is_internal(addr), addr
    for addr in ("cara@gmail.com", "derek@gmail.com", "someone@sidequest.com"):
        assert not pipe.is_internal(addr), addr


# ─── sending ─────────────────────────────────────────────────────────────────

class _Conn:
    """sent_trial_emails in memory. `sent` names emails already sent to every
    user; rows inserted during the test are kept per (user_id, email_name)."""

    def __init__(self, sent=()):
        self.sent = set(sent)

    def execute(self, sql, params=()):
        conn = self
        if sql.lstrip().upper().startswith("INSERT"):
            conn.sent.add((params[0], params[1]))

        class _R:
            def fetchone(self_inner):
                if "FROM sent_trial_emails" in sql:
                    hit = params[1] in conn.sent or (params[0], params[1]) in conn.sent
                    return (1,) if hit else None
                return None
        return _R()

    def commit(self):
        pass


def _user(age_days: float, addr: str = "cara@example.com", meta: dict | None = None) -> dict:
    return {"id": "user_1", "created_at": (time.time() - age_days * 86400) * 1000,
            "public_metadata": meta or {}, "first_name": None,
            "email_addresses": [{"id": "e1", "email_address": addr}], "primary_email_address_id": "e1"}


def _capture(monkeypatch):
    sent = []
    real_build = pipe._build_email

    def build(name, user, conn):
        return real_build(name, user, conn) if name == "founder_note" else (f"subject {name}", "<p>hi</p>")

    def fake_send(to, subject, html=None, **kw):
        sent.append({"to": to, "subject": subject, "html": html, **kw})
        return True
    monkeypatch.setattr(pipe, "_build_email", build)
    monkeypatch.setattr(pipe, "send_email", fake_send)
    return sent


def test_the_note_goes_as_plain_text_from_and_replied_to_support(monkeypatch):
    sent = _capture(monkeypatch)
    assert pipe.process_user(_user(1.2), _Conn(sent={"welcome"})) == 1
    (mail,) = sent
    assert mail["subject"] == t.FOUNDER_NOTE_SUBJECT
    assert mail["html"] is None and "Replying with a number" in mail["text"]
    assert mail["from_address"] == f"{t.FOUNDER_NOTE_SENDER} <{api_email.SUPPORT_ADDRESS}>"
    assert mail["reply_to"] == api_email.SUPPORT_ADDRESS
    assert mail["campaign"] == "founder_note" and "unsubscribe" in mail["unsubscribe_url"]


def test_nobody_gets_two_emails_in_one_run(monkeypatch):
    # Day 2 with nothing sent (the welcome missed during an outage): both are
    # owed. The welcome goes now, the founder note on the next hourly run.
    sent = _capture(monkeypatch)
    conn = _Conn()
    assert pipe.process_user(_user(2.0), conn) == 1
    assert [m["subject"] for m in sent] == ["subject welcome"]
    assert pipe.process_user(_user(2.0), conn) == 1
    assert [m["subject"] for m in sent] == ["subject welcome", t.FOUNDER_NOTE_SUBJECT]
    assert pipe.process_user(_user(2.0), conn) == 0, "nothing else is due on day 2"


def test_a_failed_send_stops_the_account_for_this_run(monkeypatch):
    calls = []
    monkeypatch.setattr(pipe, "_build_email", lambda n, u, c: (f"subject {n}", "<p>hi</p>"))
    monkeypatch.setattr(pipe, "send_email", lambda to, subject, html=None, **kw: calls.append(subject) and False)
    assert pipe.process_user(_user(2.5), _Conn()) == 0
    assert calls == ["subject welcome"], "a failed welcome must not let a later email jump the queue"


def test_internal_and_paying_accounts_never_get_the_note(monkeypatch):
    sent = _capture(monkeypatch)
    assert pipe.process_user(_user(1.2, "derek.gorthy@gmail.com"), _Conn(sent={"welcome"})) == 0
    assert pipe.process_user(_user(1.2, meta={"tier": "pro"}), _Conn(sent={"welcome"})) == 0
    assert sent == []


def test_the_backfill_reaches_only_older_unasked_strangers(monkeypatch):
    now = datetime.utcnow()
    since = now - timedelta(days=60)
    ms = lambda days: (now - timedelta(days=days)).timestamp() * 1000  # noqa: E731

    def user(uid, days, addr, meta=None):
        return {"id": uid, "created_at": ms(days), "public_metadata": meta or {}, "first_name": None,
                "email_addresses": [{"id": "e", "email_address": addr}], "primary_email_address_id": "e"}

    users = [
        user("u_old", 12, "old@example.com"),                       # yes
        user("u_older", 50, "older@example.com"),                   # yes
        user("u_ancient", 90, "ancient@example.com"),               # before --since
        user("u_new", 2, "new@example.com"),                        # the sequence sends this one
        user("u_paid", 20, "paid@example.com", {"tier": "pro"}),     # paying
        user("u_me", 20, "derek+test_nov14@sidequestgroup.com"),     # internal
        user("u_asked", 20, "asked@example.com"),                   # already sent
    ]

    class Conn(_Conn):
        def execute(self, sql, params=()):
            if "FROM sent_trial_emails" in sql and params[0] == "u_asked":
                class _R:
                    def fetchone(self_inner):
                        return (1,)
                return _R()
            return super().execute(sql, params)

    assert pipe.founder_backfill(users, Conn(), since, dry_run=True, now=now) == \
        ["old@example.com", "older@example.com"]

    bodies = []
    monkeypatch.setattr(pipe, "send_email",
                        lambda to, subject, html=None, **kw: bodies.append(kw["text"]) or True)
    assert len(pipe.founder_backfill(users, Conn(), since, dry_run=False, now=now)) == 2
    assert all("You signed up for Form4 in " in b for b in bodies)


def test_send_email_sends_text_alone_when_asked(monkeypatch):
    posted = []

    class _Resp:
        status_code = 200
        text = ""
    monkeypatch.setattr(api_email, "RESEND_API_KEY", "re_test")
    monkeypatch.setattr(api_email.httpx, "post", lambda url, json, headers, timeout: posted.append(json) or _Resp())

    assert api_email.send_email("a@example.com", "s", text="hello", campaign="founder_note")
    assert posted[-1]["text"] == "hello" and "html" not in posted[-1]

    assert api_email.send_email("a@example.com", "s", '<a href="https://form4.app/feed">x</a>', campaign="welcome")
    assert "utm_campaign=welcome" in posted[-1]["html"] and "text" not in posted[-1]

    n = len(posted)
    assert not api_email.send_email("a@example.com", "s")
    assert len(posted) == n, "an email with no body must not be posted"


def test_support_address_has_one_definition():
    # A second literal is how a reply-to drifts from the mailbox the reader
    # reads. Prose in docstrings may name it; code may not.
    literal = re.compile(r"""["']support@form4\.app["']""")
    for path in list((REPO / "api").rglob("*.py")) + list((REPO / "pipelines").rglob("*.py")):
        if path == REPO / "api" / "email.py":
            continue
        assert not literal.search(path.read_text(errors="ignore")), \
            f"{path.relative_to(REPO)} types the support address; import api.email.SUPPORT_ADDRESS"


# ─── reading replies ─────────────────────────────────────────────────────────

ACCOUNTS = {"cara@gmail.com": ("user_cara", 1763548000000)}


def _msg(headers: str, body: str = "", ctype: str = "text/plain") -> object:
    return message_from_string(f"{headers}\nContent-Type: {ctype}; charset=utf-8\n\n{body}")


def test_the_author_is_found_behind_a_google_groups_rewrite():
    rewritten = _msg('From: "\'Jane Doe\' via support" <support@form4.app>\n'
                     "Reply-To: Jane Doe <jane@yahoo.com>\nX-Original-Sender: jane@yahoo.com\nSubject: hi")
    assert replies.original_sender(rewritten) == "jane@yahoo.com"
    plain = _msg("From: Cara <Cara@Gmail.com>\nSubject: hi")
    assert replies.original_sender(plain) == "cara@gmail.com"


def test_only_people_count_and_tiktok_and_resend_do_not():
    note = "Re: " + t.FOUNDER_NOTE_SUBJECT
    assert replies.classify("cara@gmail.com", note, ACCOUNTS) == ("founder_note_reply", "user_cara")
    assert replies.classify("cara@gmail.com", "Billing question", ACCOUNTS) == ("support_email", "user_cara")
    assert replies.classify("other@yahoo.com", note, ACCOUNTS) == ("founder_note_reply", None)
    assert replies.classify("noreply@service.tiktok.com", "zhangsta posted: NVDA", ACCOUNTS) == (None, None)
    assert replies.classify("team@notifications.resend.com", "You have reached 100% of your daily quota",
                            ACCOUNTS) == (None, None)
    assert replies.classify("alerts@form4.app", note, ACCOUNTS) == (None, None)
    assert replies.classify("support@form4.app", note, ACCOUNTS) == (None, None)


def test_the_quoted_original_is_cut_off_whatever_the_client():
    gmail = ("2\n\nOn Thu, Nov 19, 2026 at 3:07 AM Derek from Form4 <\nsupport@form4.app> wrote:\n\n"
             "> Hi there,\n> What were you hoping to do?\n")
    assert replies.reply_text(_msg("Subject: x", gmail)) == "2"
    outlook = ("I want to follow what the strategies buy.\n\nFrom: Derek from Form4 <support@form4.app>\n"
               "Sent: Thursday\nSubject: What brought you to Form4?\n\n1. Get an email")
    assert replies.reply_text(_msg("Subject: x", outlook)) == "I want to follow what the strategies buy."
    iphone = "1 and 3\n\nSent from my iPhone\n\n> On Nov 19, 2026, at 3:07 AM, Derek wrote:\n> Hi"
    assert replies.reply_text(_msg("Subject: x", iphone)) == "1 and 3"
    html = ('<div dir="ltr">2</div><br><div class="gmail_quote"><div class="gmail_attr">On Thu, Nov 19, 2026 '
            'at 3:07 AM Derek from Form4 &lt;support@form4.app&gt; wrote:<br></div><blockquote>Hi there,'
            '</blockquote></div>')
    assert replies.reply_text(_msg("Subject: x", html, "text/html")) == "2"


def test_a_numbered_answer_is_read_and_a_sentence_is_left_for_derek():
    keys = {n: k for n, (k, _) in t.FOUNDER_NOTE_OPTIONS.items()}
    assert replies.parse_answer("2") == [keys[2]]
    assert replies.parse_answer("#2.") == [keys[2]]
    assert replies.parse_answer("1 and 3") == [keys[1], keys[3]]
    assert replies.parse_answer("mostly 2") == [keys[2]]
    assert replies.parse_answer("2. I want to see what the books buy before I do anything else") == [keys[2]]
    assert replies.parse_answer("Option 4 - I build my own screens and wanted the raw filings") == [keys[4]]
    assert replies.parse_answer("I was looking for something like 13F data for funds, 2 of them") == []
    assert replies.parse_answer("$2 million buys") == []
    assert replies.parse_answer("10") == [] and replies.parse_answer("2024") == []
    assert replies.parse_answer("") == []


def test_the_push_names_the_pick_and_never_carries_the_reply():
    params = set(inspect.signature(replies.push_text).parameters)
    assert not params & {"body", "text", "reply"}, "the reply's text must never go to ntfy.sh"
    title, text = replies.push_text("founder_note_reply", "cara@gmail.com", "Re: x", "follow_strategies",
                                    datetime(2026, 11, 19))
    assert "ca***@gmail.com" in text and "cara@gmail.com" not in text
    assert "2: Follow the strategies" in text and "Nov 19" in text
    _, words = replies.push_text("founder_note_reply", "x@yahoo.com", "Re: x", None, None)
    assert "answered in words" in words and "no account" in words


def test_the_reader_cannot_change_the_mailbox():
    src = (REPO / "pipelines" / "support_replies.py").read_text()
    code = re.sub(r'"""[\s\S]*?"""', "", src)  # docstrings may describe what it does not do
    assert "readonly=True" in code, "the folder must be opened with EXAMINE"
    fetches = re.findall(r'"\(BODY[^"]*"', code)
    assert fetches and all("PEEK" in f for f in fetches), "a non-PEEK fetch marks mail as read"
    for forbidden in ('"STORE"', '"EXPUNGE"', '"COPY"', '"MOVE"', ".store(", ".expunge(", ".copy(", ".move(",
                      "RFC822", "\\\\Seen", "\\\\Deleted"):
        assert forbidden not in code, forbidden


def test_the_reader_is_scheduled():
    src = (REPO / "dataplane" / "dagster_project" / "assets" / "form4_ops.py").read_text()
    assert "def ops_support_replies(" in src
    block = src[src.index("form4_ops_assets = ["):]
    assert "ops_support_replies," in block[:block.index("]")], "defined but never run"
    assert re.search(r'_sched\("ops_support_replies_15min",\s*\[ops_support_replies\]', src)
