#!/usr/bin/env python3
"""Replies to support@form4.app: keep them, record the answer, push Derek.

The founder note (api/email_templates.founder_note_email) asks each new
account one question and is sent from support@form4.app. support@ is a Google
Group that delivers to Derek's Workspace inbox -- the mailbox
pipelines/ceowatcher_reader.py already reads over IMAP with IMAP_EMAIL /
IMAP_APP_PASSWORD. Verified 2026-11-19: 241 messages had reached it.

Every 15 minutes (Dagster ops_support_replies_15min) this reads that mailbox
READ-ONLY -- the folder is opened with EXAMINE and bodies are fetched with
BODY.PEEK, so nothing is marked read, moved or deleted -- and keeps the
messages a PERSON sent to support@: the sender is a Clerk account, or the
subject is the founder note's. For each new one it

  * stores it in notifications.support_replies, one row per Message-ID, with
    the quoted original cut off, so any session can read what people said;
  * records a numbered answer ("2", "1 and 3") on
    notifications.user_profiles.primary_use_case, if that is still empty;
  * pushes Derek on ntfy: who (masked), and which option they picked. Never
    the text of the reply -- ntfy.sh is a third-party service. The text is in
    the inbox and in the table.

Everything else that reaches support@ is ignored: today that is TikTok
notifications and Resend's quota warnings.

    python3 pipelines/support_replies.py             # normal run
    python3 pipelines/support_replies.py --dry-run   # read and parse; store and push nothing
    python3 pipelines/support_replies.py --days 30   # look further back (default 14)
"""
from __future__ import annotations

import argparse
import email
import imaplib
import logging
import os
import re
import sys
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parseaddr, parsedate_to_datetime
from html import unescape
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from api.email import SUPPORT_ADDRESS
from api.email_templates import FOUNDER_NOTE_OPTIONS, FOUNDER_NOTE_SUBJECT
from framework.alerts.ntfy import send_ntfy

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("support_replies")

IMAP_SERVER = "imap.gmail.com"
LOOKBACK_DAYS = 14
#: Our own sending domain. Mail from it is never a person replying.
OWN_DOMAIN = "form4.app"

SCHEMA = """\
CREATE TABLE IF NOT EXISTS notifications.support_replies (
    message_id   TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,          -- founder_note_reply | support_email
    user_id      TEXT,                   -- NULL when the sender is not an account
    sender       TEXT NOT NULL,
    subject      TEXT,
    received_at  TIMESTAMPTZ,
    answer       TEXT,                   -- FOUNDER_NOTE_OPTIONS keys, comma-joined
    body         TEXT,                   -- the new text, quoted original removed
    alerted_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
)"""


# ───────────────────────────────────────────────────────────────────
# Pure parsing — tested
# ───────────────────────────────────────────────────────────────────

def _header(msg: Message, name: str) -> str:
    raw = msg.get(name)
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return str(raw)


def original_sender(msg: Message) -> str:
    """The address of the person who wrote it.

    Google Groups rewrites From to the group's own address ("Jane via support
    <support@form4.app>") when the author's domain publishes a strict DMARC
    policy, and keeps the author in X-Original-Sender and Reply-To."""
    for name in ("X-Original-Sender", "Reply-To", "From"):
        addr = parseaddr(_header(msg, name))[1].strip().lower()
        if addr and addr != SUPPORT_ADDRESS:
            return addr
    return parseaddr(_header(msg, "From"))[1].strip().lower()


def _decode(part: Message) -> str:
    payload = part.get_payload(decode=True) or b""
    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")


def _strip_html(html: str) -> str:
    html = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", html)
    html = re.sub(r"(?i)<br\s*/?>|</p>|</div>|</li>", "\n", html)
    return unescape(re.sub(r"<[^>]+>", "", html))


def plain_body(msg: Message) -> str:
    """The text/plain part, else the html part with its tags removed."""
    parts = list(msg.walk()) if msg.is_multipart() else [msg]
    for want in ("text/plain", "text/html"):
        for part in parts:
            if part.get_content_type() == want and not part.get_filename():
                text = _decode(part)
                return _strip_html(text) if want == "text/html" else text
    return ""


_QUOTE_HEADER = re.compile(r"^(-{2,}\s*original message\s*-{2,}|from:\s.+|sent from my\s.+)$", re.I)


def reply_text(msg: Message) -> str:
    """What they wrote, with the quoted original cut off.

    Clients mark the quote differently: '>' lines, an 'On <date>, <who>
    wrote:' line that Gmail wraps across two lines, Outlook's 'From:' block,
    'Sent from my iPhone'. Cut at the first of any of them."""
    lines = plain_body(msg).replace("\r\n", "\n").split("\n")
    keep: list[str] = []
    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith(">") or _QUOTE_HEADER.match(s):
            break
        if s.lower().startswith("on ") and any(l.rstrip().endswith("wrote:") for l in lines[i:i + 3]):
            break
        keep.append(line)
    return "\n".join(keep).strip()[:4000]


_LEADING_PICK = re.compile(r"^\W*(?:option|number|no\.?)?\s*#?\s*([1-4])(?!\d)", re.I)
_ANY_PICK = re.compile(r"(?<![\d$])([1-4])(?![\d%kKmM])")


def parse_answer(text: str) -> list[str]:
    """The options a reply picked, as FOUNDER_NOTE_OPTIONS keys, in order.

    Read from the first line only. A short line ('2', '#2', '1 and 3',
    'mostly 2') gives every option it names; a longer one counts only when
    it STARTS with a number ('2. I want to see what the books buy'). A
    sentence that merely contains a digit is an answer in words, and Derek
    reads those himself."""
    first = next((l.strip() for l in text.split("\n") if l.strip()), "")
    if not first:
        return []
    if len(first) <= 40:
        digits = _ANY_PICK.findall(first)
    else:
        m = _LEADING_PICK.match(first)
        digits = [m.group(1)] if m else []
    picks: list[str] = []
    for d in digits:
        key = FOUNDER_NOTE_OPTIONS[int(d)][0]
        if key not in picks:
            picks.append(key)
    return picks


def classify(sender: str, subject: str, accounts: dict[str, tuple[str, int]]) -> tuple[str | None, str | None]:
    """(kind, user_id) for a message to support@, or (None, None) to ignore it.

    A person wrote it when the sender is a Clerk account, or the subject is
    the founder note's (a reply from another address they own). Our own
    domain never counts."""
    if not sender or sender == SUPPORT_ADDRESS or sender.endswith("@" + OWN_DOMAIN):
        return None, None
    is_note = FOUNDER_NOTE_SUBJECT.lower() in (subject or "").lower()
    account = accounts.get(sender)
    if not account and not is_note:
        return None, None
    return ("founder_note_reply" if is_note else "support_email"), (account[0] if account else None)


def mask(addr: str) -> str:
    local, _, domain = (addr or "").partition("@")
    return f"{local[:2]}***@{domain}" if domain else "someone"


def labels(answer: str) -> str:
    by_key = {key: (n, label) for n, (key, label) in FOUNDER_NOTE_OPTIONS.items()}
    return "; ".join(f"{by_key[k][0]}: {by_key[k][1]}" for k in answer.split(",") if k in by_key)


def push_text(kind: str, sender: str, subject: str, answer: str | None,
              signed_up: datetime | None) -> tuple[str, str]:
    """(title, text) for the ntfy push. Never the body of the reply."""
    who = mask(sender) + (f" (signed up {signed_up:%b %-d})" if signed_up else " (no account with that address)")
    if kind == "founder_note_reply":
        if answer:
            return "Reply to your founder note", f"{who} picked {labels(answer)}. The full reply is in the support@ inbox."
        return "Reply to your founder note", f"{who} answered in words. Read it in the support@ inbox."
    return "A user emailed support@", f"{who}: {subject[:80]}"


# ───────────────────────────────────────────────────────────────────
# IMAP, read-only
# ───────────────────────────────────────────────────────────────────

def _connect() -> imaplib.IMAP4_SSL:
    user, password = os.environ.get("IMAP_EMAIL", ""), os.environ.get("IMAP_APP_PASSWORD", "")
    if not user or not password:
        raise SystemExit("IMAP_EMAIL / IMAP_APP_PASSWORD not set in .env")
    imap = imaplib.IMAP4_SSL(IMAP_SERVER)
    imap.login(user, password)
    _, boxes = imap.list()
    all_mail = next((re.search(rb'"([^"]+)"$', b).group(1).decode() for b in boxes or [] if b and b"\\All" in b),
                    "INBOX")
    # readonly=True issues EXAMINE: the mailbox cannot be changed through this session.
    imap.select(f'"{all_mail}"', readonly=True)
    return imap


def fetch_messages(imap: imaplib.IMAP4_SSL, days: int) -> list[tuple[str, Message]]:
    """(uid, headers-only message) for everything sent to support@ in the window."""
    query = f"{{to:{SUPPORT_ADDRESS} cc:{SUPPORT_ADDRESS}}} newer_than:{days}d"
    _, data = imap.uid("SEARCH", "X-GM-RAW", f'"{query}"')
    out = []
    for uid in (data[0] or b"").split():
        _, msg = imap.uid("FETCH", uid, "(BODY.PEEK[HEADER])")
        if msg and isinstance(msg[0], tuple):
            out.append((uid.decode(), email.message_from_bytes(msg[0][1])))
    return out


def fetch_full(imap: imaplib.IMAP4_SSL, uid: str) -> Message:
    _, msg = imap.uid("FETCH", uid, "(BODY.PEEK[])")
    return email.message_from_bytes(msg[0][1])


# ───────────────────────────────────────────────────────────────────
# Run
# ───────────────────────────────────────────────────────────────────

def _accounts() -> dict[str, tuple[str, int]]:
    """Primary email -> (user_id, created_at ms), from Clerk."""
    from pipelines.trial_emails import _fetch_all_clerk_users, _get_user_email
    out = {}
    for u in _fetch_all_clerk_users():
        addr = (_get_user_email(u) or "").lower()
        if addr:
            out[addr] = (u["id"], u.get("created_at") or 0)
    return out


def _signed_up(accounts: dict[str, tuple[str, int]], sender: str) -> datetime | None:
    ms = accounts.get(sender, (None, 0))[1]
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc) if ms else None


def run(days: int, dry_run: bool) -> int:
    accounts = _accounts()
    if not accounts:
        raise SystemExit("No Clerk accounts fetched; check CLERK_SECRET_KEY")

    conn = None
    if not dry_run:
        from config.database import get_connection
        conn = get_connection(readonly=False)
        conn.execute(SCHEMA)
        conn.commit()

    imap = _connect()
    new = 0
    try:
        for uid, head in fetch_messages(imap, days):
            message_id = _header(head, "Message-ID").strip() or f"uid:{uid}"
            sender, subject = original_sender(head), _header(head, "Subject")
            kind, user_id = classify(sender, subject, accounts)
            if not kind:
                continue
            if conn and conn.execute("SELECT 1 FROM notifications.support_replies WHERE message_id = ?",
                                     (message_id,)).fetchone():
                continue
            text = reply_text(fetch_full(imap, uid))
            answer = ",".join(parse_answer(text)) if kind == "founder_note_reply" else ""
            try:
                received = parsedate_to_datetime(_header(head, "Date"))
            except (TypeError, ValueError):
                received = None
            new += 1
            if dry_run:
                logger.info("[DRY RUN] %s from %s, answer=%s, %d chars", kind, mask(sender), answer or "-", len(text))
                continue
            conn.execute(
                """INSERT INTO notifications.support_replies
                       (message_id, kind, user_id, sender, subject, received_at, answer, body)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (message_id) DO NOTHING""",
                (message_id, kind, user_id, sender, subject, received, answer or None, text),
            )
            if answer and user_id:
                conn.execute(
                    """INSERT INTO notifications.user_profiles (user_id, primary_use_case) VALUES (?, ?)
                       ON CONFLICT (user_id) DO UPDATE
                          SET primary_use_case = EXCLUDED.primary_use_case, updated_at = NOW()::text
                        WHERE COALESCE(notifications.user_profiles.primary_use_case, '') = ''""",
                    (user_id, answer),
                )
            conn.commit()
            logger.info("Stored %s from %s, answer=%s", kind, mask(sender), answer or "-")
    finally:
        imap.logout()

    if conn:
        # Push every stored row not yet pushed, so a failed push is retried next run.
        pending = conn.execute(
            """SELECT message_id, kind, sender, subject, answer FROM notifications.support_replies
                WHERE alerted_at IS NULL ORDER BY received_at"""
        ).fetchall()
        for row in pending:
            r = dict(row)
            title, body = push_text(r["kind"], r["sender"], r["subject"] or "", r["answer"],
                                    _signed_up(accounts, r["sender"]))
            if send_ntfy(body, title=title, priority=4, tags=["envelope_with_arrow"]):
                conn.execute("UPDATE notifications.support_replies SET alerted_at = NOW() WHERE message_id = ?",
                             (r["message_id"],))
                conn.commit()
            else:
                logger.error("Push failed for %s; will retry next run", mask(r["sender"]))
        conn.close()
    logger.info("%sDone. %d new message(s) from people.", "[DRY RUN] " if dry_run else "", new)
    return new


def main() -> None:
    parser = argparse.ArgumentParser(description="Read replies to support@form4.app and push Derek")
    parser.add_argument("--dry-run", action="store_true", help="Read and parse; store and push nothing")
    parser.add_argument("--days", type=int, default=LOOKBACK_DAYS, help="How far back to look")
    args = parser.parse_args()
    run(args.days, args.dry_run)


if __name__ == "__main__":
    main()
