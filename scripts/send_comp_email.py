#!/usr/bin/env python3
"""Send the comped-Pro note to users who hold an active comp.

Pairs with scripts/comp_user.py: that grants the access, this tells them.
Reads the recipient list from Clerk rather than taking addresses on the
command line, so it can only ever mail someone who actually holds an
unexpired comp.

Dry-run by default. Pass --send to actually deliver via Resend.

    python3 scripts/send_comp_email.py                      # preview
    python3 scripts/send_comp_email.py --write-preview x.html
    python3 scripts/send_comp_email.py --send               # deliver
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import os  # noqa: E402

from api.comp import comp_lapsed  # noqa: E402
from api.email import send_email, generate_unsubscribe_token  # noqa: E402
from api.email_templates import APP_URL, comp_grant_email  # noqa: E402

CLERK_API = "https://api.clerk.com/v1"
CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY", "")

# This is a founder note, not an alert, so it must not arrive from the default
# alerts@form4.app sender the templates use.
#
# Sending and receiving are governed by different things, and mixing them up
# bounces real replies:
#   SENDING  — Resend verifies form4.app at the DOMAIN level, so any
#              address@form4.app sends fine with no extra setup.
#   RECEIVING — form4.app's MX points at Google Workspace, but only addresses
#              provisioned THERE accept mail. support@form4.app was not, so
#              replies to it bounced with "mailbox does not exist".
#
# Both From and Reply-To are support@form4.app. That address must exist in
# Google Workspace (user, alias, or group) or replies bounce with "mailbox
# does not exist" — sending works regardless, so a bounce is the only signal.
FROM_ADDRESS = "Derek at Form4 <support@form4.app>"
REPLY_TO = "support@form4.app"


def human_date(iso: str) -> str:
    """2026-11-11 -> 'November 11th'."""
    d = datetime.strptime(iso, "%Y-%m-%d")
    n = d.day
    suffix = "th" if 11 <= n <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{d.strftime('%B')} {n}{suffix}"

# Paying customers who were refunded for the 2026-07-28 → 08-11 outage. Their
# email states the refund as fact, so only add an address once the refund has
# actually settled in Stripe.
REFUNDED = {"saivarun4492@gmail.com"}


def active_comps(reason: str = "") -> list[dict]:
    """Clerk users holding an unexpired comp, optionally filtered by reason."""
    resp = httpx.get(f"{CLERK_API}/users", params={"limit": 100},
                     headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
                     timeout=30)
    resp.raise_for_status()

    out = []
    for u in resp.json():
        meta = u.get("public_metadata") or {}
        if not meta.get("pro_until") or comp_lapsed(meta):
            continue
        if reason and meta.get("comp_reason") != reason:
            continue
        addrs = u.get("email_addresses", [])
        if not addrs:
            continue
        out.append({
            "id": u["id"],
            "email": addrs[0]["email_address"],
            "first_name": (u.get("first_name") or "").strip().title(),
            "pro_until": meta["pro_until"],
        })
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--reason", default="", help="only users with this comp_reason")
    p.add_argument("--write-preview", metavar="PATH",
                   help="write the rendered HTML somewhere to eyeball it")
    p.add_argument("--send", action="store_true", help="deliver (default: dry run)")
    p.add_argument("--test-to", metavar="EMAIL",
                   help="proof the copy: render one real email and deliver it "
                        "to this address instead of the live recipient list. "
                        "The address does not need to hold a comp. Implies --send.")
    p.add_argument("--test-refunded", action="store_true",
                   help="with --test-to, proof the refunded variant "
                        "(the one REFUNDED addresses receive)")
    p.add_argument("--from", dest="from_address", metavar="ADDR",
                   help=f"override the sender (default: {FROM_ADDRESS!r}). "
                        "Any address on a Resend-verified domain works for "
                        "SENDING; replies only arrive if the mailbox or alias "
                        "actually exists in Google Workspace.")
    p.add_argument("--reply-to", dest="reply_to", metavar="ADDR",
                   help=f"override the Reply-To (default: {REPLY_TO!r})")
    p.add_argument("--only", metavar="EMAIL", action="append", default=[],
                   help="restrict to this recipient (repeatable). Lets the "
                        "real send go out one person at a time, reviewed "
                        "individually, instead of the whole list at once.")
    args = p.parse_args()

    sender = args.from_address or FROM_ADDRESS
    reply_to = args.reply_to or REPLY_TO

    if not CLERK_SECRET_KEY:
        sys.exit("CLERK_SECRET_KEY not set — check .env")

    if args.test_to:
        # Proof send. Uses the real template and the real comp end date, with
        # the no-first-name greeting ("Hi there,") so what lands in the inbox
        # is the copy itself, not a personalised variant.
        live = active_comps(args.reason)
        until_raw = live[0]["pro_until"] if live else "2026-11-11"
        until = human_date(until_raw)
        subject, html = comp_grant_email(
            first_name="", until_date=until, unsubscribe_url="",
            refunded=args.test_refunded,
        )
        print(f"\nTEST SEND → {args.test_to}")
        print(f"  subject : {subject}")
        print(f"  comp end: {until_raw}  →  rendered as \"{until}\"")
        print(f"  from    : {sender}")
        print(f"  reply-to: {reply_to}")
        print(f"  variant : {'REFUNDED' if args.test_refunded else 'no refund'}"
              f", no-name greeting (\"Hi there,\")")
        ok = send_email(args.test_to, subject, html, reply_to=reply_to,
                        from_address=sender)
        print(f"  {'SENT' if ok else 'FAILED — check RESEND_API_KEY and logs'}")
        return 0 if ok else 1

    recipients = active_comps(args.reason)
    if args.only:
        wanted = {e.lower() for e in args.only}
        skipped = [r["email"] for r in recipients if r["email"].lower() not in wanted]
        recipients = [r for r in recipients if r["email"].lower() in wanted]
        missing = wanted - {r["email"].lower() for r in recipients}
        if missing:
            sys.exit(f"--only address(es) hold no active comp: {sorted(missing)}")
        if skipped:
            print(f"--only: holding back {len(skipped)} recipient(s): "
                  f"{', '.join(skipped)}")
    if not recipients:
        print("No users hold an active comp"
              + (f" with reason {args.reason!r}" if args.reason else "") + ".")
        return 0

    for r in recipients:
        until = human_date(r["pro_until"])
        unsub = ""
        token = generate_unsubscribe_token(r["id"])
        if token:
            unsub = (f"{APP_URL}/api/v1/notifications/unsubscribe"
                     f"?user_id={r['id']}&token={token}")

        was_refunded = r["email"] in REFUNDED
        subject, html = comp_grant_email(
            first_name=r["first_name"], until_date=until, unsubscribe_url=unsub,
            refunded=was_refunded,
        )

        print(f"\n{r['email']}  ({r['id']})")
        print(f"  name    : {r['first_name'] or '(none — falls back to \"Hi there\")'}")
        print(f"  refunded: {'YES — email states the refund as fact' if was_refunded else 'no'}")
        print(f"  subject : {subject}")
        print(f"  comp ends: {r['pro_until']}  →  rendered as \"{until}\"")
        print(f"  from    : {sender}")
        print(f"  reply-to: {reply_to}")

        if args.write_preview:
            path = Path(args.write_preview)
            if len(recipients) > 1:
                path = path.with_name(f"{path.stem}-{r['first_name'] or r['id'][-6:]}{path.suffix}")
            path.write_text(html)
            print(f"  preview : {path}")

        if not args.send:
            print("  DRY RUN — not sent")
            continue

        ok = send_email(r["email"], subject, html, reply_to=reply_to,
                        from_address=sender)
        print(f"  {'SENT' if ok else 'FAILED — check RESEND_API_KEY and logs'}")

    if not args.send:
        print(f"\nDry run over {len(recipients)} recipient(s). "
              f"Re-run with --send to deliver.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
