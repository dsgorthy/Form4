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

# Replies should reach a person, not the alerts@ sender the templates use.
REPLY_TO = "derek@sidequestgroup.com"


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
    args = p.parse_args()

    if not CLERK_SECRET_KEY:
        sys.exit("CLERK_SECRET_KEY not set — check .env")

    recipients = active_comps(args.reason)
    if not recipients:
        print("No users hold an active comp"
              + (f" with reason {args.reason!r}" if args.reason else "") + ".")
        return 0

    for r in recipients:
        until = datetime.strptime(r["pro_until"], "%Y-%m-%d").strftime("%B %-d")
        unsub = ""
        token = generate_unsubscribe_token(r["id"])
        if token:
            unsub = (f"{APP_URL}/api/v1/notifications/unsubscribe"
                     f"?user_id={r['id']}&token={token}")

        subject, html = comp_grant_email(
            first_name=r["first_name"], until_date=until, unsubscribe_url=unsub,
        )

        print(f"\n{r['email']}  ({r['id']})")
        print(f"  name    : {r['first_name'] or '(none — falls back to \"Hi there\")'}")
        print(f"  subject : {subject}")
        print(f"  comp ends: {r['pro_until']}  →  rendered as \"{until}\"")
        print(f"  reply-to: {REPLY_TO}")

        if args.write_preview:
            path = Path(args.write_preview)
            if len(recipients) > 1:
                path = path.with_name(f"{path.stem}-{r['first_name'] or r['id'][-6:]}{path.suffix}")
            path.write_text(html)
            print(f"  preview : {path}")

        if not args.send:
            print("  DRY RUN — not sent")
            continue

        ok = send_email(r["email"], subject, html, reply_to=REPLY_TO)
        print(f"  {'SENT' if ok else 'FAILED — check RESEND_API_KEY and logs'}")

    if not args.send:
        print(f"\nDry run over {len(recipients)} recipient(s). "
              f"Re-run with --send to deliver.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
