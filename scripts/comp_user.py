#!/usr/bin/env python3
"""Comp a user to Pro for a fixed window, or revoke a comp.

Clerk is the system of record for tier — there is no subscriptions table, and
the only other writer of ``public_metadata.tier`` is the Stripe webhook. This
script writes that same field plus ``pro_until``, which ``api.comp.comp_lapsed``
reads to expire the comp on its own (mirrored in the frontend by ``compLapsed``
in src/lib/subscription.ts). Nothing else needs to run for the comp to end.

Dry-run by default. Pass --apply to actually write.

    # preview a 3-month comp
    python3 scripts/comp_user.py --email someone@example.com --months 3 \
        --reason july-signup-outreach

    # write it
    python3 scripts/comp_user.py --email someone@example.com --months 3 \
        --reason july-signup-outreach --apply

    # end a comp early (drops to free, or back to trial/grace if new)
    python3 scripts/comp_user.py --email someone@example.com --revoke --apply

    # see who currently holds a comp
    python3 scripts/comp_user.py --list
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import os  # noqa: E402  (after load_dotenv)

from api.comp import comp_lapsed  # noqa: E402

CLERK_API = "https://api.clerk.com/v1"
CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY", "")

# Keys this script owns. Everything else in public_metadata is preserved —
# stripe_customer_id in particular must survive, or the billing portal breaks.
COMP_KEYS = ("tier", "pro_until", "comp_reason", "comped_at")


def _headers() -> dict:
    if not CLERK_SECRET_KEY:
        sys.exit("CLERK_SECRET_KEY not set — check .env")
    return {"Authorization": f"Bearer {CLERK_SECRET_KEY}"}


def fetch_users() -> list[dict]:
    resp = httpx.get(f"{CLERK_API}/users", params={"limit": 100},
                     headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def resolve(users: list[dict], ident: str) -> dict:
    """Find a user by Clerk ID or email address."""
    for u in users:
        if u["id"] == ident:
            return u
        if any(e["email_address"].lower() == ident.lower()
               for e in u.get("email_addresses", [])):
            return u
    sys.exit(f"No Clerk user matches {ident!r}")


def email_of(user: dict) -> str:
    addrs = user.get("email_addresses", [])
    return addrs[0]["email_address"] if addrs else "(no email)"


def describe(meta: dict) -> str:
    tier = meta.get("tier") or "free"
    until = meta.get("pro_until")
    if not until:
        return tier
    state = "LAPSED" if comp_lapsed(meta) else "active"
    return f"{tier} until {until} ({state})"


def write_metadata(user_id: str, meta: dict) -> dict:
    """PATCH the full public_metadata object.

    Clerk replaces public_metadata wholesale on this endpoint, so callers must
    pass an already-merged dict. Same endpoint the Stripe webhook uses.
    """
    resp = httpx.patch(f"{CLERK_API}/users/{user_id}",
                       json={"public_metadata": meta},
                       headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json().get("public_metadata", {})


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--email", "--user", dest="idents", action="append", default=[],
                   metavar="EMAIL_OR_CLERK_ID",
                   help="who to comp; repeat for multiple users")
    p.add_argument("--months", type=int, help="comp length in months (30d each)")
    p.add_argument("--until", metavar="YYYY-MM-DD", help="explicit end date")
    p.add_argument("--reason", default="", help="recorded as comp_reason")
    p.add_argument("--tier", default="pro", choices=("pro", "pro_plus"))
    p.add_argument("--revoke", action="store_true", help="end a comp now")
    p.add_argument("--list", action="store_true", help="show all users and tiers")
    p.add_argument("--apply", action="store_true", help="write (default: dry run)")
    args = p.parse_args()

    users = fetch_users()

    if args.list:
        print(f"{'clerk id':<36} {'email':<34} tier")
        print("-" * 88)
        for u in sorted(users, key=lambda x: x["created_at"]):
            print(f"{u['id']:<36} {email_of(u):<34} "
                  f"{describe(u.get('public_metadata') or {})}")
        return 0

    if not args.idents:
        p.error("give at least one --email/--user, or use --list")

    if not args.revoke:
        if bool(args.months) == bool(args.until):
            p.error("give exactly one of --months or --until")
        if args.until:
            try:
                datetime.strptime(args.until, "%Y-%m-%d")
            except ValueError:
                p.error(f"--until must be YYYY-MM-DD, got {args.until!r}")
            until = args.until
        else:
            until = (datetime.now(timezone.utc)
                     + timedelta(days=30 * args.months)).strftime("%Y-%m-%d")

    for ident in args.idents:
        user = resolve(users, ident)
        current = dict(user.get("public_metadata") or {})
        merged = dict(current)

        if args.revoke:
            for k in COMP_KEYS:
                merged.pop(k, None)
        else:
            merged.update({
                "tier": args.tier,
                "pro_until": until,
                "comp_reason": args.reason,
                "comped_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            })

        print(f"\n{email_of(user)}  ({user['id']})")
        print(f"  before: {describe(current)}")
        print(f"  after : {describe(merged)}")
        if current.get("stripe_customer_id"):
            print(f"  note  : has stripe_customer_id "
                  f"{current['stripe_customer_id']} — preserved, but a live "
                  f"Stripe subscription event will overwrite tier")

        if not args.apply:
            print(f"  DRY RUN — would PATCH {json.dumps(merged)}")
            continue

        result = write_metadata(user["id"], merged)
        print(f"  WROTE  {json.dumps(result)}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to write.")
    else:
        print("\nDone. Clerk metadata is cached 60s by the API "
              "(api/auth.py:_CACHE_TTL); users may need to re-open the app.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
