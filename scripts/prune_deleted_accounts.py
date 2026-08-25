#!/usr/bin/env python3
"""Remove notification data belonging to deleted Clerk accounts.

NOT SCHEDULED, ON PURPOSE. This deletes user data on the strength of a Clerk
404, and a 404 is also what a wrong key, a wrong environment, or a Clerk
incident can look like from the outside. Automating an irreversible delete on
a single external signal is how you lose a real user's watchlist.

Run it, read what it proposes, then pass --apply.

    python3 scripts/prune_deleted_accounts.py           # report only
    python3 scripts/prune_deleted_accounts.py --apply

Found 2026-08-24: 3 of 7 preference rows pointed at deleted accounts, carrying
5,621 notification rows between them. The scanner already skips them, so this
is dead weight rather than a correctness bug — but it inflates every subscriber
count and every notification total.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO / ".env")
except ImportError:
    pass

import os

from config.database import get_connection

#: Never prune these. The alert canary owns a preference row deliberately and
#: is not, and must not be, a Clerk account.
PROTECTED = {"canary__alert_pipeline"}

TABLES = ("notifications", "watchlist", "alert_filters", "notification_preferences")


def clerk_status(user_id: str) -> int:
    req = urllib.request.Request(
        f"https://api.clerk.com/v1/users/{user_id}",
        headers={"Authorization": f"Bearer {os.environ['CLERK_SECRET_KEY']}",
                 "User-Agent": "form4-prune/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0        # unreachable — treat as "do not touch"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    rows = [r["user_id"] for r in conn.execute(
        "SELECT user_id FROM notifications.notification_preferences")]

    gone, kept, unknown = [], [], []
    for uid in rows:
        if uid in PROTECTED:
            kept.append((uid, "protected"))
            continue
        st = clerk_status(uid)
        if st == 404:
            gone.append(uid)
        elif st == 200:
            kept.append((uid, "active"))
        else:
            unknown.append((uid, st))

    print(f"  {len(rows)} preference rows: {len(kept)} kept, {len(gone)} deleted, "
          f"{len(unknown)} unknown")
    for uid, why in kept:
        print(f"    keep   {uid}  ({why})")
    for uid, st in unknown:
        print(f"    SKIP   {uid}  (Clerk returned {st} — not a definite 404)")
    for uid in gone:
        counts = {t: conn.execute(
            f"SELECT count(*) c FROM notifications.{t} WHERE user_id = ?",
            (uid,)).fetchone()["c"] for t in TABLES}
        print(f"    prune  {uid}  " + ", ".join(f"{t}={n}" for t, n in counts.items()))

    if not gone:
        print("  nothing to do")
        return 0
    if not args.apply:
        print("\n  report only — pass --apply to delete")
        return 0

    for uid in gone:
        for t in TABLES:
            conn.execute(f"DELETE FROM notifications.{t} WHERE user_id = ?", (uid,))
    conn.commit()
    print(f"\n  pruned {len(gone)} account(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
