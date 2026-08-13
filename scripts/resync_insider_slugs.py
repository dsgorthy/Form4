#!/usr/bin/env python3
"""Bring insider slugs back in line with corrected display names.

Slugs were originally write-once: never regenerate, because rewriting a live
URL throws away accumulated ranking. That protected URLs but froze mistakes —
correcting "Prestridge III John R" to "John R. Prestridge III" left the page
stranded at /insider/iii-john-r-prestridge.

insider_slug_aliases (migration 2026-08-14) changes the invariant to "never
BREAK a URL". Every slug this script retires is recorded as an alias first, so
the old URL keeps resolving and 301s to the new one. Only then is the slug
rewritten.

Safe to re-run: a row already matching its name is skipped, and an alias that
already exists is left alone.

Conflict rule matches backfill_insider_slugs.py — lowest insider_id wins the
clean slug; a loser takes the sqid-suffixed form. A slug currently held by
another insider is never stolen.

Usage (on Studio):
    python3 scripts/resync_insider_slugs.py            # dry run
    python3 scripts/resync_insider_slugs.py --apply
"""
from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.id_encoding import encode_insider_id  # noqa: E402
from config.database import get_connection  # noqa: E402

MAX_SLUG = 60


def slugify(name: str) -> str:
    """Mirror of slugifyName() in frontend/src/lib/insider-url.ts."""
    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:MAX_SLUG].rstrip("-")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--samples", type=int, default=12)
    args = ap.parse_args()

    conn = get_connection()
    rows = conn.execute(
        """SELECT insider_id, COALESCE(display_name, name) AS nm, slug
             FROM insiders
            WHERE slug IS NOT NULL
              AND COALESCE(display_name, name) IS NOT NULL
              AND COALESCE(display_name, name) <> ''
            ORDER BY insider_id"""
    ).fetchall()

    # Every slug in play, so a regenerated one never collides with a live page.
    taken = {r["slug"] for r in rows}
    aliased = {
        r["old_slug"] for r in conn.execute(
            "SELECT old_slug FROM insider_slug_aliases"
        ).fetchall()
    }

    plan: list[tuple[int, str, str]] = []   # (insider_id, old_slug, new_slug)
    for r in rows:
        base = slugify(r["nm"])
        if not base or r["slug"] == base:
            continue
        # Already the disambiguated form of the SAME name — nothing stale.
        if r["slug"] == f"{base}-{encode_insider_id(r['insider_id'])}":
            continue
        new = base if base not in taken else f"{base}-{encode_insider_id(r['insider_id'])}"
        if new in taken or new in aliased:
            continue        # would collide with a live page or a retired URL
        taken.discard(r["slug"])
        taken.add(new)
        plan.append((r["insider_id"], r["slug"], new))

    print(f"  slugs to resync: {len(plan)}")
    for iid, old, new in plan[: args.samples]:
        print(f"    {old:<44} -> {new}")

    if not args.apply:
        print("  DRY RUN — re-run with --apply to write")
        conn.close()
        return 0

    for iid, old, new in plan:
        # Alias FIRST: if this run dies midway, the old URL still resolves.
        conn.execute(
            "INSERT INTO insider_slug_aliases (old_slug, insider_id) VALUES (?, ?) "
            "ON CONFLICT DO NOTHING",
            (old, iid),
        )
        conn.execute("UPDATE insiders SET slug = ? WHERE insider_id = ?", (new, iid))
    conn.commit()
    conn.close()
    print(f"  DONE — resynced {len(plan)} slug(s); {len(plan)} alias(es) recorded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
