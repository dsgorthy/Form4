#!/usr/bin/env python3
"""Assign write-once URL slugs to insiders.

Insider URLs carry the legal name for SEO. Measured collision rate on the
name alone is 0.71%, so the overwhelming majority get a clean
/insider/roger-s-penske; the rest keep the disambiguated
/insider/{name}-{sqid} form.

WRITE-ONCE. A row that already has a slug is never touched, even if the
underlying name changes. SEC filings spell the same person inconsistently,
so regenerating slugs from names would silently rewrite live URLs and throw
away accumulated ranking. Renames get a redirect, never a rewrite.

Conflict rule: lowest insider_id wins the clean slug. insider_id is
immutable, unlike trade count, so today's clean URL stays clean even if a
more prominent namesake files tomorrow.

The slugify here MUST match slugifyName() in frontend/src/lib/insider-url.ts
or generated links will not match stored slugs.

Usage (on Studio):
    python3 scripts/backfill_insider_slugs.py            # dry run
    python3 scripts/backfill_insider_slugs.py --apply
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
    s = s.strip("-")[:MAX_SLUG]
    return s.rstrip("-")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="cap rows (testing)")
    args = ap.parse_args()

    conn = get_connection()

    # Lowest insider_id first == the clean-slug winner is deterministic.
    rows = conn.execute(
        """SELECT insider_id, COALESCE(display_name, name) AS nm
             FROM insiders
            WHERE slug IS NULL
              AND COALESCE(display_name, name) IS NOT NULL
              AND COALESCE(display_name, name) <> ''
            ORDER BY insider_id"""
    ).fetchall()
    if args.limit:
        rows = rows[: args.limit]
    print(f"  candidates without a slug: {len(rows)}")

    # Slugs already taken by previous runs must not be reissued.
    taken = {
        r["slug"] for r in conn.execute(
            "SELECT slug FROM insiders WHERE slug IS NOT NULL"
        ).fetchall()
    }
    print(f"  slugs already assigned    : {len(taken)}")

    clean = suffixed = skipped = 0
    updates: list[tuple[str, int]] = []

    for r in rows:
        base = slugify(r["nm"])
        if not base:
            skipped += 1          # unnamed entities keep the bare-ID URL
            continue
        if base not in taken:
            slug = base
            clean += 1
        else:
            # Deterministic and permanent: the sqid never changes.
            slug = f"{base}-{encode_insider_id(r['insider_id'])}"
            if slug in taken:
                skipped += 1
                continue
            suffixed += 1
        taken.add(slug)
        updates.append((slug, r["insider_id"]))

    print(f"  clean slugs   : {clean}")
    print(f"  suffixed      : {suffixed}")
    print(f"  skipped       : {skipped}")
    for slug, iid in updates[:5]:
        print(f"     e.g. {iid} -> {slug}")

    if not args.apply:
        print("  DRY RUN — re-run with --apply to write")
        conn.close()
        return 0

    for i, (slug, iid) in enumerate(updates, 1):
        conn.execute("UPDATE insiders SET slug = ? WHERE insider_id = ? AND slug IS NULL",
                     (slug, iid))
        if i % 20000 == 0:
            conn.commit()
            print(f"  ...{i}/{len(updates)}")
    conn.commit()
    conn.close()
    print(f"  DONE — assigned {len(updates)} slug(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
