#!/usr/bin/env python3
"""Flip insiders that are plainly organizations but stored as is_entity=0.

The misclassification is visible on the site: a person-cleaned entity gets its
first token rotated to the end, so "Bulldog Investors General Partnership"
renders as "Investors General Partnership Bulldog". recompute_display_names.py
refuses to touch those rows precisely because the person cleaner would make
them worse, which leaves them stuck until the CLASSIFIER is fixed.

Classification stays in entity_resolution.is_entity_name — the one definition
used by the ingest path — so a name fixed here is also classified correctly
the next time it is seen. This script only applies that definition to rows
already in the table.

Monotonic by design: it flips 0 -> 1 and never 1 -> 0. A row deliberately
marked an entity is never argued with, and a bad new pattern can only
over-capture, which the dry run is there to catch.

is_entity is not cosmetic — leaderboard.py and companies.py both filter on it,
so a flipped row also drops off the insider leaderboard. That is the correct
outcome for an LLC, but it is a visible change, so the dry run reports any
flip that currently holds a leaderboard score.

Usage (on Studio):
    python3 scripts/reclassify_entities.py            # dry run
    python3 scripts/reclassify_entities.py --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# entity_resolution does `from backfill import ...`, which only resolves when
# its own directory is importable.
sys.path.insert(0, str(ROOT / "strategies" / "insider_catalog"))

from config.database import get_connection  # noqa: E402
from strategies.insider_catalog.entity_resolution import is_entity_name  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--samples", type=int, default=25)
    args = ap.parse_args()

    conn = get_connection()
    rows = conn.execute(
        """SELECT insider_id, name, display_name
             FROM insiders
            WHERE COALESCE(is_entity, 0) = 0 AND name IS NOT NULL AND name <> ''"""
    ).fetchall()

    flips = [r for r in rows if is_entity_name(r["name"])]
    print(f"  person-classified rows : {len(rows)}")
    print(f"  would flip to entity   : {len(flips)}")

    # A flip removes the row from the leaderboard. Surface any that are
    # actually ranked today so the blast radius is known before writing.
    ranked = []
    if flips:
        ids = [r["insider_id"] for r in flips]
        marks = ",".join("?" for _ in ids)
        ranked = conn.execute(
            f"""SELECT DISTINCT i.insider_id, COALESCE(i.display_name, i.name) AS nm
                  FROM insiders i
                  JOIN insider_ticker_scores s ON s.insider_id = i.insider_id
                 WHERE i.insider_id IN ({marks})""",
            ids,
        ).fetchall()
    print(f"  ...of which are scored : {len(ranked)}")
    for r in ranked[:10]:
        print(f"      scored: {r['nm']}")

    print(f"  {'raw name':<40}currently displays as")
    for r in flips[: args.samples]:
        print(f"    {r['name'][:38]:<40}{r['display_name']}")

    if not args.apply:
        print("  DRY RUN — re-run with --apply to write")
        conn.close()
        return 0

    for r in flips:
        conn.execute(
            "UPDATE insiders SET is_entity = 1 WHERE insider_id = ?", (r["insider_id"],)
        )
    conn.commit()
    conn.close()
    print(f"  DONE — reclassified {len(flips)} row(s) as entities")
    print("  next: python3 scripts/recompute_display_names.py --apply")
    return 0


if __name__ == "__main__":
    sys.exit(main())
