#!/usr/bin/env python3
"""Re-run name_cleaner over the insiders table and reconcile display_name.

Two jobs:

1. Audit. Stored display_name was written by whatever name_cleaner looked
   like at ingest time. Re-running the current cleaner over every row shows
   exactly where the stored values have drifted from what the code would
   produce today — including whether a cleaner change causes churn far wider
   than intended.

2. Backfill. With --apply, rows are brought in line with current logic. This
   supersedes the one-shot scripts/fix_compound_surnames.py: the reordering
   rule now lives in name_cleaner itself, so backfill and live ingest share
   one implementation instead of two that can disagree.

Slugs are write-once and deliberately NOT recomputed — a name correction must
never rewrite a live URL. Renames get a redirect, never a rewrite.

Usage (on Studio):
    python3 scripts/recompute_display_names.py            # dry run + samples
    python3 scripts/recompute_display_names.py --apply
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402
from strategies.insider_catalog.name_cleaner import clean_name  # noqa: E402


# Rows carrying an obvious company suffix but flagged is_entity=0 get run
# through the PERSON cleaner, which rotates them into nonsense ("Bulldog
# Investors General Partnership" -> "Investors General Partnership Bulldog").
# That mislabel predates this script and is a separate fix; recomputing such a
# row would replace a correct stored value with a wrong one, so they are
# reported and skipped rather than written.
ENTITY_MARKERS = {
    # legal forms, incl. non-US ones that appear in Form 4 reporting-owner rows
    "llc", "l.l.c", "lp", "l.p", "llp", "lllp", "plc", "inc", "corp", "co",
    "ltd", "gmbh", "sa", "sarl", "scsp", "nv", "bv", "ag", "ab", "oy", "as",
    "kg", "spa", "srl", "pte", "pty", "gp", "coop", "cooperatief", "u.a",
    # descriptive words that only ever appear in organization names
    "trust", "fund", "funds", "partnership", "partners", "holdings", "holding",
    "capital", "ventures", "venture", "group", "associates", "management",
    "advisors", "advisers", "investment", "investments", "securities", "bank",
    "insurance", "systems", "technologies", "industries", "enterprises",
    "international", "global", "equity", "asset", "assets",
    # forms seen in the live table that the generic list misses
    "mhc", "lle", "fz", "grat", "conservatorship", "bancorp", "s.a.b", "c.v",
    "gestion", "servicios", "sicav", "spc", "ospc", "reit",
}


def looks_like_entity(name: str) -> bool:
    # Periods are dropped entirely, not just trimmed, so "S.A." matches "sa"
    # and "L.L.C." matches "llc".
    toks = {t.lower().replace(".", "").strip(",") for t in name.split()}
    return bool(toks & {m.replace(".", "") for m in ENTITY_MARKERS})


def classify(raw: str, stored: str, fresh: str) -> str:
    """Bucket a diff so wide churn is distinguishable from the intended fix."""
    if not stored:
        return "was-empty"
    if sorted(stored.lower().replace(".", "").split()) == sorted(fresh.lower().replace(".", "").split()):
        return "reordered"          # same tokens, new order — the compound fix
    if stored.lower().replace(".", "") == fresh.lower().replace(".", ""):
        return "punctuation"
    if stored.lower() == fresh.lower():
        return "casing"
    return "other"                  # tokens gained/lost — inspect before applying


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--samples", type=int, default=6, help="samples per category")
    args = ap.parse_args()

    conn = get_connection()
    rows = conn.execute(
        """SELECT insider_id, name, display_name, COALESCE(is_entity, 0) AS is_entity
             FROM insiders
            WHERE name IS NOT NULL AND name <> ''"""
    ).fetchall()
    print(f"  rows: {len(rows)}")

    counts: Counter = Counter()
    samples: dict[str, list] = {}
    updates: list[tuple[str, int]] = []

    for r in rows:
        fresh = clean_name(r["name"], bool(r["is_entity"]))
        stored = r["display_name"] or ""
        if fresh == stored:
            counts["unchanged"] += 1
            continue
        if not r["is_entity"] and looks_like_entity(r["name"]):
            counts["skipped-entity-mislabel"] += 1
            samples.setdefault("skipped-entity-mislabel", [])
            if len(samples["skipped-entity-mislabel"]) < args.samples:
                samples["skipped-entity-mislabel"].append((r["name"], stored, fresh))
            continue
        cat = classify(r["name"], stored, fresh)
        counts[cat] += 1
        samples.setdefault(cat, [])
        if len(samples[cat]) < args.samples:
            samples[cat].append((r["name"], stored, fresh))
        updates.append((fresh, r["insider_id"]))

    for cat, n in counts.most_common():
        print(f"  {cat:<14} {n}")
    for cat, rec in samples.items():
        print(f"\n  [{cat}]")
        for raw, stored, fresh in rec:
            print(f"    {raw[:30]:<32}{stored[:30]:<32}-> {fresh}")

    if not args.apply:
        print(f"\n  DRY RUN — {len(updates)} row(s) would change; re-run with --apply")
        conn.close()
        return 0

    for i, (fresh, iid) in enumerate(updates, 1):
        conn.execute("UPDATE insiders SET display_name = ? WHERE insider_id = ?", (fresh, iid))
        if i % 20000 == 0:
            conn.commit()
            print(f"  ...{i}/{len(updates)}")
    conn.commit()
    conn.close()
    print(f"  DONE — updated {len(updates)} display_name(s); slugs untouched")
    return 0


if __name__ == "__main__":
    sys.exit(main())
