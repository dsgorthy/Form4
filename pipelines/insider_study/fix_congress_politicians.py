"""
Fix 77 unmatched House politicians: dedup name variants + assign party.

Problems:
1. Embedded honorifics: "James E Hon Banks", "Kim Dr Schrier", "Marjorie Taylor Mrs Greene"
2. Trailing punctuation variants: "Beyer, Jr" vs "Beyer, Jr."
3. True duplicates: same person with 2-3 politician_ids
4. All 77 are missing party affiliation

Usage:
    python3 pipelines/insider_study/fix_congress_politicians.py [--dry-run]
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"

# ── Known party affiliations for all 77 unmatched House members ─────────────
# Source: public congressional records
PARTY_MAP: dict[str, str] = {
    "Ada Norah Henriquez": "R",           # PR delegate
    "Anthony E. Gonzalez": "R",           # OH-16
    "Ashley Hinson Arenholz": "R",        # IA-02
    "Aston Donald McEachin": "D",         # VA-04
    "August Lee Pfluger, II": "R",        # TX-11
    "Bradley S. Schneider": "D",          # IL-10
    "Brandon McDonald Williams": "R",     # NY-22
    "C. Scott Franklin": "R",             # FL-18
    "Carol Devine Miller": "R",           # WV-01
    "Christopher L. Jacobs": "R",         # NY-27
    "Cindy Axne": "D",                    # IA-03
    "Dan Daniel Bishop": "R",             # NC-08
    "Daniel Crenshaw": "R",               # TX-02
    "Daniel Goldman": "D",                # NY-10
    "Darin McKay LaHood": "R",            # IL-16
    "Darrell E. Issa": "R",               # CA-48
    "David Cheston Rouzer": "R",          # NC-07
    "David Madison Cawthorn": "R",        # NC-11
    "Donald Sternoff Beyer, Jr": "D",     # VA-08
    "Donald Sternoff Beyer, Jr.": "D",
    "Donald Sternoff Honorable Beyer, Jr": "D",
    "Donald W. Norcross": "D",            # NJ-01
    "Earl Leroy Carter": "R",             # GA-01
    "Felix Barry Moore": "R",             # AL-02
    "Glenn S. Grothman": "R",             # WI-06
    "Greg Francis Murphy": "R",           # NC-03
    "Greg Steube": "R",                   # FL-17
    "Guy Mr Reschenthaler": "R",          # PA-14
    "Harley E. Rouda, Jr": "D",           # CA-48
    "Harley E. Rouda, Jr.": "D",
    "Harold Dallas Rogers": "R",          # KY-05
    "James E Hon Banks": "R",             # IN-03
    "James E. Banks": "R",
    "James French Hill": "R",             # AR-02
    "James Hagedorn": "R",                # MN-01 (deceased)
    "James M. Costa": "D",                # CA-21
    "Joseph P. Kennedy, III": "D",        # MA-04
    "Keith Alan Self": "R",               # TX-03
    "Kelly Louise Morrison": "D",         # MN-05
    "Kenneth R. Buck": "R",               # CO-04
    "Kim Dr Schrier": "D",                # WA-08
    "Laurel Mrs Lee": "R",                # FL-15
    "Linda T. Sanchez": "D",              # CA-38
    "Lloyd K. Smucker": "R",              # PA-11
    "Marjorie Taylor Mrs Greene": "R",    # GA-14
    "Mark Dr Green": "R",                 # TN-07
    "Michael A. Collins": "R",            # GA-10
    "Michael A. Collins, Jr": "R",
    "Michael Fq San Nicolas": "D",        # GU delegate
    "Michael Garcia": "R",                # CA-27
    "Michael John Gallagher": "R",        # WI-08
    "Michael Patrick Guest": "R",         # MS-03
    "Nanette Barragan": "D",              # CA-44
    "Neal Patrick Dunn MD, FACS": "R",    # FL-02
    "Neal Patrick Dunn, MD, FACS": "R",
    "Neal Patrick MD, Facs Dunn": "R",
    "Nicholas V. Taylor": "R",            # TX-03
    "Nicholas Van Taylor": "R",
    "Peter Allen Stauber": "R",           # MN-08
    "Richard B. Reisdorf": "R",           # MN-01
    "Richard Dean Dr McCormick": "R",     # GA-06
    "Richard W. Allen": "R",              # GA-12
    "Ritchie John Torres": "D",           # NY-15
    "Rob Bresnahan": "R",                 # PA-08
    "Roger W. Marshall": "R",             # KS-01 (now Senator)
    "Rohit Khanna": "D",                  # CA-17
    "Rudy C. Yakym, III": "R",            # IN-02
    "S. Raja Krishnamoorthi": "D",        # IL-08
    "Scott Mr Franklin": "R",             # FL-18
    "Scott Scott Franklin": "R",
    "TJ John (Tj) Cox": "D",             # CA-21
    "Thomas H. Kean": "R",               # NJ-07
    "Thomas H. Kean, Jr": "R",
    "Tom O'Halleran": "D",               # AZ-01
    "Tracey Robert Mann": "R",           # KS-01
    "W. Greg Steube": "R",               # FL-17
    "William R. Timmons, IV": "R",       # SC-04
}

# ── Duplicate groups: (canonical_name, [variant_names_to_merge]) ────────────
# The first entry in each group is the primary (keeps its politician_id).
# All others get their congress_trades reassigned to the primary.
MERGE_GROUPS: list[list[str]] = [
    ["Donald Sternoff Beyer, Jr", "Donald Sternoff Beyer, Jr.", "Donald Sternoff Honorable Beyer, Jr"],
    ["Harley E. Rouda, Jr", "Harley E. Rouda, Jr."],
    ["James E. Banks", "James E Hon Banks"],
    ["C. Scott Franklin", "Scott Mr Franklin", "Scott Scott Franklin"],
    ["Neal Patrick Dunn MD, FACS", "Neal Patrick Dunn, MD, FACS", "Neal Patrick MD, Facs Dunn"],
    ["Nicholas Van Taylor", "Nicholas V. Taylor"],
    ["Thomas H. Kean, Jr", "Thomas H. Kean"],
    ["Michael A. Collins", "Michael A. Collins, Jr"],
    ["Greg Steube", "W. Greg Steube"],
]


def clean_politician_name(name: str) -> str:
    """Strip embedded honorifics and normalize whitespace."""
    # Remove common embedded honorifics
    cleaned = re.sub(r'\b(Hon|Honorable|Mr|Mrs|Ms|Dr|MD|FACS|Facs|Fq|M\.?D\.?)\b', '', name, flags=re.IGNORECASE)
    # Remove empty parenthetical
    cleaned = re.sub(r'\(\s*\)', '', cleaned)
    # Clean up trailing/double commas and spaces
    cleaned = re.sub(r',\s*,', ',', cleaned)
    cleaned = re.sub(r',\s*$', '', cleaned)
    cleaned = re.sub(r'\s*,\s*(?=[,\s])', '', cleaned)
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
    # Remove trailing comma before suffix
    cleaned = re.sub(r',\s*$', '', cleaned).strip()
    return cleaned


def run(dry_run: bool = False):
    conn = sqlite3.connect(str(DB_PATH), timeout=300)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=300000")  # 5 min busy timeout

    # ── Step 1: Assign party to all unmatched politicians ───────────────
    print("=== Step 1: Assign party affiliations ===")
    unmatched = conn.execute(
        "SELECT politician_id, name FROM politicians WHERE party IS NULL OR party = ''"
    ).fetchall()

    updated_party = 0
    missing_party = []
    party_updates = []
    for row in unmatched:
        pid, name = row["politician_id"], row["name"]
        party = PARTY_MAP.get(name)
        if party:
            party_updates.append((party, pid))
            updated_party += 1
            print(f"  {name} -> {party}")
        else:
            missing_party.append(name)

    if missing_party:
        print(f"\n  WARNING: {len(missing_party)} politicians still unmatched:")
        for n in missing_party:
            print(f"    {n}")

    if not dry_run and party_updates:
        conn.executemany("UPDATE politicians SET party = ? WHERE politician_id = ?", party_updates)
        conn.commit()
    print(f"\n  Updated party for {updated_party} politicians")

    # ── Step 2: Merge duplicate politicians ─────────────────────────────
    print("\n=== Step 2: Merge duplicate politicians ===")
    merged_count = 0
    trades_reassigned = 0

    for group in MERGE_GROUPS:
        primary_name = group[0]
        variant_names = group[1:]

        primary = conn.execute(
            "SELECT politician_id FROM politicians WHERE name = ?", (primary_name,)
        ).fetchone()

        if not primary:
            print(f"  SKIP: Primary '{primary_name}' not found")
            continue

        primary_id = primary["politician_id"]

        for variant in variant_names:
            variant_row = conn.execute(
                "SELECT politician_id FROM politicians WHERE name = ?", (variant,)
            ).fetchone()

            if not variant_row:
                print(f"  SKIP: Variant '{variant}' not found")
                continue

            variant_id = variant_row["politician_id"]

            # Count trades to reassign
            trade_count = conn.execute(
                "SELECT COUNT(*) as n FROM congress_trades WHERE politician_id = ?",
                (variant_id,),
            ).fetchone()["n"]

            print(f"  Merge: '{variant}' (id={variant_id}, {trade_count} trades) -> '{primary_name}' (id={primary_id})")

            if not dry_run:
                # Reassign trades
                conn.execute(
                    "UPDATE congress_trades SET politician_id = ? WHERE politician_id = ?",
                    (primary_id, variant_id),
                )
                # Delete the variant politician
                conn.execute("DELETE FROM politicians WHERE politician_id = ?", (variant_id,))

            merged_count += 1
            trades_reassigned += trade_count

    if not dry_run:
        conn.commit()
    print(f"\n  Merged {merged_count} duplicate politicians, reassigned {trades_reassigned} trades")

    # ── Step 3: Clean politician names (strip honorifics) ───────────────
    print("\n=== Step 3: Clean politician names ===")
    # Only clean names that have embedded honorifics
    honorific_names = conn.execute("""
        SELECT politician_id, name, name_normalized
        FROM politicians
        WHERE name LIKE '% Hon %' OR name LIKE '% Mr %' OR name LIKE '% Mrs %'
           OR name LIKE '% Dr %' OR name LIKE '% MD,%' OR name LIKE '% FACS%'
           OR name LIKE '% Honorable %' OR name LIKE '% Facs %'
           OR name LIKE '% Fq %'
    """).fetchall()

    cleaned = 0
    for row in honorific_names:
        pid = row["politician_id"]
        old_name = row["name"]
        new_name = clean_politician_name(old_name)
        new_normalized = new_name.lower().strip()

        if new_name != old_name:
            print(f"  '{old_name}' -> '{new_name}'")
            if not dry_run:
                # Check if cleaned name conflicts with existing
                existing = conn.execute(
                    "SELECT politician_id FROM politicians WHERE name_normalized = ? AND chamber = 'House'",
                    (new_normalized,),
                ).fetchone()
                if existing and existing["politician_id"] != pid:
                    # Merge into existing
                    trade_count = conn.execute(
                        "SELECT COUNT(*) as n FROM congress_trades WHERE politician_id = ?", (pid,)
                    ).fetchone()["n"]
                    print(f"    -> Merging into existing id={existing['politician_id']} ({trade_count} trades)")
                    conn.execute(
                        "UPDATE congress_trades SET politician_id = ? WHERE politician_id = ?",
                        (existing["politician_id"], pid),
                    )
                    conn.execute("DELETE FROM politicians WHERE politician_id = ?", (pid,))
                else:
                    conn.execute(
                        "UPDATE politicians SET name = ?, name_normalized = ? WHERE politician_id = ?",
                        (new_name, new_normalized, pid),
                    )
            cleaned += 1

    if not dry_run:
        conn.commit()
    print(f"\n  Cleaned {cleaned} politician names")

    # ── Summary ─────────────────────────────────────────────────────────
    print("\n=== Final State ===")
    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN party IS NOT NULL AND party != '' THEN 1 ELSE 0 END) as has_party,
            SUM(CASE WHEN party IS NULL OR party = '' THEN 1 ELSE 0 END) as missing_party
        FROM politicians
    """).fetchone()
    print(f"  Total politicians: {stats['total']}")
    print(f"  With party: {stats['has_party']}")
    print(f"  Missing party: {stats['missing_party']}")

    still_missing = conn.execute(
        "SELECT name, state, chamber FROM politicians WHERE party IS NULL OR party = ''"
    ).fetchall()
    if still_missing:
        print(f"\n  Still missing party ({len(still_missing)}):")
        for r in still_missing:
            print(f"    {r['name']} ({r['state']}, {r['chamber']})")

    conn.close()
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}Done.")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    run(dry_run=dry_run)
