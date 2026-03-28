#!/usr/bin/env python3
"""
Backfill party affiliations for House politicians from unitedstates/congress-legislators.

Downloads current + historical legislator YAML files and matches by name + state
to fill in NULL party values for ~279 House members.

Usage:
    python3 pipelines/insider_study/backfill_congress_party.py [--dry-run]
"""

import argparse
import logging
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"

# URLs for legislator data
CURRENT_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"
HISTORICAL_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-historical.yaml"


def download_legislators() -> List[dict]:
    """Download and parse legislator YAML data."""
    import urllib.request

    try:
        import yaml
    except ImportError:
        logger.error("PyYAML not installed. Run: pip3 install pyyaml")
        raise SystemExit(1)

    all_legislators = []

    for label, url in [("current", CURRENT_URL), ("historical", HISTORICAL_URL)]:
        logger.info("Downloading %s legislators from GitHub...", label)
        req = urllib.request.Request(url, headers={"User-Agent": "trading-framework/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = yaml.safe_load(resp.read())
        logger.info("  Got %d %s legislators", len(data), label)
        all_legislators.extend(data)

    return all_legislators


def build_lookup(legislators: List[dict]) -> Dict[str, str]:
    """
    Build a lookup from (normalized_name, state) -> party.

    For legislators with multiple terms, use the most recent term's party.
    """
    lookup: Dict[str, str] = {}  # key: "name_normalized|state" -> party letter

    for leg in legislators:
        name_info = leg.get("name", {})
        terms = leg.get("terms", [])
        if not terms:
            continue

        # Get the most recent term
        latest_term = terms[-1]
        party_full = latest_term.get("party", "")
        state = latest_term.get("state", "")

        # Map to party letter
        party_map = {"Republican": "R", "Democrat": "D", "Independent": "I"}
        party = party_map.get(party_full, party_full[:1] if party_full else None)

        if not party or not state:
            continue

        # Build multiple name variations for matching
        first = name_info.get("first", "")
        last = name_info.get("last", "")
        official = name_info.get("official_full", "")
        nickname = name_info.get("nickname", "")

        name_variants = set()
        if first and last:
            name_variants.add(f"{first} {last}".lower().strip())
            name_variants.add(f"{last}, {first}".lower().strip())
        if official:
            name_variants.add(official.lower().strip())
        if nickname and last:
            name_variants.add(f"{nickname} {last}".lower().strip())

        # Also handle suffixes
        suffix = name_info.get("suffix", "")
        if suffix and first and last:
            name_variants.add(f"{first} {last}, {suffix}".lower().strip())
            name_variants.add(f"{first} {last} {suffix}".lower().strip())

        for nv in name_variants:
            key = f"{nv}|{state.lower()}"
            lookup[key] = party

    logger.info("Built lookup with %d name+state entries", len(lookup))
    return lookup


def normalize_for_match(name: str) -> str:
    """Normalize a politician name for matching."""
    n = name.lower().strip()
    # Remove common prefixes
    n = re.sub(r'^(hon\.|honorable|rep\.|representative)\s+', '', n)
    # Remove extra whitespace
    n = re.sub(r'\s+', ' ', n)
    return n


def backfill_parties(conn: sqlite3.Connection, lookup: Dict[str, str], dry_run: bool = False) -> dict:
    """Update party for politicians with NULL party."""
    # Get politicians with missing party
    missing = conn.execute("""
        SELECT politician_id, name, name_normalized, state, chamber
        FROM politicians
        WHERE party IS NULL
    """).fetchall()

    logger.info("Politicians with missing party: %d", len(missing))

    updated = 0
    unmatched = []

    for pid, name, name_norm, state, chamber in missing:
        if not state:
            unmatched.append((name, state, chamber))
            continue

        # Try multiple matching strategies
        matched_party = None
        name_clean = normalize_for_match(name)

        # Strategy 1: exact name + state
        key = f"{name_clean}|{state.lower()}"
        if key in lookup:
            matched_party = lookup[key]

        # Strategy 2: normalized name + state
        if not matched_party:
            key = f"{name_norm}|{state.lower()}"
            if key in lookup:
                matched_party = lookup[key]

        # Strategy 3: try removing suffix
        if not matched_party:
            name_no_suffix = re.sub(r',?\s*(jr\.?|sr\.?|iii|ii|iv|v)\s*$', '', name_clean, flags=re.IGNORECASE)
            key = f"{name_no_suffix}|{state.lower()}"
            if key in lookup:
                matched_party = lookup[key]

        if matched_party:
            if not dry_run:
                conn.execute(
                    "UPDATE politicians SET party = ? WHERE politician_id = ?",
                    (matched_party, pid),
                )
            updated += 1
        else:
            unmatched.append((name, state, chamber))

    if not dry_run:
        conn.commit()

    logger.info("Updated %d politicians with party data", updated)
    if unmatched:
        logger.info("Could not match %d politicians:", len(unmatched))
        for name, state, chamber in unmatched[:10]:
            logger.info("  %s (%s, %s)", name, state or "??", chamber)
        if len(unmatched) > 10:
            logger.info("  ... and %d more", len(unmatched) - 10)

    return {
        "missing_before": len(missing),
        "updated": updated,
        "still_missing": len(unmatched),
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill congress party affiliations")
    parser.add_argument("--dry-run", action="store_true", help="Report matches without updating")
    parser.add_argument("--db-path", type=str, default=str(DB_PATH))
    args = parser.parse_args()

    legislators = download_legislators()
    lookup = build_lookup(legislators)

    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    stats = backfill_parties(conn, lookup, dry_run=args.dry_run)

    print(f"\n{'='*50}")
    print(f"PARTY BACKFILL {'(DRY RUN) ' if args.dry_run else ''}RESULTS")
    print(f"{'='*50}")
    print(f"Missing before: {stats['missing_before']}")
    print(f"Updated:        {stats['updated']}")
    print(f"Still missing:  {stats['still_missing']}")
    print(f"{'='*50}")

    conn.close()


if __name__ == "__main__":
    main()
