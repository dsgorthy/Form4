#!/usr/bin/env python3
"""
Entity Resolution for the Insider Catalog.

Detects entity insiders (trusts, LLCs, funds, etc.) and links them to
the controlling individual insider so that track records are consolidated.

Three resolution passes:
  A. Accession-based: entity + individual share same Form 4 accession number
  B. Name substring: entity name contains a person's name (e.g., "Jane Smith Trust")
  C. Trade overlap: exact match on (ticker, trade_date, trade_type, value)

Usage:
  python entity_resolution.py                           # default DB path
  python entity_resolution.py --db-path /path/to/db
  INSIDER_DEDUP=1 python entity_resolution.py           # feature flag acknowledged
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.database import get_connection

from backfill import DB_PATH, normalize_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Entity detection patterns ────────────────────────────────────────────

ENTITY_PATTERNS = [
    r'\bllc\b', r'\bl\.?l\.?c\.?\b', r'\bl\.?p\.?\b', r'\btrust\b',
    r'\bfund\b', r'\bholdings?\b', r'\binc\.?\b', r'\bcorp\.?\b',
    r'\bcorporation\b', r'\bcapital\b', r'\bpartners?\b', r'\bgroup\b',
    r'\bmanagement\b', r'\binvestments?\b', r'\blimited\b', r'\bltd\.?\b',
    r'\benterprise[s]?\b', r'\bassociates?\b', r'\badvisors?\b',
    r'\bfamily\b', r'\bestate\b', r'\bfoundation\b', r'\bventures?\b',
]

_ENTITY_RE = re.compile('|'.join(ENTITY_PATTERNS), re.IGNORECASE)


def is_entity_name(name: str) -> bool:
    """Check if a normalized insider name looks like an entity (trust, LLC, etc.)."""
    if not name:
        return False
    name_norm = normalize_name(name)
    return bool(_ENTITY_RE.search(name_norm))


# ── Schema migration ────────────────────────────────────────────────────

def ensure_schema(conn):
    """Safely add entity resolution columns to existing tables.

    Uses try/except to handle 'duplicate column' errors so this is
    idempotent and safe to run on every startup.
    """
    # Add is_entity column to insiders
    try:
        conn.execute("ALTER TABLE insiders ADD COLUMN is_entity INTEGER NOT NULL DEFAULT 0")
        logger.info("Added is_entity column to insiders")
    except Exception as e:
        if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
            pass  # already exists
        else:
            raise

    # Add index on is_entity
    conn.execute("CREATE INDEX IF NOT EXISTS idx_insiders_entity ON insiders(is_entity)")

    # Add effective_insider_id column to trades
    try:
        conn.execute("ALTER TABLE trades ADD COLUMN effective_insider_id INTEGER")
        logger.info("Added effective_insider_id column to trades")
    except Exception as e:
        if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
            pass  # already exists
        else:
            raise

    # Add index on effective_insider_id
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_effective ON trades(effective_insider_id)")

    # Create group tables if they don't exist
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS insider_groups (
            group_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            primary_insider_id INTEGER NOT NULL REFERENCES insiders(insider_id),
            group_name      TEXT    NOT NULL,
            confidence      REAL    NOT NULL DEFAULT 1.0,
            method          TEXT    NOT NULL,
            created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_groups_primary ON insider_groups(primary_insider_id);

        CREATE TABLE IF NOT EXISTS insider_group_members (
            group_id        INTEGER NOT NULL REFERENCES insider_groups(group_id),
            insider_id      INTEGER NOT NULL REFERENCES insiders(insider_id),
            is_primary      INTEGER NOT NULL DEFAULT 0,
            is_entity       INTEGER NOT NULL DEFAULT 0,
            relationship    TEXT,
            PRIMARY KEY (group_id, insider_id)
        );
        CREATE INDEX IF NOT EXISTS idx_group_members_insider ON insider_group_members(insider_id);
    """)

    conn.commit()
    logger.info("Entity resolution schema verified")


# ── Flag entity insiders ─────────────────────────────────────────────────

def flag_entity_insiders(conn) -> int:
    """Flag insiders whose names match entity patterns. Returns count flagged."""
    # Reset all to 0 first for a clean pass
    conn.execute("UPDATE insiders SET is_entity = 0")

    # Fetch all insiders
    rows = conn.execute("SELECT insider_id, name_normalized FROM insiders").fetchall()

    flagged = 0
    for insider_id, name_norm in rows:
        if _ENTITY_RE.search(name_norm or ""):
            conn.execute("UPDATE insiders SET is_entity = 1 WHERE insider_id = ?", (insider_id,))
            flagged += 1

    conn.commit()
    logger.info("Flagged %d entity insiders out of %d total", flagged, len(rows))
    return flagged


# ── Pass A: Link by accession ───────────────────────────────────────────

def link_by_accession(conn) -> int:
    """Find entity/individual pairs sharing the same Form 4 accession number.

    Co-filings on the same Form 4 indicate the entity is controlled by
    the individual. Creates insider_groups with method='accession'.

    Returns count of groups created.
    """
    # Find accession numbers that have both entity and individual filers
    pairs = conn.execute("""
        SELECT t_entity.insider_id AS entity_id,
               t_indiv.insider_id AS indiv_id,
               i_entity.name_normalized AS entity_name,
               i_indiv.name_normalized AS indiv_name,
               t_entity.accession
        FROM trades t_entity
        JOIN trades t_indiv ON t_entity.accession = t_indiv.accession
            AND t_entity.insider_id != t_indiv.insider_id
        JOIN insiders i_entity ON t_entity.insider_id = i_entity.insider_id
        JOIN insiders i_indiv ON t_indiv.insider_id = i_indiv.insider_id
        WHERE i_entity.is_entity = 1
          AND i_indiv.is_entity = 0
          AND t_entity.accession IS NOT NULL
          AND t_entity.accession != ''
        GROUP BY t_entity.insider_id, t_indiv.insider_id
    """).fetchall()

    # Group by (entity_id, indiv_id) pair — one group per unique pair
    seen_pairs = set()
    groups_created = 0

    for entity_id, indiv_id, entity_name, indiv_name, accession in pairs:
        pair_key = (entity_id, indiv_id)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        group_name = f"{indiv_name} + {entity_name}"
        cur = conn.execute("""
            INSERT INTO insider_groups (primary_insider_id, group_name, confidence, method)
            VALUES (?, ?, 0.95, 'accession')
            RETURNING group_id
        """, (indiv_id, group_name))
        group_id = cur.lastrowid

        # Add individual as primary member
        conn.execute("""
            INSERT OR IGNORE INTO insider_group_members
                (group_id, insider_id, is_primary, is_entity, relationship)
            VALUES (?, ?, 1, 0, 'controlling_person')
        """, (group_id, indiv_id))

        # Add entity as member
        conn.execute("""
            INSERT OR IGNORE INTO insider_group_members
                (group_id, insider_id, is_primary, is_entity, relationship)
            VALUES (?, ?, 0, 1, 'entity')
        """, (group_id, entity_id))

        groups_created += 1

    conn.commit()
    logger.info("Pass A (accession): created %d groups from %d candidate pairs",
                groups_created, len(pairs))
    return groups_created


# ── Pass B: Link by name substring ──────────────────────────────────────

def _extract_person_name(entity_name: str) -> str | None:
    """Try to extract a person's name from an entity name.

    Examples:
      'jane smith trust' -> 'jane smith'
      'john doe family llc' -> 'john doe'
      'smith holdings' -> None (ambiguous, just a surname)
    """
    name = entity_name.lower().strip()

    # Remove entity suffixes
    for pattern in ENTITY_PATTERNS:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    # Remove common connector words
    name = re.sub(r'\b(the|of|for|and|irrevocable|revocable|living|dated|dtd|u/a)\b', '', name)

    # Remove dates (e.g., "01/15/2020", "2020")
    name = re.sub(r'\d{1,2}/\d{1,2}/\d{2,4}', '', name)
    name = re.sub(r'\b\d{4}\b', '', name)

    # Clean up
    name = re.sub(r'[^a-z\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    # Must have at least two name parts (first + last) to be useful
    parts = name.split()
    if len(parts) >= 2:
        return name
    return None


def link_by_name_substring(conn) -> int:
    """Link entities to individuals by extracting person names from entity names.

    For each entity insider, extract a candidate person name and look for
    matching individual insiders who also share at least one common ticker.

    Creates groups with method='name_substring', confidence=0.8.
    Returns count of groups created.
    """
    # Get all entity insiders not already in a group
    entities = conn.execute("""
        SELECT i.insider_id, i.name_normalized
        FROM insiders i
        WHERE i.is_entity = 1
          AND i.insider_id NOT IN (
              SELECT insider_id FROM insider_group_members
          )
    """).fetchall()

    groups_created = 0

    for entity_id, entity_name in entities:
        person_name = _extract_person_name(entity_name)
        if not person_name:
            continue

        # Find individual insiders whose name matches the extracted person name
        # and who share at least one common ticker
        candidates = conn.execute("""
            SELECT DISTINCT i.insider_id, i.name_normalized
            FROM insiders i
            WHERE i.is_entity = 0
              AND i.name_normalized LIKE ?
              AND i.insider_id IN (
                  SELECT t2.insider_id FROM trades t2
                  WHERE t2.ticker IN (
                      SELECT t1.ticker FROM trades t1 WHERE t1.insider_id = ?
                  )
              )
        """, (f"%{person_name}%", entity_id)).fetchall()

        if len(candidates) == 0:
            continue

        # Use the best match (exact match preferred, then first)
        best = None
        for cand_id, cand_name in candidates:
            if cand_name == person_name:
                best = (cand_id, cand_name)
                break
        if best is None:
            best = candidates[0]

        indiv_id, indiv_name = best
        group_name = f"{indiv_name} + {entity_name}"

        cur = conn.execute("""
            INSERT INTO insider_groups (primary_insider_id, group_name, confidence, method)
            VALUES (?, ?, 0.8, 'name_substring')
            RETURNING group_id
        """, (indiv_id, group_name))
        group_id = cur.lastrowid

        conn.execute("""
            INSERT OR IGNORE INTO insider_group_members
                (group_id, insider_id, is_primary, is_entity, relationship)
            VALUES (?, ?, 1, 0, 'controlling_person')
        """, (group_id, indiv_id))

        conn.execute("""
            INSERT OR IGNORE INTO insider_group_members
                (group_id, insider_id, is_primary, is_entity, relationship)
            VALUES (?, ?, 0, 1, 'entity')
        """, (group_id, entity_id))

        groups_created += 1

    conn.commit()
    logger.info("Pass B (name_substring): created %d groups from %d entity candidates",
                groups_created, len(entities))
    return groups_created


# ── Pass C: Link by trade overlap ───────────────────────────────────────

def link_by_trade_overlap(conn) -> int:
    """Link entities to individuals with exact matching trades.

    Uses a single bulk SQL query to find all entity-individual pairs with
    exact match on (ticker, trade_date, trade_type, value), then filters
    to entities with exactly ONE individual match (ambiguous skipped).

    Creates groups with method='trade_overlap', confidence=0.7.
    Returns count of groups created.
    """
    # Bulk query: find all entity→individual matches via trade overlap
    # Only consider entities not yet in a group
    logger.info("Pass C: running bulk trade overlap query...")
    pairs = conn.execute("""
        SELECT
            te.insider_id AS entity_id,
            ie.name_normalized AS entity_name,
            ti.insider_id AS indiv_id,
            ii.name_normalized AS indiv_name,
            COUNT(*) AS overlap_count
        FROM trades te
        JOIN insiders ie ON te.insider_id = ie.insider_id
        JOIN trades ti ON te.ticker = ti.ticker
            AND te.trade_date = ti.trade_date
            AND te.trade_type = ti.trade_type
            AND te.value = ti.value
            AND te.insider_id != ti.insider_id
        JOIN insiders ii ON ti.insider_id = ii.insider_id
        WHERE ie.is_entity = 1
          AND ii.is_entity = 0
          AND ie.insider_id NOT IN (SELECT insider_id FROM insider_group_members)
        GROUP BY te.insider_id, ti.insider_id
    """).fetchall()

    logger.info("Pass C: found %d entity-individual overlap pairs", len(pairs))

    # Group by entity_id → list of (indiv_id, indiv_name, overlap_count)
    from collections import defaultdict
    entity_matches = defaultdict(list)
    entity_names = {}
    for entity_id, entity_name, indiv_id, indiv_name, overlap_count in pairs:
        entity_matches[entity_id].append((indiv_id, indiv_name, overlap_count))
        entity_names[entity_id] = entity_name

    groups_created = 0

    for entity_id, matches in entity_matches.items():
        if len(matches) != 1:
            # Ambiguous — multiple individual matches, skip
            continue

        indiv_id, indiv_name, _ = matches[0]
        entity_name = entity_names[entity_id]

        group_name = f"{indiv_name} + {entity_name}"
        cur = conn.execute("""
            INSERT INTO insider_groups (primary_insider_id, group_name, confidence, method)
            VALUES (?, ?, 0.7, 'trade_overlap')
            RETURNING group_id
        """, (indiv_id, group_name))
        group_id = cur.lastrowid

        conn.execute("""
            INSERT OR IGNORE INTO insider_group_members
                (group_id, insider_id, is_primary, is_entity, relationship)
            VALUES (?, ?, 1, 0, 'controlling_person')
        """, (group_id, indiv_id))

        conn.execute("""
            INSERT OR IGNORE INTO insider_group_members
                (group_id, insider_id, is_primary, is_entity, relationship)
            VALUES (?, ?, 0, 1, 'entity')
        """, (group_id, entity_id))

        groups_created += 1

    conn.commit()
    logger.info("Pass C (trade_overlap): created %d groups from %d entities with overlaps",
                groups_created, len(entity_matches))
    return groups_created


# ── Pass D: Link co-trading entity pairs ────────────────────────────────

def link_entity_cotrades(conn) -> int:
    """Link entity pairs that co-trade the same ticker on many of the same dates.

    When two entities (e.g., SLTA IV and SLTA V) trade the same ticker on the
    same dates, they likely share beneficial ownership. This pass groups them
    together so cluster counting collapses them into one.

    Uses union-find to transitively merge chains (A↔B, B↔C → A,B,C in one group).

    Creates groups with method='entity_cotrade', confidence=0.65.
    Returns count of groups created.
    """
    from collections import defaultdict

    logger.info("Pass D: finding co-trading entity pairs...")

    # Find entity pairs that share ≥3 trade dates on the same ticker
    # and where that overlap is ≥50% of the smaller entity's trade count
    pairs = conn.execute("""
        SELECT
            t1.insider_id AS eid1,
            t2.insider_id AS eid2,
            t1.ticker,
            COUNT(DISTINCT t1.trade_date) AS overlap_dates
        FROM trades t1
        JOIN trades t2 ON t1.ticker = t2.ticker
            AND t1.trade_date = t2.trade_date
            AND t1.trade_type = t2.trade_type
            AND t1.insider_id < t2.insider_id
        JOIN insiders i1 ON t1.insider_id = i1.insider_id AND i1.is_entity = 1
        JOIN insiders i2 ON t2.insider_id = i2.insider_id AND i2.is_entity = 1
        WHERE t1.insider_id NOT IN (SELECT insider_id FROM insider_group_members)
          AND t2.insider_id NOT IN (SELECT insider_id FROM insider_group_members)
        GROUP BY t1.insider_id, t2.insider_id, t1.ticker
        HAVING COUNT(DISTINCT t1.trade_date) >= 3
    """).fetchall()

    logger.info("Pass D: found %d candidate entity pairs", len(pairs))

    # Get trade date counts per entity per ticker for overlap ratio
    entity_date_counts: dict[tuple[int, str], int] = {}
    for eid1, eid2, ticker, overlap in pairs:
        for eid in (eid1, eid2):
            key = (eid, ticker)
            if key not in entity_date_counts:
                row = conn.execute(
                    "SELECT COUNT(DISTINCT trade_date) FROM trades WHERE insider_id = ? AND ticker = ?",
                    (eid, ticker),
                ).fetchone()
                entity_date_counts[key] = row[0]

    # Filter to pairs with meaningful overlap relative to the smaller entity.
    # Use 30% threshold — entities filing the same trades often have different
    # lot breakdowns resulting in different date distributions.
    strong_pairs: list[tuple[int, int, str, int]] = []
    for eid1, eid2, ticker, overlap in pairs:
        min_dates = min(
            entity_date_counts.get((eid1, ticker), 0),
            entity_date_counts.get((eid2, ticker), 0),
        )
        if min_dates > 0 and overlap / min_dates >= 0.3:
            strong_pairs.append((eid1, eid2, ticker, overlap))

    logger.info("Pass D: %d pairs pass ≥50%% overlap threshold", len(strong_pairs))

    if not strong_pairs:
        return 0

    # Union-find to merge transitive chains
    parent: dict[int, int] = {}

    def find(x: int) -> int:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a: int, b: int):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for eid1, eid2, _, _ in strong_pairs:
        parent.setdefault(eid1, eid1)
        parent.setdefault(eid2, eid2)
        union(eid1, eid2)

    # Build components
    components: dict[int, set[int]] = defaultdict(set)
    for eid in parent:
        components[find(eid)].add(eid)

    # For each component, check if any member is already linked to a person
    # via another group. If so, use that person as primary.
    names = dict(conn.execute(
        "SELECT insider_id, name_normalized FROM insiders WHERE insider_id IN ({})".format(
            ",".join("?" * len(parent)),
        ),
        list(parent.keys()),
    ).fetchall())

    groups_created = 0

    for root, members in components.items():
        if len(members) < 2:
            continue

        # Check if any member already has a person link
        existing_primary = None
        for mid in members:
            row = conn.execute("""
                SELECT ig.primary_insider_id
                FROM insider_group_members igm
                JOIN insider_groups ig ON igm.group_id = ig.group_id
                WHERE igm.insider_id = ? AND igm.is_primary = 0
            """, (mid,)).fetchone()
            if row:
                existing_primary = row[0]
                break

        if existing_primary:
            # Add ungrouped members to the existing person's group
            group_row = conn.execute("""
                SELECT group_id FROM insider_groups WHERE primary_insider_id = ? LIMIT 1
            """, (existing_primary,)).fetchone()
            if group_row:
                for mid in members:
                    conn.execute("""
                        INSERT OR IGNORE INTO insider_group_members
                            (group_id, insider_id, is_primary, is_entity, relationship)
                        VALUES (?, ?, 0, 1, 'entity_cotrade')
                    """, (group_row[0], mid))
                groups_created += 1
                continue

        # No person link — create entity-only group, pick entity with most trades as primary
        trade_counts = {}
        for mid in members:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE insider_id = ?", (mid,)
            ).fetchone()[0]
            trade_counts[mid] = cnt

        primary_entity = max(members, key=lambda m: trade_counts.get(m, 0))
        member_names = [names.get(m, str(m)) for m in members]
        group_name = " + ".join(sorted(member_names))

        cur = conn.execute("""
            INSERT INTO insider_groups (primary_insider_id, group_name, confidence, method)
            VALUES (?, ?, 0.65, 'entity_cotrade')
            RETURNING group_id
        """, (primary_entity, group_name))
        group_id = cur.lastrowid

        for mid in members:
            is_primary = 1 if mid == primary_entity else 0
            conn.execute("""
                INSERT OR IGNORE INTO insider_group_members
                    (group_id, insider_id, is_primary, is_entity, relationship)
                VALUES (?, ?, ?, 1, ?)
            """, (group_id, mid, is_primary,
                  'entity_primary' if is_primary else 'entity_cotrade'))

        groups_created += 1

    conn.commit()
    logger.info("Pass D (entity_cotrade): created/updated %d groups from %d components",
                groups_created, len([c for c in components.values() if len(c) >= 2]))
    return groups_created


# ── Apply effective IDs ─────────────────────────────────────────────────

def apply_effective_ids(conn) -> int:
    """Set effective_insider_id on trades for all grouped insiders.

    For each group, UPDATE trades SET effective_insider_id = primary_insider_id
    WHERE insider_id is any member of the group (and effective_insider_id IS NULL
    or still equals insider_id).

    Returns count of trades updated.
    """
    groups = conn.execute("""
        SELECT g.group_id, g.primary_insider_id
        FROM insider_groups g
    """).fetchall()

    total_updated = 0

    for group_id, primary_id in groups:
        members = conn.execute("""
            SELECT insider_id FROM insider_group_members
            WHERE group_id = ?
        """, (group_id,)).fetchall()

        for (member_id,) in members:
            cur = conn.execute("""
                UPDATE trades
                SET effective_insider_id = ?
                WHERE insider_id = ?
                  AND (effective_insider_id IS NULL OR effective_insider_id = insider_id)
            """, (primary_id, member_id))
            total_updated += cur.rowcount

    conn.commit()
    logger.info("Applied effective_insider_id to %d trades", total_updated)
    return total_updated


# ── Main entry point ────────────────────────────────────────────────────

def run_entity_resolution(db_path: Path | str):
    """Run the full entity resolution pipeline."""
    dedup_flag = os.environ.get("INSIDER_DEDUP")
    if dedup_flag != "1":
        logger.warning(
            "INSIDER_DEDUP env var is not set to '1'. "
            "Entity resolution will run but dedup won't be used in track record computation "
            "until INSIDER_DEDUP=1 is set."
        )

    conn = get_connection()

    # 1. Ensure schema columns exist
    ensure_schema(conn)

    # 2. Flag entity insiders
    entity_count = flag_entity_insiders(conn)

    # 3. Clear old groups for a fresh run
    conn.execute("DELETE FROM insider_group_members")
    conn.execute("DELETE FROM insider_groups")
    conn.commit()
    logger.info("Cleared existing entity groups")

    # 4. Run resolution passes
    groups_a = link_by_accession(conn)
    groups_b = link_by_name_substring(conn)
    groups_c = link_by_trade_overlap(conn)
    groups_d = link_entity_cotrades(conn)

    # 5. Apply effective IDs
    trades_updated = apply_effective_ids(conn)

    # 6. Summary
    total_groups = conn.execute("SELECT COUNT(*) FROM insider_groups").fetchone()[0]
    total_members = conn.execute("SELECT COUNT(*) FROM insider_group_members").fetchone()[0]

    print(f"\n{'='*50}")
    print(f"ENTITY RESOLUTION SUMMARY")
    print(f"{'='*50}")
    print(f"Entity insiders flagged:  {entity_count:,}")
    print(f"Groups created:           {total_groups:,}")
    print(f"  Pass A (accession):     {groups_a:,}")
    print(f"  Pass B (name_substr):   {groups_b:,}")
    print(f"  Pass C (trade_overlap): {groups_c:,}")
    print(f"  Pass D (entity_cotrade):{groups_d:,}")
    print(f"Total group members:      {total_members:,}")
    print(f"Trades with effective ID: {trades_updated:,}")
    print(f"INSIDER_DEDUP flag:       {dedup_flag or 'not set'}")
    print(f"{'='*50}\n")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run entity resolution on insider catalog DB")
    parser.add_argument("--db-path", type=str, default=str(DB_PATH),
                        help=f"Path to insiders.db (default: {DB_PATH})")
    args = parser.parse_args()

    run_entity_resolution(Path(args.db_path))
