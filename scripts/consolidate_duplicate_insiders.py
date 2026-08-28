#!/usr/bin/env python3
"""Repoint trades at one insider_id per CIK. Reversible.

WHY

EDGAR renders a CIK both zero-padded ("0002014440") and bare ("2014440"), and
get_or_create_insider compared it as a raw string, so the same filer arriving
down the historical path and the live path was minted twice. 36 filers are split across 72
insider rows carrying 1,876 trades. (A first pass grouped on CIK alone and
found 1,557 "duplicates" -- but insiders.cik holds the ISSUER's CIK on many
rows, so 1,562 of those groups were different people at the same company.
Name agreement is required.)

A split identity halves an insider's filing history. career_grade -- the
primary gate on all three published books -- is therefore computed on partial
evidence and can fall below MIN_SCORED_FILINGS entirely. It also produces
visible nonsense: on 2026-08-27 a generated post had Woodrow D. Anderson
buying "$54K alongside them", alongside himself, and doubled one $54,420
purchase into "$108K from 2 insiders".

WHY REPOINT insider_id RATHER THAN SET THE ALIAS

effective_insider_id exists for exactly this, but compute_career_grades.py
groups by insider_id and does not consult it. Setting the alias would fix the
API and the notification scanner while leaving grades -- the thing that
actually gates money -- still split. Repointing fixes every consumer at once
instead of hunting for the readers that forgot to COALESCE.

REVERSIBILITY

Every change is written to insider_merge_log (trade_id, from, to) before it is
applied, so the whole operation reverses with a single UPDATE ... FROM.

Usage:
    python3 scripts/consolidate_duplicate_insiders.py --dry-run
    python3 scripts/consolidate_duplicate_insiders.py
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH = 5000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    conn.execute("SET lock_timeout = '5s'")

    # The canonical id for a CIK is the one with the MOST trades -- it already
    # carries the longer history, so the fewest rows move and any stale
    # per-insider aggregate keyed to it stays closest to correct.
    logger.info("finding duplicate groups by normalized CIK...")
    groups = conn.execute("""
        WITH norm AS (
            SELECT insider_id, name_normalized, ltrim(cik, '0') AS cik_norm
              FROM insiders
             WHERE cik IS NOT NULL AND cik <> '' AND cik ~ '^[0-9]+$'
        ), dup AS (
            -- NAME MUST AGREE TOO. insiders.cik is populated with the ISSUER's
            -- CIK on many rows, not the reporting owner's, so CIK alone is not
            -- an identity. Grouping on it by itself put six different people
            -- -- BEAUDOUIN MARK T, TERRICCIANO DAVID, Kelly Terrence P, Cassis
            -- Eugene G, CAPUTO ARTHUR G, Khanna Rohit -- under CIK 0001000697,
            -- and MEDALLION FINANCIAL CORP together with two of its officers
            -- under 0001000209. 1,562 of 1,597 CIK-only groups had disagreeing
            -- names; merging them would have collapsed every officer of a
            -- company into one person.
            SELECT cik_norm, name_normalized FROM norm
             GROUP BY cik_norm, name_normalized HAVING count(*) > 1
        ), counted AS (
            SELECT n.cik_norm, n.name_normalized, n.insider_id,
                   (SELECT count(*) FROM trades t WHERE t.insider_id = n.insider_id) AS n_trades
              FROM norm n
              JOIN dup d ON d.cik_norm = n.cik_norm
                        AND d.name_normalized = n.name_normalized
        )
        SELECT cik_norm,
               (array_agg(insider_id ORDER BY n_trades DESC, insider_id))[1] AS keep,
               array_agg(insider_id ORDER BY n_trades DESC, insider_id) AS all_ids
          FROM counted GROUP BY cik_norm, name_normalized
    """).fetchall()
    logger.info("%d CIKs with more than one insider_id", len(groups))

    remap: dict[int, int] = {}
    for g in groups:
        keep, ids = g["keep"], g["all_ids"]
        for i in ids:
            if i != keep:
                remap[i] = keep
    logger.info("%d insider_id(s) will be repointed", len(remap))

    if not remap:
        logger.info("nothing to do")
        return 0

    affected = conn.execute(
        "SELECT count(*) AS n FROM trades WHERE insider_id = ANY(%s)",
        (list(remap),)).fetchone()["n"]
    logger.info("%d trade rows affected", affected)

    if args.dry_run:
        for src, dst in list(remap.items())[:10]:
            logger.info("  %s -> %s", src, dst)
        logger.info("dry run: nothing written")
        return 0

    # Log BEFORE applying, so the operation is reversible even if it dies
    # halfway through.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS insider_merge_log (
            trade_id          TEXT PRIMARY KEY,
            from_insider_id   INTEGER NOT NULL,
            to_insider_id     INTEGER NOT NULL,
            merged_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""")
    conn.commit()

    pairs = list(remap.items())
    total = 0
    for i in range(0, len(pairs), 200):
        chunk = pairs[i:i + 200]
        srcs = [s for s, _ in chunk]
        conn.execute("""
            INSERT INTO insider_merge_log (trade_id, from_insider_id, to_insider_id)
            SELECT t.trade_id, t.insider_id, m.dst
              FROM trades t
              JOIN (SELECT unnest(%s::int[]) AS src, unnest(%s::int[]) AS dst) m
                ON m.src = t.insider_id
             ON CONFLICT (trade_id) DO NOTHING
        """, (srcs, [d for _, d in chunk]))
        cur = conn.execute("""
            UPDATE trades t SET insider_id = m.dst,
                                effective_insider_id = m.dst
              FROM (SELECT unnest(%s::int[]) AS src, unnest(%s::int[]) AS dst) m
             WHERE t.insider_id = m.src
        """, (srcs, [d for _, d in chunk]))
        conn.commit()
        total += cur.rowcount or 0
        logger.info("  %d/%d groups, %d rows repointed",
                    min(i + 200, len(pairs)), len(pairs), total)

    logger.info("Done. %d trade rows repointed onto %d canonical insiders",
                total, len(set(remap.values())))
    logger.info("Reverse with: UPDATE trades t SET insider_id = l.from_insider_id "
                "FROM insider_merge_log l WHERE l.trade_id = t.trade_id;")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
