#!/usr/bin/env python3
"""Assign trades.line_no to accessions that were ingested exactly once.

PHASE 1 of the data-layer plan. See migrations/2026-09-04_trades_line_no.sql
for why (accession, line_no) is the key that should always have existed.

WHY THIS IS A SCRIPT AND NOT A MIGRATION

The obvious version --

    WITH numbered AS (SELECT trade_id, ROW_NUMBER() OVER (...) FROM trades ...)
    UPDATE trades SET line_no = ... FROM numbered

-- is one statement over 6.7M rows of a 28 GB table. It ran for 30 minutes,
hit statement_timeout and rolled back every row. Nothing partial survives a
rolled-back transaction, so a retry starts from zero and hits the same wall.

Batched by accession instead: each batch is its own transaction, commits, and
is skipped on a re-run because its rows already have line_no. Interruptible at
any point.

WHY DUPLICATED ACCESSIONS ARE SKIPPED

Numbering across an accession that was ingested TWICE turns a five-line filing
into line_no 0..9 and calls it ten distinct lines -- unique, and false. Those
stay NULL until Phase 2 retires the duplicates.

Usage:
    python3 scripts/backfill_line_no.py                # run
    python3 scripts/backfill_line_no.py --batch 5000   # smaller batches
    python3 scripts/backfill_line_no.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH = 20_000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=BATCH)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    logger.info("finding accessions ingested exactly once and still unnumbered...")
    cur.execute("""
        SELECT accession
          FROM trades
         WHERE accession IS NOT NULL AND accession <> ''
         GROUP BY accession
        HAVING count(DISTINCT ingested_at::date) = 1
           AND count(*) FILTER (WHERE line_no IS NULL) > 0
    """)
    todo = [r["accession"] for r in cur.fetchall()]
    logger.info("  %d accessions to number", len(todo))
    if args.dry_run or not todo:
        return 0

    done = rows = 0
    t0 = time.monotonic()
    for i in range(0, len(todo), args.batch):
        chunk = todo[i:i + args.batch]
        ph = ",".join(["?"] * len(chunk))
        cur.execute(f"""
            WITH numbered AS (
              SELECT trade_id,
                     ROW_NUMBER() OVER (PARTITION BY accession ORDER BY trade_id) - 1 AS ln
                FROM trades
               WHERE accession IN ({ph})
            )
            UPDATE trades t SET line_no = n.ln
              FROM numbered n
             WHERE t.trade_id = n.trade_id AND t.line_no IS DISTINCT FROM n.ln
        """, chunk)
        rows += cur.rowcount or 0
        conn.commit()
        done += len(chunk)
        el = time.monotonic() - t0
        logger.info("  %d/%d accessions, %d rows, %.0fs elapsed (%.0f acc/s)",
                    done, len(todo), rows, el, done / el if el else 0)

    cur.execute("""SELECT count(*) AS total, count(line_no) AS numbered FROM trades""")
    r = cur.fetchone()
    logger.info("DONE: %d/%d rows carry line_no (%.1f%%)",
                r["numbered"], r["total"], 100.0 * r["numbered"] / r["total"])
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
