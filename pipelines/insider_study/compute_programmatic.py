#!/usr/bin/env python3
"""THE writer for trades.is_programmatic. One definition, one place.

Consolidates four overlapping notions of "routine" — see api/programmatic.py
for what each was and why cohen_routine deliberately stays separate.

POINT IN TIME. For a filing on date D the programme status uses only filings
up to and including D. Computing the CV over an insider's whole sequence would
let a 2019 row know about 2024 filings, which is exactly the class of leak the
PIT checklist exists to stop. So this walks each sequence forward and writes a
value per filing rather than one per insider.

Filings, never lots: a purchase filled in five tranches is one decision, and
counting tranches would make every large fill look like a metronome.

Usage:
    python3 pipelines/insider_study/compute_programmatic.py
    python3 pipelines/insider_study/compute_programmatic.py --since 2024-01-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from api.programmatic import score_sequence  # noqa: E402
from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS is_programmatic INTEGER",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS prog_cv_interval DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS prog_cv_value DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS prog_n_filings INTEGER",
    # FREQUENCY, which is a different question from regularity: "sells every
    # quarter" holds even when the amounts are erratic, and that case is
    # deliberately NOT programmatic.
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS prog_median_interval_days DOUBLE PRECISION",
    "CREATE INDEX IF NOT EXISTS idx_trades_programmatic ON trades (is_programmatic)",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2016-01-01",
                    help="only WRITE rows filed on/after this; history before "
                         "it is still read, because a programme that started "
                         "in 2014 is still a programme in 2016")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    for ddl in DDL:
        try:
            conn.execute(ddl)
        except Exception as exc:
            logger.warning("ddl skipped: %s", exc)
    conn.commit()

    logger.info("loading filings...")
    rows = conn.execute("""
        SELECT insider_id, ticker, signal_class,
               COALESCE(filing_key, accession) AS fk,
               MIN(filing_date) AS fd,
               SUM(value)       AS v
          FROM trades
         WHERE signal_class IN ('discretionary_buy','discretionary_sell')
           AND value > 0 AND insider_id IS NOT NULL AND ticker <> 'NONE'
         GROUP BY insider_id, ticker, signal_class,
                  COALESCE(filing_key, accession)
    """).fetchall()
    logger.info("%d filings across buy+sell sequences", len(rows))

    seqs = defaultdict(list)
    for r in rows:
        try:
            d = date.fromisoformat(str(r[4])[:10])
        except (ValueError, TypeError):
            continue
        seqs[(r[0], r[1], r[2])].append((d, float(r[5] or 0), r[3]))

    updates, seq_done = [], 0
    for key, items in seqs.items():
        items.sort(key=lambda x: x[0])
        # WALK FORWARD. Filing i is scored on filings 0..i only.
        for i in range(len(items)):
            d, _, fk = items[i]
            if str(d) < args.since:
                continue
            s = score_sequence([(x[0], x[1]) for x in items[: i + 1]])
            updates.append((s["is_programmatic"], s["cv_interval"],
                            s["cv_value"], s["n_filings"],
                            s["median_interval_days"], fk))
        seq_done += 1
        if seq_done % 200_000 == 0:
            logger.info("  %d sequences scored...", seq_done)

    logger.info("%d filing-rows to write", len(updates))
    if args.dry_run:
        flagged = sum(1 for u in updates if u[0])
        logger.info("DRY RUN: %d would be flagged programmatic (%.2f%%)",
                    flagged, 100.0 * flagged / max(len(updates), 1))
        return 0

    written = 0
    for i, u in enumerate(updates):
        conn.execute("""
            UPDATE trades
               SET is_programmatic = ?, prog_cv_interval = ?,
                   prog_cv_value = ?, prog_n_filings = ?,
                   prog_median_interval_days = ?
             WHERE COALESCE(filing_key, accession) = ?
        """, u)
        written += 1
        if i % 20_000 == 0:
            conn.commit()
            logger.info("  %d/%d written", i, len(updates))
    conn.commit()
    logger.info("done: %d filing-rows written", written)
    return 0


if __name__ == "__main__":
    sys.exit(main())
