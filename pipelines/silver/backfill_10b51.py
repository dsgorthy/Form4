#!/usr/bin/env python3
"""Set silver.form4_transaction.aff_10b5_1 for every filing, quarter by quarter.

The rule is trades' rule (strategies/insider_catalog/backfill_live.py), so
that Gold classifies planned trades the way the product already does:

    flagged  =  <aff10b5One> is 1/true            (the checkbox, 2023+)
             or "10b5" appears in <remarks>
             or "10b5" appears in any <footnote>

Filing-level: every line of a flagged filing is true, every line of an
unflagged one false. The whole rule runs in SQL, one quarter at a time
(three regexes over ~60k submissions in seconds; no filing text leaves the
database, no XML parse, no Silver rebuild).

Resumable: a quarter is recorded in silver.backfill_10b51_progress when it
is complete, and skipped on the next run. Runs on the Studio:

    python3 pipelines/silver/backfill_10b51.py            # all quarters not yet done
    python3 pipelines/silver/backfill_10b51.py --quarter 2024QTR1
    python3 pipelines/silver/backfill_10b51.py --dry-run  # count, write nothing
    python3 pipelines/silver/backfill_10b51.py --pending  # only unflagged lines (hourly, via keep_up.py)
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# THE RULE, IN SQL, SO NO FILING TEXT LEAVES THE DATABASE. Shipping the
# candidate submissions to Python cost 2m24s per quarter (~3 h for the
# corpus); the three regexes over one quarter of Bronze run in seconds.
#   checkbox   <aff10b5One>1</aff10b5One> (2023+)
#   remarks    "10b5" inside <remarks>
#   footnote   "10b5" inside any <footnote>
# "[^<]*" scopes the match to the element's own text; footnotes and remarks
# carry no child elements. No "?" anywhere in this SQL: the compat layer
# turns every one into a placeholder.
FLAG_EXPR = """(s.content ~  '<aff10b5One>\\s*(1|true)'
             OR s.content ~* '<remarks>[^<]*10b5'
             OR s.content ~* '<footnote[^>]*>[^<]*10b5')"""

COUNT_SQL = f"""
SELECT count(*) AS submissions, count(*) FILTER (WHERE {FLAG_EXPR}) AS flagged
  FROM bronze.edgar_submission s
  JOIN bronze.edgar_index i USING (accession)
 WHERE i.quarter = %s
"""

APPLY_SQL = f"""
WITH f AS (
    SELECT s.accession, {FLAG_EXPR} AS flag
      FROM bronze.edgar_submission s
      JOIN bronze.edgar_index i USING (accession)
     WHERE i.quarter = %s
)
UPDATE silver.form4_transaction t SET aff_10b5_1 = f.flag
  FROM f
 WHERE f.accession = t.accession
"""

_BOX = re.compile(r"<aff10b5One>\s*(1|true)\s*</aff10b5One>", re.I)
_REMARKS = re.compile(r"<remarks>(.*?)</remarks>", re.S | re.I)
_FOOTNOTE = re.compile(r"<footnote\b[^>]*>(.*?)</footnote>", re.S | re.I)


def plan_flag(content: str) -> bool:
    """The same rule, in Python, for tests and spot checks. Pure."""
    if _BOX.search(content):
        return True
    for m in _REMARKS.finditer(content):
        if "10b5" in m.group(1).lower():
            return True
    for m in _FOOTNOTE.finditer(content):
        if "10b5" in m.group(1).lower():
            return True
    return False


PENDING_SQL = f"""
WITH f AS (
    SELECT s.accession, {FLAG_EXPR} AS flag
      FROM bronze.edgar_submission s
     WHERE s.accession IN (SELECT DISTINCT accession FROM silver.form4_transaction WHERE aff_10b5_1 IS NULL)
)
UPDATE silver.form4_transaction t SET aff_10b5_1 = f.flag
  FROM f
 WHERE f.accession = t.accession AND t.aff_10b5_1 IS NULL
"""


def run_pending(conn) -> int:
    """Flag every line the hourly Silver build has added since the last
    pass (aff_10b5_1 IS NULL). The steady-state path; quarters are the backfill."""
    conn.execute("SET statement_timeout = '1800s'")
    cur = conn.execute(PENDING_SQL)
    n = cur.rowcount
    conn.commit()
    return n


def run_quarter(conn, quarter: str, dry_run: bool) -> tuple[int, int, int]:
    """(submissions, flagged filings, silver lines updated)."""
    conn.execute("SET statement_timeout = '1800s'")
    r = conn.execute(COUNT_SQL, (quarter,)).fetchone()
    n_sub, flagged = r["submissions"], r["flagged"]
    if dry_run:
        return n_sub, flagged, 0
    cur = conn.execute(APPLY_SQL, (quarter,))
    lines = cur.rowcount
    conn.execute(
        """INSERT INTO silver.backfill_10b51_progress (quarter, submissions, flagged)
           VALUES (?, ?, ?)
           ON CONFLICT (quarter) DO UPDATE SET submissions = excluded.submissions,
                                               flagged = excluded.flagged, done_at = now()""",
        (quarter, n_sub, flagged),
    )
    conn.commit()
    return n_sub, flagged, lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarter", help="one quarter, e.g. 2024QTR1 (default: every quarter not yet done)")
    ap.add_argument("--pending", action="store_true",
                    help="only lines with no flag yet (what the hourly build added); the steady-state mode")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    conn = get_connection()
    if args.pending:
        n = run_pending(conn)
        logger.info("pending: %d silver lines flagged", n)
        return 0
    if args.quarter:
        quarters = [args.quarter]
    else:
        done = {r["quarter"] for r in conn.execute("SELECT quarter FROM silver.backfill_10b51_progress").fetchall()}
        quarters = [r["quarter"] for r in conn.execute(
            "SELECT DISTINCT quarter FROM bronze.edgar_index ORDER BY quarter").fetchall() if r["quarter"] not in done]
    logger.info("%d quarter(s) to do%s", len(quarters), " (dry run)" if args.dry_run else "")
    total_flagged = 0
    for q in quarters:
        n, f, lines = run_quarter(conn, q, args.dry_run)
        total_flagged += f
        logger.info("%s: %d submissions, %d flagged 10b5-1, %d silver lines set", q, n, f, lines)
    logger.info("done: %d filings flagged across %d quarter(s)", total_flagged, len(quarters))
    return 0


if __name__ == "__main__":
    sys.exit(main())
