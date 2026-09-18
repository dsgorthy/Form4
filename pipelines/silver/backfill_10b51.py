#!/usr/bin/env python3
"""Set silver.form4_transaction.aff_10b5_1 for every filing, quarter by quarter.

The rule is trades' rule (strategies/insider_catalog/backfill_live.py), so
that Gold classifies planned trades the way the product already does:

    flagged  =  <aff10b5One> is 1/true            (the checkbox, 2023+)
             or "10b5" appears in <remarks>
             or "10b5" appears in any <footnote>

Filing-level: every line of a flagged filing is true, every line of an
unflagged one false. Candidates are found in SQL (a regex over Bronze runs
~3 s per quarter of ~60k submissions) and only candidates are parsed here,
with regexes -- no XML parse, no Silver rebuild.

Resumable: a quarter is recorded in silver.backfill_10b51_progress when it
is complete, and skipped on the next run. Runs on the Studio:

    python3 pipelines/silver/backfill_10b51.py            # all quarters not yet done
    python3 pipelines/silver/backfill_10b51.py --quarter 2024QTR1
    python3 pipelines/silver/backfill_10b51.py --dry-run  # count, write nothing
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

# The element name itself contains "10b5", so a bare content match is every
# filing since 2023. "10b5 followed by anything but an O" keeps "10b5One"
# out; the checkbox is tested separately. No "?" anywhere in this SQL: the
# compat layer turns every one into a placeholder, and a regex lookahead
# cost the first run an IndexError.
CANDIDATE_SQL = """
SELECT s.accession, s.content
  FROM bronze.edgar_submission s
  JOIN bronze.edgar_index i USING (accession)
 WHERE i.quarter = %(quarter)s
   AND (s.content ~ '<aff10b5One>\\s*(1|true)' OR s.content ~* '10b5([^Oo]|$)')
"""

_BOX = re.compile(r"<aff10b5One>\s*(1|true)\s*</aff10b5One>", re.I)
_REMARKS = re.compile(r"<remarks>(.*?)</remarks>", re.S | re.I)
_FOOTNOTE = re.compile(r"<footnote\b[^>]*>(.*?)</footnote>", re.S | re.I)


def plan_flag(content: str) -> bool:
    """trades' rule, on the raw submission text. Pure."""
    if _BOX.search(content):
        return True
    for m in _REMARKS.finditer(content):
        if "10b5" in m.group(1).lower():
            return True
    for m in _FOOTNOTE.finditer(content):
        if "10b5" in m.group(1).lower():
            return True
    return False


def run_quarter(conn, quarter: str, dry_run: bool) -> tuple[int, int]:
    """(submissions in the quarter, filings flagged)."""
    n_sub = conn.execute("SELECT count(*) AS n FROM bronze.edgar_index WHERE quarter = ?", (quarter,)).fetchone()["n"]
    flagged = [r["accession"] for r in conn.execute(CANDIDATE_SQL, {"quarter": quarter}).fetchall() if plan_flag(r["content"])]
    if dry_run:
        return n_sub, len(flagged)
    conn.execute("SET statement_timeout = '1800s'")
    # Every line of a flagged filing true, every other line in the quarter false.
    if flagged:
        conn.execute(
            "UPDATE silver.form4_transaction SET aff_10b5_1 = TRUE WHERE accession = ANY(?)",
            (flagged,),
        )
    conn.execute(
        """UPDATE silver.form4_transaction t SET aff_10b5_1 = FALSE
             FROM bronze.edgar_index i
            WHERE i.accession = t.accession AND i.quarter = ? AND t.aff_10b5_1 IS NULL""",
        (quarter,),
    )
    conn.execute(
        """INSERT INTO silver.backfill_10b51_progress (quarter, submissions, flagged)
           VALUES (?, ?, ?)
           ON CONFLICT (quarter) DO UPDATE SET submissions = excluded.submissions,
                                               flagged = excluded.flagged, done_at = now()""",
        (quarter, n_sub, len(flagged)),
    )
    conn.commit()
    return n_sub, len(flagged)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarter", help="one quarter, e.g. 2024QTR1 (default: every quarter not yet done)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    conn = get_connection()
    if args.quarter:
        quarters = [args.quarter]
    else:
        done = {r["quarter"] for r in conn.execute("SELECT quarter FROM silver.backfill_10b51_progress").fetchall()}
        quarters = [r["quarter"] for r in conn.execute(
            "SELECT DISTINCT quarter FROM bronze.edgar_index ORDER BY quarter").fetchall() if r["quarter"] not in done]
    logger.info("%d quarter(s) to do%s", len(quarters), " (dry run)" if args.dry_run else "")
    total_flagged = 0
    for q in quarters:
        n, f = run_quarter(conn, q, args.dry_run)
        total_flagged += f
        logger.info("%s: %d submissions, %d flagged 10b5-1", q, n, f)
    logger.info("done: %d filings flagged across %d quarter(s)", total_flagged, len(quarters))
    return 0


if __name__ == "__main__":
    sys.exit(main())
