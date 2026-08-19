#!/usr/bin/env python3
"""Recurring writer for `insider_companies`.

WHY THIS EXISTS

`insider_companies` had no recurring writer. It was built once, inside
`strategies/insider_catalog/backfill.py::_build_insider_companies`, which runs
only as part of a full historical backfill. On 2026-08-18 its newest row was
`last_trade = 2026-05-15` — three months stale, missing 9,779 of 126,839
(insider, ticker) pairs, 7.7% of the table.

It was in neither `config/freshness_contracts.yaml` nor
`config/writer_registry.yaml`, so nothing alerted. This is precisely the orphan
class the writer registry was introduced to end; both entries land with this
script.

WHAT IT BROKE

Two things, one visible and one not.

  Visible:  /insider/<slug> reads it for the company list. Gator Capital
            Management showed "11 insider transactions across 0 companies"
            beside "1 company traded" — the trades were there, the mapping was
            not.

  Not:      strategies/insider_catalog/pit_scoring.py reads
            insider_companies.title for `role_at_ticker` and derives
            `is_primary` from trade_count. An insider absent from the table
            scores with no role weight at all, so every grade computed for a
            new insider since May was missing a scoring input.

The second is why this is a nightly job and not a one-off repair.

EVERY TRANSACTION CODE, NOT JUST P AND S

`backfill.py::_build_insider_companies` filters `trans_code IN ('P','S')`. The
table in production was not built that way: 40,384 of its pairs have no P or S
trade at all, only F (tax withholding), A (award), M (option exercise), X and G
(gift). Stored trade_count matches an all-codes count, not a P/S count —
insider 35768 at MSTR is stored as 2,114, which is all codes; P/S alone is
2,034.

That is the right rule. This is a mapping of which companies a person is an
insider AT, and an executive who has only ever received awards and had shares
withheld for tax is still an insider there. pit_scoring reads
`role_at_ticker` from this table by (insider_id, ticker), so dropping those
pairs would strip the role weight from their scores.

Rebuilding on the narrow rule would have deleted 36,719 rows. The >5% shrink
guard below caught exactly that during development, which is why it is here.
`backfill.py` has been corrected to match, or a future full backfill would
undo this.

COST

A full DELETE + INSERT over ~117k rows, a few seconds. Rebuilding wholesale
rather than merging is deliberate: trade_count, total_value, first_trade and
last_trade are aggregates over the entire history of a pair, and a title can
change, so an incremental upsert would have to recompute the same aggregates
anyway.

Usage:
    python3 pipelines/insider_study/rebuild_insider_companies.py
    python3 pipelines/insider_study/rebuild_insider_companies.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.database import get_connection
from framework.observability.pipeline_runner import pipeline_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("rebuild_insider_companies")

BUILD_SQL = """
    INSERT INTO insider_companies (
        insider_id, ticker, company, title,
        trade_count, total_value, first_trade, last_trade
    )
    SELECT
        t.insider_id,
        t.ticker,
        MAX(t.company),
        -- Most recent title at this company. Ordered by trade_date rather than
        -- filing_date because the title is a fact about when they held the
        -- role, not about when the paperwork arrived.
        (SELECT t2.title FROM trades t2
          WHERE t2.insider_id = t.insider_id AND t2.ticker = t.ticker
            AND t2.title IS NOT NULL
          ORDER BY t2.trade_date DESC LIMIT 1),
        COUNT(*),
        SUM(t.value),
        MIN(t.trade_date),
        MAX(t.trade_date)
    FROM trades t
    WHERE t.trade_date <= CURRENT_DATE::text
      AND COALESCE(t.is_duplicate, 0) = 0
      AND t.ticker IS NOT NULL
    GROUP BY t.insider_id, t.ticker
"""


def rebuild(dry_run: bool = False) -> dict:
    conn = get_connection()
    started = time.time()

    before = conn.execute("SELECT COUNT(*) AS n FROM insider_companies").fetchone()["n"]
    stale = conn.execute(
        "SELECT MAX(last_trade) AS d FROM insider_companies").fetchone()["d"]
    logger.info("before: %d rows, newest last_trade=%s", before, stale)

    if dry_run:
        expected = conn.execute("""
            SELECT COUNT(*) AS n FROM (
                SELECT 1 FROM trades t
                 WHERE t.trade_date <= CURRENT_DATE::text
                   AND COALESCE(t.is_duplicate,0)=0
                   AND t.ticker IS NOT NULL
                 GROUP BY t.insider_id, t.ticker
            ) x
        """).fetchone()["n"]
        logger.info("dry run: would write %d rows (%+d)", expected, expected - before)
        return {"before": before, "after": expected, "dry_run": True}

    # One transaction: a half-rebuilt mapping table would silently drop the
    # role weight out of every score computed while it was empty.
    conn.execute("DELETE FROM insider_companies")
    conn.execute(BUILD_SQL)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) AS n FROM insider_companies").fetchone()["n"]
    newest = conn.execute(
        "SELECT MAX(last_trade) AS d FROM insider_companies").fetchone()["d"]
    logger.info("after:  %d rows (%+d), newest last_trade=%s, %.1fs",
                after, after - before, newest, time.time() - started)

    if after < before * 0.95:
        raise RuntimeError(
            f"rebuild produced {after} rows against {before} before — refusing "
            f"to treat a >5% shrink as success"
        )
    return {"before": before, "after": after, "newest_last_trade": newest}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change without writing")
    args = ap.parse_args()

    with pipeline_run("rebuild-insider-companies") as run:
        result = rebuild(dry_run=args.dry_run)
        run.metadata.update(result)
        run.rows_written = result.get("after", 0)
        run.rows_deleted = result.get("before", 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
