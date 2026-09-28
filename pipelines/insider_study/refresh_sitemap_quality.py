#!/usr/bin/env python3
"""Materialize the per-entity facts the sitemap uses to decide what to submit.

WHY THIS TABLE EXISTS

Page indexing on 2026-09-20: **44.6K indexed, 126K not** — 59,022 "Discovered -
currently not indexed" and 56,197 "Crawled - currently not indexed", 91% of the
not-indexed total. Google is not failing to find these pages. It is declining
them. Crawl stats said the same thing from the other side: 67% of crawl budget
went to Discovery and 33% to Refresh, so the pages that could rank were being
re-read least.

We submitted ~70,000 URLs (51,797 insiders + 18,267 companies) against a rule
that cannot tell a substantial page from a stub:

    insider_track_records.buy_count >= 2

`buy_count` COUNTS EXECUTION LOTS, NOT FILINGS. Measured 2026-09-27: insider
14368 has 1,167 decision filings and a buy_count+sell_count of 24,994; insider
84 has 1,539 against 4,114. A purchase filled in five tranches counts five
times, so `>= 2` admits insiders who made exactly one decision. It is the same
lot-vs-filing defect that cost A-List its headline (see the memory
`feedback_filing_not_lot_grouping`) — it was fixed in the published track record
and never in the sitemap rule.

`insider_track_records` also cannot answer "when did they last make a DECISION":
its `buy_last_date` / `sell_last_date` include compensation grants and option
exercises, so for insider 14368 it says 2025-11-03 where the last discretionary
filing was 2024-12-18.

So this script computes the two facts the rule actually needs, on the ONE basis
the rest of the product uses — one row per FILING, discretionary only, the same
superseded/derivative/duplicate hygiene as the published track record:

    decision_filings   distinct filings of a discretionary buy or sell
    last_decision      the most recent one

It stores FACTS, not the policy. Which threshold to submit at lives in
`api/routers/sitemap.py`, so the rule can move without a re-materialization.

FAIL-OPEN BY CONSTRUCTION. The tables are rebuilt into temporaries and swapped
in one transaction, so a reader never sees a half-built table, and the sitemap
falls back to the old rule when a table is missing or stale. A sitemap that
silently shrinks to nothing is worse than one that is too generous.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.filters import MEANINGFUL_CLASSES  # noqa: E402
from config.database import get_connection  # noqa: E402

#: Never typed out. Derived from KIND_META via api.classification; see
#: tests/unit/test_meaningful_is_one_definition.py.
_CLASSES = tuple(sorted(MEANINGFUL_CLASSES))
_IN = ", ".join(f"'{c}'" for c in _CLASSES)

# One row per filing, discretionary only. COALESCE order matters: filing_key is
# the grouping key the rest of the product uses, accession is the fallback for
# rows predating it, and trade_date is a last resort that groups a day's lots
# together rather than counting each one.
_HYGIENE = f"""
      t.trans_code IN ('P', 'S')
  AND t.superseded_by IS NULL
  AND t.is_derivative = 0
  AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
  AND t.signal_class IN ({_IN})
"""

INSIDER_SQL = f"""
CREATE TABLE {{tmp}} AS
SELECT t.insider_id,
       COUNT(DISTINCT COALESCE(t.filing_key, t.accession, t.trade_date::text))
           AS decision_filings,
       MAX(t.trade_date) AS last_decision
  FROM trades t
 WHERE {_HYGIENE}
 GROUP BY t.insider_id
"""

COMPANY_SQL = f"""
CREATE TABLE {{tmp}} AS
SELECT t.ticker,
       COUNT(DISTINCT COALESCE(t.filing_key, t.accession, t.trade_date::text))
           AS decision_filings,
       MAX(t.trade_date) AS last_decision
  FROM trades t
 WHERE {_HYGIENE}
   AND t.ticker IS NOT NULL AND t.ticker <> '' AND t.ticker <> 'NONE'
 GROUP BY t.ticker
"""

TABLES = (
    ("sitemap_quality_insiders", INSIDER_SQL, "insider_id"),
    ("sitemap_quality_companies", COMPANY_SQL, "ticker"),
)


def refresh(*, dry_run: bool = False) -> dict[str, int]:
    """Rebuild both tables. Returns {table: row_count}."""
    counts: dict[str, int] = {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        # A queued ALTER/DROP blocks every later read on the table; see the
        # memory `feedback_alter_table_needs_lock_timeout`, which took
        # form4.app down on 2026-08-27. The swap below holds an ACCESS
        # EXCLUSIVE lock for the duration of two renames.
        cur.execute("SET lock_timeout = '10s'")
        cur.execute("SET statement_timeout = '1800s'")

        for name, sql, key in TABLES:
            tmp = f"{name}_new"
            old = f"{name}_old"
            cur.execute(f"DROP TABLE IF EXISTS {tmp}")
            cur.execute(sql.format(tmp=tmp))
            cur.execute(f"CREATE UNIQUE INDEX ON {tmp} ({key})")
            cur.execute(f"SELECT COUNT(*) FROM {tmp}")
            n = cur.fetchone()[0]
            counts[name] = int(n)

            if n == 0:
                raise RuntimeError(
                    f"{name} rebuilt to zero rows; refusing the swap. The "
                    "sitemap reads this to decide what to submit, and an empty "
                    "table would unpublish the site."
                )

            if dry_run:
                cur.execute(f"DROP TABLE {tmp}")
                continue

            # Atomic from a reader's point of view: the rename happens in
            # the same transaction as the build, so nothing ever sees the
            # table absent or half-built.
            cur.execute(f"DROP TABLE IF EXISTS {old}")
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (name,),
            )
            if cur.fetchone():
                cur.execute(f"ALTER TABLE {name} RENAME TO {old}")
            cur.execute(f"ALTER TABLE {tmp} RENAME TO {name}")
            cur.execute(f"DROP TABLE IF EXISTS {old}")
            # COMMENT ON takes a literal, not an expression, so the stamp is
            # built here. The sitemap reads it to decide whether the table is
            # stale enough to distrust; no stamp is treated as stale.
            stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
            cur.execute(f"COMMENT ON TABLE {name} IS %s", (f"refreshed_at={stamp}",))

        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    finally:
        conn.close()
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="build and count, then roll back")
    args = ap.parse_args()

    counts = refresh(dry_run=args.dry_run)
    for name, n in counts.items():
        print(f"{name}: {n:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
