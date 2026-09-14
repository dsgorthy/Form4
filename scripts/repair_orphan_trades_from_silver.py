#!/usr/bin/env python3
"""Fill owner-less trades rows from Silver, then retire their reload copies.

WHAT WAS FOUND (2026-09-14, via the Silver parity report)

48,052 rows in `trades` carry rptowner_cik IS NULL AND security_title IS NULL
with source='edgar_live': 13,780 filed 2026-01-02..03-16 and 34,272 filed
2016-2019. All were written 2026-03-12..03-16 by a pre-git revision of
backfill_live.insert_trades that did not yet write the "extended" block
(security_title, rptowner_cik, direct_indirect, shares_owned_after,
trans_acquired_disp, equity_swap, deemed_execution_date, trans_form_type,
line_no). insider_id IS populated on every one (name-resolved; it agrees with
Silver's CIK in 1,241 of 1,241 checkable rows) -- the rows are on their insider
pages, they are just missing the columns every other reader keys on.

Then, on 2026-08-26, the ingestion-loss reload loaded the same filings from the
SEC quarterly datasets as source='sec_form345'. Its dedupe could not recognise
the owner-less rows as the same lines, so 6,418 lots now exist twice under the
same insider and count twice: $22.3B of company aggregate value on the 2026
window alone.

WHAT THIS DOES

Step 1, FILL. For each owner-less row, find its line in silver.form4_transaction
(the faithful parse of the SEC bytes) by (accession, non-derivative, trade date,
code, shares, price within half a cent) and copy the missing columns. Two
rules from the data:
  - a joint filing yields one Silver row per reporting owner; the row's owner is
    the one whose rptowner_name equals the insiders.name of the row's insider_id.
    That resolves every one of the 689 such rows in the 2026 window.
  - identical repeated lots (242 rows) fix cik and title unambiguously but not
    line_no; line_no stays NULL rather than be guessed.
Rows that still do not pair (180 in 2026: fractional shares truncated into a
bigint qty, or price off by more than half a cent) are reported, not touched.

Step 2, DEDUPE. Mark the sec_form345 copy of each filled lot is_duplicate=1
where it carries the SAME insider_id. The 403 copies attributed to a different
insider are two real reporting owners on one line (an entity and a person) and
are left alone; which to suppress is a rule to set once, elsewhere.

WHAT THIS DOES NOT DO

Never touches insider_id. Never touches price, qty or value. Never inserts: the
116 Silver lines that trades never stored at all are a separate job. Never
runs without --apply; the default is a dry run that prints every count the
apply would assert.

SAFETY

--apply first copies the before-state of every candidate row (and every
sec_form345 row in those accessions) into a dated backup table, then runs each
UPDATE inside a transaction with lock_timeout and a row-count guard: if the
count differs from the dry run computed in the same transaction, it rolls back.

    python3 scripts/repair_orphan_trades_from_silver.py --window 2026            # dry run
    python3 scripts/repair_orphan_trades_from_silver.py --window 2026 --apply
    python3 scripts/repair_orphan_trades_from_silver.py --window 2016-2019 --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402
from framework.observability.pipeline_runner import pipeline_run  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("repair_orphans")

WINDOWS = {
    "2026": "t.filed_at >= '2026-01-01'",
    "2016-2019": "t.filed_at >= '2016-01-01' AND t.filed_at < '2020-01-01'",
    "all": "t.filed_at >= '2016-01-01'",
}

BACKUP_TABLE = "repair_orphans_20260914_before"

FILL_COLS = ["rptowner_cik", "security_title", "direct_indirect", "shares_owned_after",
             "trans_acquired_disp", "equity_swap", "nature_of_ownership",
             "deemed_execution_date", "trans_form_type", "line_no"]


def orphan_predicate(window: str, alias: str = "t") -> str:
    w = WINDOWS[window].replace("t.", f"{alias}.")
    return (f"{alias}.rptowner_cik IS NULL AND {alias}.security_title IS NULL "
            f"AND {alias}.source = 'edgar_live' AND {w}")


def pick_cte(window: str) -> str:
    """The pairing. One row per orphan trade_id, or none."""
    return f"""
    WITH o AS (
        SELECT t.trade_id, t.accession, t.trade_date, t.trans_code, t.qty, t.price,
               upper(i.name) AS iname
          FROM trades t JOIN insiders i USING (insider_id)
         WHERE {orphan_predicate(window)}
    ), m AS (
        SELECT o.trade_id, o.iname,
               s.rptowner_cik, s.rptowner_name, s.line_no, s.security_title, s.direct_indirect,
               s.shares_owned_after, s.trans_acquired_disp, s.equity_swap,
               s.nature_of_ownership, s.deemed_execution_date, s.trans_form_type,
               (upper(s.rptowner_name) = o.iname) AS name_match
          FROM o JOIN silver.form4_transaction s
            ON s.accession = o.accession AND NOT s.is_derivative
           AND s.trans_date::text = o.trade_date AND s.trans_code = o.trans_code
           AND s.shares = o.qty
           AND abs(coalesce(s.price_per_share, 0) - o.price) < 0.006
    ), stats AS (
        SELECT trade_id,
               count(DISTINCT rptowner_cik) AS n_cik,
               count(DISTINCT rptowner_cik) FILTER (WHERE name_match) AS n_named
          FROM m GROUP BY trade_id
    ), resolved AS (
        SELECT m.*, st.n_cik, st.n_named,
               count(*) OVER (PARTITION BY m.trade_id, m.rptowner_cik) AS n_lines_this_cik
          FROM m JOIN stats st USING (trade_id)
         WHERE st.n_cik = 1 OR (st.n_named = 1 AND m.name_match)
    ), pick AS (
        SELECT DISTINCT ON (trade_id) *
          FROM resolved
         ORDER BY trade_id, line_no
    )"""


COUNT_SQL = """
    SELECT
      (SELECT count(*) FROM trades t WHERE {orphan}) AS orphans,
      (SELECT count(*) FROM pick) AS will_fill,
      (SELECT count(*) FROM pick WHERE n_lines_this_cik = 1) AS with_line_no,
      (SELECT count(*) FROM pick WHERE n_cik > 1) AS joint_resolved_by_name
"""

UPDATE_SQL = """
    UPDATE trades t SET
        rptowner_cik          = p.rptowner_cik,
        security_title        = p.security_title,
        direct_indirect       = p.direct_indirect,
        shares_owned_after    = p.shares_owned_after::double precision,
        trans_acquired_disp   = p.trans_acquired_disp,
        equity_swap           = CASE WHEN p.equity_swap IS NULL THEN NULL
                                     WHEN p.equity_swap THEN 1 ELSE 0 END,
        nature_of_ownership   = p.nature_of_ownership,
        deemed_execution_date = p.deemed_execution_date::text,
        trans_form_type       = p.trans_form_type,
        line_no               = CASE WHEN p.n_lines_this_cik = 1 THEN p.line_no ELSE NULL END
      FROM pick p
     WHERE t.trade_id = p.trade_id
       AND t.rptowner_cik IS NULL AND t.security_title IS NULL   -- never overwrite a filled row
"""

# The reload copy of a lot this run filled, under the SAME insider. Joined on
# the lot, not on the new cik, so a wrong pairing above cannot widen this.
DEDUPE_COUNT_SQL = """
    SELECT count(DISTINCT s.trade_id)
      FROM trades s
      JOIN trades o ON o.accession = s.accession AND o.insider_id = s.insider_id
                   AND o.trans_code = s.trans_code AND o.trade_date = s.trade_date
                   AND o.qty = s.qty AND abs(o.price - s.price) < 0.006
     WHERE s.source = 'sec_form345' AND coalesce(s.is_duplicate, 0) = 0
       AND o.source = 'edgar_live' AND coalesce(o.is_duplicate, 0) = 0
       AND o.rptowner_cik IS NOT NULL AND o.trade_id IN (SELECT trade_id FROM {backup} WHERE {win})
"""
DEDUPE_SQL = """
    UPDATE trades s SET is_duplicate = 1
     WHERE s.source = 'sec_form345' AND coalesce(s.is_duplicate, 0) = 0
       AND EXISTS (
           SELECT 1 FROM trades o
            WHERE o.accession = s.accession AND o.insider_id = s.insider_id
              AND o.trans_code = s.trans_code AND o.trade_date = s.trade_date
              AND o.qty = s.qty AND abs(o.price - s.price) < 0.006
              AND o.source = 'edgar_live' AND coalesce(o.is_duplicate, 0) = 0
              AND o.rptowner_cik IS NOT NULL
              AND o.trade_id IN (SELECT trade_id FROM {backup} WHERE {win}))
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", choices=list(WINDOWS), default="2026")
    ap.add_argument("--apply", action="store_true", help="write; default is a dry run")
    ap.add_argument("--no-dedupe", action="store_true", help="fill only")
    args = ap.parse_args()

    orphan = orphan_predicate(args.window)
    with pipeline_run("repair_orphans_from_silver") as run:
        conn = get_connection()
        conn.execute("SET lock_timeout = '5s'")
        conn.execute("SET statement_timeout = '1800s'")

        r = conn.execute(pick_cte(args.window) + COUNT_SQL.format(orphan=orphan)).fetchone()
        logger.info("window=%s orphans=%d will_fill=%d (line_no on %d, joint filings resolved by name %d) unmatched=%d",
                    args.window, r["orphans"], r["will_fill"], r["with_line_no"],
                    r["joint_resolved_by_name"], r["orphans"] - r["will_fill"])
        sample = conn.execute(pick_cte(args.window) + """
            SELECT t.trade_id, t.accession, t.trade_date, t.trans_code, t.qty, t.price
              FROM trades t WHERE {orphan}
               AND NOT EXISTS (SELECT 1 FROM pick p WHERE p.trade_id = t.trade_id)
             ORDER BY t.trade_id LIMIT 8""".format(orphan=orphan)).fetchall()
        for s in sample:
            logger.info("  unmatched: %s %s %s %s qty=%s price=%s", s["trade_id"], s["accession"],
                        s["trade_date"], s["trans_code"], s["qty"], s["price"])
        run.set_metadata({"window": args.window, "orphans": r["orphans"], "will_fill": r["will_fill"],
                          "with_line_no": r["with_line_no"], "unmatched": r["orphans"] - r["will_fill"],
                          "apply": args.apply})

        if not args.apply:
            logger.info("dry run; nothing written")
            return 0

        # Before-state, once, for every window: the orphans and every reload
        # copy in their accessions. Restore = UPDATE trades t SET col = b.col
        # FROM %s b WHERE b.trade_id = t.trade_id.
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {BACKUP_TABLE} AS
            SELECT t.trade_id, t.source, t.filed_at, {', '.join('t.' + c for c in FILL_COLS)}, t.is_duplicate,
                   now() AS taken_at
              FROM trades t
             WHERE ({orphan_predicate('all')})
                OR (t.source = 'sec_form345' AND t.accession IN
                      (SELECT accession FROM trades x WHERE {orphan_predicate('all', 'x')}))
        """)
        conn.commit()
        n_backup = conn.execute(f"SELECT count(*) AS n FROM {BACKUP_TABLE}").fetchone()["n"]
        logger.info("backup table %s holds %d rows", BACKUP_TABLE, n_backup)
        if n_backup == 0:
            logger.error("backup table is empty; refusing to write")
            return 2

        # FILL, guarded.
        try:
            expected = conn.execute(pick_cte(args.window) + "SELECT count(*) AS n FROM pick").fetchone()["n"]
            cur = conn.execute(pick_cte(args.window) + UPDATE_SQL)
            got = cur.rowcount
            if got != expected:
                raise RuntimeError(f"fill updated {got} rows, expected {expected}")
            conn.commit()
            logger.info("FILL committed: %d rows", got)
        except Exception:
            conn.rollback()
            logger.exception("FILL rolled back")
            return 1

        if args.no_dedupe:
            return 0

        # DEDUPE, guarded. Only lots that were in the backup as orphans in this
        # window and are now filled.
        win = WINDOWS[args.window].replace("t.", "")
        try:
            expected = conn.execute(DEDUPE_COUNT_SQL.format(backup=BACKUP_TABLE, win=win)).fetchone()["count"]
            cur = conn.execute(DEDUPE_SQL.format(backup=BACKUP_TABLE, win=win))
            got = cur.rowcount
            if got != expected:
                raise RuntimeError(f"dedupe marked {got} rows, expected {expected}")
            conn.commit()
            logger.info("DEDUPE committed: %d sec_form345 copies marked is_duplicate=1", got)
        except Exception:
            conn.rollback()
            logger.exception("DEDUPE rolled back (fill stays committed)")
            return 1
        run.set_rows_written(got)
    return 0


if __name__ == "__main__":
    sys.exit(main())
