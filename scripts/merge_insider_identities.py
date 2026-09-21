#!/usr/bin/env python3
"""Plan (and, with --apply, perform) the merge of duplicate insider rows.

Runs on the Studio. Default is a DRY RUN that writes the plan and touches
nothing. See api/identity.py for what "duplicate" means: rows that share
one FILED reporting-owner CIK (trades.rptowner_cik) and whose names are
identical or compatible variants. Rows that share a CIK with unrelated names
are joint filings mis-stamped with one owner's CIK; they go to the review
file, never into the plan.

    python3 scripts/merge_insider_identities.py --out reports/identity_merge_plan.md
    python3 scripts/merge_insider_identities.py --apply     # after Derek has read the plan

What a merge does, per group: the survivor is the insider_id with the most
trades rows; every other row's references move to it in every table keyed on
insider_id (below), its slug becomes an alias so the URL keeps working
(insider_slug_aliases), and the row is deleted. Within one transaction, with
row-count guards, rolled back on any surprise.

Tables keyed on insider_id (information_schema, 2026-09-21):
    public.trades, public.insider_companies, public.insider_group_members,
    public.insider_similarity, public.insider_slug_aliases,
    public.insider_ticker_scores, public.insider_ticker_scores_pre_reload,
    public.insider_track_records, public.insider_track_records_retired_win_rates,
    public.score_history, public.bad_trades, notifications.watchlist,
    research.derivative_trades, research.nonderiv_holdings, gold.insider_by_cik
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.identity import classify_group  # noqa: E402
from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GROUPS_SQL = """
SELECT t.rptowner_cik AS cik, t.insider_id, i.name, i.slug, count(*) AS n_trades
  FROM trades t JOIN insiders i USING (insider_id)
 WHERE t.rptowner_cik IN (SELECT rptowner_cik FROM trades WHERE rptowner_cik IS NOT NULL
                           GROUP BY 1 HAVING count(DISTINCT insider_id) > 1)
 GROUP BY 1, 2, 3, 4
 ORDER BY 1, 5 DESC, 2
"""

# (table, column) pairs to rewrite. insider_similarity has two id columns.
REFS = [
    ("public.trades", "insider_id"), ("public.trades", "effective_insider_id"),
    ("public.insider_companies", "insider_id"), ("public.insider_group_members", "insider_id"),
    ("public.insider_similarity", "insider_id"), ("public.insider_similarity", "similar_insider_id"),
    ("public.insider_ticker_scores", "insider_id"), ("public.insider_ticker_scores_pre_reload", "insider_id"),
    ("public.insider_track_records", "insider_id"), ("public.insider_track_records_retired_win_rates", "insider_id"),
    ("public.score_history", "insider_id"), ("public.bad_trades", "insider_id"),
    ("notifications.watchlist", "insider_id"),
    ("research.derivative_trades", "insider_id"), ("research.nonderiv_holdings", "insider_id"),
]


def plan(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """(merges, review). Pure. A merge is {cik, survivor, survivor_name,
    merged: [{insider_id, name, slug, n_trades}], kind}."""
    by: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by[r["cik"]].append(r)
    merges, review = [], []
    for cik, rs in by.items():
        rs = sorted(rs, key=lambda r: (-int(r["n_trades"]), int(r["insider_id"])))
        kind = classify_group([r["name"] for r in rs])
        entry = {"cik": cik, "survivor": int(rs[0]["insider_id"]), "survivor_name": rs[0]["name"],
                 "merged": [{"insider_id": int(r["insider_id"]), "name": r["name"], "slug": r["slug"],
                             "n_trades": int(r["n_trades"])} for r in rs[1:]], "kind": kind}
        (merges if kind in ("identical", "variants") else review).append(entry)
    return merges, review


def write_report(merges: list[dict], review: list[dict], out: Path) -> None:
    moved = sum(m["n_trades"] for g in merges for m in g["merged"])
    lines = [f"# Insider identity merge plan — {date.today().isoformat()}", "",
             f"Groups keyed on the filed owner CIK. **{len(merges)} groups mergeable** "
             f"({sum(1 for g in merges if g['kind'] == 'identical')} identical names, "
             f"{sum(1 for g in merges if g['kind'] == 'variants')} compatible variants), "
             f"{sum(len(g['merged']) for g in merges)} rows would fold into their survivors, "
             f"moving {moved:,} trades rows. **{len(review)} groups need a human** (renames, or joint "
             f"filings stamped with one owner's CIK — those are a data repair, not a merge).", "",
             "Nothing here has been applied. `--apply` performs the mergeable groups only.", "",
             "## Mergeable (first 80 by trades moved)", "",
             "| CIK | survivor | merged into it | trades moved | kind |", "|---|---|---|---|---|"]
    for g in sorted(merges, key=lambda g: -sum(m["n_trades"] for m in g["merged"]))[:80]:
        lines.append(f"| {g['cik']} | {g['survivor_name']} ({g['survivor']}) | "
                     + "; ".join(f"{m['name']} ({m['insider_id']})" for m in g["merged"])
                     + f" | {sum(m['n_trades'] for m in g['merged']):,} | {g['kind']} |")
    lines += ["", "## Needs review (first 80 by trades)", "", "| CIK | names (trades) |", "|---|---|"]
    for g in sorted(review, key=lambda g: -sum(m["n_trades"] for m in g["merged"]))[:80]:
        names = [f"{g['survivor_name']} ({g['survivor']})"] + [f"{m['name']} ({m['n_trades']:,})" for m in g["merged"]]
        lines.append(f"| {g['cik']} | " + "; ".join(names) + " |")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n")


def apply(conn, merges: list[dict]) -> None:
    conn.execute("SET lock_timeout = '5s'")
    conn.execute("SET statement_timeout = '1800s'")
    total_rows = 0
    try:
        for g in merges:
            surv = g["survivor"]
            for m in g["merged"]:
                old = m["insider_id"]
                for table, col in REFS:
                    cur = conn.execute(f"UPDATE {table} SET {col} = ? WHERE {col} = ?", (surv, old))
                    total_rows += cur.rowcount
                if m["slug"]:
                    conn.execute("INSERT INTO insider_slug_aliases (slug, insider_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                                 (m["slug"], surv))
                cur = conn.execute("DELETE FROM insiders WHERE insider_id = ?", (old,))
                if cur.rowcount != 1:
                    raise RuntimeError(f"expected to delete one insiders row for {old}, deleted {cur.rowcount}")
        conn.commit()
        logger.info("APPLIED: %d groups, %d reference rows rewritten", len(merges), total_rows)
    except Exception:
        conn.rollback()
        logger.exception("rolled back; nothing changed")
        raise


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="reports/identity_merge_plan.md")
    ap.add_argument("--apply", action="store_true", help="perform the mergeable groups (default: dry run)")
    args = ap.parse_args()
    conn = get_connection()
    rows = [dict(r) for r in conn.execute(GROUPS_SQL).fetchall()]
    merges, review = plan(rows)
    write_report(merges, review, Path(args.out))
    logger.info("plan: %d mergeable groups, %d for review -> %s", len(merges), len(review), args.out)
    if args.apply:
        apply(conn, merges)
    return 0


if __name__ == "__main__":
    sys.exit(main())
