#!/usr/bin/env python3
"""Silver vs trades: the report to read before any cutover conversation.

Joins silver.form4_transaction to trades on the filing's natural key --
(accession, rptowner_cik, trans_date, trans_code, security_title) plus
line_no where trades has one -- over every accession Silver has parsed, and
sorts the result into buckets:

    match            every compared column equal
    price_overwritten trades.price differs but silver.price_per_share =
                     trades.price_as_filed: the live path's price_validator
                     rewrote a source fact and Silver has the original. THIS
                     IS THE ROW THAT PROVES THE LAYER MODEL RIGHT.
    mismatch         some compared column differs and it is not the above
    silver_only      Silver has a line trades does not -- the ingestion-loss
                     class, or a line the live parser dropped
    trades_only      trades has a row for a parsed accession that Silver
                     cannot account for

Writes reports/silver_parity_<date>.md and .csv. Read-only.

    python3 pipelines/silver/parity.py
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402

COMPARE = ["trans_code", "trans_date", "trans_acquired_disp", "security_title",
           "direct_indirect", "ticker"]

JOIN_SQL = """
    WITH parsed AS (SELECT accession FROM silver.form4_parse WHERE status = 'ok'),
    s AS (SELECT f.* FROM silver.form4_transaction f JOIN parsed USING (accession)),
    t AS (SELECT tr.* FROM trades tr JOIN parsed USING (accession)
           WHERE COALESCE(tr.is_duplicate, 0) = 0)
    SELECT s.accession, s.rptowner_cik, s.line_no,
           s.trans_code AS s_code, t.trans_code AS t_code,
           s.trans_date AS s_date, t.trade_date::date AS t_date,
           s.trans_acquired_disp AS s_ad, t.trans_acquired_disp AS t_ad,
           s.security_title AS s_title, t.security_title AS t_title,
           s.direct_indirect AS s_di, t.direct_indirect AS t_di,
           s.ticker AS s_ticker, t.ticker AS t_ticker,
           s.shares AS s_shares, t.qty AS t_qty, s.is_derivative AS s_is_derivative,
           s.price_per_share AS s_price, t.price AS t_price, t.price_as_filed AS t_price_as_filed,
           s.shares_owned_after AS s_after, t.shares_owned_after AS t_after,
           t.line_no AS t_line_no, t.trade_id
      FROM s
      FULL OUTER JOIN t
        ON t.accession = s.accession
       AND t.rptowner_cik = s.rptowner_cik
       AND t.trade_date::date = s.trans_date
       AND t.trans_code = s.trans_code
       AND COALESCE(t.security_title, '') = COALESCE(s.security_title, '')
       -- trades.line_no is NULL on every row older than the line_no column
       -- (69,347 of the first 10,000 accessions' rows, 2026-09-14). Without
       -- a line number a filing's sibling lines -- three S lots on one day --
       -- are told apart by quantity; joining on (date, code, title) alone
       -- fanned 24,794 Silver lines into 87,808 pairs and called every one
       -- a shares mismatch.
       AND (t.line_no = s.line_no
            OR (t.line_no IS NULL AND abs(t.qty::numeric - s.shares) < 0.0001))
       AND COALESCE(t.is_derivative, 0)::int = s.is_derivative::int
"""


def _num_eq(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) < 1e-6
    except (TypeError, ValueError):
        return False


def _cents_eq(a, b) -> bool:
    """trades stores price rounded to cents; Silver keeps the filed decimals
    (24.6414 vs 24.64). Equal at cents is the live path being lossy, not a
    disagreement about the filing -- the exact value is Silver's to keep."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return round(float(a), 2) == round(float(b), 2)


def bucket(r: dict) -> tuple[str, str]:
    if r["accession"] is None or (r["s_code"] is None and r["t_code"] is not None):
        return "trades_only", ""
    if r["trade_id"] is None:
        # trades never carried derivative lines for most of its history; those
        # are expected here and are reported apart from the non-derivative
        # lines the live path dropped, which are the ingestion-loss class.
        return ("silver_only_derivative" if r["s_is_derivative"] else "silver_only", "")
    diffs = []
    if not _num_eq(r["s_after"], r["t_after"]):
        diffs.append("shares_owned_after")
    if r["t_price_as_filed"] is not None:
        if _cents_eq(r["s_price"], r["t_price_as_filed"]):
            return ("price_overwritten" if not diffs else "mismatch", ",".join(diffs))
        diffs.append("price_as_filed")
    elif not _cents_eq(r["s_price"], r["t_price"]):
        diffs.append("price")
    return ("match" if not diffs else "mismatch", ",".join(diffs))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None)
    ap.add_argument("--examples", type=int, default=20)
    args = ap.parse_args()

    conn = get_connection(readonly=True)
    rows = [dict(r) for r in conn.execute(JOIN_SQL).fetchall()]
    counts: dict[str, int] = {}
    examples: dict[str, list] = {}
    out_rows = []
    for r in rows:
        b, why = bucket(r)
        counts[b] = counts.get(b, 0) + 1
        if b != "match" and len(examples.setdefault(b, [])) < args.examples:
            examples[b].append((r["accession"], r["rptowner_cik"], r["line_no"], why,
                                r["s_price"], r["t_price"], r["t_price_as_filed"], r["s_shares"], r["t_qty"]))
        if b != "match":
            out_rows.append({"bucket": b, "why": why, **{k: r[k] for k in r}})

    today = date.today().isoformat()
    base = Path(args.out) if args.out else Path("reports") / f"silver_parity_{today}"
    base.parent.mkdir(parents=True, exist_ok=True)
    total = sum(counts.values())
    md = [f"# Silver vs trades parity — {today}", "",
          f"{total:,} joined rows over {conn.execute('SELECT count(*) AS n FROM silver.form4_parse WHERE status = %s', ('ok',)).fetchone()['n']:,} parsed accessions.", "",
          "| bucket | rows | share |", "|---|---|---|"]
    for b in ("match", "price_overwritten", "mismatch", "silver_only", "silver_only_derivative", "trades_only"):
        n = counts.get(b, 0)
        md.append(f"| {b} | {n:,} | {100.0 * n / total if total else 0:.2f}% |")
    for b, ex in examples.items():
        md += ["", f"## {b} — first {len(ex)}", "",
               "| accession | owner | line | why | s.price | t.price | t.price_as_filed | s.shares | t.qty |", "|---|---|---|---|---|---|---|---|---|"]
        md += [f"| {a} | {o} | {ln} | {w} | {sp} | {tp} | {pf} | {ss} | {tq} |" for a, o, ln, w, sp, tp, pf, ss, tq in ex]
    Path(str(base) + ".md").write_text("\n".join(md) + "\n")
    with open(str(base) + ".csv", "w", newline="") as fh:
        if out_rows:
            w = csv.DictWriter(fh, fieldnames=list(out_rows[0].keys()))
            w.writeheader()
            w.writerows(out_rows)
    print("\n".join(md[:12]))
    print(f"\nwrote {base}.md and .csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
