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
    -- Pair sibling lines POSITIONALLY. trades.line_no is NULL on every row
    -- older than the column, and quantity cannot tell siblings apart either
    -- (0000001750-06-000062 has four same-day S lots of equal size, so a
    -- quantity join fanned each line into three). Silver's line_no is
    -- document order; the live parser inserted in document order, so trade_id
    -- is the same order on the trades side. Rank both within the filing's
    -- natural key and join on rank.
    s AS (SELECT f.*,
                 row_number() OVER (PARTITION BY f.accession, f.rptowner_cik, f.trans_date, f.trans_code,
                                                 COALESCE(f.security_title, ''), f.is_derivative
                                    ORDER BY f.line_no) AS rk
            FROM silver.form4_transaction f JOIN parsed USING (accession)),
    t AS (SELECT tr.*,
                 row_number() OVER (PARTITION BY tr.accession, tr.rptowner_cik, tr.trade_date, tr.trans_code,
                                                 COALESCE(tr.security_title, ''), COALESCE(tr.is_derivative, 0)
                                    ORDER BY tr.trade_id) AS rk
            FROM trades tr JOIN parsed USING (accession)
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
       AND COALESCE(t.is_derivative, 0)::int = s.is_derivative::int
       AND t.rk = s.rk
"""


# Full-corpus mode: the buckets are computed SERVER-SIDE. The first full run
# tried to pull every joined row over 4.08M accessions into Python and did
# not finish; the smoke run (10k accessions) was the last time that was a
# reasonable thing to do. Same strict pairing as JOIN_SQL, same cents rule,
# no loose pass (it needs Python and is a small correction at this scale --
# report it from the sample run instead).
SUMMARY_SQL = """
    WITH parsed AS (SELECT accession FROM silver.form4_parse WHERE status = 'ok'),
    s AS (SELECT f.*,
                 row_number() OVER (PARTITION BY f.accession, f.rptowner_cik, f.trans_date, f.trans_code,
                                                 COALESCE(f.security_title, ''), f.is_derivative
                                    ORDER BY f.line_no) AS rk
            FROM silver.form4_transaction f JOIN parsed USING (accession)),
    t AS (SELECT tr.*,
                 row_number() OVER (PARTITION BY tr.accession, tr.rptowner_cik, tr.trade_date, tr.trans_code,
                                                 COALESCE(tr.security_title, ''), COALESCE(tr.is_derivative, 0)
                                    ORDER BY tr.trade_id) AS rk
            FROM trades tr JOIN parsed USING (accession)
           WHERE COALESCE(tr.is_duplicate, 0) = 0),
    j AS (
        SELECT s.accession AS s_acc, t.trade_id, s.is_derivative AS s_der, t.trans_code AS t_code, s.trans_code AS s_code,
               s.shares, t.qty, s.shares_owned_after AS s_after, t.shares_owned_after AS t_after,
               s.price_per_share AS s_price, t.price AS t_price, t.price_as_filed
          FROM s FULL OUTER JOIN t
            ON t.accession = s.accession AND t.rptowner_cik = s.rptowner_cik
           AND t.trade_date::date = s.trans_date AND t.trans_code = s.trans_code
           AND COALESCE(t.security_title, '') = COALESCE(s.security_title, '')
           AND COALESCE(t.is_derivative, 0)::int = s.is_derivative::int
           AND t.rk = s.rk),
    b AS (
        SELECT CASE
                 WHEN s_acc IS NULL THEN 'trades_only'
                 WHEN trade_id IS NULL AND s_der THEN 'silver_only_derivative'
                 WHEN trade_id IS NULL THEN 'silver_only'
                 WHEN price_as_filed IS NOT NULL AND round(s_price::numeric, 2) = round(price_as_filed::numeric, 2)
                      AND round(COALESCE(shares, 0)::numeric, 2) = round(COALESCE(qty, 0)::numeric, 2)
                      AND round(COALESCE(s_after, 0)::numeric, 2) = round(COALESCE(t_after, 0)::numeric, 2) THEN 'price_overwritten'
                 WHEN round(COALESCE(s_price, 0)::numeric, 2) = round(COALESCE(t_price, 0)::numeric, 2)
                      AND round(COALESCE(shares, 0)::numeric, 2) = round(COALESCE(qty, 0)::numeric, 2)
                      AND round(COALESCE(s_after, 0)::numeric, 2) = round(COALESCE(t_after, 0)::numeric, 2) THEN 'match'
                 ELSE 'mismatch' END AS bucket
          FROM j)
    SELECT bucket, count(*) AS n FROM b GROUP BY 1 ORDER BY 2 DESC
"""


def summary(conn) -> list[dict]:
    return [dict(r) for r in conn.execute(SUMMARY_SQL).fetchall()]


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



def _loose_pair(rows: list[dict]) -> list[dict]:
    """Second pass for lines the strict join could not pair because the
    trades row has no owner or no security title.

    Found 2026-09-14: 2026-dated sell rows in trades with rptowner_cik NULL
    and security_title NULL (0000002488-26-000024 holds ten). A join on owner
    cannot see them, so Silver's line reads as "silver_only" and the trades
    row as "trades_only" when in fact they are the same transaction. Pair the
    strict leftovers within (accession, date, code, is_derivative) in
    document order and say what was missing on the trades side.
    """
    unmatched_s = [r for r in rows if r["trade_id"] is None and r["accession"] is not None]
    unmatched_t = [r for r in rows if r["s_code"] is None and r["t_code"] is not None]
    if not unmatched_s or not unmatched_t:
        return rows
    key_s = lambda r: (r["accession"], str(r["s_date"]), r["s_code"], bool(r["s_is_derivative"]))
    key_t = lambda r: (r["accession"], str(r["t_date"]), r["t_code"], False)  # trades_only rows: derivative flag not selected; assume non-derivative
    from collections import defaultdict
    by_t: dict = defaultdict(list)
    for r in unmatched_t:
        if r["rptowner_cik"] is None or r["t_title"] is None:
            by_t[key_t(r)].append(r)
    for lst in by_t.values():
        lst.sort(key=lambda r: r["trade_id"])
    paired = set()
    merged = []
    for r in sorted(unmatched_s, key=lambda r: (r["accession"], r["line_no"])):
        cands = by_t.get(key_s(r))
        if not cands:
            continue
        t = cands.pop(0)
        paired.add(id(t))
        m = dict(r)
        for k in ("t_code", "t_date", "t_ad", "t_title", "t_di", "t_ticker", "t_qty",
                  "t_price", "t_price_as_filed", "t_after", "t_line_no", "trade_id"):
            m[k] = t[k]
        m["loose"] = "trades_owner_null" if t["rptowner_cik"] is None else "trades_title_null"
        merged.append((id(r), m))
    if not merged:
        return rows
    replace = dict(merged)
    out = []
    for r in rows:
        if id(r) in replace:
            out.append(replace[id(r)])
        elif id(r) in paired:
            continue
        else:
            out.append(r)
    return out


def bucket(r: dict) -> tuple[str, str]:
    if r["accession"] is None or (r["s_code"] is None and r["t_code"] is not None):
        return "trades_only", ""
    if r["trade_id"] is None:
        # trades never carried derivative lines for most of its history; those
        # are expected here and are reported apart from the non-derivative
        # lines the live path dropped, which are the ingestion-loss class.
        return ("silver_only_derivative" if r["s_is_derivative"] else "silver_only", "")
    diffs = []
    # shares (a key now) and shares_owned_after are compared at cents for the
    # same reason as price: trades rounds (3925.009 -> 3925.01).
    if not _cents_eq(r["s_shares"], r["t_qty"]):
        diffs.append("shares")
    if not _cents_eq(r["s_after"], r["t_after"]):
        diffs.append("shares_owned_after")
    if r["t_price_as_filed"] is not None:
        if _cents_eq(r["s_price"], r["t_price_as_filed"]):
            return ("price_overwritten" if not diffs else "mismatch", ",".join(diffs))
        diffs.append("price_as_filed")
    elif not _cents_eq(r["s_price"], r["t_price"]):
        diffs.append("price")
    if r.get("loose"):
        diffs.append(r["loose"])
    return ("match" if not diffs else "mismatch", ",".join(diffs))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None)
    ap.add_argument("--examples", type=int, default=20)
    ap.add_argument("--summary", action="store_true",
                    help="full-corpus bucket counts computed in SQL; no per-row examples")
    args = ap.parse_args()

    conn = get_connection(readonly=True)
    if args.summary:
        conn.execute("SET statement_timeout = '3600s'")
        rows_s = summary(conn)
        total = sum(r["n"] for r in rows_s)
        parsed = conn.execute("SELECT count(*) AS n FROM silver.form4_parse WHERE status = 'ok'").fetchone()["n"]
        print(f"# Silver vs trades parity (summary) — {date.today().isoformat()}\n")
        print(f"{total:,} joined rows over {parsed:,} parsed accessions.\n")
        print("| bucket | rows | share |\n|---|---|---|")
        for r in rows_s:
            print(f"| {r['bucket']} | {r['n']:,} | {100.0 * r['n'] / total if total else 0:.2f}% |")
        return 0
    rows = [dict(r) for r in conn.execute(JOIN_SQL).fetchall()]
    rows = _loose_pair(rows)
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
