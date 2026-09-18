#!/usr/bin/env python3
"""What would change on the product if it read Gold instead of `trades`?

Runs on the Studio (needs the form4 database). For each company page in the
sample it computes the numbers the page actually shows -- open-market filing
count, distinct insiders, 6-month buy and sell value, first and last trade --
two ways: from `trades` exactly as api/routers/companies.py does, and from
gold.form4_line with the equivalent predicates. Then the same for insider
pages (discretionary buy / sell filing counts). Buckets the differences and
writes a report a person can read before any cutover conversation.

    python3 pipelines/silver/gold_parity.py --tickers file --slugs file --out reports/gold_parity_2026-09-18.md

Predicates, side by side (trades -> gold):
    is_duplicate = 0            -> NOT is_joint_copy
    superseded_by IS NULL       -> superseded_by IS NULL
    signal_class IN meaningful  -> signal_class IN meaningful   (Gold cannot see 10b5-1 yet; see the plan)
    is_derivative = 0           -> NOT is_derivative
    value filter: NOT value_suspect AND price_quality <> implausible
                                -> price_quality <> 'implausible'
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.filters import MEANINGFUL_CLASSES  # noqa: E402
from config.database import get_connection  # noqa: E402

MEANINGFUL = tuple(sorted(MEANINGFUL_CLASSES))

TRADES_COMPANY_SQL = f"""
SELECT COUNT(DISTINCT COALESCE(filing_key, accession)) AS filings,
       COUNT(DISTINCT insider_id) AS insiders,
       COALESCE(SUM(CASE WHEN trade_type = 'buy'  AND trade_date >= ? THEN value ELSE 0 END)
           FILTER (WHERE NOT COALESCE(value_suspect, FALSE) AND price_quality IS DISTINCT FROM 'implausible'), 0) AS buy_6mo,
       COALESCE(SUM(CASE WHEN trade_type = 'sell' AND trade_date >= ? THEN value ELSE 0 END)
           FILTER (WHERE NOT COALESCE(value_suspect, FALSE) AND price_quality IS DISTINCT FROM 'implausible'), 0) AS sell_6mo,
       MIN(trade_date) AS first_trade, MAX(trade_date) AS last_trade
  FROM trades
 WHERE ticker = ?
   AND (is_duplicate = 0 OR is_duplicate IS NULL)
   AND superseded_by IS NULL
   AND signal_class IN ({",".join("?" * len(MEANINGFUL))})
   AND is_derivative = 0
"""

GOLD_COMPANY_SQL = f"""
SELECT COUNT(DISTINCT accession) AS filings,
       COUNT(DISTINCT COALESCE(insider_id::text, 'cik:' || rptowner_cik)) AS insiders,
       COALESCE(SUM(CASE WHEN trans_code = 'P' AND trans_date >= ?::date THEN value ELSE 0 END)
           FILTER (WHERE price_quality IS DISTINCT FROM 'implausible'), 0) AS buy_6mo,
       COALESCE(SUM(CASE WHEN trans_code = 'S' AND trans_date >= ?::date THEN value ELSE 0 END)
           FILTER (WHERE price_quality IS DISTINCT FROM 'implausible'), 0) AS sell_6mo,
       MIN(trans_date)::text AS first_trade, MAX(trans_date)::text AS last_trade
  FROM gold.form4_line
 WHERE ticker = ?
   AND NOT is_joint_copy
   AND superseded_by IS NULL
   AND signal_class IN ({",".join("?" * len(MEANINGFUL))})
   AND NOT is_derivative
"""

TRADES_INSIDER_SQL = f"""
SELECT COUNT(DISTINCT COALESCE(filing_key, accession)) FILTER (WHERE signal_class = 'discretionary_buy')  AS buys,
       COUNT(DISTINCT COALESCE(filing_key, accession)) FILTER (WHERE signal_class = 'discretionary_sell') AS sells,
       MAX(filing_date) AS last_filing
  FROM trades
 WHERE insider_id = ?
   AND (is_duplicate = 0 OR is_duplicate IS NULL) AND superseded_by IS NULL
   AND signal_class IN ({",".join("?" * len(MEANINGFUL))}) AND is_derivative = 0
"""

# No is_joint_copy filter here, deliberately. A joint filing is one
# transaction on the COMPANY page (distinct accession already collapses it)
# but it belongs on EVERY co-filer's page: Boaz Weinstein's 4,949 lines are all
# co-filed with Saba entities, and with the filter his page read 0 buys
# against 1,178 in trades. Ordering the copies by CIK and keeping the first
# is a company-count device, not an ownership claim.
GOLD_INSIDER_SQL = f"""
SELECT COUNT(DISTINCT accession) FILTER (WHERE signal_class = 'discretionary_buy')  AS buys,
       COUNT(DISTINCT accession) FILTER (WHERE signal_class = 'discretionary_sell') AS sells,
       MAX(filed_at)::date::text AS last_filing
  FROM gold.form4_line
 WHERE insider_id = ?
   AND superseded_by IS NULL
   AND signal_class IN ({",".join("?" * len(MEANINGFUL))}) AND NOT is_derivative
"""

YEARLY_SQL = """
SELECT yr, SUM(t) AS trades_filings, SUM(g) AS gold_filings FROM (
    SELECT substr(filing_date, 1, 4) AS yr, COUNT(DISTINCT COALESCE(filing_key, accession)) AS t, 0 AS g
      FROM trades WHERE is_derivative = 0 AND (is_duplicate = 0 OR is_duplicate IS NULL) AND superseded_by IS NULL
       AND signal_class IN ('discretionary_buy', 'discretionary_sell', 'planned_buy', 'planned_sell')
     GROUP BY 1
    UNION ALL
    SELECT to_char(filed_at, 'YYYY'), 0, COUNT(DISTINCT accession)
      FROM gold.form4_line WHERE NOT is_derivative AND NOT is_joint_copy AND superseded_by IS NULL
       AND trans_code IN ('P', 'S')
     GROUP BY 1
) x WHERE yr >= '2016' GROUP BY yr ORDER BY yr
"""


def pct(a, b) -> float | None:
    if not b:
        return None if not a else 999.0
    return round(100.0 * (a - b) / b, 1)


def bucket_company(t: dict, g: dict) -> str:
    if t["filings"] == g["filings"] and abs(pct(g["buy_6mo"], t["buy_6mo"]) or 0) <= 2 and abs(pct(g["sell_6mo"], t["sell_6mo"]) or 0) <= 2:
        return "same"
    if g["filings"] > t["filings"] * 1.05:
        return "gold_has_more_filings"
    if t["filings"] > g["filings"] * 1.05:
        return "trades_has_more_filings"
    return "value_differs"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", required=True, help="file, one ticker per line (the SEO sample)")
    ap.add_argument("--slugs", required=True, help="file, one insider slug per line")
    ap.add_argument("--top", type=int, default=150, help="also the N tickers with the most trades rows")
    ap.add_argument("--out", required=True, help="markdown report path; a .csv is written beside it")
    args = ap.parse_args()

    conn = get_connection(readonly=True)
    six = (date.today() - timedelta(days=182)).isoformat()

    sample = [l.strip().upper() for l in Path(args.tickers).read_text().splitlines() if l.strip()]
    top = [r["ticker"] for r in conn.execute(
        "SELECT ticker FROM trades WHERE ticker IS NOT NULL AND ticker <> '' GROUP BY ticker ORDER BY count(*) DESC LIMIT ?",
        (args.top,)).fetchall()]
    tickers = list(dict.fromkeys(sample + top))

    rows = []
    for tk in tickers:
        t = dict(conn.execute(TRADES_COMPANY_SQL, (six, six, tk, *MEANINGFUL)).fetchone())
        g = dict(conn.execute(GOLD_COMPANY_SQL, (six, six, tk, *MEANINGFUL)).fetchone())
        rows.append({
            "ticker": tk, "in_seo_sample": tk in sample, "bucket": bucket_company(t, g),
            "trades_filings": t["filings"], "gold_filings": g["filings"], "filings_pct": pct(g["filings"], t["filings"]),
            "trades_insiders": t["insiders"], "gold_insiders": g["insiders"],
            "trades_buy_6mo": round(t["buy_6mo"] or 0), "gold_buy_6mo": round(g["buy_6mo"] or 0), "buy_pct": pct(g["buy_6mo"], t["buy_6mo"]),
            "trades_sell_6mo": round(t["sell_6mo"] or 0), "gold_sell_6mo": round(g["sell_6mo"] or 0), "sell_pct": pct(g["sell_6mo"], t["sell_6mo"]),
            "trades_last": t["last_trade"], "gold_last": g["last_trade"],
        })

    slugs = [l.strip() for l in Path(args.slugs).read_text().splitlines() if l.strip()]
    ins = conn.execute("SELECT slug, insider_id, COALESCE(display_name, name) AS name FROM insiders WHERE slug = ANY(?)", (slugs,)).fetchall()
    irows = []
    for r in ins:
        t = dict(conn.execute(TRADES_INSIDER_SQL, (r["insider_id"], *MEANINGFUL)).fetchone())
        g = dict(conn.execute(GOLD_INSIDER_SQL, (r["insider_id"], *MEANINGFUL)).fetchone())
        irows.append({"insider": r["name"], "slug": r["slug"], "insider_id": r["insider_id"],
                      "trades_buys": t["buys"], "gold_buys": g["buys"], "trades_sells": t["sells"], "gold_sells": g["sells"],
                      "trades_last": t["last_filing"], "gold_last": g["last_filing"],
                      "same": t["buys"] == g["buys"] and t["sells"] == g["sells"]})

    yearly = [dict(r) for r in conn.execute(YEARLY_SQL).fetchall()]
    gaps = dict(conn.execute("""
        SELECT COUNT(*) AS lines, COUNT(insider_id) AS with_insider,
               COUNT(*) FILTER (WHERE superseded_by IS NOT NULL) AS superseded,
               COUNT(*) FILTER (WHERE is_joint_copy) AS joint_copies
          FROM gold.form4_line""").fetchone())

    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    csv_path = out.with_suffix(".csv")
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

    from collections import Counter
    buckets = Counter(r["bucket"] for r in rows)
    seo = [r for r in rows if r["in_seo_sample"]]
    seo_buckets = Counter(r["bucket"] for r in seo)

    def table(hdr, data, keys, limit=None):
        lines = ["| " + " | ".join(hdr) + " |", "|" + "---|" * len(hdr)]
        for r in (data if limit is None else data[:limit]):
            lines.append("| " + " | ".join(str(r.get(k, "")) for k in keys) + " |")
        return "\n".join(lines)

    md = [f"# Gold parity — {date.today().isoformat()}", "",
          "What the product would show if it read `gold.form4_line` (Silver + identity, joint-copy, amendment and classification) instead of `trades`. "
          "Company numbers are the ones `api/routers/companies.py` puts under the H1; insider numbers are discretionary buy/sell filing counts.", "",
          "## Gold, at a glance", "",
          f"- {gaps['lines']:,} lines; {gaps['with_insider']:,} carry an `insider_id` ({100.0*gaps['with_insider']/gaps['lines']:.1f}%); "
          f"{gaps['joint_copies']:,} are joint-filer copies; {gaps['superseded']:,} lines are superseded by an amendment.",
          "- Known gap: Gold classifies every P/S as discretionary — Silver does not carry the 10b5-1 attribute yet. Where `trades` says `planned_*`, Gold counts it as a decision.", "",
          "## Open-market filings per year (P/S, one per accession)", "",
          table(["year", "trades", "gold", "gold − trades"], [{**r, "d": (r["gold_filings"] or 0) - (r["trades_filings"] or 0)} for r in yearly], ["yr", "trades_filings", "gold_filings", "d"]), "",
          f"## Company pages — {len(rows)} tickers ({len(seo)} from the last 60 days of search landings + top {args.top} by volume)", "",
          "| bucket | all | SEO sample |", "|---|---|---|"]
    for b in ("same", "gold_has_more_filings", "trades_has_more_filings", "value_differs"):
        md.append(f"| {b} | {buckets.get(b, 0)} | {seo_buckets.get(b, 0)} |")
    md += ["", "### The SEO sample, every ticker", "",
           table(["ticker", "bucket", "filings t→g", "Δ%", "insiders t→g", "buy 6mo t→g", "sell 6mo t→g", "last t→g"],
                 [{"ticker": r["ticker"], "bucket": r["bucket"],
                   "f": f"{r['trades_filings']}→{r['gold_filings']}", "fp": r["filings_pct"],
                   "i": f"{r['trades_insiders']}→{r['gold_insiders']}",
                   "b": f"${r['trades_buy_6mo']:,}→${r['gold_buy_6mo']:,}", "s": f"${r['trades_sell_6mo']:,}→${r['gold_sell_6mo']:,}",
                   "l": f"{r['trades_last']}→{r['gold_last']}"} for r in seo],
                 ["ticker", "bucket", "f", "fp", "i", "b", "s", "l"]), "",
           "### Largest filing-count gaps, all tickers", "",
           table(["ticker", "bucket", "filings t→g", "Δ%"],
                 [{"ticker": r["ticker"], "bucket": r["bucket"], "f": f"{r['trades_filings']}→{r['gold_filings']}", "fp": r["filings_pct"]}
                  for r in sorted(rows, key=lambda r: -abs((r["gold_filings"] or 0) - (r["trades_filings"] or 0)))],
                 ["ticker", "bucket", "f", "fp"], limit=25), "",
           f"## Insider pages — {len(irows)} of {len(slugs)} sampled slugs resolve to an insider", "",
           f"- identical buy and sell counts: {sum(1 for r in irows if r['same'])} of {len(irows)}", "",
           table(["insider", "buys t→g", "sells t→g", "last filing t→g"],
                 [{"n": r["insider"], "b": f"{r['trades_buys']}→{r['gold_buys']}", "s": f"{r['trades_sells']}→{r['gold_sells']}", "l": f"{r['trades_last']}→{r['gold_last']}"}
                  for r in sorted(irows, key=lambda r: r["same"])],
                 ["n", "b", "s", "l"], limit=60), "",
           f"CSV of every ticker: `{csv_path.name}`.", ""]
    out.write_text("\n".join(md))
    print(f"wrote {out} and {csv_path}: {len(rows)} tickers, {len(irows)} insiders; buckets {dict(buckets)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
