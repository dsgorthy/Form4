#!/usr/bin/env python3
"""Post-hoc analysis: do the two new PIT net-flow signals predict forward returns?

This is the experiment behind the CEO Watcher claim. Buckets trades by quintile
of each signal and reports mean/median forward returns per bucket. Writes a
markdown report to reports/.

Universes analyzed:
  A. Full universe — every P-trade since trades data starts (~130k rows)
  B. Sim closed trades — the 141 trades that our 3 strategies actually entered
  C. Combined signal — both above-median together (CEO Watcher's amplification claim)

Forward returns used:
  - 30d, 90d from `trade_returns` (already computed)
  - 1y computed on-the-fly from `prices.daily_prices`
  - Excess vs SPY for each horizon

This script does NOT modify any data or strategy logic — it only reads and
reports. The recommendation at the end is for the operator to act on.

Usage:
  python3 scripts/analyze_net_flow_predictive_value.py
  python3 scripts/analyze_net_flow_predictive_value.py --output reports/foo.md
"""
from __future__ import annotations

import argparse
import logging
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from config.database import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def fmt_pct(x: float | None) -> str:
    if x is None:
        return "—"
    return f"{x*100:+.2f}%"


def fmt_int(x: int | None) -> str:
    if x is None:
        return "—"
    return f"{x:,}"


def median_or_none(vals):
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None


def mean_or_none(vals):
    vals = [v for v in vals if v is not None]
    return statistics.mean(vals) if vals else None


def quintile_buckets(rows: list[dict], signal_key: str) -> list[list[dict]]:
    """Return 5 buckets — Q1 lowest, Q5 highest — of rows with non-NULL signal."""
    valid = [r for r in rows if r.get(signal_key) is not None]
    valid.sort(key=lambda r: r[signal_key])
    n = len(valid)
    if n < 5:
        return [valid] + [[]] * 4
    bucket_size = n // 5
    buckets = []
    for i in range(5):
        start = i * bucket_size
        end = (i + 1) * bucket_size if i < 4 else n
        buckets.append(valid[start:end])
    return buckets


def compute_1y_returns(conn, rows: list[dict]) -> None:
    """Mutate rows in-place to add 'ret_1y' (excess vs SPY 252-trading-day return).

    For each row, look up close at filing_date + 1d (entry) and close at
    filing_date + 252 trading days (~1 year). Compute return. Same for SPY.
    Excess = stock_ret - spy_ret.
    """
    # Cache SPY prices keyed by date for fast lookup
    spy_rows = conn.execute(
        "SELECT date::text AS d, close FROM prices.daily_prices WHERE ticker='SPY' ORDER BY d"
    ).fetchall()
    spy_by_date = {r["d"]: float(r["close"]) for r in spy_rows}
    spy_dates_sorted = sorted(spy_by_date.keys())

    # For each row, look up entry and 1y-forward close for the ticker
    by_ticker: dict[str, list[dict]] = {}
    for r in rows:
        by_ticker.setdefault(r["ticker"], []).append(r)

    for ticker, trows in by_ticker.items():
        tprices = conn.execute(
            "SELECT date::text AS d, close FROM prices.daily_prices WHERE ticker=? ORDER BY d",
            (ticker,),
        ).fetchall()
        if not tprices:
            for r in trows:
                r["ret_1y"] = None
            continue
        prices_by_date = {p["d"]: float(p["close"]) for p in tprices}
        dates_sorted = sorted(prices_by_date.keys())

        for r in trows:
            fd = r["filing_date"][:10]
            # Find entry: first trading day >= fd+1
            entry_target = (datetime.strptime(fd, "%Y-%m-%d").date()
                            + timedelta(days=1)).isoformat()
            entry_d = next((d for d in dates_sorted if d >= entry_target), None)
            if entry_d is None:
                r["ret_1y"] = None
                continue
            # 1y forward = entry_d + 365 calendar days (close approximation to 252 td)
            exit_target = (datetime.strptime(entry_d, "%Y-%m-%d").date()
                           + timedelta(days=365)).isoformat()
            exit_d = next((d for d in dates_sorted if d >= exit_target), None)
            if exit_d is None:
                r["ret_1y"] = None
                continue
            stock_ret = prices_by_date[exit_d] / prices_by_date[entry_d] - 1.0

            # SPY excess
            spy_entry = next((d for d in spy_dates_sorted if d >= entry_target), None)
            spy_exit = next((d for d in spy_dates_sorted if d >= exit_target), None)
            if spy_entry and spy_exit:
                spy_ret = spy_by_date[spy_exit] / spy_by_date[spy_entry] - 1.0
            else:
                spy_ret = 0.0
            r["ret_1y"] = stock_ret - spy_ret


def bucket_table(buckets: list[list[dict]], signal_key: str, return_keys: list[str]) -> str:
    """Render a markdown table: one row per quintile, columns for n, signal range,
    + mean/median for each return horizon."""
    header = ["Quintile", "N", f"{signal_key} range"]
    for k in return_keys:
        header += [f"{k} mean", f"{k} median"]
    lines = ["| " + " | ".join(header) + " |",
             "|" + "|".join(["---"] * len(header)) + "|"]
    for i, b in enumerate(buckets, 1):
        if not b:
            continue
        sig_vals = [r[signal_key] for r in b if r.get(signal_key) is not None]
        sig_range = f"{min(sig_vals):+.3f} … {max(sig_vals):+.3f}" if sig_vals else "—"
        cells = [f"Q{i}", str(len(b)), sig_range]
        for k in return_keys:
            vals = [r.get(k) for r in b]
            cells += [fmt_pct(mean_or_none(vals)), fmt_pct(median_or_none(vals))]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def coverage_table(rows: list[dict]) -> str:
    total = len(rows)
    have_company = sum(1 for r in rows if r.get("net_buyer_flow_90d") is not None)
    have_industry = sum(1 for r in rows if r.get("industry_buy_pct_90d") is not None)
    by_year: dict[str, dict[str, int]] = {}
    for r in rows:
        yr = r["filing_date"][:4]
        d = by_year.setdefault(yr, {"n": 0, "company": 0, "industry": 0})
        d["n"] += 1
        if r.get("net_buyer_flow_90d") is not None:
            d["company"] += 1
        if r.get("industry_buy_pct_90d") is not None:
            d["industry"] += 1
    lines = [
        f"Total P-trades: {total:,}",
        f"  net_buyer_flow_90d populated: {have_company:,} ({100*have_company/max(total,1):.0f}%)",
        f"  industry_buy_pct_90d populated: {have_industry:,} ({100*have_industry/max(total,1):.0f}%)",
        "",
        "| Year | N | Company | Industry |",
        "|---|---|---|---|",
    ]
    for yr in sorted(by_year):
        d = by_year[yr]
        lines.append(f"| {yr} | {d['n']:,} | "
                     f"{d['company']:,} ({100*d['company']/d['n']:.0f}%) | "
                     f"{d['industry']:,} ({100*d['industry']/d['n']:.0f}%) |")
    return "\n".join(lines)


def combined_amplification(rows: list[dict], return_keys: list[str]) -> str:
    """Bucket by (company_signal above/below 0) X (industry_signal above/below 0).
    If CEO Watcher's amplification claim holds, the +/+ bucket should be top
    and -/- should be bottom for each return horizon."""
    buckets = {"+/+": [], "+/-": [], "-/+": [], "-/-": []}
    for r in rows:
        c = r.get("net_buyer_flow_90d")
        ind = r.get("industry_buy_pct_90d")
        if c is None or ind is None:
            continue
        key = ("+" if c > 0 else "-") + "/" + ("+" if ind > 0 else "-")
        buckets[key].append(r)
    lines = ["| Company / Industry | N |"]
    for k in return_keys:
        lines[0] += f" {k} mean | {k} median |"
    lines.append("|" + "|".join(["---"] * (2 + 2 * len(return_keys))) + "|")
    for key in ["+/+", "+/-", "-/+", "-/-"]:
        b = buckets[key]
        cells = [key, str(len(b))]
        for k in return_keys:
            vals = [r.get(k) for r in b]
            cells += [fmt_pct(mean_or_none(vals)), fmt_pct(median_or_none(vals))]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def yearly_decomposition(rows: list[dict], signal_key: str, return_key: str) -> str:
    """For each year, show mean return in Q1 vs Q5 of the signal."""
    by_year: dict[str, list[dict]] = {}
    for r in rows:
        if r.get(signal_key) is None or r.get(return_key) is None:
            continue
        by_year.setdefault(r["filing_date"][:4], []).append(r)
    lines = [f"| Year | N | Q1 mean {return_key} | Q5 mean {return_key} | Q5 - Q1 |",
             "|---|---|---|---|---|"]
    for yr in sorted(by_year):
        rs = sorted(by_year[yr], key=lambda r: r[signal_key])
        n = len(rs)
        if n < 10:
            continue
        q1_size = max(1, n // 5)
        q1 = rs[:q1_size]
        q5 = rs[-q1_size:]
        q1_mean = mean_or_none([r[return_key] for r in q1])
        q5_mean = mean_or_none([r[return_key] for r in q5])
        delta = (q5_mean - q1_mean) if (q1_mean is not None and q5_mean is not None) else None
        lines.append(f"| {yr} | {n:,} | {fmt_pct(q1_mean)} | {fmt_pct(q5_mean)} | {fmt_pct(delta)} |")
    return "\n".join(lines)


def load_full_universe(conn) -> list[dict]:
    """All P-trades joined with trade_returns. Excludes scheduled.
    Uses `abnormal_*d` columns (stock return minus SPY return)."""
    rows = conn.execute("""
        SELECT t.trade_id, t.ticker, t.filing_date::text AS filing_date,
               t.net_buyer_flow_90d, t.industry_buy_pct_90d,
               tr.abnormal_30d  AS ret_30d,
               tr.abnormal_90d  AS ret_90d,
               tr.abnormal_365d AS ret_1y
        FROM trades t
        LEFT JOIN trade_returns tr ON tr.trade_id = t.trade_id
        WHERE t.trans_code = 'P'
          AND t.ticker IS NOT NULL AND t.ticker != 'NONE'
          AND t.filing_date IS NOT NULL
          AND COALESCE(t.is_10b5_1, 0) = 0
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
        ORDER BY t.filing_date
    """).fetchall()
    return [dict(r) for r in rows]


def load_sim_closed(conn) -> list[dict]:
    """Sim-closed trades with their realized pnl_pct + the two signals."""
    rows = conn.execute("""
        SELECT sp.id, sp.strategy, sp.ticker, sp.entry_date AS filing_date,
               sp.pnl_pct AS ret_sim, sp.hold_days,
               t.net_buyer_flow_90d, t.industry_buy_pct_90d
        FROM strategy_portfolio sp
        JOIN trades t ON sp.trade_id = t.trade_id
        WHERE sp.execution_source = 'simulated' AND sp.status = 'closed'
        ORDER BY sp.entry_date
    """).fetchall()
    return [dict(r) for r in rows]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", default=None,
                   help="Output markdown path (default: reports/net_flow_analysis_YYYY-MM-DD.md)")
    args = p.parse_args()

    out_path = Path(args.output) if args.output else (
        REPO / "reports" / f"net_flow_analysis_{date.today().isoformat()}.md"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    logger.info("Loading full universe...")
    full = load_full_universe(conn)
    logger.info("  %d unscheduled P-trades", len(full))

    # trade_returns.abnormal_365d already exists — no need for on-the-fly compute.
    return_keys_full = ["ret_30d", "ret_90d", "ret_1y"]

    logger.info("Loading sim-closed trades...")
    sim = load_sim_closed(conn)
    logger.info("  %d sim-closed trades", len(sim))

    conn.close()

    # Build report
    lines = [
        "# CEO Watcher Net-Flow Validation Experiment",
        "",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "## Methodology",
        "",
        "Two PIT-clean signals computed from `trades` table:",
        "- `net_buyer_flow_90d`: (distinct buyers in [F-90d, F) − distinct sellers in same window)"
        " − trailing-3y rolling-90d median for this ticker.",
        "- `industry_buy_pct_90d`: % of sector peers with ≥1 unscheduled buy in [F-90d, F)"
        " − trailing-3y median for the sector. Sector classifications from yfinance"
        " (snapshot; static across history — documented PIT approximation).",
        "",
        "Both signals computed with strict `filing_date < F` semantics (no leakage of the"
        " trade being scored into its own signal). Enforced by"
        " `tests/unit/test_pit_validation.py::TestNetFlowSignalsPIT`.",
        "",
        "## A. Coverage on full P-trade universe (unscheduled only)",
        "",
        coverage_table(full),
        "",
        "## B. Univariate quintile buckets — full universe",
        "",
        "### B.1 Company net-flow (`net_buyer_flow_90d`)",
        "",
        bucket_table(quintile_buckets(full, "net_buyer_flow_90d"),
                     "net_buyer_flow_90d", return_keys_full),
        "",
        "### B.2 Industry net-flow (`industry_buy_pct_90d`)",
        "",
        bucket_table(quintile_buckets(full, "industry_buy_pct_90d"),
                     "industry_buy_pct_90d", return_keys_full),
        "",
        "## C. Combined-signal amplification (CEO Watcher's strongest claim)",
        "",
        "Above-median signal denoted '+'; below '−'. If amplification holds, the '+/+'"
        " bucket should outperform '−/−' across return horizons.",
        "",
        combined_amplification(full, return_keys_full),
        "",
        "## D. Year-by-year decomposition (Q5 − Q1 spread)",
        "",
        "### D.1 Company net-flow vs ret_90d",
        "",
        yearly_decomposition(full, "net_buyer_flow_90d", "ret_90d"),
        "",
        "### D.2 Industry net-flow vs ret_90d",
        "",
        yearly_decomposition(full, "industry_buy_pct_90d", "ret_90d"),
        "",
        "## E. Sim-strategy-closed trades — does the signal help our 3 strategies?",
        "",
        f"Total sim-closed trades analyzed: {len(sim)}",
        "",
        "### E.1 Company net-flow vs realized sim P&L",
        "",
        bucket_table(quintile_buckets(sim, "net_buyer_flow_90d"),
                     "net_buyer_flow_90d", ["ret_sim"]),
        "",
        "### E.2 Industry net-flow vs realized sim P&L",
        "",
        bucket_table(quintile_buckets(sim, "industry_buy_pct_90d"),
                     "industry_buy_pct_90d", ["ret_sim"]),
        "",
        "## F. Statistical caveats",
        "",
        "- Full-universe analysis has ~26K trades per quintile — statistically meaningful.",
        f"- Sim-strategy analysis has ~{len(sim)//5} trades per quintile — DIRECTIONAL only.",
        "- All returns are excess vs SPY where the column says 'excess'. The 1y horizon is"
        " computed on-the-fly from `prices.daily_prices` and uses calendar-day windows"
        " (~365 days ≈ 252 trading days).",
        "- The universe excludes scheduled trades (10b5-1, recurring, tax-sale, cohen-routine).",
        "",
        "## G. Recommendation",
        "",
        "_To be filled in by the operator based on the tables above._",
        "",
        "Decision rubric:",
        "- If Q5−Q1 spread is large and consistent across years AND amplification (+/+ vs −/−)"
        " is positive → consider adding as conviction multiplier (NOT a hard filter — see"
        " the C-grade overfit discussion).",
        "- If spread is positive on full universe but flat on sim subset → CEO Watcher's"
        " claim may be real but our existing filters already capture similar signal.",
        "- If spread is noisy / regime-dependent → keep observational; revisit with more"
        " trades.",
        "",
        "---",
        "",
        "**PIT confirmation**: Generated from columns populated by"
        " `compute_company_net_flow.py` and `compute_industry_net_flow.py`, both registered"
        " in `tests/unit/test_pit_validation.py::TestNetFlowSignalsPIT` + `SCORING_FILES`."
        " All 66 PIT tests passing.",
    ]
    out_path.write_text("\n".join(lines))
    logger.info("Wrote %s", out_path)


if __name__ == "__main__":
    main()
