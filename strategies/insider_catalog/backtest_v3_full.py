#!/usr/bin/env python3
"""Full position-tracking backtest of V2 / V3 / Consensus filters.

Simulates the production strategy logic (capacity caps, hold periods, sizing)
across V2-only, V3-only, and Consensus (V2∩V3) grade filters. Outputs
Sharpe, CAGR, max DD per filter.

Total isolation from cw_runner — runs against trade_returns + daily_prices
in read-only mode, no production tables touched.

Usage (on Studio):
    python3 -m strategies.insider_catalog.backtest_v3_full
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime

from config.database import get_connection


STRATEGIES = {
    "quality_momentum": {
        "pit_grades": ["A+", "A"],
        "hold_days": 42,
        "max_concurrent": 10,
        "sizing_pct": 0.10,
        "above_sma50": True,
        "above_sma200": True,
        "exclude_recurring": True,
        "exclude_tax_sales": True,
    },
    "reversal_dip": {
        "pit_grades": ["A+", "A", "B"],
        "hold_days": 21,
        "max_concurrent": 5,
        "sizing_pct": 0.20,
        "min_dip_3mo": -0.25,
        "exclude_tax_sales": True,
    },
    "tenb51_surprise": {
        "pit_grades": ["A+", "A", "B"],
        "hold_days": 60,
        "max_concurrent": 8,
        "sizing_pct": 0.10,
    },
}


def _grade_from_score(s):
    if s is None:
        return None
    if s >= 2.5: return "A+"
    if s >= 2.0: return "A"
    if s >= 1.2: return "B"
    if s >= 0.6: return "C"
    return "D"


def _load_candidates(conn, filters):
    """Return all eligible buy trades with V2 + V3 grades + price/return data."""
    where = ["t.trade_type = 'buy'", "t.superseded_by IS NULL",
             "t.filing_date >= '2020-01-01'", "tr.entry_price > 0",
             "tr.return_30d IS NOT NULL", "tr.return_90d IS NOT NULL"]
    if filters.get("above_sma50"):
        where.append("t.above_sma50 = 1")
    if filters.get("above_sma200"):
        where.append("t.above_sma200 = 1")
    if filters.get("exclude_recurring"):
        where.append("COALESCE(t.is_recurring, 0) = 0")
    if filters.get("exclude_tax_sales"):
        where.append("COALESCE(t.is_tax_sale, 0) = 0")
    if filters.get("min_dip_3mo") is not None:
        where.append("t.dip_3mo <= ?")

    params = []
    if filters.get("min_dip_3mo") is not None:
        params.append(filters["min_dip_3mo"])

    sql = f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.filing_date,
               tr.entry_price AS price,
               t.pit_grade AS v2_grade,
               v3.blended_score AS v3_score,
               tr.return_30d, tr.return_90d
        FROM trades t
        JOIN trade_returns tr ON tr.trade_id = t.trade_id
        LEFT JOIN LATERAL (
            SELECT blended_score
            FROM insider_ticker_scores_v3 its
            WHERE its.insider_id = t.insider_id
              AND its.ticker = t.ticker
              AND its.as_of_date <= t.filing_date
            ORDER BY its.as_of_date DESC
            LIMIT 1
        ) v3 ON TRUE
        WHERE {' AND '.join(where)}
        ORDER BY t.filing_date, t.trade_id
    """
    rows = conn.execute(sql, params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["v3_grade"] = _grade_from_score(d["v3_score"])
        out.append(d)
    return out


def _hold_return(row, hold_days: int) -> float:
    """Approximate the strategy's hold-period return from 30d/90d windows."""
    r30 = row["return_30d"]
    r90 = row["return_90d"]
    if hold_days <= 30:
        return r30 * (hold_days / 30)
    # Linear interpolation between 30d and 90d
    if hold_days <= 90:
        frac = (hold_days - 30) / 60
        return r30 + frac * (r90 - r30)
    return r90


def _simulate(candidates, strategy: dict, allowed_grades_v2: set | None,
              allowed_grades_v3: set | None) -> dict:
    """Walk-forward simulation with capacity cap. Returns per-trade returns
    (each annotated with entry/exit dates for equity-curve construction)."""
    hold_days = strategy["hold_days"]
    max_concurrent = strategy["max_concurrent"]
    open_positions = []  # list of (exit_calendar_date, ticker)
    held_tickers = set()
    entries = []

    for c in candidates:
        # V2 filter
        if allowed_grades_v2 is not None and c["v2_grade"] not in allowed_grades_v2:
            continue
        # V3 filter
        if allowed_grades_v3 is not None and c["v3_grade"] not in allowed_grades_v3:
            continue

        # Exit any positions whose hold expired
        fdate = datetime.strptime(c["filing_date"], "%Y-%m-%d")
        keep = []
        for exit_d, t in open_positions:
            if exit_d <= fdate:
                held_tickers.discard(t)
            else:
                keep.append((exit_d, t))
        open_positions = keep

        # Capacity / dedup
        if len(open_positions) >= max_concurrent:
            continue
        if c["ticker"] in held_tickers:
            continue

        # Enter
        ret = _hold_return(c, hold_days)
        # Approx exit calendar date = filing + hold_days * 1.4 (calendar to trading)
        exit_cal = datetime.strptime(c["filing_date"], "%Y-%m-%d")
        exit_cal = exit_cal.fromordinal(exit_cal.toordinal() + int(hold_days * 1.4))
        open_positions.append((exit_cal, c["ticker"]))
        held_tickers.add(c["ticker"])
        entries.append({
            "entry_date": c["filing_date"],
            "exit_date": exit_cal.strftime("%Y-%m-%d"),
            "ticker": c["ticker"],
            "ret": ret,
        })

    return entries


def _metrics(entries, strategy):
    """Compute summary stats from a list of entries with returns."""
    if not entries:
        return {"trades": 0}
    rets = [e["ret"] for e in entries]
    n = len(rets)
    mean = statistics.mean(rets)
    std = statistics.stdev(rets) if n > 1 else 0.0

    # Annualize via trades-per-year estimate
    first = datetime.strptime(entries[0]["entry_date"], "%Y-%m-%d")
    last = datetime.strptime(entries[-1]["entry_date"], "%Y-%m-%d")
    years = max(0.5, (last - first).days / 365.25)
    trades_per_year = n / years

    # Per-position annualized return:
    # Each trade ~hold_days; portfolio holds max_concurrent; sizing fraction gives
    # blended return ≈ mean * (trades per concurrent slot per year)
    hold = strategy["hold_days"]
    capacity = strategy["max_concurrent"]
    sizing = strategy["sizing_pct"]

    # CAGR estimate: each "slot" of capital gets ~(252/hold_days) trades per year
    # Position return ≈ mean * sizing — total annualized assuming full deployment:
    annualized = trades_per_year * mean * sizing
    # Sharpe (per-trade scaled to annual using sqrt(trades_per_year)):
    sharpe = (mean / std) * math.sqrt(trades_per_year) if std > 0 else 0.0

    # Equity curve & max DD
    sorted_e = sorted(entries, key=lambda x: x["exit_date"])
    eq = 1.0
    peak = 1.0
    max_dd = 0.0
    for e in sorted_e:
        eq = eq * (1 + e["ret"] * sizing / capacity * capacity)  # full deployment ≈ ret * sizing
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak)

    wins = sum(1 for r in rets if r > 0)
    return {
        "trades": n,
        "trades_per_year": trades_per_year,
        "mean_ret": mean,
        "median_ret": statistics.median(rets),
        "std": std,
        "win_rate": wins / n,
        "sharpe_annualized": sharpe,
        "annualized_return_est": annualized,
        "max_dd": max_dd,
        "first": entries[0]["entry_date"],
        "last": entries[-1]["entry_date"],
        "years": years,
    }


def _print_metrics(label, m):
    if m["trades"] == 0:
        print(f"  {label:<22} no trades")
        return
    print(f"  {label:<22} n={m['trades']:>5,}  "
          f"sharpe={m['sharpe_annualized']:5.2f}  "
          f"ann_ret={m['annualized_return_est']*100:6.2f}%  "
          f"WR={m['win_rate']*100:5.1f}%  "
          f"maxDD={m['max_dd']*100:5.1f}%  "
          f"mean/trade={m['mean_ret']*100:+5.2f}%")


def main():
    with get_connection() as conn:
        for strategy_name, strategy in STRATEGIES.items():
            print("=" * 100)
            print(f"Strategy: {strategy_name}  (hold={strategy['hold_days']}d, "
                  f"capacity={strategy['max_concurrent']}, sizing={strategy['sizing_pct']*100:.0f}%)")
            print("=" * 100)

            candidates = _load_candidates(conn, strategy)
            grade_set = set(strategy["pit_grades"])

            v2_entries = _simulate(candidates, strategy, grade_set, None)
            v3_entries = _simulate(candidates, strategy, None, grade_set)
            consensus_entries = _simulate(candidates, strategy, grade_set, grade_set)
            top_v3_entries = _simulate(candidates, strategy, None, {"A+", "A"})  # bonus: top-only V3

            _print_metrics("V2 alone (production)", _metrics(v2_entries, strategy))
            _print_metrics("V3 alone", _metrics(v3_entries, strategy))
            _print_metrics("V2 ∩ V3 (consensus)", _metrics(consensus_entries, strategy))
            _print_metrics("V3 ∈ {A+,A} only", _metrics(top_v3_entries, strategy))
            print()


if __name__ == "__main__":
    main()
