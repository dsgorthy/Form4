#!/usr/bin/env python3
"""V2-vs-V3 strategy backtest harness — total isolation from cw_runner.

For each productized strategy (quality_momentum, reversal_dip, tenb51_surprise),
runs the entry-filter logic with V2 pit_grade vs V3 pit_grade and reports:
  - Trade-set overlap and divergence
  - Avg abnormal_30d return per set
  - Win rate per set
  - Trade count per set
  - Top entries unique to each version

This is a "filter-impact" analysis — it does NOT simulate position sizing,
capacity caps, or time exits. Use it to gauge directional impact before
committing to a full position-tracking backtest.

Usage (on Studio, after V3 backfill has run mode=all):
    python3 -m strategies.insider_catalog.backtest_v3 --strategy quality_momentum
    python3 -m strategies.insider_catalog.backtest_v3 --strategy all
"""
from __future__ import annotations

import argparse
import statistics
from collections import Counter

from config.database import get_connection


# Strategy filter definitions — pulled from the production yaml configs
STRATEGY_FILTERS = {
    "quality_momentum": {
        "pit_grades": ["A+", "A"],
        "above_sma50": 1,
        "above_sma200": 1,
        "exclude_recurring": True,
        "exclude_tax_sales": True,
        "trade_type": "buy",
    },
    "reversal_dip": {
        # "Persistent sellers reversing into depressed stocks." Approximation —
        # actual cw_runner uses additional logic. Captures the core grade
        # dependency for V3-vs-V2 comparison.
        "pit_grades": ["A+", "A", "B"],
        "min_dip_3mo": -0.25,
        "exclude_tax_sales": True,
        "trade_type": "buy",
    },
    "tenb51_surprise": {
        # "10b5-1 schedulers breaking pattern to buy" — grade-gated.
        "pit_grades": ["A+", "A", "B"],
        "trade_type": "buy",
    },
}


def _build_query(filters: dict, grade_source: str) -> tuple[str, list]:
    """Returns SQL + params for a strategy + grade source combo."""
    grade_expr = (
        "t.pit_grade" if grade_source == "v2" else """
        CASE
            WHEN v3.blended_score IS NULL THEN NULL
            WHEN v3.blended_score >= 2.5 THEN 'A+'
            WHEN v3.blended_score >= 2.0 THEN 'A'
            WHEN v3.blended_score >= 1.2 THEN 'B'
            WHEN v3.blended_score >= 0.6 THEN 'C'
            ELSE 'D'
        END
    """)

    joins = ""
    if grade_source == "v3":
        joins = """
        LEFT JOIN LATERAL (
            SELECT blended_score
            FROM insider_ticker_scores_v3 its
            WHERE its.insider_id = t.insider_id
              AND its.ticker = t.ticker
              AND its.as_of_date <= t.filing_date
            ORDER BY its.as_of_date DESC
            LIMIT 1
        ) v3 ON TRUE
        """

    where = [
        "t.trade_type = ?",
        "t.superseded_by IS NULL",
        "t.filing_date >= '2020-01-01'",
        "tr.abnormal_30d IS NOT NULL",
    ]
    params = [filters["trade_type"]]

    placeholders = ",".join("?" for _ in filters["pit_grades"])
    where.append(f"({grade_expr}) IN ({placeholders})")
    params.extend(filters["pit_grades"])

    if filters.get("above_sma50") == 1:
        where.append("t.above_sma50 = 1")
    if filters.get("above_sma200") == 1:
        where.append("t.above_sma200 = 1")
    if filters.get("exclude_recurring"):
        where.append("COALESCE(t.is_recurring, 0) = 0")
    if filters.get("exclude_tax_sales"):
        where.append("COALESCE(t.is_tax_sale, 0) = 0")
    if filters.get("min_dip_3mo") is not None:
        where.append("t.dip_3mo <= ?")
        params.append(filters["min_dip_3mo"])

    sql = f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.filing_date, t.value,
               tr.abnormal_30d, tr.abnormal_90d, tr.return_30d
        FROM trades t
        JOIN trade_returns tr ON tr.trade_id = t.trade_id
        {joins}
        WHERE {' AND '.join(where)}
    """
    return sql, params


def _run_filter(conn, filters: dict, grade_source: str) -> list[dict]:
    sql, params = _build_query(filters, grade_source)
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _metrics(trades: list[dict]) -> dict:
    if not trades:
        return {"n": 0, "avg_alpha_30d": None, "win_rate_30d": None,
                "median_alpha_30d": None}
    alphas = [t["abnormal_30d"] for t in trades if t["abnormal_30d"] is not None]
    if not alphas:
        return {"n": len(trades), "avg_alpha_30d": None, "win_rate_30d": None,
                "median_alpha_30d": None}
    return {
        "n": len(trades),
        "avg_alpha_30d": statistics.mean(alphas),
        "median_alpha_30d": statistics.median(alphas),
        "win_rate_30d": sum(1 for a in alphas if a > 0) / len(alphas),
    }


def _by_year(trades: list[dict]) -> dict[str, dict]:
    buckets = Counter()
    alpha_buckets: dict[str, list[float]] = {}
    for t in trades:
        year = t["filing_date"][:4]
        buckets[year] += 1
        if t["abnormal_30d"] is not None:
            alpha_buckets.setdefault(year, []).append(t["abnormal_30d"])
    return {
        year: {
            "n": buckets[year],
            "avg_alpha_30d": statistics.mean(alpha_buckets.get(year, [])) if alpha_buckets.get(year) else None,
            "win_rate_30d": (sum(1 for a in alpha_buckets.get(year, []) if a > 0) / len(alpha_buckets[year])) if alpha_buckets.get(year) else None,
        }
        for year in sorted(buckets)
    }


def run_strategy(name: str, filters: dict):
    print("=" * 78)
    print(f"Strategy: {name}")
    print("=" * 78)

    with get_connection() as conn:
        v2 = _run_filter(conn, filters, "v2")
        v3 = _run_filter(conn, filters, "v3")

    v2_ids = {t["trade_id"] for t in v2}
    v3_ids = {t["trade_id"] for t in v3}
    overlap = v2_ids & v3_ids
    only_v2 = v2_ids - v3_ids
    only_v3 = v3_ids - v2_ids

    v2_only_trades = [t for t in v2 if t["trade_id"] in only_v2]
    v3_only_trades = [t for t in v3 if t["trade_id"] in only_v3]
    overlap_trades = [t for t in v2 if t["trade_id"] in overlap]

    m_v2 = _metrics(v2)
    m_v3 = _metrics(v3)
    m_v2_only = _metrics(v2_only_trades)
    m_v3_only = _metrics(v3_only_trades)
    m_overlap = _metrics(overlap_trades)

    def fmt(m, key):
        v = m[key]
        if v is None:
            return "—"
        if "rate" in key:
            return f"{v*100:.1f}%"
        if "alpha" in key:
            return f"{v*100:+.2f}%"
        return f"{v:,}"

    rows = [
        ("ALL V2",   m_v2),
        ("ALL V3",   m_v3),
        ("Overlap (both)", m_overlap),
        ("V2-only", m_v2_only),
        ("V3-only", m_v3_only),
    ]
    print(f"{'set':<18} {'n':>8} {'avg_alpha_30d':>14} {'median_alpha':>14} {'win_rate_30d':>14}")
    print("-" * 78)
    for label, m in rows:
        print(f"{label:<18} {fmt(m,'n'):>8} {fmt(m,'avg_alpha_30d'):>14} "
              f"{fmt(m,'median_alpha_30d'):>14} {fmt(m,'win_rate_30d'):>14}")

    print()
    print(f"Trade-set overlap: {len(overlap):,} / V2={len(v2_ids):,} V3={len(v3_ids):,}")
    print(f"Diff: V3 adds {len(only_v3):,}, V3 drops {len(only_v2):,}")

    print()
    print("By year (V2 vs V3):")
    by_v2 = _by_year(v2)
    by_v3 = _by_year(v3)
    years = sorted(set(by_v2) | set(by_v3))
    print(f"{'year':<6} {'V2 n':>8} {'V2 alpha':>10} {'V2 WR':>8}   {'V3 n':>8} {'V3 alpha':>10} {'V3 WR':>8}")
    for y in years:
        v2y = by_v2.get(y, {"n": 0, "avg_alpha_30d": None, "win_rate_30d": None})
        v3y = by_v3.get(y, {"n": 0, "avg_alpha_30d": None, "win_rate_30d": None})
        v2_alpha = f"{v2y['avg_alpha_30d']*100:+.2f}%" if v2y["avg_alpha_30d"] is not None else "—"
        v3_alpha = f"{v3y['avg_alpha_30d']*100:+.2f}%" if v3y["avg_alpha_30d"] is not None else "—"
        v2_wr = f"{v2y['win_rate_30d']*100:.1f}%" if v2y["win_rate_30d"] is not None else "—"
        v3_wr = f"{v3y['win_rate_30d']*100:.1f}%" if v3y["win_rate_30d"] is not None else "—"
        print(f"{y:<6} {v2y['n']:>8,} {v2_alpha:>10} {v2_wr:>8}   {v3y['n']:>8,} {v3_alpha:>10} {v3_wr:>8}")

    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default="all",
                        choices=["all"] + list(STRATEGY_FILTERS.keys()))
    args = parser.parse_args()

    if args.strategy == "all":
        for name, filters in STRATEGY_FILTERS.items():
            run_strategy(name, filters)
    else:
        run_strategy(args.strategy, STRATEGY_FILTERS[args.strategy])


if __name__ == "__main__":
    main()
