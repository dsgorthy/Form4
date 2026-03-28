#!/usr/bin/env python3
"""
Convergence Signal Analysis
============================
Measures whether stocks perform better when BOTH insiders and politicians
are buying within a 30-day window.

Groups:
  - convergence:    insider buy with a politician buy on same ticker within ±30 days
  - insider_only:   insider buy with NO politician buy within ±30 days
  - politician_only: politician buy with NO insider buy within ±30 days
  - baseline:       all insider buys (convergence + insider_only)

Output: formatted table to stdout + JSON to reports/convergence_analysis.json
"""

import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median, stdev

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FRAMEWORK_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = FRAMEWORK_ROOT / "strategies" / "insider_catalog" / "insiders.db"
REPORT_PATH = FRAMEWORK_ROOT / "reports" / "convergence_analysis.json"


def parse_date(s):
    """Parse YYYY-MM-DD string to date object."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def days_between(d1, d2):
    """Signed days between two date strings."""
    return (parse_date(d2) - parse_date(d1)).days


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_insider_buys(conn):
    """Load all insider buys that have forward returns computed."""
    sql = """
        SELECT
            t.trade_id,
            t.ticker,
            t.trade_date,
            t.value,
            t.is_csuite,
            tr.return_7d,
            tr.return_30d,
            tr.return_90d,
            tr.spy_return_7d,
            tr.spy_return_30d,
            tr.spy_return_90d,
            tr.abnormal_7d,
            tr.abnormal_30d,
            tr.abnormal_90d
        FROM trades t
        JOIN trade_returns tr ON tr.trade_id = t.trade_id
        WHERE t.trade_type = 'buy'
          AND tr.return_7d IS NOT NULL
        ORDER BY t.ticker, t.trade_date
    """
    rows = conn.execute(sql).fetchall()
    cols = [
        "trade_id", "ticker", "trade_date", "value", "is_csuite",
        "return_7d", "return_30d", "return_90d",
        "spy_return_7d", "spy_return_30d", "spy_return_90d",
        "abnormal_7d", "abnormal_30d", "abnormal_90d",
    ]
    return [dict(zip(cols, r)) for r in rows]


def load_congress_buys(conn):
    """Load all politician buy trades."""
    sql = """
        SELECT
            ct.congress_trade_id,
            ct.ticker,
            ct.trade_date,
            ct.value_estimate,
            p.name,
            p.chamber,
            p.party
        FROM congress_trades ct
        JOIN politicians p ON p.politician_id = ct.politician_id
        WHERE ct.trade_type = 'buy'
        ORDER BY ct.ticker, ct.trade_date
    """
    rows = conn.execute(sql).fetchall()
    cols = [
        "congress_trade_id", "ticker", "trade_date", "value_estimate",
        "politician_name", "chamber", "party",
    ]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Convergence detection
# ---------------------------------------------------------------------------

def build_congress_index(congress_buys):
    """Build ticker -> sorted list of trade_date strings."""
    idx = defaultdict(list)
    for cb in congress_buys:
        idx[cb["ticker"]].append(cb["trade_date"])
    # sort each list
    for ticker in idx:
        idx[ticker].sort()
    return idx


def has_congress_buy_nearby(ticker, trade_date, congress_idx, window=30):
    """Check if any politician bought this ticker within ±window days."""
    dates = congress_idx.get(ticker)
    if not dates:
        return False
    td = parse_date(trade_date)
    # Binary-style scan — dates are sorted
    for cd_str in dates:
        cd = parse_date(cd_str)
        diff = abs((cd - td).days)
        if diff <= window:
            return True
        # Optimization: if congress date is way past our window, and list is
        # sorted, earlier dates are even further behind — but since both
        # before and after matter, we can't break early trivially.
        # For ~4K congress trades this is fast enough.
    return False


def build_insider_index(insider_buys):
    """Build ticker -> sorted list of trade_date strings for insider buys."""
    idx = defaultdict(list)
    for ib in insider_buys:
        idx[ib["ticker"]].append(ib["trade_date"])
    for ticker in idx:
        idx[ticker].sort()
    return idx


def has_insider_buy_nearby(ticker, trade_date, insider_idx, window=30):
    """Check if any insider bought this ticker within ±window days."""
    dates = insider_idx.get(ticker)
    if not dates:
        return False
    td = parse_date(trade_date)
    for id_str in dates:
        id_ = parse_date(id_str)
        diff = abs((id_ - td).days)
        if diff <= window:
            return True
    return False


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def safe_mean(vals):
    return mean(vals) if vals else None


def safe_median(vals):
    return median(vals) if vals else None


def safe_stdev(vals):
    return stdev(vals) if len(vals) >= 2 else None


def compute_group_stats(trades, label):
    """Compute summary statistics for a group of insider trades."""
    n = len(trades)
    if n == 0:
        return {"group": label, "count": 0}

    def returns(field):
        return [t[field] for t in trades if t[field] is not None]

    def win_rate(field):
        vals = returns(field)
        if not vals:
            return None
        return sum(1 for v in vals if v > 0) / len(vals)

    r7 = returns("return_7d")
    r30 = returns("return_30d")
    r90 = returns("return_90d")
    a7 = returns("abnormal_7d")
    a30 = returns("abnormal_30d")
    a90 = returns("abnormal_90d")

    stats = {
        "group": label,
        "count": n,
        "win_rate_7d": win_rate("return_7d"),
        "win_rate_30d": win_rate("return_30d"),
        "win_rate_90d": win_rate("return_90d"),
        "avg_return_7d": safe_mean(r7),
        "avg_return_30d": safe_mean(r30),
        "avg_return_90d": safe_mean(r90),
        "avg_abnormal_7d": safe_mean(a7),
        "avg_abnormal_30d": safe_mean(a30),
        "avg_abnormal_90d": safe_mean(a90),
        "median_return_7d": safe_median(r7),
        "median_return_30d": safe_median(r30),
        "median_return_90d": safe_median(r90),
        "best_return_7d": max(r7) if r7 else None,
        "best_return_30d": max(r30) if r30 else None,
        "best_return_90d": max(r90) if r90 else None,
        "worst_return_7d": min(r7) if r7 else None,
        "worst_return_30d": min(r30) if r30 else None,
        "worst_return_90d": min(r90) if r90 else None,
        "stdev_return_7d": safe_stdev(r7),
        "stdev_return_30d": safe_stdev(r30),
        "stdev_return_90d": safe_stdev(r90),
    }
    return stats


def t_test(group_a, group_b, field):
    """
    Two-sample Welch's t-test for unequal variances.
    Returns (t_statistic, approximate_p_value, significant_at_05).
    Uses a rough p-value approximation via the normal distribution.
    """
    import math

    vals_a = [t[field] for t in group_a if t[field] is not None]
    vals_b = [t[field] for t in group_b if t[field] is not None]

    n_a, n_b = len(vals_a), len(vals_b)
    if n_a < 2 or n_b < 2:
        return None, None, None

    mean_a, mean_b = mean(vals_a), mean(vals_b)
    var_a = sum((x - mean_a) ** 2 for x in vals_a) / (n_a - 1)
    var_b = sum((x - mean_b) ** 2 for x in vals_b) / (n_b - 1)

    se = math.sqrt(var_a / n_a + var_b / n_b)
    if se == 0:
        return None, None, None

    t_stat = (mean_a - mean_b) / se

    # Approximate p-value using normal CDF (good enough for large n)
    # Two-tailed
    z = abs(t_stat)
    # Abramowitz & Stegun approximation of normal CDF
    p = 0.5 * math.erfc(z / math.sqrt(2))
    p_two_tailed = 2 * p

    return round(t_stat, 4), round(p_two_tailed, 6), p_two_tailed < 0.05


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def fmt_pct(val, width=8):
    if val is None:
        return "N/A".rjust(width)
    return f"{val * 100:>{width}.2f}%"


def fmt_float(val, width=8):
    if val is None:
        return "N/A".rjust(width)
    return f"{val * 100:>{width}.2f}%"


def print_summary(groups, convergence_trades, t_tests, top_tickers, top_events,
                  politician_only_count):
    """Print formatted summary to stdout."""
    print("=" * 80)
    print("CONVERGENCE SIGNAL ANALYSIS")
    print("Insider + Politician Buy Convergence (±30 day window)")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Summary table
    header = (
        f"{'Group':<20} {'Count':>7} "
        f"{'WR 7d':>8} {'WR 30d':>8} {'WR 90d':>8} "
        f"{'Avg 7d':>8} {'Avg 30d':>8} {'Avg 90d':>8} "
        f"{'Abn 30d':>8} {'Abn 90d':>8}"
    )
    print()
    print(header)
    print("-" * len(header))

    for g in groups:
        row = (
            f"{g['group']:<20} {g['count']:>7} "
            f"{fmt_pct(g['win_rate_7d'])} {fmt_pct(g['win_rate_30d'])} {fmt_pct(g['win_rate_90d'])} "
            f"{fmt_float(g['avg_return_7d'])} {fmt_float(g['avg_return_30d'])} {fmt_float(g['avg_return_90d'])} "
            f"{fmt_float(g['avg_abnormal_30d'])} {fmt_float(g['avg_abnormal_90d'])}"
        )
        print(row)

    print()
    print(f"Politician-only buys (no insider activity within ±30d): {politician_only_count}")

    # T-tests
    print()
    print("-" * 60)
    print("STATISTICAL SIGNIFICANCE (convergence vs baseline)")
    print("-" * 60)
    print(f"  {'Metric':<20} {'t-stat':>10} {'p-value':>12} {'Sig?':>6}")
    for label, (t_stat, p_val, sig) in t_tests.items():
        t_str = f"{t_stat:.4f}" if t_stat is not None else "N/A"
        p_str = f"{p_val:.6f}" if p_val is not None else "N/A"
        s_str = "YES" if sig else ("no" if sig is not None else "N/A")
        print(f"  {label:<20} {t_str:>10} {p_str:>12} {s_str:>6}")

    # Top tickers
    print()
    print("-" * 60)
    print("TOP 10 CONVERGENCE TICKERS (by event count)")
    print("-" * 60)
    print(f"  {'Ticker':<8} {'Events':>7} {'Avg 30d':>10} {'Avg 90d':>10}")
    for t in top_tickers[:10]:
        a30 = fmt_float(t["avg_return_30d"], 9)
        a90 = fmt_float(t["avg_return_90d"], 9)
        print(f"  {t['ticker']:<8} {t['count']:>7} {a30} {a90}")

    # Top events
    print()
    print("-" * 60)
    print("TOP 10 CONVERGENCE EVENTS (by 90d return)")
    print("-" * 60)
    print(f"  {'Ticker':<8} {'Date':<12} {'Value':>12} {'Ret 30d':>10} {'Ret 90d':>10}")
    for e in top_events[:10]:
        val = f"${e['value']:>10,.0f}" if e["value"] else "N/A".rjust(11)
        r30 = fmt_float(e.get("return_30d"), 9)
        r90 = fmt_float(e.get("return_90d"), 9)
        print(f"  {e['ticker']:<8} {e['trade_date']:<12} {val} {r30} {r90}")

    print()
    print("=" * 80)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = None

    # Check that congress_trades exists
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "congress_trades" not in tables:
        print("ERROR: congress_trades table not found in database.", file=sys.stderr)
        sys.exit(1)

    print("Loading insider buys with returns...", flush=True)
    insider_buys = load_insider_buys(conn)
    print(f"  {len(insider_buys):,} insider buys with forward returns")

    print("Loading congress buys...", flush=True)
    congress_buys = load_congress_buys(conn)
    print(f"  {len(congress_buys):,} politician buys")

    conn.close()

    # Build indexes
    print("Building convergence index...", flush=True)
    congress_idx = build_congress_index(congress_buys)
    insider_idx = build_insider_index(insider_buys)

    congress_tickers = set(congress_idx.keys())
    insider_tickers = set(insider_idx.keys())
    overlap_tickers = congress_tickers & insider_tickers
    print(f"  Tickers with both insider + politician buys: {len(overlap_tickers)}")

    # Tag each insider buy
    print("Tagging insider buys (convergence vs insider_only)...", flush=True)
    convergence_trades = []
    insider_only_trades = []

    for i, trade in enumerate(insider_buys):
        ticker = trade["ticker"]
        # Quick filter: if ticker has no congress buys at all, it's insider_only
        if ticker not in congress_idx:
            insider_only_trades.append(trade)
            continue
        if has_congress_buy_nearby(ticker, trade["trade_date"], congress_idx, window=30):
            trade["_tag"] = "convergence"
            convergence_trades.append(trade)
        else:
            insider_only_trades.append(trade)

        if (i + 1) % 10000 == 0:
            print(f"    processed {i + 1:,} / {len(insider_buys):,}...", flush=True)

    print(f"  Convergence events: {len(convergence_trades):,}")
    print(f"  Insider-only events: {len(insider_only_trades):,}")

    # Count politician-only buys (congress buys with no insider buy within ±30d)
    print("Counting politician-only events...", flush=True)
    politician_only_count = 0
    for cb in congress_buys:
        ticker = cb["ticker"]
        if ticker not in insider_idx:
            politician_only_count += 1
            continue
        if not has_insider_buy_nearby(ticker, cb["trade_date"], insider_idx, window=30):
            politician_only_count += 1
    print(f"  Politician-only events: {politician_only_count:,}")

    # Compute group statistics
    print("Computing statistics...", flush=True)
    baseline_stats = compute_group_stats(insider_buys, "baseline")
    convergence_stats = compute_group_stats(convergence_trades, "convergence")
    insider_only_stats = compute_group_stats(insider_only_trades, "insider_only")

    groups = [convergence_stats, insider_only_stats, baseline_stats]

    # T-tests: convergence vs baseline
    t_tests = {}
    for field in ["return_7d", "return_30d", "return_90d",
                   "abnormal_7d", "abnormal_30d", "abnormal_90d"]:
        t_stat, p_val, sig = t_test(convergence_trades, insider_buys, field)
        t_tests[field] = (t_stat, p_val, sig)

    # Top convergence tickers
    ticker_groups = defaultdict(list)
    for t in convergence_trades:
        ticker_groups[t["ticker"]].append(t)

    top_tickers = []
    for ticker, trades in ticker_groups.items():
        r30_vals = [t["return_30d"] for t in trades if t["return_30d"] is not None]
        r90_vals = [t["return_90d"] for t in trades if t["return_90d"] is not None]
        top_tickers.append({
            "ticker": ticker,
            "count": len(trades),
            "avg_return_30d": safe_mean(r30_vals),
            "avg_return_90d": safe_mean(r90_vals),
        })
    top_tickers.sort(key=lambda x: x["count"], reverse=True)

    # Top convergence events by 90d return
    top_events = sorted(
        [t for t in convergence_trades if t["return_90d"] is not None],
        key=lambda x: x["return_90d"],
        reverse=True,
    )[:10]
    top_events_out = [
        {
            "trade_id": e["trade_id"],
            "ticker": e["ticker"],
            "trade_date": e["trade_date"],
            "value": e["value"],
            "return_7d": e["return_7d"],
            "return_30d": e["return_30d"],
            "return_90d": e["return_90d"],
            "abnormal_90d": e["abnormal_90d"],
        }
        for e in top_events
    ]

    # Print summary
    print_summary(
        groups, convergence_trades, t_tests, top_tickers, top_events_out,
        politician_only_count,
    )

    # Build JSON output
    result = {
        "generated_at": datetime.now().isoformat(),
        "window_days": 30,
        "counts": {
            "insider_buys_with_returns": len(insider_buys),
            "congress_buys": len(congress_buys),
            "convergence_events": len(convergence_trades),
            "insider_only_events": len(insider_only_trades),
            "politician_only_events": politician_only_count,
            "overlap_tickers": len(overlap_tickers),
        },
        "group_stats": {g["group"]: g for g in groups},
        "t_tests": {
            field: {"t_statistic": t, "p_value": p, "significant_at_05": s}
            for field, (t, p, s) in t_tests.items()
        },
        "top_convergence_tickers": top_tickers[:10],
        "top_convergence_events": top_events_out,
    }

    # Save JSON
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"JSON report saved to {REPORT_PATH}")


if __name__ == "__main__":
    main()
