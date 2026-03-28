#!/usr/bin/env python3
"""
Walk-forward study for the "Follow Proven Insiders" strategy.

For each insider with enough data:
  1. TRAIN (first 75% of trades): compute per-window Sharpe on abnormal returns,
     find their best window (optimal hold time), compute win rate and score.
  2. TEST (last 25% of trades): simulate trading alongside that insider at their
     optimal hold, measure actual performance.

Then aggregate: does selecting insiders based on training-period track records
produce alpha in the out-of-sample test period?

Also evaluates shares vs options potential by analyzing return distributions.

Usage:
  python walkforward_study.py                  # full study
  python walkforward_study.py --min-trades 10  # require 10+ trades to qualify
"""

from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import statistics
from dataclasses import dataclass, field
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"

WINDOWS = ["7d", "30d", "90d"]
# Annualization factors for Sharpe (approximate trading days per period)
ANNUALIZE = {"7d": (252 / 7) ** 0.5, "30d": (252 / 30) ** 0.5, "90d": (252 / 90) ** 0.5}


@dataclass
class WindowStats:
    window: str
    n: int = 0
    returns: list = field(default_factory=list)
    abnormals: list = field(default_factory=list)
    win_rate: float = 0.0
    avg_return: float = 0.0
    avg_abnormal: float = 0.0
    std_abnormal: float = 0.0
    sharpe: float = 0.0          # annualized Sharpe on abnormal returns
    sharpe_n_adj: float = 0.0    # N-adjusted: Sharpe * (1 - 2/N) penalty


@dataclass
class InsiderProfile:
    insider_id: int
    name: str
    title: str
    primary_ticker: str
    total_trades: int
    train_n: int
    test_n: int
    train_stats: dict = field(default_factory=dict)  # window -> WindowStats
    test_stats: dict = field(default_factory=dict)
    best_window: str = ""
    best_train_sharpe: float = 0.0
    qualified: bool = False
    tier: int = 0


def compute_window_stats(returns: list, abnormals: list, window: str) -> WindowStats:
    """Compute stats for one window's returns."""
    ws = WindowStats(window=window)
    ws.n = len(abnormals)
    ws.returns = returns
    ws.abnormals = abnormals

    if ws.n == 0:
        return ws

    ws.win_rate = sum(1 for r in abnormals if r > 0) / ws.n
    ws.avg_return = statistics.mean(returns) if returns else 0.0
    ws.avg_abnormal = statistics.mean(abnormals)
    ws.std_abnormal = statistics.stdev(abnormals) if ws.n > 1 else 0.0

    if ws.std_abnormal > 0 and ws.n > 1:
        ws.sharpe = (ws.avg_abnormal / ws.std_abnormal) * ANNUALIZE[window]
        # N-adjusted Sharpe: penalize small samples
        # At N=3: 33% penalty, N=5: 60% retained, N=10: 80%, N=20+: ~90%
        n_confidence = max(0, 1.0 - 2.0 / ws.n)
        ws.sharpe_n_adj = ws.sharpe * n_confidence

    return ws


def load_insider_trades(conn: sqlite3.Connection, min_total_trades: int = 8):
    """
    Load all insiders with enough buy trades that have return data.
    Returns dict of insider_id -> list of trade dicts sorted by trade_date.
    """
    logger.info("Loading insider trades...")

    # Get insiders with enough trades
    insiders = conn.execute("""
        SELECT t.insider_id, i.name,
               (SELECT title FROM trades WHERE insider_id = t.insider_id
                ORDER BY trade_date DESC LIMIT 1) as title,
               (SELECT ticker FROM trades WHERE insider_id = t.insider_id
                GROUP BY ticker ORDER BY COUNT(*) DESC LIMIT 1) as primary_ticker,
               COUNT(*) as n
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND (tr.abnormal_7d IS NOT NULL OR tr.abnormal_30d IS NOT NULL OR tr.abnormal_90d IS NOT NULL)
        GROUP BY t.insider_id
        HAVING n >= ?
    """, (min_total_trades,)).fetchall()

    logger.info("Found %d insiders with %d+ trades", len(insiders), min_total_trades)

    result = {}
    for insider_id, name, title, primary_ticker, n in insiders:
        trades = conn.execute("""
            SELECT t.trade_id, t.ticker, t.trade_date, t.filing_date, t.value, t.title,
                   tr.return_7d, tr.abnormal_7d,
                   tr.return_30d, tr.abnormal_30d,
                   tr.return_90d, tr.abnormal_90d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ? AND t.trade_type = 'buy'
            ORDER BY t.trade_date ASC
        """, (insider_id,)).fetchall()

        result[insider_id] = {
            "name": name,
            "title": title or "",
            "primary_ticker": primary_ticker or "",
            "trades": [
                {
                    "trade_id": t[0], "ticker": t[1], "trade_date": t[2],
                    "filing_date": t[3], "value": t[4], "title": t[5],
                    "return_7d": t[6], "abnormal_7d": t[7],
                    "return_30d": t[8], "abnormal_30d": t[9],
                    "return_90d": t[10], "abnormal_90d": t[11],
                }
                for t in trades
            ],
        }

    return result


def analyze_insider(
    insider_id: int,
    data: dict,
    train_frac: float = 0.75,
    min_train_per_window: int = 3,
    min_sharpe: float = 0.5,
) -> InsiderProfile:
    """
    Split an insider's trades into train/test, compute per-window stats,
    find optimal hold, and evaluate OOS.
    """
    trades = data["trades"]
    n = len(trades)
    split = int(n * train_frac)

    profile = InsiderProfile(
        insider_id=insider_id,
        name=data["name"],
        title=data["title"],
        primary_ticker=data["primary_ticker"],
        total_trades=n,
        train_n=split,
        test_n=n - split,
    )

    train_trades = trades[:split]
    test_trades = trades[split:]

    # Compute train stats per window
    for w in WINDOWS:
        ret_key = f"return_{w}"
        abn_key = f"abnormal_{w}"
        rets = [t[ret_key] for t in train_trades if t[ret_key] is not None]
        abns = [t[abn_key] for t in train_trades if t[abn_key] is not None]
        profile.train_stats[w] = compute_window_stats(rets, abns, w)

    # Find best window by N-adjusted Sharpe on abnormal returns
    qualified_windows = [
        (w, profile.train_stats[w])
        for w in WINDOWS
        if profile.train_stats[w].n >= min_train_per_window
    ]

    if not qualified_windows:
        return profile

    best_w, best_stats = max(qualified_windows, key=lambda x: x[1].sharpe_n_adj)
    profile.best_window = best_w
    profile.best_train_sharpe = best_stats.sharpe_n_adj

    # Qualification: positive N-adjusted Sharpe above threshold
    if best_stats.sharpe_n_adj >= min_sharpe and best_stats.avg_abnormal > 0:
        profile.qualified = True

    # Compute test stats at the BEST window (the one selected in training)
    for w in WINDOWS:
        ret_key = f"return_{w}"
        abn_key = f"abnormal_{w}"
        rets = [t[ret_key] for t in test_trades if t[ret_key] is not None]
        abns = [t[abn_key] for t in test_trades if t[abn_key] is not None]
        profile.test_stats[w] = compute_window_stats(rets, abns, w)

    # Assign tier based on train Sharpe
    if profile.best_train_sharpe >= 2.0:
        profile.tier = 3
    elif profile.best_train_sharpe >= 1.0:
        profile.tier = 2
    elif profile.best_train_sharpe >= 0.5:
        profile.tier = 1

    return profile


def print_results(profiles: list[InsiderProfile]):
    """Print comprehensive walk-forward results."""
    qualified = [p for p in profiles if p.qualified]
    unqualified = [p for p in profiles if not p.qualified]

    print(f"\n{'='*70}")
    print(f"  WALK-FORWARD STUDY: Follow Proven Insiders")
    print(f"{'='*70}")
    print(f"  Total insiders analyzed: {len(profiles)}")
    print(f"  Qualified (train Sharpe >= 0.5): {len(qualified)}")
    print(f"  Unqualified: {len(unqualified)}")

    if not qualified:
        print("  No qualified insiders found.")
        return

    # Best window distribution
    bw_counts = {"7d": 0, "30d": 0, "90d": 0}
    for p in qualified:
        bw_counts[p.best_window] = bw_counts.get(p.best_window, 0) + 1
    print(f"  Best window distribution: {bw_counts}")

    # Tier distribution
    for tier in [3, 2, 1]:
        cnt = sum(1 for p in qualified if p.tier == tier)
        print(f"  Tier {tier}: {cnt} insiders")

    # ── TRAIN vs TEST comparison ──
    print(f"\n{'='*70}")
    print(f"  TRAIN vs TEST — Qualified Insiders (at their best window)")
    print(f"{'='*70}")
    print(f"  {'Metric':<30} {'TRAIN':>12} {'TEST':>12} {'Delta':>12}")
    print(f"  {'-'*30} {'-'*12} {'-'*12} {'-'*12}")

    # Aggregate train and test abnormal returns at each insider's best window
    train_abnormals = []
    test_abnormals = []
    train_returns = []
    test_returns = []

    for p in qualified:
        w = p.best_window
        train_abnormals.extend(p.train_stats[w].abnormals)
        test_abnormals.extend(p.test_stats[w].abnormals)
        train_returns.extend(p.train_stats[w].returns)
        test_returns.extend(p.test_stats[w].returns)

    def _stats(vals):
        if not vals:
            return 0, 0, 0, 0, 0
        n = len(vals)
        avg = statistics.mean(vals)
        med = statistics.median(vals)
        wr = sum(1 for v in vals if v > 0) / n
        std = statistics.stdev(vals) if n > 1 else 0
        return n, avg, med, wr, std

    tn, ta, tm, tw, ts = _stats(train_abnormals)
    en, ea, em, ew, es = _stats(test_abnormals)

    print(f"  {'N trades':<30} {tn:>12,} {en:>12,}")
    print(f"  {'Avg abnormal return':<30} {ta*100:>11.2f}% {ea*100:>11.2f}% {(ea-ta)*100:>11.2f}%")
    print(f"  {'Median abnormal return':<30} {tm*100:>11.2f}% {em*100:>11.2f}%")
    print(f"  {'Win rate (vs SPY)':<30} {tw*100:>11.1f}% {ew*100:>11.1f}%")
    print(f"  {'Std dev':<30} {ts*100:>11.2f}% {es*100:>11.2f}%")

    # Compute aggregate Sharpe (approximate — use median window for annualization)
    if ts > 0:
        train_sharpe = (ta / ts) * ANNUALIZE["30d"]
    else:
        train_sharpe = 0
    if es > 0:
        test_sharpe = (ea / es) * ANNUALIZE["30d"]
    else:
        test_sharpe = 0
    print(f"  {'Sharpe (annualized, ~30d)':<30} {train_sharpe:>12.2f} {test_sharpe:>12.2f} {test_sharpe-train_sharpe:>12.2f}")

    # ── Raw returns for shares/options evaluation ──
    tn2, ta2, tm2, tw2, ts2 = _stats(train_returns)
    en2, ea2, em2, ew2, es2 = _stats(test_returns)

    print(f"\n  {'--- Raw Returns (not vs SPY) ---'}")
    print(f"  {'Avg raw return':<30} {ta2*100:>11.2f}% {ea2*100:>11.2f}%")
    print(f"  {'Median raw return':<30} {tm2*100:>11.2f}% {em2*100:>11.2f}%")
    print(f"  {'Win rate (absolute)':<30} {tw2*100:>11.1f}% {ew2*100:>11.1f}%")

    # ── Per-window breakdown (test only) ──
    print(f"\n{'='*70}")
    print(f"  TEST RESULTS BY WINDOW (qualified insiders only)")
    print(f"{'='*70}")
    print(f"  {'Window':<8} {'N Insiders':>12} {'N Trades':>10} {'Avg Alpha':>12} {'WR':>8} {'Sharpe':>8}")
    print(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*12} {'-'*8} {'-'*8}")

    for w in WINDOWS:
        w_insiders = [p for p in qualified if p.best_window == w]
        if not w_insiders:
            continue
        w_abnormals = []
        for p in w_insiders:
            w_abnormals.extend(p.test_stats[w].abnormals)
        if not w_abnormals:
            continue
        n_w = len(w_abnormals)
        avg_w = statistics.mean(w_abnormals)
        wr_w = sum(1 for v in w_abnormals if v > 0) / n_w
        std_w = statistics.stdev(w_abnormals) if n_w > 1 else 0
        sh_w = (avg_w / std_w) * ANNUALIZE[w] if std_w > 0 else 0
        print(f"  {w:<8} {len(w_insiders):>12} {n_w:>10} {avg_w*100:>11.2f}% {wr_w*100:>7.1f}% {sh_w:>8.2f}")

    # ── Per-tier breakdown (test) ──
    print(f"\n{'='*70}")
    print(f"  TEST RESULTS BY TIER")
    print(f"{'='*70}")
    print(f"  {'Tier':<8} {'N Insiders':>12} {'N Trades':>10} {'Avg Alpha':>12} {'WR':>8} {'Sharpe':>8}")
    print(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*12} {'-'*8} {'-'*8}")

    for tier in [3, 2, 1]:
        t_insiders = [p for p in qualified if p.tier == tier]
        if not t_insiders:
            continue
        t_abnormals = []
        for p in t_insiders:
            w = p.best_window
            t_abnormals.extend(p.test_stats[w].abnormals)
        if not t_abnormals:
            continue
        n_t = len(t_abnormals)
        avg_t = statistics.mean(t_abnormals)
        wr_t = sum(1 for v in t_abnormals if v > 0) / n_t
        std_t = statistics.stdev(t_abnormals) if n_t > 1 else 0
        sh_t = (avg_t / std_t) * ANNUALIZE["30d"] if std_t > 0 else 0
        print(f"  T{tier:<7} {len(t_insiders):>12} {n_t:>10} {avg_t*100:>11.2f}% {wr_t*100:>7.1f}% {sh_t:>8.2f}")

    # ── Baseline: unqualified insiders test performance ──
    print(f"\n{'='*70}")
    print(f"  BASELINE: Unqualified Insiders (test period, 7d window)")
    print(f"{'='*70}")
    baseline_abnormals = []
    for p in unqualified:
        baseline_abnormals.extend(p.test_stats["7d"].abnormals)
    if baseline_abnormals:
        bn = len(baseline_abnormals)
        ba = statistics.mean(baseline_abnormals)
        bw = sum(1 for v in baseline_abnormals if v > 0) / bn
        bs = statistics.stdev(baseline_abnormals) if bn > 1 else 0
        bsh = (ba / bs) * ANNUALIZE["7d"] if bs > 0 else 0
        print(f"  N trades: {bn:,}")
        print(f"  Avg alpha: {ba*100:.2f}%")
        print(f"  Win rate: {bw*100:.1f}%")
        print(f"  Sharpe: {bsh:.2f}")
    else:
        print(f"  No data")

    # ── Options viability analysis ──
    print(f"\n{'='*70}")
    print(f"  OPTIONS VIABILITY (test period, qualified insiders)")
    print(f"{'='*70}")

    for w in WINDOWS:
        w_insiders = [p for p in qualified if p.best_window == w]
        if not w_insiders:
            continue
        w_returns = []
        for p in w_insiders:
            w_returns.extend(p.test_stats[w].returns)
        if not w_returns:
            continue

        n_w = len(w_returns)
        avg_r = statistics.mean(w_returns)
        med_r = statistics.median(w_returns)
        wr_r = sum(1 for v in w_returns if v > 0) / n_w

        # Options metrics: % of trades with >5%, >10%, >20% returns
        pct_5 = sum(1 for v in w_returns if v > 0.05) / n_w
        pct_10 = sum(1 for v in w_returns if v > 0.10) / n_w
        pct_20 = sum(1 for v in w_returns if v > 0.20) / n_w

        # Tail risk: % with >-15%, >-25% drawdown
        pct_neg15 = sum(1 for v in w_returns if v < -0.15) / n_w
        pct_neg25 = sum(1 for v in w_returns if v < -0.25) / n_w

        print(f"\n  {w} window ({len(w_insiders)} insiders, {n_w} trades):")
        print(f"    Avg return: {avg_r*100:+.2f}%  |  Median: {med_r*100:+.2f}%  |  WR: {wr_r*100:.1f}%")
        print(f"    >+5%: {pct_5*100:.1f}%  |  >+10%: {pct_10*100:.1f}%  |  >+20%: {pct_20*100:.1f}%")
        print(f"    <-15%: {pct_neg15*100:.1f}%  |  <-25%: {pct_neg25*100:.1f}%")

        # Options edge estimate: if you buy ATM calls at entry
        # Rough delta ~0.5, so need >2x the move to break even on premium
        # For OTM 5% calls, need >5% move + premium
        if w in ("30d", "90d") and avg_r > 0.03:
            print(f"    -> Options viable: avg return {avg_r*100:.1f}% exceeds typical premium decay")
        elif w == "7d" and avg_r > 0.02:
            print(f"    -> Options marginal: 7d is short for options (theta decay)")
        else:
            print(f"    -> Options NOT recommended at this window")

    # ── Top 20 insiders by test performance ──
    print(f"\n{'='*70}")
    print(f"  TOP 20 INSIDERS BY TEST ALPHA (at their best window)")
    print(f"{'='*70}")
    print(f"  {'Name':<30} {'Title':<15} {'Ticker':<6} {'BestW':>5} {'Train':>7} {'Test':>7} {'TrSh':>6} {'TeSh':>6} {'N_te':>5}")
    print(f"  {'-'*30} {'-'*15} {'-'*6} {'-'*5} {'-'*7} {'-'*7} {'-'*6} {'-'*6} {'-'*5}")

    # Sort by test abnormal at best window
    def test_alpha(p):
        w = p.best_window
        s = p.test_stats.get(w)
        if s and s.n > 0:
            return s.avg_abnormal
        return -999

    for p in sorted(qualified, key=test_alpha, reverse=True)[:20]:
        w = p.best_window
        tr = p.train_stats[w]
        te = p.test_stats.get(w, WindowStats(window=w))
        tr_alpha = f"{tr.avg_abnormal*100:+.1f}%" if tr.n > 0 else "-"
        te_alpha = f"{te.avg_abnormal*100:+.1f}%" if te.n > 0 else "-"
        tr_sh = f"{tr.sharpe_n_adj:.2f}" if tr.n > 0 else "-"
        te_sh = f"{te.sharpe:.2f}" if te.n > 1 else "-"
        name_trunc = p.name[:29]
        title_trunc = (p.title or "?")[:14]
        print(f"  {name_trunc:<30} {title_trunc:<15} {p.primary_ticker:<6} {w:>5} {tr_alpha:>7} {te_alpha:>7} {tr_sh:>6} {te_sh:>6} {te.n:>5}")

    # ── Strategy recommendation ──
    print(f"\n{'='*70}")
    print(f"  STRATEGY RECOMMENDATION")
    print(f"{'='*70}")

    # Test if qualified insiders outperform unqualified
    q_test_abns = []
    for p in qualified:
        q_test_abns.extend(p.test_stats[p.best_window].abnormals)

    if q_test_abns and baseline_abnormals:
        q_avg = statistics.mean(q_test_abns)
        b_avg = statistics.mean(baseline_abnormals)
        delta = q_avg - b_avg

        # T-test (simplified)
        q_n = len(q_test_abns)
        b_n = len(baseline_abnormals)
        q_std = statistics.stdev(q_test_abns) if q_n > 1 else 0
        b_std = statistics.stdev(baseline_abnormals) if b_n > 1 else 0
        pooled_se = ((q_std**2 / q_n) + (b_std**2 / b_n)) ** 0.5 if q_n > 0 and b_n > 0 else 0
        t_stat = delta / pooled_se if pooled_se > 0 else 0

        print(f"  Qualified vs Unqualified (test period):")
        print(f"    Qualified avg alpha: {q_avg*100:+.2f}% (N={q_n:,})")
        print(f"    Unqualified avg alpha: {b_avg*100:+.2f}% (N={b_n:,})")
        print(f"    Delta: {delta*100:+.2f}%")
        print(f"    t-statistic: {t_stat:.2f}")
        print(f"    Edge is {'CONFIRMED' if t_stat > 1.96 else 'NOT CONFIRMED'} at p<0.05")

    print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(description="Walk-forward insider strategy study")
    parser.add_argument("--min-trades", type=int, default=8,
                        help="Minimum total trades per insider (default: 8, gives 6 train + 2 test)")
    parser.add_argument("--min-sharpe", type=float, default=0.5,
                        help="Minimum N-adjusted Sharpe to qualify (default: 0.5)")
    parser.add_argument("--train-frac", type=float, default=0.75,
                        help="Fraction of trades for training (default: 0.75)")
    args = parser.parse_args()

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)

    data = load_insider_trades(conn, min_total_trades=args.min_trades)
    conn.close()

    logger.info("Analyzing %d insiders...", len(data))

    profiles = []
    for insider_id, insider_data in data.items():
        profile = analyze_insider(
            insider_id, insider_data,
            train_frac=args.train_frac,
            min_sharpe=args.min_sharpe,
        )
        profiles.append(profile)

    qualified = [p for p in profiles if p.qualified]
    logger.info("Qualified: %d / %d", len(qualified), len(profiles))

    print_results(profiles)


if __name__ == "__main__":
    main()
