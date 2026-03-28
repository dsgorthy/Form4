#!/usr/bin/env python3
"""
Walk-forward study for insider SELL signals — short selling and put buying.

Same methodology as walkforward_study.py but for the sell side:
  - For each insider with enough sell trades + return data:
    1. TRAIN (first 75%): score their ability to predict declines
       (negative abnormal returns = stock underperforms after they sell)
    2. TEST (last 25%): simulate shorting or buying puts when they sell
  - Also runs the BUY side in parallel for direct comparison

"Win" from short/put perspective = stock drops after insider sells.
Abnormal returns are NEGATED for scoring (positive = good short signal).

Usage:
  python walkforward_sell_study.py                    # full study
  python walkforward_sell_study.py --min-trades 10    # require 10+ trades
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
    sharpe: float = 0.0
    sharpe_n_adj: float = 0.0


@dataclass
class InsiderProfile:
    insider_id: int
    name: str
    title: str
    primary_ticker: str
    trade_type: str  # 'buy' or 'sell'
    total_trades: int
    train_n: int
    test_n: int
    train_stats: dict = field(default_factory=dict)
    test_stats: dict = field(default_factory=dict)
    best_window: str = ""
    best_train_sharpe: float = 0.0
    qualified: bool = False
    tier: int = 0


def compute_window_stats(returns: list, abnormals: list, window: str) -> WindowStats:
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
        n_confidence = max(0, 1.0 - 2.0 / ws.n)
        ws.sharpe_n_adj = ws.sharpe * n_confidence

    return ws


def load_insider_trades(conn: sqlite3.Connection, trade_type: str, min_total_trades: int = 8):
    """Load insiders with enough trades of the given type that have return data."""
    logger.info("Loading %s trades...", trade_type)

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
        WHERE t.trade_type = ?
          AND (tr.abnormal_7d IS NOT NULL OR tr.abnormal_30d IS NOT NULL OR tr.abnormal_90d IS NOT NULL)
        GROUP BY t.insider_id
        HAVING n >= ?
    """, (trade_type, min_total_trades)).fetchall()

    logger.info("Found %d insiders with %d+ %s trades", len(insiders), min_total_trades, trade_type)

    result = {}
    for insider_id, name, title, primary_ticker, n in insiders:
        trades = conn.execute("""
            SELECT t.trade_id, t.ticker, t.trade_date, t.filing_date, t.value, t.title,
                   tr.return_7d, tr.abnormal_7d,
                   tr.return_30d, tr.abnormal_30d,
                   tr.return_90d, tr.abnormal_90d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ? AND t.trade_type = ?
            ORDER BY t.trade_date ASC
        """, (insider_id, trade_type)).fetchall()

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
    trade_type: str,
    negate: bool = False,
    train_frac: float = 0.75,
    min_train_per_window: int = 3,
    min_sharpe: float = 0.5,
) -> InsiderProfile:
    """
    Split trades into train/test, compute per-window stats, find best window.

    If negate=True (for sells), we negate returns and abnormals so that
    "stock drops after sell" = positive = win. This lets us use the same
    Sharpe/WR machinery to find insiders who consistently predict declines.
    """
    trades = data["trades"]
    n = len(trades)
    split = int(n * train_frac)

    profile = InsiderProfile(
        insider_id=insider_id,
        name=data["name"],
        title=data["title"],
        primary_ticker=data["primary_ticker"],
        trade_type=trade_type,
        total_trades=n,
        train_n=split,
        test_n=n - split,
    )

    train_trades = trades[:split]
    test_trades = trades[split:]

    sign = -1.0 if negate else 1.0

    for subset, stats_dict in [(train_trades, profile.train_stats), (test_trades, profile.test_stats)]:
        for w in WINDOWS:
            ret_key = f"return_{w}"
            abn_key = f"abnormal_{w}"
            rets = [sign * t[ret_key] for t in subset if t[ret_key] is not None]
            abns = [sign * t[abn_key] for t in subset if t[abn_key] is not None]
            stats_dict[w] = compute_window_stats(rets, abns, w)

    # Find best window by N-adjusted Sharpe
    qualified_windows = [
        (w, profile.train_stats[w])
        for w in WINDOWS
        if profile.train_stats[w].n >= min_train_per_window
        and profile.train_stats[w].sharpe_n_adj > 0
    ]

    if not qualified_windows:
        return profile

    best_w, best_stats = max(qualified_windows, key=lambda x: x[1].sharpe_n_adj)
    profile.best_window = best_w
    profile.best_train_sharpe = best_stats.sharpe_n_adj

    if best_stats.sharpe_n_adj >= min_sharpe and best_stats.avg_abnormal > 0:
        profile.qualified = True

    if profile.best_train_sharpe >= 2.0:
        profile.tier = 3
    elif profile.best_train_sharpe >= 1.0:
        profile.tier = 2
    elif profile.best_train_sharpe >= 0.5:
        profile.tier = 1

    return profile


def print_side_results(profiles: list[InsiderProfile], side_label: str, is_short: bool = False):
    """Print results for one side (buy-long or sell-short)."""
    qualified = [p for p in profiles if p.qualified]
    unqualified = [p for p in profiles if not p.qualified]

    direction = "SHORT/PUT" if is_short else "LONG/CALL"

    print(f"\n{'='*70}")
    print(f"  {side_label} — {direction}")
    print(f"{'='*70}")
    print(f"  Total insiders analyzed: {len(profiles)}")
    print(f"  Qualified (train Sharpe >= 0.5): {len(qualified)}")
    print(f"  Unqualified: {len(unqualified)}")

    if not qualified:
        print("  No qualified insiders found.")
        return qualified

    # Best window distribution
    bw_counts = {w: sum(1 for p in qualified if p.best_window == w) for w in WINDOWS}
    print(f"  Best window distribution: {bw_counts}")

    for tier in [3, 2, 1]:
        cnt = sum(1 for p in qualified if p.tier == tier)
        print(f"  Tier {tier}: {cnt} insiders")

    # TRAIN vs TEST
    print(f"\n  {'Metric':<30} {'TRAIN':>12} {'TEST':>12} {'Delta':>12}")
    print(f"  {'-'*30} {'-'*12} {'-'*12} {'-'*12}")

    train_abns, test_abns = [], []
    train_rets, test_rets = [], []
    for p in qualified:
        w = p.best_window
        train_abns.extend(p.train_stats[w].abnormals)
        test_abns.extend(p.test_stats[w].abnormals)
        train_rets.extend(p.train_stats[w].returns)
        test_rets.extend(p.test_stats[w].returns)

    def _stats(vals):
        if not vals:
            return 0, 0, 0, 0, 0
        n = len(vals)
        return n, statistics.mean(vals), statistics.median(vals), sum(1 for v in vals if v > 0) / n, (statistics.stdev(vals) if n > 1 else 0)

    tn, ta, tm, tw, ts = _stats(train_abns)
    en, ea, em, ew, es = _stats(test_abns)

    ret_label = "Avg short alpha" if is_short else "Avg alpha"
    print(f"  {'N trades':<30} {tn:>12,} {en:>12,}")
    print(f"  {ret_label:<30} {ta*100:>11.2f}% {ea*100:>11.2f}% {(ea-ta)*100:>11.2f}%")
    print(f"  {'Median alpha':<30} {tm*100:>11.2f}% {em*100:>11.2f}%")
    print(f"  {'Win rate':<30} {tw*100:>11.1f}% {ew*100:>11.1f}%")

    if ts > 0:
        train_sh = (ta / ts) * ANNUALIZE["30d"]
    else:
        train_sh = 0
    if es > 0:
        test_sh = (ea / es) * ANNUALIZE["30d"]
    else:
        test_sh = 0
    print(f"  {'Sharpe (annualized)':<30} {train_sh:>12.2f} {test_sh:>12.2f} {test_sh-train_sh:>12.2f}")

    # Raw returns
    tn2, ta2, tm2, tw2, ts2 = _stats(train_rets)
    en2, ea2, em2, ew2, es2 = _stats(test_rets)
    raw_label = "Avg short return" if is_short else "Avg raw return"
    print(f"\n  {'--- Raw Returns ---'}")
    print(f"  {raw_label:<30} {ta2*100:>11.2f}% {ea2*100:>11.2f}%")
    print(f"  {'Median raw return':<30} {tm2*100:>11.2f}% {em2*100:>11.2f}%")
    print(f"  {'Win rate (absolute)':<30} {tw2*100:>11.1f}% {ew2*100:>11.1f}%")

    # Per-window breakdown (test)
    print(f"\n  TEST BY WINDOW:")
    print(f"  {'Window':<8} {'N Insiders':>12} {'N Trades':>10} {'Avg Alpha':>12} {'WR':>8} {'Sharpe':>8}")
    print(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*12} {'-'*8} {'-'*8}")

    for w in WINDOWS:
        w_insiders = [p for p in qualified if p.best_window == w]
        if not w_insiders:
            continue
        w_abns = []
        for p in w_insiders:
            w_abns.extend(p.test_stats[w].abnormals)
        if not w_abns:
            continue
        n_w = len(w_abns)
        avg_w = statistics.mean(w_abns)
        wr_w = sum(1 for v in w_abns if v > 0) / n_w
        std_w = statistics.stdev(w_abns) if n_w > 1 else 0
        sh_w = (avg_w / std_w) * ANNUALIZE[w] if std_w > 0 else 0
        print(f"  {w:<8} {len(w_insiders):>12} {n_w:>10} {avg_w*100:>11.2f}% {wr_w*100:>7.1f}% {sh_w:>8.2f}")

    # Per-tier breakdown (test)
    print(f"\n  TEST BY TIER:")
    print(f"  {'Tier':<8} {'N Insiders':>12} {'N Trades':>10} {'Avg Alpha':>12} {'WR':>8} {'Sharpe':>8}")
    print(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*12} {'-'*8} {'-'*8}")

    for tier in [3, 2, 1]:
        t_insiders = [p for p in qualified if p.tier == tier]
        if not t_insiders:
            continue
        t_abns = []
        for p in t_insiders:
            t_abns.extend(p.test_stats[p.best_window].abnormals)
        if not t_abns:
            continue
        n_t = len(t_abns)
        avg_t = statistics.mean(t_abns)
        wr_t = sum(1 for v in t_abns if v > 0) / n_t
        std_t = statistics.stdev(t_abns) if n_t > 1 else 0
        sh_t = (avg_t / std_t) * ANNUALIZE["30d"] if std_t > 0 else 0
        print(f"  T{tier:<7} {len(t_insiders):>12} {n_t:>10} {avg_t*100:>11.2f}% {wr_t*100:>7.1f}% {sh_t:>8.2f}")

    # Options/short viability
    print(f"\n  {'SHORT/PUT' if is_short else 'CALL'} VIABILITY (test period):")
    for w in WINDOWS:
        w_insiders = [p for p in qualified if p.best_window == w]
        if not w_insiders:
            continue
        w_rets = []
        for p in w_insiders:
            w_rets.extend(p.test_stats[w].returns)
        if not w_rets:
            continue

        n_w = len(w_rets)
        avg_r = statistics.mean(w_rets)
        wr_r = sum(1 for v in w_rets if v > 0) / n_w
        pct_5 = sum(1 for v in w_rets if v > 0.05) / n_w
        pct_10 = sum(1 for v in w_rets if v > 0.10) / n_w
        pct_20 = sum(1 for v in w_rets if v > 0.20) / n_w
        pct_neg15 = sum(1 for v in w_rets if v < -0.15) / n_w

        print(f"\n    {w} ({len(w_insiders)} insiders, {n_w} trades):")
        print(f"      Avg return: {avg_r*100:+.2f}% | WR: {wr_r*100:.1f}%")
        print(f"      >+5%: {pct_5*100:.1f}% | >+10%: {pct_10*100:.1f}% | >+20%: {pct_20*100:.1f}%")
        print(f"      <-15% (loss): {pct_neg15*100:.1f}%")

    # Baseline: unqualified
    print(f"\n  BASELINE (unqualified, 7d):")
    baseline_abns = []
    for p in unqualified:
        baseline_abns.extend(p.test_stats["7d"].abnormals)
    if baseline_abns:
        bn = len(baseline_abns)
        ba = statistics.mean(baseline_abns)
        bw_rate = sum(1 for v in baseline_abns if v > 0) / bn
        bs = statistics.stdev(baseline_abns) if bn > 1 else 0
        bsh = (ba / bs) * ANNUALIZE["7d"] if bs > 0 else 0
        print(f"    N={bn:,} | Avg alpha: {ba*100:.2f}% | WR: {bw_rate*100:.1f}% | Sharpe: {bsh:.2f}")

    # Top 15 insiders
    print(f"\n  TOP 15 INSIDERS BY TEST ALPHA:")
    print(f"  {'Name':<28} {'Title':<12} {'Tkr':<6} {'BW':>4} {'Tr%':>6} {'Te%':>6} {'TrSh':>6} {'TeSh':>6} {'Nte':>5}")
    print(f"  {'-'*28} {'-'*12} {'-'*6} {'-'*4} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*5}")

    def test_alpha(p):
        s = p.test_stats.get(p.best_window)
        return s.avg_abnormal if s and s.n > 0 else -999

    for p in sorted(qualified, key=test_alpha, reverse=True)[:15]:
        w = p.best_window
        tr = p.train_stats[w]
        te = p.test_stats.get(w, WindowStats(window=w))
        tr_a = f"{tr.avg_abnormal*100:+.1f}%" if tr.n > 0 else "-"
        te_a = f"{te.avg_abnormal*100:+.1f}%" if te.n > 0 else "-"
        tr_sh = f"{tr.sharpe_n_adj:.2f}" if tr.n > 0 else "-"
        te_sh = f"{te.sharpe:.2f}" if te.n > 1 else "-"
        print(f"  {p.name[:27]:<28} {(p.title or '?')[:11]:<12} {p.primary_ticker:<6} {w:>4} {tr_a:>6} {te_a:>6} {tr_sh:>6} {te_sh:>6} {te.n:>5}")

    return qualified


def print_comparison(buy_qualified, sell_qualified):
    """Print head-to-head comparison of buy-long vs sell-short."""
    print(f"\n{'='*70}")
    print(f"  HEAD-TO-HEAD: BUY-LONG vs SELL-SHORT")
    print(f"{'='*70}")

    rows = []
    for label, qualified, is_short in [("Buy-Long", buy_qualified, False), ("Sell-Short", sell_qualified, True)]:
        if not qualified:
            rows.append((label, 0, 0, 0, 0, 0, 0))
            continue
        all_abns = []
        all_rets = []
        for p in qualified:
            w = p.best_window
            all_abns.extend(p.test_stats[w].abnormals)
            all_rets.extend(p.test_stats[w].returns)
        n = len(all_abns)
        avg_a = statistics.mean(all_abns) if all_abns else 0
        med_a = statistics.median(all_abns) if all_abns else 0
        wr = (sum(1 for v in all_abns if v > 0) / n) if n > 0 else 0
        std = statistics.stdev(all_abns) if n > 1 else 0
        sh = (avg_a / std) * ANNUALIZE["30d"] if std > 0 else 0
        rows.append((label, len(qualified), n, avg_a, med_a, wr, sh))

    print(f"  {'Metric':<25} {'Buy-Long':>15} {'Sell-Short':>15}")
    print(f"  {'-'*25} {'-'*15} {'-'*15}")
    print(f"  {'Qualified insiders':<25} {rows[0][1]:>15,} {rows[1][1]:>15,}")
    print(f"  {'Test trades':<25} {rows[0][2]:>15,} {rows[1][2]:>15,}")
    print(f"  {'Avg alpha':<25} {rows[0][3]*100:>14.2f}% {rows[1][3]*100:>14.2f}%")
    print(f"  {'Median alpha':<25} {rows[0][4]*100:>14.2f}% {rows[1][4]*100:>14.2f}%")
    print(f"  {'Win rate':<25} {rows[0][5]*100:>14.1f}% {rows[1][5]*100:>14.1f}%")
    print(f"  {'Sharpe':<25} {rows[0][6]:>15.2f} {rows[1][6]:>15.2f}")

    # Trades per year estimate
    for label, qualified in [("Buy-Long", buy_qualified), ("Sell-Short", sell_qualified)]:
        if not qualified:
            continue
        # Count test trades and estimate annualized frequency
        test_trades = sum(p.test_n for p in qualified)
        # Rough: test is 25% of data spanning ~5 years
        est_per_year = test_trades / 1.25 if test_trades > 0 else 0
        print(f"  {label} est. trades/year: ~{est_per_year:.0f}")

    # Combined portfolio suggestion
    print(f"\n  COMBINED PORTFOLIO SUGGESTION:")
    print(f"  {'Strategy':<20} {'Sizing':>10} {'Max Pos':>10} {'Hold':>8} {'Instrument':>12}")
    print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*8} {'-'*12}")
    print(f"  {'Solo Long (7d)':<20} {'3%':>10} {'5':>10} {'7d':>8} {'Shares':>12}")
    print(f"  {'Solo Long (90d)':<20} {'1%':>10} {'3':>10} {'60d':>8} {'Calls':>12}")
    if rows[1][1] > 0 and rows[1][5] > 0.53:
        print(f"  {'Solo Short (7d)':<20} {'2%':>10} {'3':>10} {'7d':>8} {'Short/Puts':>12}")
        print(f"  {'Solo Short (90d)':<20} {'1%':>10} {'2':>10} {'60d':>8} {'Puts':>12}")
    else:
        print(f"  {'Solo Short':<20} {'---':>10} {'NOT VIABLE based on data':>30}")


def main():
    parser = argparse.ArgumentParser(description="Walk-forward: buy-long vs sell-short")
    parser.add_argument("--min-trades", type=int, default=8,
                        help="Min total trades per insider (default: 8)")
    parser.add_argument("--min-sharpe", type=float, default=0.5,
                        help="Min N-adjusted Sharpe to qualify (default: 0.5)")
    parser.add_argument("--train-frac", type=float, default=0.75,
                        help="Training fraction (default: 0.75)")
    args = parser.parse_args()

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)

    # Load both sides
    buy_data = load_insider_trades(conn, "buy", min_total_trades=args.min_trades)
    sell_data = load_insider_trades(conn, "sell", min_total_trades=args.min_trades)
    conn.close()

    # Analyze buy side (long — same as original study, for comparison)
    logger.info("Analyzing %d buy insiders...", len(buy_data))
    buy_profiles = []
    for insider_id, data in buy_data.items():
        p = analyze_insider(insider_id, data, "buy", negate=False,
                            train_frac=args.train_frac, min_sharpe=args.min_sharpe)
        buy_profiles.append(p)
    buy_qualified = [p for p in buy_profiles if p.qualified]
    logger.info("Buy qualified: %d / %d", len(buy_qualified), len(buy_profiles))

    # Analyze sell side (short — negate returns so "stock drops" = positive)
    logger.info("Analyzing %d sell insiders...", len(sell_data))
    sell_profiles = []
    for insider_id, data in sell_data.items():
        p = analyze_insider(insider_id, data, "sell", negate=True,
                            train_frac=args.train_frac, min_sharpe=args.min_sharpe)
        sell_profiles.append(p)
    sell_qualified = [p for p in sell_profiles if p.qualified]
    logger.info("Sell qualified: %d / %d", len(sell_qualified), len(sell_profiles))

    # Print results
    buy_q = print_side_results(buy_profiles, "BUY SIDE (Proven Buyers → Go Long)", is_short=False)
    sell_q = print_side_results(sell_profiles, "SELL SIDE (Proven Sellers → Go Short/Buy Puts)", is_short=True)

    print_comparison(buy_q, sell_q)

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
