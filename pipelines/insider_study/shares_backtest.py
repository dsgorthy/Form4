#!/usr/bin/env python3
"""
Phase 3: Shares Backtest with Confidence-Based Sizing
------------------------------------------------------
Backtest insider buy signals on the training period (2021-2024)
with composite confidence scoring and stop-loss optimization.

Based on Phase 1-2 findings:
- Buy signals only (sells not reliable)
- 7d hold is the strongest window
- Cluster > individual, C-Suite > Director > Officer
- Tier 1 insiders have highest Sharpe OOS
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DIR = Path(__file__).parent / "data" / "prices"
OUTPUT_DIR = Path(__file__).parent / "data"
TRACK_RECORDS_PATH = OUTPUT_DIR / "track_records.json"

TRAIN_START = "2021-01-01"
TRAIN_END = "2024-12-31"

PORTFOLIO_VALUE = 30_000
MAX_CONCURRENT = 5


def load_track_records() -> dict:
    """Load insider tier assignments from Phase 2."""
    if TRACK_RECORDS_PATH.exists():
        with open(TRACK_RECORDS_PATH) as f:
            data = json.load(f)
        return {int(k): v for k, v in data["records"].items()}
    return {}


def load_prices(ticker: str, cache: dict) -> pd.DataFrame | None:
    if ticker in cache:
        return cache[ticker]
    path = PRICES_DIR / f"{ticker.upper()}.csv"
    if not path.exists():
        cache[ticker] = None
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
        df.columns = [c.lower() for c in df.columns]
        cache[ticker] = df
        return df
    except Exception:
        cache[ticker] = None
        return None


def compute_confidence(trade: dict, track_records: dict) -> tuple[float, str]:
    """
    Compute composite confidence score (0-1) for a trade.
    Returns (score, reasoning).
    """
    factors = []

    # Factor 1: Cluster vs Individual (0.3 weight)
    if trade["is_cluster"]:
        factors.append(("cluster", 1.0, 0.3))
    else:
        factors.append(("individual", 0.4, 0.3))

    # Factor 2: Insider tier (0.3 weight)
    insider_id = trade["insider_id"]
    if insider_id in track_records:
        tier = track_records[insider_id]["tier"]
        tier_scores = {1: 1.0, 2: 0.7, 3: 0.4, 4: 0.2}
        factors.append((f"tier_{tier}", tier_scores[tier], 0.3))
    else:
        factors.append(("unscored", 0.5, 0.3))

    # Factor 3: Seniority (0.2 weight)
    tw = trade.get("title_weight", 0) or 0
    if tw >= 3.0 or trade.get("is_csuite"):
        factors.append(("c_suite", 1.0, 0.2))
    elif tw >= 1.5:
        factors.append(("director", 0.6, 0.2))
    else:
        factors.append(("officer", 0.3, 0.2))

    # Factor 4: Trade value (0.2 weight)
    val = abs(trade.get("value", 0) or 0)
    if val >= 5_000_000:
        factors.append(("5M+", 1.0, 0.2))
    elif val >= 1_000_000:
        factors.append(("1M-5M", 0.8, 0.2))
    elif val >= 100_000:
        factors.append(("100K-1M", 0.5, 0.2))
    else:
        factors.append(("<100K", 0.3, 0.2))

    score = sum(s * w for _, s, w in factors)
    reasoning = ", ".join(f"{f[0]}={f[1]:.1f}" for f in factors)
    return score, reasoning


def compute_confidence_v2(trade: dict, conn: sqlite3.Connection) -> tuple[float, str]:
    """
    Compute composite confidence score (0-1) using PIT scores from insider_ticker_scores.

    Factors:
      - Cluster signal (0.25 weight): multiple insiders buying = stronger
      - PIT blended score (0.30 weight): honest, point-in-time track record
      - Signal quality (0.20 weight): transaction type informativeness
      - Seniority (0.15 weight): C-suite > director > officer
      - Trade value (0.10 weight): larger trades = more conviction

    Falls back to compute_confidence() if PIT scores aren't available.
    """
    factors = []

    # Factor 1: Cluster vs Individual (0.25 weight)
    if trade["is_cluster"]:
        factors.append(("cluster", 1.0, 0.25))
    else:
        factors.append(("individual", 0.4, 0.25))

    # Factor 2: PIT blended score (0.30 weight)
    pit_score = None
    insider_id = trade["insider_id"]
    ticker = trade["ticker"]
    trade_date = trade["trade_date"]

    try:
        row = conn.execute("""
            SELECT blended_score, sufficient_data
            FROM insider_ticker_scores
            WHERE insider_id = ? AND ticker = ? AND as_of_date <= ?
            ORDER BY as_of_date DESC LIMIT 1
        """, (insider_id, ticker, trade_date)).fetchone()

        if row and row[1]:  # sufficient_data = 1
            pit_score = row[0]
            # Normalize 0-3 score to 0-1
            pit_norm = min(1.0, pit_score / 3.0)
            factors.append((f"pit_{pit_score:.1f}", pit_norm, 0.30))
        else:
            factors.append(("pit_unknown", 0.4, 0.30))
    except Exception:
        factors.append(("pit_unknown", 0.4, 0.30))

    # Factor 3: Signal quality (0.20 weight)
    signal_quality = trade.get("signal_quality")
    if signal_quality is not None:
        factors.append((f"sq_{signal_quality:.1f}", signal_quality, 0.20))
    else:
        # Default: assume open market buy if no signal_quality
        factors.append(("sq_default", 1.0, 0.20))

    # Factor 4: Seniority (0.15 weight)
    tw = trade.get("title_weight", 0) or 0
    if tw >= 3.0 or trade.get("is_csuite"):
        factors.append(("c_suite", 1.0, 0.15))
    elif tw >= 1.5:
        factors.append(("director", 0.6, 0.15))
    else:
        factors.append(("officer", 0.3, 0.15))

    # Factor 5: Trade value (0.10 weight)
    val = abs(trade.get("value", 0) or 0)
    if val >= 5_000_000:
        factors.append(("5M+", 1.0, 0.10))
    elif val >= 1_000_000:
        factors.append(("1M-5M", 0.8, 0.10))
    elif val >= 100_000:
        factors.append(("100K-1M", 0.5, 0.10))
    else:
        factors.append(("<100K", 0.3, 0.10))

    score = sum(s * w for _, s, w in factors)
    reasoning = ", ".join(f"{f[0]}={f[1]:.1f}" for f in factors)
    return score, reasoning


def confidence_to_size(score: float) -> float:
    """Map confidence score to position size as % of portfolio."""
    if score >= 0.7:
        return 0.05  # 5% — high confidence
    elif score >= 0.5:
        return 0.03  # 3% — medium
    else:
        return 0.01  # 1% — low


def precompute_stop_loss_returns(db: sqlite3.Connection, start: str, end: str,
                                  price_cache: dict) -> dict:
    """
    For all buy trades, pre-compute what the return would be under different
    stop-loss levels by checking intraday lows during the 7-day hold.

    Returns: trade_id -> {
        "return_7d_no_stop": float,
        "min_intraday_dd": float (max drawdown from entry during hold),
        "stopped_at_10": bool,
        "stopped_at_15": bool,
        "stopped_at_20": bool,
    }
    """
    rows = db.execute("""
        SELECT t.trade_id, t.ticker, t.trade_date, tr.return_7d, tr.entry_price
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND t.trade_date >= ? AND t.trade_date <= ?
          AND tr.return_7d IS NOT NULL
    """, (start, end)).fetchall()

    results = {}
    loaded = 0
    for tid, ticker, trade_date, ret_7d, entry_price in rows:
        if entry_price is None or entry_price <= 0:
            continue

        df = load_prices(ticker, price_cache)
        if df is None:
            # Use the DB return with no stop-loss info
            results[tid] = {
                "return_7d_no_stop": ret_7d,
                "min_intraday_dd": 0.0,
                "has_daily_data": False,
            }
            continue

        # Find entry point (T+1 after trade_date)
        entry_dt = pd.Timestamp(trade_date)
        mask = df.index > entry_dt
        if mask.sum() == 0:
            results[tid] = {
                "return_7d_no_stop": ret_7d,
                "min_intraday_dd": 0.0,
                "has_daily_data": False,
            }
            continue

        entry_idx_pos = np.argmax(mask)
        entry_price_actual = df.iloc[entry_idx_pos]["open"]
        if entry_price_actual <= 0 or np.isnan(entry_price_actual):
            results[tid] = {
                "return_7d_no_stop": ret_7d,
                "min_intraday_dd": 0.0,
                "has_daily_data": False,
            }
            continue

        # Walk 7 trading days, tracking intraday lows
        future = df.iloc[entry_idx_pos + 1:]
        min_dd = 0.0
        days = 0
        for _, row in future.iterrows():
            if row.name.weekday() >= 5:
                continue
            days += 1
            if days > 7:
                break
            low = row["low"]
            dd = (low - entry_price_actual) / entry_price_actual
            min_dd = min(min_dd, dd)

        results[tid] = {
            "return_7d_no_stop": ret_7d,
            "min_intraday_dd": float(min_dd),
            "has_daily_data": True,
        }
        loaded += 1
        if loaded % 2000 == 0:
            print(f"    Pre-computed {loaded} trades...")

    print(f"  Pre-computed stop-loss data for {loaded} trades ({len(results)} total)")
    return results


def apply_stop_loss(trade_sl_data: dict, stop_pct: float | None) -> tuple[float, str]:
    """Given pre-computed stop-loss data, return (adjusted_return, exit_reason)."""
    ret = trade_sl_data["return_7d_no_stop"]
    if stop_pct is None or not trade_sl_data.get("has_daily_data", False):
        return ret, "time"
    if trade_sl_data["min_intraday_dd"] <= stop_pct:
        return stop_pct, "stop_loss"
    return ret, "time"


def run_backtest(trades: list[dict], track_records: dict,
                 stop_loss_pct: float | None, use_confidence_sizing: bool,
                 sl_data: dict, confidence_fn=None, confidence_fn_kwargs=None) -> dict:
    """
    Run a full portfolio-level backtest with proper concurrent position handling.

    Trades are sized based on equity at entry time. P&L is applied when the
    7-trading-day hold closes. Multiple positions can be open simultaneously.
    Equity and drawdown are tracked on each calendar event (entry or exit).
    """
    HOLD_DAYS = 7  # trading days

    # Build events: each trade creates an entry event and an exit event
    # Exit date = entry date + ~10 calendar days (approx 7 trading days)
    events = []  # (date_str, event_type, trade_idx)
    valid_trades = []
    for trade in trades:
        tid = trade["trade_id"]
        if tid not in sl_data:
            continue
        idx = len(valid_trades)
        valid_trades.append(trade)
        entry_date = trade["trade_date"]
        # Approximate exit: 7 trading days ≈ 10 calendar days
        exit_dt = datetime.strptime(entry_date, "%Y-%m-%d") + timedelta(days=10)
        exit_date = exit_dt.strftime("%Y-%m-%d")
        events.append((entry_date, "entry", idx))
        events.append((exit_date, "exit", idx))

    # Sort events: entries before exits on same day, then by date
    events.sort(key=lambda e: (e[0], 0 if e[1] == "exit" else 1))

    equity = PORTFOLIO_VALUE
    peak_equity = equity
    max_dd = 0.0
    open_positions = {}  # trade_idx -> {position_value, conf_score, size_pct}
    results = []
    max_concurrent = 0

    for date_str, event_type, idx in events:
        trade = valid_trades[idx]
        tid = trade["trade_id"]

        if event_type == "entry":
            if confidence_fn is not None:
                conf_score, _ = confidence_fn(trade, **(confidence_fn_kwargs or {}))
            else:
                conf_score, _ = compute_confidence(trade, track_records)
            size_pct = confidence_to_size(conf_score) if use_confidence_sizing else 0.05
            position_value = equity * size_pct
            open_positions[idx] = {
                "position_value": position_value,
                "conf_score": conf_score,
                "size_pct": size_pct,
            }
            max_concurrent = max(max_concurrent, len(open_positions))

        elif event_type == "exit":
            if idx not in open_positions:
                continue
            pos = open_positions.pop(idx)
            pnl_pct, exit_reason = apply_stop_loss(sl_data[tid], stop_loss_pct)
            dollar_pnl = pos["position_value"] * pnl_pct
            equity += dollar_pnl

            results.append({
                "ticker": trade["ticker"],
                "trade_date": trade["trade_date"],
                "pnl_pct": pnl_pct,
                "exit_reason": exit_reason,
                "confidence": pos["conf_score"],
                "size_pct": pos["size_pct"],
                "dollar_pnl": dollar_pnl,
                "is_cluster": trade["is_cluster"],
                "insider_tier": track_records.get(trade["insider_id"], {}).get("tier", 0),
            })

            # Update drawdown tracking after each exit
            if equity > peak_equity:
                peak_equity = equity
            if peak_equity > 0:
                dd = (equity - peak_equity) / peak_equity
                max_dd = min(max_dd, dd)

    if not results:
        return {"n": 0}

    pnl_pcts = [r["pnl_pct"] for r in results]
    arr = np.array(pnl_pcts)
    n = len(arr)
    mean = np.mean(arr)
    std = np.std(arr, ddof=1)
    wins = np.sum(arr > 0)
    sharpe = (mean / std) * np.sqrt(min(252, n)) if std > 0 else 0

    return {
        "n": n,
        "mean_return": float(mean),
        "median_return": float(np.median(arr)),
        "win_rate": float(wins / n),
        "sharpe": float(sharpe),
        "max_portfolio_dd": float(max_dd),
        "final_equity": float(equity),
        "total_pnl": float(equity - PORTFOLIO_VALUE),
        "total_pnl_pct": float((equity - PORTFOLIO_VALUE) / PORTFOLIO_VALUE),
        "stops_hit": sum(1 for r in results if r["exit_reason"] == "stop_loss"),
        "avg_confidence": float(np.mean([r["confidence"] for r in results])),
        "max_concurrent_positions": max_concurrent,
    }


def build_cluster_flags(db, start, end):
    """Build cluster flags for buy trades."""
    rows = db.execute("""
        SELECT trade_id, insider_id, ticker, trade_date
        FROM trades WHERE trade_type = 'buy' AND trade_date >= ? AND trade_date <= ?
        ORDER BY ticker, trade_date
    """, (start, end)).fetchall()

    by_ticker = defaultdict(list)
    for tid, iid, ticker, td in rows:
        by_ticker[ticker].append((tid, iid, td))

    flags = {}
    for ticker, trades in by_ticker.items():
        parsed = [(tid, iid, datetime.strptime(td, "%Y-%m-%d")) for tid, iid, td in trades]
        n = len(parsed)
        for i in range(n):
            tid, iid, td = parsed[i]
            nearby = {iid}
            for j in range(i - 1, -1, -1):
                if (td - parsed[j][2]).days > 30:
                    break
                nearby.add(parsed[j][1])
            for j in range(i + 1, n):
                if (parsed[j][2] - td).days > 30:
                    break
                nearby.add(parsed[j][1])
            flags[tid] = len(nearby) >= 2
    return flags


def main():
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    import notify

    import sys as _sys
    # Force unbuffered output
    _sys.stdout = open(_sys.stdout.fileno(), mode='w', buffering=1)

    print("=" * 60)
    print("Phase 3: Shares Backtest — Confidence-Based Sizing")
    print("=" * 60)

    notify.phase_start("Phase 3: Shares Backtest",
                       "Running backtest grid: 2 sizing modes x 4 stop-loss levels x 2 signal filters")

    db = sqlite3.connect(str(DB_PATH))
    track_records = load_track_records()
    print(f"Loaded {len(track_records)} insider track records")

    # Build cluster flags
    print("Building cluster flags...")
    cluster_flags = build_cluster_flags(db, TRAIN_START, TRAIN_END)

    # Load all buy trades with returns (include signal_quality for v2)
    rows = db.execute("""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date, t.value,
               t.is_csuite, t.title_weight, t.title,
               tr.return_7d, tr.abnormal_7d, t.signal_quality
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'buy'
          AND t.trade_date >= ? AND t.trade_date <= ?
          AND tr.return_7d IS NOT NULL
    """, (TRAIN_START, TRAIN_END)).fetchall()

    all_trades = []
    for r in rows:
        all_trades.append({
            "trade_id": r[0],
            "insider_id": r[1],
            "ticker": r[2],
            "trade_date": r[3],
            "value": r[4],
            "is_csuite": r[5],
            "title_weight": r[6],
            "title": r[7],
            "is_cluster": cluster_flags.get(r[0], False),
            "signal_quality": r[10],
        })

    print(f"Total buy trades with returns: {len(all_trades)}")

    # Filter sets
    filter_sets = {
        "all_buys": all_trades,
        "cluster_only": [t for t in all_trades if t["is_cluster"]],
    }

    # Pre-compute stop-loss data (one-time expensive step)
    print("\nPre-computing stop-loss data from daily prices...")
    price_cache = {}
    sl_data = precompute_stop_loss_returns(db, TRAIN_START, TRAIN_END, price_cache)

    # Keep db open for v2 (PIT score lookups)
    # db.close()

    # ── V1 vs V2 comparison ──────────────────────────────────────────
    # Run a focused comparison: best config from v1 (cluster_only, no stop, confidence sizing)
    # with both v1 (global static scores) and v2 (PIT scores)

    stop_losses = [None, -0.10, -0.15, -0.20]
    sizing_modes = [True, False]  # confidence-based vs flat

    all_results = {}

    # ── V1: Original confidence scoring (global, static) ──
    print("\n" + "=" * 60)
    print("V1: Original confidence scoring (global, static track records)")
    print("=" * 60)

    total_configs = len(filter_sets) * len(stop_losses) * len(sizing_modes)
    done = 0
    for filter_name, trades in filter_sets.items():
        for stop_loss in stop_losses:
            for use_conf in sizing_modes:
                config_name = (
                    f"v1|{filter_name}|"
                    f"stop={'none' if stop_loss is None else f'{stop_loss:.0%}'}|"
                    f"sizing={'confidence' if use_conf else 'flat_5pct'}"
                )
                done += 1
                print(f"\n[{done}/{total_configs}] {config_name} (N={len(trades)})")

                result = run_backtest(trades, track_records, stop_loss, use_conf, sl_data)
                all_results[config_name] = result

                if result["n"] > 0:
                    print(f"  N={result['n']}, Sharpe={result['sharpe']:.2f}, "
                          f"WR={result['win_rate']:.1%}, Mean={result['mean_return']:+.2%}, "
                          f"MaxDD={result['max_portfolio_dd']:.2%}, "
                          f"Final=${result['final_equity']:,.0f}")

    # ── V2: PIT confidence scoring (per-ticker, point-in-time) ──
    print("\n" + "=" * 60)
    print("V2: PIT confidence scoring (per-ticker, point-in-time)")
    print("=" * 60)

    def _conf_v2_wrapper(trade, conn=None):
        return compute_confidence_v2(trade, conn)

    done = 0
    for filter_name, trades in filter_sets.items():
        for stop_loss in stop_losses:
            for use_conf in sizing_modes:
                config_name = (
                    f"v2|{filter_name}|"
                    f"stop={'none' if stop_loss is None else f'{stop_loss:.0%}'}|"
                    f"sizing={'confidence' if use_conf else 'flat_5pct'}"
                )
                done += 1
                print(f"\n[{done}/{total_configs}] {config_name} (N={len(trades)})")

                result = run_backtest(
                    trades, track_records, stop_loss, use_conf, sl_data,
                    confidence_fn=_conf_v2_wrapper,
                    confidence_fn_kwargs={"conn": db},
                )
                all_results[config_name] = result

                if result["n"] > 0:
                    print(f"  N={result['n']}, Sharpe={result['sharpe']:.2f}, "
                          f"WR={result['win_rate']:.1%}, Mean={result['mean_return']:+.2%}, "
                          f"MaxDD={result['max_portfolio_dd']:.2%}, "
                          f"Final=${result['final_equity']:,.0f}")

    db.close()

    # ── Head-to-head comparison ──
    print("\n" + "=" * 60)
    print("HEAD-TO-HEAD: V1 vs V2")
    print("=" * 60)

    # Compare matching configs
    v1_keys = sorted([k for k in all_results if k.startswith("v1|")])
    for v1_key in v1_keys:
        v2_key = "v2|" + v1_key[3:]
        if v2_key not in all_results:
            continue
        r1, r2 = all_results[v1_key], all_results[v2_key]
        if r1["n"] == 0 or r2["n"] == 0:
            continue
        config_short = v1_key[3:]
        delta_sharpe = r2["sharpe"] - r1["sharpe"]
        delta_wr = r2["win_rate"] - r1["win_rate"]
        print(f"  {config_short}")
        print(f"    V1: Sharpe={r1['sharpe']:.2f}  WR={r1['win_rate']:.1%}  "
              f"Mean={r1['mean_return']:+.2%}  Final=${r1['final_equity']:,.0f}")
        print(f"    V2: Sharpe={r2['sharpe']:.2f}  WR={r2['win_rate']:.1%}  "
              f"Mean={r2['mean_return']:+.2%}  Final=${r2['final_equity']:,.0f}")
        print(f"    Delta: Sharpe={delta_sharpe:+.2f}  WR={delta_wr:+.1%}")
        print()

    # Find best v1 and best v2
    best_v1 = max([k for k in all_results if k.startswith("v1|")],
                  key=lambda k: all_results[k].get("sharpe", 0))
    best_v2 = max([k for k in all_results if k.startswith("v2|")],
                  key=lambda k: all_results[k].get("sharpe", 0))
    bv1, bv2 = all_results[best_v1], all_results[best_v2]

    print("BEST V1:", best_v1)
    print(f"  Sharpe={bv1['sharpe']:.2f}  WR={bv1['win_rate']:.1%}  "
          f"Mean={bv1['mean_return']:+.2%}  MaxDD={bv1['max_portfolio_dd']:.2%}  "
          f"Final=${bv1['final_equity']:,.0f}")
    print("BEST V2:", best_v2)
    print(f"  Sharpe={bv2['sharpe']:.2f}  WR={bv2['win_rate']:.1%}  "
          f"Mean={bv2['mean_return']:+.2%}  MaxDD={bv2['max_portfolio_dd']:.2%}  "
          f"Final=${bv2['final_equity']:,.0f}")
    print("=" * 60)

    # Save results
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_DIR / "shares_backtest_results.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    # Format summary table
    lines = ["# Shares Backtest: V1 vs V2 Comparison", "",
             f"{'Config':<60} | {'N':>5} | {'Sharpe':>7} | {'WR':>6} | {'Mean':>8} | {'MaxDD':>7} | {'Stops':>5} | {'Final$':>10}",
             "-" * 125]
    for name, r in sorted(all_results.items(), key=lambda x: -x[1].get("sharpe", 0)):
        if r["n"] == 0:
            continue
        lines.append(
            f"{name:<60} | {r['n']:>5} | {r['sharpe']:>7.2f} | "
            f"{r['win_rate']:>5.1%} | {r['mean_return']:>+7.2%} | "
            f"{r['max_portfolio_dd']:>6.2%} | {r['stops_hit']:>5} | "
            f"${r['final_equity']:>9,.0f}"
        )
    summary = "\n".join(lines)
    print("\n" + summary)

    with open(OUTPUT_DIR / "shares_backtest_summary.txt", "w") as f:
        f.write(summary)

    notify.phase_end("Phase 3: Shares Backtest",
                     f"V1 vs V2 comparison complete.\n\n"
                     f"Best V1: {best_v1}\n"
                     f"  Sharpe={bv1['sharpe']:.2f}, WR={bv1['win_rate']:.1%}\n"
                     f"Best V2: {best_v2}\n"
                     f"  Sharpe={bv2['sharpe']:.2f}, WR={bv2['win_rate']:.1%}")

    return all_results


if __name__ == "__main__":
    main()
