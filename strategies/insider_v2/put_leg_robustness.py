#!/usr/bin/env python3
"""
Put Leg Robustness — Skeptic Demands
-------------------------------------
1. Alternative walk-forward split (train 2020-2023, test 2024-2025)
2. Parameter perturbation: test ALL 1-step neighbors of the best config
3. Multi-fold cross-validation (3 non-overlapping folds)

Uses the same per-trade simulation from put_leg_analysis.py
"""

from __future__ import annotations

import csv
import json
import math
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, date
from itertools import product

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.join(os.path.dirname(os.path.dirname(SCRIPT_DIR)), "pipelines", "insider_study")
sys.path.insert(0, PIPELINE_DIR)

from theta_client import get_eod_date, find_nearest_expiration, find_nearest_strike, add_trading_days

SELL_EVENTS_CSV = os.path.join(PIPELINE_DIR, "data", "results_sells_7d.csv")
THETA_DB = os.path.join(PIPELINE_DIR, "data", "theta_cache.db")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(SCRIPT_DIR)), "reports", "insider_v2")

STRIKES_PUT = {"5pct_itm": 1.05, "atm": 1.00, "5pct_otm": 0.95, "10pct_otm": 0.90}
HOLD_DTE_MAP = {7: (14, 21), 14: (28, 45), 30: (50, 60), 60: (90, 120)}
NOTIONAL_PER_TRADE = 1000
COMMISSION_PER_CONTRACT = 0.65

# Best config from put_leg_analysis.py
BEST = {"ins": 2, "val": 5_000_000, "qual": 2.14, "spread": 0.10,
        "hold": 7, "dte": "tight", "strike": "5pct_itm", "stop": -0.25}

# 1-step perturbations for each parameter
PERTURBATIONS = {
    "ins":    [2, 3],           # loosen/tighten cluster
    "val":    [1_000_000, 5_000_000],  # loosen value
    "qual":   [1.50, 2.14],    # loosen quality
    "spread": [0.10, 0.20],    # widen spread tolerance
    "hold":   [7, 14],         # longer hold
    "dte":    ["tight", "comfortable"],  # wider DTE
    "strike": ["5pct_itm", "atm"],  # different strike
    "stop":   [-0.25, -0.50],  # wider stop
}

# Walk-forward splits
SPLITS = {
    "primary":     (date(2022, 12, 31), "train ≤2022, test ≥2023"),
    "alternative":  (date(2023, 12, 31), "train ≤2023, test ≥2024"),
}

# 3-fold CV
FOLDS = [
    ("fold1", date(2020, 1, 1), date(2021, 12, 31)),
    ("fold2", date(2022, 1, 1), date(2023, 12, 31)),
    ("fold3", date(2024, 1, 1), date(2025, 12, 31)),
]


def parse_list(raw, field, convert=str):
    if raw is None or raw == "__NONE__":
        return []
    result = []
    for r in raw:
        try:
            s = r.get(field, "").strip().strip('"') if isinstance(r, dict) else str(r).strip().strip('"')
            result.append(convert(s))
        except (ValueError, KeyError):
            continue
    return sorted(result)


def get_float(row, key):
    try:
        val = float(str(row.get(key, "0")).strip().strip('"'))
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def resolve_contract(cache, ticker, entry_date, entry_price, hold_days, dte_type, strike_type):
    expirations = parse_list(cache.get(f"opt_exp|{ticker}"), "expiration",
                             lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    if not expirations:
        return None
    tight_dte, comf_dte = HOLD_DTE_MAP[hold_days]
    target_dte = tight_dte if dte_type == "tight" else comf_dte
    matched_exp = find_nearest_expiration(expirations, entry_date, target_dte)
    if matched_exp is None:
        return None
    exit_date = add_trading_days(entry_date, hold_days)
    if exit_date >= matched_exp:
        return None
    exp_str = matched_exp.strftime("%Y-%m-%d")
    strikes = parse_list(cache.get(f"opt_strikes|{ticker}|{exp_str}"), "strike", float)
    if not strikes:
        return None
    real_strike = find_nearest_strike(strikes, entry_price * STRIKES_PUT[strike_type])
    if real_strike is None:
        return None
    ps = (entry_date - timedelta(days=2)).strftime("%Y-%m-%d")
    pe = (exit_date + timedelta(days=5)).strftime("%Y-%m-%d")
    cache_key = f"opt_eod_daily|{ticker}|{exp_str}|{real_strike}|P|{ps}|{pe}"
    return {"exit_date": exit_date, "cache_key": cache_key}


def parse_eod_rows(eod_rows):
    parsed = []
    for row in eod_rows:
        d = get_eod_date(row)
        if d is None:
            continue
        parsed.append((d, get_float(row, "bid"), get_float(row, "ask"),
                        get_float(row, "low"), get_float(row, "close"),
                        int(row.get("volume", 0) or 0)))
    parsed.sort(key=lambda x: x[0])
    return parsed


def simulate_trade(parsed_rows, entry_date, exit_date, stop_loss):
    if not parsed_rows:
        return None
    entry_row = None
    for row in parsed_rows:
        if row[0] >= entry_date:
            entry_row = row
            break
    if entry_row is None:
        return None
    d, bid, ask, low, close, volume = entry_row
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    spread_pct = (ask - bid) / mid
    entry_px = ask
    if entry_px <= 0:
        return None
    num_contracts = int(NOTIONAL_PER_TRADE // (entry_px * 100))
    if num_contracts <= 0:
        return None
    commission = num_contracts * COMMISSION_PER_CONTRACT * 2
    stop_hit = False
    final_px = None
    if stop_loss is not None:
        stop_level = entry_px * (1.0 + stop_loss)
        for row in parsed_rows:
            rd = row[0]
            if rd <= d or rd > exit_date:
                continue
            if row[3] is not None and row[3] <= stop_level:
                final_px = stop_level
                stop_hit = True
                break
    if not stop_hit:
        exit_row = None
        for row in parsed_rows:
            if row[0] >= exit_date:
                exit_row = row
                break
        if exit_row is None and parsed_rows:
            exit_row = parsed_rows[-1]
        if exit_row is None:
            return None
        final_px = exit_row[1]  # bid
        if final_px is None or final_px <= 0:
            final_px = exit_row[4]  # close
            if final_px is None or final_px <= 0:
                return None
    pct_ret = (final_px - entry_px) / entry_px
    dollar_pnl = (final_px - entry_px) * num_contracts * 100 - commission
    return {"pct_ret": pct_ret, "dollar_pnl": dollar_pnl, "spread_pct": spread_pct,
            "entry_date": d, "stop_hit": stop_hit}


def calc_sharpe(trades, min_n=10):
    if len(trades) < min_n:
        return None, len(trades)
    rets = np.array([t["pct_ret"] for t in trades])
    n = len(rets)
    mean_r = float(np.mean(rets))
    std_r = float(np.std(rets, ddof=1))
    if std_r <= 0:
        return 0.0, n
    sharpe = (mean_r / std_r) * np.sqrt(min(252, n))
    wr = float(np.sum(rets > 0) / n)
    return round(float(sharpe), 2), n


def run_config(cfg, events, cache_dict):
    """Simulate all trades for a config, return list of trade dicts."""
    trades = []
    for ev in events:
        if (ev["_n_insiders"] < cfg["ins"] or ev["_total_value"] < cfg["val"]
                or ev["_quality_score"] < cfg["qual"]):
            continue
        c = resolve_contract(cache_dict, ev["_ticker"], ev["_entry_date"],
                             ev["_entry_price"], cfg["hold"], cfg["dte"], cfg["strike"])
        if c is None:
            continue
        raw = cache_dict.get(c["cache_key"])
        if raw is None or raw == "__NONE__":
            continue
        rows = parse_eod_rows(raw)
        if not rows:
            continue
        # Spread filter check
        entry_row = None
        for row in rows:
            if row[0] >= ev["_entry_date"]:
                entry_row = row
                break
        if entry_row is None:
            continue
        _, bid, ask, _, _, _ = entry_row
        if bid is None or ask is None:
            continue
        mid_val = (bid + ask) / 2.0
        if mid_val <= 0:
            continue
        if (ask - bid) / mid_val > cfg["spread"]:
            continue

        result = simulate_trade(rows, ev["_entry_date"], c["exit_date"], cfg["stop"])
        if result is not None:
            trades.append(result)
    return trades


def main():
    print("=" * 80)
    print("PUT LEG ROBUSTNESS — Skeptic Demands")
    print("=" * 80)

    # Load events
    print("\nLoading sell events...")
    with open(SELL_EVENTS_CSV) as f:
        raw_events = list(csv.DictReader(f))
    events = [e for e in raw_events if e.get("is_cluster") == "True"]
    for e in events:
        e["_ticker"] = e["ticker"]
        e["_entry_date"] = datetime.strptime(e["entry_date"], "%Y-%m-%d").date()
        e["_entry_price"] = float(e["entry_price"])
        e["_n_insiders"] = int(e["n_insiders"])
        e["_total_value"] = float(e["total_value"])
        e["_quality_score"] = float(e["quality_score"])
    print(f"  {len(events)} cluster sell events")

    # Load theta cache
    print(f"Loading theta_cache.db...")
    t0 = time.monotonic()
    conn = sqlite3.connect(THETA_DB)
    cur = conn.cursor()
    cur.execute("SELECT cache_key, response_json FROM cache")
    cache_dict = {}
    for key, val in cur:
        cache_dict[key] = json.loads(val) if val else None
    conn.close()
    print(f"  {len(cache_dict)} entries in {time.monotonic() - t0:.1f}s")

    results = {"generated": datetime.now().isoformat()}

    # ═══════════════════════════════════════════════════════════════
    # TEST 1: Alternative Walk-Forward Splits
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("TEST 1: WALK-FORWARD — Multiple Splits (Best Config)")
    print("=" * 80)

    best_trades = run_config(BEST, events, cache_dict)
    print(f"  Best config total trades: {len(best_trades)}")

    split_results = {}
    for split_name, (cutoff, desc) in SPLITS.items():
        train = [t for t in best_trades if t["entry_date"] <= cutoff]
        test = [t for t in best_trades if t["entry_date"] > cutoff]
        train_sh, train_n = calc_sharpe(train)
        test_sh, test_n = calc_sharpe(test)
        deg = round((1 - test_sh / train_sh) * 100, 1) if train_sh and test_sh and train_sh > 0 else None

        split_results[split_name] = {
            "description": desc,
            "train": {"sharpe": train_sh, "n": train_n},
            "test": {"sharpe": test_sh, "n": test_n},
            "degradation_pct": deg,
        }
        print(f"\n  {split_name} ({desc}):")
        print(f"    Train: N={train_n}, Sharpe={train_sh}")
        print(f"    Test:  N={test_n}, Sharpe={test_sh}")
        if deg is not None:
            print(f"    Degradation: {deg:+.1f}%")

    results["walk_forward_splits"] = split_results

    # ═══════════════════════════════════════════════════════════════
    # TEST 2: Parameter Perturbation (1-step neighbors)
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("TEST 2: PARAMETER PERTURBATION (1-step neighbors)")
    print("=" * 80)

    # Generate all 1-step perturbations from BEST
    perturbation_configs = []
    for param, values in PERTURBATIONS.items():
        for val in values:
            cfg = dict(BEST)
            if cfg[param] == val:
                continue  # skip the best config itself
            cfg[param] = val
            label = f"{param}={val}"
            perturbation_configs.append((label, cfg))

    # Add the best config itself
    perturbation_configs.insert(0, ("BEST (baseline)", dict(BEST)))

    print(f"  Testing {len(perturbation_configs)} configs (1 best + {len(perturbation_configs)-1} perturbations)")
    print(f"\n  {'Config':<35} {'All Sh':>7} {'All N':>6} | {'Train Sh':>8} {'Test Sh':>8} {'Test N':>6} {'Deg':>7}")
    print(f"  {'-'*35} {'-'*7} {'-'*6} | {'-'*8} {'-'*8} {'-'*6} {'-'*7}")

    perturbation_results = {}
    # Use primary split for perturbation test
    primary_cutoff = SPLITS["primary"][0]

    for label, cfg in perturbation_configs:
        trades = run_config(cfg, events, cache_dict)
        all_sh, all_n = calc_sharpe(trades)

        train = [t for t in trades if t["entry_date"] <= primary_cutoff]
        test = [t for t in trades if t["entry_date"] > primary_cutoff]
        train_sh, train_n = calc_sharpe(train)
        test_sh, test_n = calc_sharpe(test)
        deg = round((1 - test_sh / train_sh) * 100, 1) if train_sh and test_sh and train_sh > 0 else None

        perturbation_results[label] = {
            "config": cfg,
            "all": {"sharpe": all_sh, "n": all_n},
            "train": {"sharpe": train_sh, "n": train_n},
            "test": {"sharpe": test_sh, "n": test_n},
            "degradation_pct": deg,
        }

        all_str = f"{all_sh:>7.2f}" if all_sh is not None else "   N/A"
        train_str = f"{train_sh:>8.2f}" if train_sh is not None else "     N/A"
        test_str = f"{test_sh:>8.2f}" if test_sh is not None else "     N/A"
        deg_str = f"{deg:>+6.1f}%" if deg is not None else "    N/A"
        marker = " <<<" if label == "BEST (baseline)" else ""
        oos_pass = " ✓" if test_sh is not None and test_sh > 0.5 else ""
        print(f"  {label:<35} {all_str} {all_n:>6} | {train_str} {test_str} {test_n:>6} {deg_str}{oos_pass}{marker}")

    results["perturbation_test"] = perturbation_results

    # Count how many perturbations have positive OOS Sharpe
    pos_oos = sum(1 for k, v in perturbation_results.items()
                  if k != "BEST (baseline)" and v["test"]["sharpe"] is not None and v["test"]["sharpe"] > 0)
    viable_oos = sum(1 for k, v in perturbation_results.items()
                     if k != "BEST (baseline)" and v["test"]["sharpe"] is not None and v["test"]["sharpe"] > 0.5)
    total_perturb = len(perturbation_results) - 1

    print(f"\n  Positive OOS Sharpe: {pos_oos}/{total_perturb}")
    print(f"  OOS Sharpe > 0.5:   {viable_oos}/{total_perturb}")

    # ═══════════════════════════════════════════════════════════════
    # TEST 3: 3-Fold Cross-Validation
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("TEST 3: 3-FOLD CROSS-VALIDATION (Best Config)")
    print("=" * 80)

    fold_results = {}
    fold_sharpes = []
    for fold_name, fold_start, fold_end in FOLDS:
        fold_trades = [t for t in best_trades
                       if fold_start <= t["entry_date"] <= fold_end]
        sh, n = calc_sharpe(fold_trades, min_n=5)
        fold_results[fold_name] = {"start": fold_start.isoformat(), "end": fold_end.isoformat(),
                                    "sharpe": sh, "n": n}
        if sh is not None:
            fold_sharpes.append(sh)
        sh_str = f"{sh:.2f}" if sh is not None else "N/A"
        print(f"  {fold_name} ({fold_start} to {fold_end}): N={n}, Sharpe={sh_str}")

    if fold_sharpes:
        avg_cv = np.mean(fold_sharpes)
        std_cv = np.std(fold_sharpes, ddof=1) if len(fold_sharpes) > 1 else 0
        min_cv = min(fold_sharpes)
        print(f"\n  CV Average Sharpe: {avg_cv:.2f} (std={std_cv:.2f}, min={min_cv:.2f})")
        fold_results["summary"] = {
            "avg_sharpe": round(float(avg_cv), 2),
            "std_sharpe": round(float(std_cv), 2),
            "min_sharpe": round(float(min_cv), 2),
            "all_positive": all(s > 0 for s in fold_sharpes),
        }

    results["cross_validation"] = fold_results

    # ═══════════════════════════════════════════════════════════════
    # TEST 4: Grid Search Statistics (skeptic asked for this)
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("TEST 4: GRID SEARCH STATISTICS")
    print("=" * 80)

    grid_csv = os.path.join(PIPELINE_DIR, "data", "grid_search_results_sells.csv")
    with open(grid_csv) as f:
        grid = list(csv.DictReader(f))
    total_configs = len(grid)
    with_trades = sum(1 for r in grid if int(r["n_trades"]) > 0)
    pos_sharpe = sum(1 for r in grid if float(r["sharpe"]) > 0)
    sharpe_gt1 = sum(1 for r in grid if float(r["sharpe"]) > 1.0)
    all_sharpes = [float(r["sharpe"]) for r in grid if int(r["n_trades"]) >= 50]
    median_sharpe = float(np.median(all_sharpes)) if all_sharpes else 0

    grid_stats = {
        "total_configs_tested": total_configs,
        "configs_with_trades": with_trades,
        "positive_sharpe": pos_sharpe,
        "sharpe_gt_1": sharpe_gt1,
        "median_sharpe_n50plus": round(median_sharpe, 3),
        "pct_positive": round(pos_sharpe / total_configs * 100, 1),
    }
    results["grid_search_stats"] = grid_stats

    print(f"  Total configs tested: {total_configs}")
    print(f"  Positive Sharpe: {pos_sharpe} ({pos_sharpe/total_configs:.0%})")
    print(f"  Sharpe > 1.0: {sharpe_gt1} ({sharpe_gt1/total_configs:.0%})")
    print(f"  Median Sharpe (N≥50): {median_sharpe:.3f}")

    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("SUMMARY — SKEPTIC DEMANDS ADDRESSED")
    print("=" * 80)

    print(f"\n1. ALTERNATIVE WALK-FORWARD:")
    for name, r in split_results.items():
        print(f"   {name}: test Sharpe={r['test']['sharpe']}, N={r['test']['n']}")

    print(f"\n2. PARAMETER PERTURBATIONS:")
    print(f"   {pos_oos}/{total_perturb} perturbations have positive OOS Sharpe")
    print(f"   {viable_oos}/{total_perturb} perturbations have OOS Sharpe > 0.5")

    print(f"\n3. CROSS-VALIDATION:")
    if fold_sharpes:
        print(f"   Avg fold Sharpe: {np.mean(fold_sharpes):.2f}")
        print(f"   All folds positive: {all(s > 0 for s in fold_sharpes)}")

    print(f"\n4. GRID SEARCH:")
    print(f"   {pos_sharpe}/{total_configs} configs positive ({pos_sharpe/total_configs:.0%})")
    print(f"   Median Sharpe: {median_sharpe:.3f}")

    # Save
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "put_leg_robustness.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
