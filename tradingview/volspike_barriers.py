#!/usr/bin/env python3
"""
VolSpike first-touch (triple-barrier) study.

Detects the signal on 5-min bars (reusing volspike_study) and, for each signal,
walks forward on 1-MINUTE bars to find which barrier is touched first:
    target  = +T% in the signal's direction   (momentum: with the spike)
    stop    = -L% against
    time    = session close (0DTE max hold) -> exit at close, signed P&L
Sweeps a grid of (T, L) and reports hit-rate + per-trade expectancy (%).

Running max-favorable / max-adverse excursions are monotonic, so first-touch
bars are found by searchsorted. If target and stop first cross in the SAME
1-min bar, that's genuine intrabar ambiguity -> counted as stop (pessimistic);
the optimistic bound counts it as target. Both are printed.
"""
from __future__ import annotations
import glob, os
import numpy as np
import pandas as pd

from volspike_study import resample_5m, day_signals, DATA_DIR

TARGETS = [0.2, 0.3, 0.4, 0.5, 0.75, 1.0]
STOPS   = [0.2, 0.3, 0.4, 0.5, 0.75, 1.0]


def collect_paths(mode: str):
    """Return list of per-signal (fav_run, adv_run, time_pnl, n_bars, direction)."""
    paths = []
    for f in sorted(glob.glob(f"{DATA_DIR}/*.parquet")):
        df1 = pd.read_parquet(f)
        if df1.empty:
            continue
        df5 = resample_5m(df1)
        if len(df5) < 5:
            continue
        sig = day_signals(df5)
        fresh_ts = sig.index[sig["fresh"].values]
        for T in fresh_ts:
            s = 1 if sig.loc[T, "up"] else -1
            if mode == "reversion":
                s = -s
            entry = float(df5.loc[T, "close"])
            fwd = df1.loc[df1.index >= (T + pd.Timedelta(minutes=5))]
            if len(fwd) == 0:
                continue
            hi = fwd["high"].values.astype(float)
            lo = fwd["low"].values.astype(float)
            cl = fwd["close"].values.astype(float)
            if s == 1:
                fav_run = (np.maximum.accumulate(hi) - entry) / entry * 100.0
                adv_run = (entry - np.minimum.accumulate(lo)) / entry * 100.0
                time_pnl = (cl[-1] - entry) / entry * 100.0
            else:
                fav_run = (entry - np.minimum.accumulate(lo)) / entry * 100.0
                adv_run = (np.maximum.accumulate(hi) - entry) / entry * 100.0
                time_pnl = (entry - cl[-1]) / entry * 100.0
            paths.append((fav_run, adv_run, time_pnl, len(fwd), "up" if (sig.loc[T,"up"]) else "dn"))
    return paths


def evaluate(paths, T, L, optimistic=False):
    """Return dict of stats for a (T,L) pair across all signals."""
    pnls, labels, bars = [], [], []
    for fav_run, adv_run, time_pnl, n, _dir in paths:
        k_tp = np.searchsorted(fav_run, T)          # first bar fav_run >= T (monotonic)
        k_sl = np.searchsorted(adv_run, L)
        hit_tp = k_tp < len(fav_run)
        hit_sl = k_sl < len(adv_run)
        if hit_tp and hit_sl:
            if k_tp < k_sl:
                out, lab, b = T, "tp", k_tp
            elif k_sl < k_tp:
                out, lab, b = -L, "sl", k_sl
            else:  # same bar — ambiguous
                out, lab, b = (T, "tp", k_tp) if optimistic else (-L, "sl", k_sl)
        elif hit_tp:
            out, lab, b = T, "tp", k_tp
        elif hit_sl:
            out, lab, b = -L, "sl", k_sl
        else:
            out, lab, b = time_pnl, "time", n
        pnls.append(out); labels.append(lab); bars.append(b)
    pnls = np.array(pnls); labels = np.array(labels)
    return {
        "n": len(pnls),
        "exp": pnls.mean(),
        "win": (pnls > 0).mean() * 100,
        "p_tp": (labels == "tp").mean() * 100,
        "p_sl": (labels == "sl").mean() * 100,
        "p_time": (labels == "time").mean() * 100,
        "med_bars": float(np.median(bars)),
    }


def grid(paths, field, optimistic=False):
    df = pd.DataFrame(index=[f"T={t}" for t in TARGETS], columns=[f"L={l}" for l in STOPS], dtype=float)
    for t in TARGETS:
        for l in STOPS:
            df.loc[f"T={t}", f"L={l}"] = evaluate(paths, t, l, optimistic)[field]
    return df.astype(float).round(3)


def main():
    mom = collect_paths("momentum")
    print(f"signals: {len(mom)}\n")

    print("================ MOMENTUM (trade WITH the spike) ================")
    print("\nExpectancy % per trade  (pessimistic: ties=stop)   rows=target, cols=stop")
    print(grid(mom, "exp", optimistic=False).to_string())
    print("\nWin rate %  (P&L > 0, pessimistic)")
    print(grid(mom, "win", optimistic=False).to_string())

    print("\nDirectional-edge check — symmetric barriers T==L, P(target first):")
    for x in TARGETS:
        st = evaluate(mom, x, x, optimistic=False)
        sto = evaluate(mom, x, x, optimistic=True)
        print(f"  +/-{x:>4}% : P(tp) pess={st['p_tp']:4.1f}%  opt={sto['p_tp']:4.1f}%   (50% = no edge)   med_bars_to_hit={st['med_bars']:.0f}")

    # best pessimistic expectancy cell
    g = grid(mom, "exp", optimistic=False)
    flat = g.stack()
    bt, bl = flat.idxmax()
    T = float(bt.split("=")[1]); L = float(bl.split("=")[1])
    sp = evaluate(mom, T, L, False); so = evaluate(mom, T, L, True)
    print(f"\nBest pessimistic cell: target {T}% / stop {L}%")
    print(f"  expectancy: pess {sp['exp']:+.3f}% .. opt {so['exp']:+.3f}% per trade")
    print(f"  win {sp['win']:.1f}%  | tp {sp['p_tp']:.0f}% / sl {sp['p_sl']:.0f}% / time {sp['p_time']:.0f}%  | median {sp['med_bars']:.0f} min to resolve")

    print("\n================ REVERSION (fade the spike) ================")
    rev = collect_paths("reversion")
    gr = grid(rev, "exp", optimistic=False)
    flat = gr.stack(); bt, bl = flat.idxmax()
    T = float(bt.split("=")[1]); L = float(bl.split("=")[1])
    sp = evaluate(rev, T, L, False); so = evaluate(rev, T, L, True)
    print(f"best cell: target {T}% / stop {L}%  expectancy pess {sp['exp']:+.3f}% .. opt {so['exp']:+.3f}%  win {sp['win']:.1f}%")
    print("\n(Expectancy is UNDERLYING %. 0DTE option P&L differs — theta + spread make")
    print(" the real bar higher; a near-zero underlying edge is a losing option trade.)")


if __name__ == "__main__":
    main()
