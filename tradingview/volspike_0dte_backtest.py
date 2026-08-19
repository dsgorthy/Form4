#!/usr/bin/env python3
"""
VolSpike 0DTE options backtest using REAL ThetaData v3 intraday prices.

Pipeline:
  1. Pull SPY 5-min bars (ThetaData stock) over the range, detect VolSpike signals
     (same logic as tradingview/volspike.pine: % path or $ path + volume gate,
     inside 10:30-15:30 ET, dedup, session-running-average volume baseline).
  2. For each signal, buy the SAME-DAY-EXPIRY (0DTE) ATM option in the signal's
     direction (momentum: up->call, down->put; reversion flips it), entering at the
     signal bar's option close.
  3. Manage with premium-based TP/SL swept over a grid, time-stop at session close.
     First-touch on the option's 5-min high/low (ties=stop, pessimistic). A per-side
     $ slippage models the bid/ask you cross.
  4. Report real expectancy / win-rate grids -> the TP/SL that actually works.

Requires ThetaTerminal running on 127.0.0.1:25503 (Options PROFESSIONAL).
Pulls are cached under tradingview/theta_cache/ so re-runs are instant.
"""
from __future__ import annotations
import os, io, time
import numpy as np
import pandas as pd
import requests

B = "http://127.0.0.1:25503"
CACHE = "tradingview/theta_cache"
os.makedirs(CACHE, exist_ok=True)

# ---- signal params (mirror the pine) ----
PCT_THR, DOL_THR = 0.5, 2.0
VOL_MULT, VOL_MULT_LT = 1.5, 1.2
WIN_START, WIN_END = "10:30", "15:30"          # ET (= 07:30-12:30 PT)

# ---- backtest config ----
START_DATE, END_DATE = "20230601", "20260612"
SLIP_PER_SIDE = 0.05                            # $ per contract crossed each side (spread proxy)
TARGETS = [25, 50, 75, 100, 150, 200]          # % premium gain
STOPS   = [25, 50, 75, 100]                    # % premium loss

# ───────────────────────── ThetaData v3 helpers ─────────────────────────

def _get_csv(path, params, cache_key):
    fp = os.path.join(CACHE, cache_key)
    if os.path.exists(fp):
        try:
            return pd.read_csv(fp)
        except Exception:
            pass
    for attempt in range(4):
        try:
            r = requests.get(B + path, params=params, timeout=90)
        except requests.RequestException:
            time.sleep(2 ** attempt); continue
        if r.status_code == 200 and r.text[:1] not in ("<", "{", ""):
            try:
                df = pd.read_csv(io.StringIO(r.text))
            except Exception:
                df = pd.DataFrame()
            df.to_csv(fp, index=False)
            return df
        if r.status_code in (429,) or r.status_code >= 500:
            time.sleep(2 ** attempt); continue
        # no-data / error -> cache an empty frame so we don't refetch
        pd.DataFrame().to_csv(fp, index=False)
        return pd.DataFrame()
    return pd.DataFrame()


def stock_5m(start, end):
    return _get_csv("/v3/stock/history/ohlc",
                    {"symbol": "SPY", "start_date": start, "end_date": end, "interval": "5m", "format": "csv"},
                    f"spy_5m_{start}_{end}.csv")


def option_5m(exp, strike, right, date):
    return _get_csv("/v3/option/history/ohlc",
                    {"symbol": "SPY", "expiration": exp, "strike": f"{strike:.2f}",
                     "right": right, "date": date, "interval": "5m", "format": "csv"},
                    f"opt_SPY_{exp}_{int(round(strike))}_{right}_{date}.csv")


def month_ranges(start, end):
    s = pd.Timestamp(start); e = pd.Timestamp(end)
    out = []
    cur = s.replace(day=1)
    while cur <= e:
        nxt = (cur + pd.offsets.MonthEnd(1))
        out.append((max(s, cur).strftime("%Y%m%d"), min(e, nxt).strftime("%Y%m%d")))
        cur = nxt + pd.Timedelta(days=1)
    return out

# ───────────────────────── signal detection ─────────────────────────

def load_spy_5m():
    frames = []
    for a, b in month_ranges(START_DATE, END_DATE):
        df = stock_5m(a, b)
        if not df.empty:
            frames.append(df)
    spy = pd.concat(frames, ignore_index=True)
    spy["timestamp"] = pd.to_datetime(spy["timestamp"])      # naive ET
    spy = spy[(spy["volume"] > 0)]                            # drop empty 16:00 prints
    spy = spy.set_index("timestamp").sort_index()
    spy = spy.between_time("09:30", "15:55")
    return spy


def detect_signals(spy):
    sigs = []
    for date, df in spy.groupby(spy.index.date):
        c = df["close"]
        pct = c.pct_change() * 100.0
        dol = c.diff()
        n = np.arange(1, len(df) + 1)
        avg_bar = df["volume"].cumsum() / n
        vrto = df["volume"] / avg_bar
        up = ((pct >= PCT_THR) & (vrto >= VOL_MULT)) | ((dol >= DOL_THR) & (vrto >= VOL_MULT_LT))
        dn = ((pct <= -PCT_THR) & (vrto >= VOL_MULT)) | ((dol <= -DOL_THR) & (vrto >= VOL_MULT_LT))
        t = df.index.time
        inwin = (t >= pd.to_datetime(WIN_START).time()) & (t < pd.to_datetime(WIN_END).time())
        gated = (up | dn) & inwin
        fresh = gated & ~np.r_[False, gated.values[:-1]]
        for ts in df.index[fresh.values]:
            sigs.append({"ts": ts, "dir": "up" if up.loc[ts] else "dn",
                         "spy": float(c.loc[ts]), "vol_x": float(vrto.loc[ts])})
    return pd.DataFrame(sigs)

# ───────────────────────── option simulation ─────────────────────────

def option_path_after_entry(sig, direction):
    """Return (entry_premium, fwd_high, fwd_low, last_close) for the 0DTE ATM option, or None."""
    date = sig["ts"].strftime("%Y%m%d")
    is_up = sig["dir"] == "up"
    if direction == "momentum":
        right = "call" if is_up else "put"
    else:
        right = "put" if is_up else "call"
    strike = round(sig["spy"])
    opt = None
    for k in (0, 1, -1, 2, -2):                # try ATM then nearest $1
        df = option_5m(date, strike + k, right, date)
        if not df.empty and df["close"].astype(float).gt(0).any():
            opt = df; break
    if opt is None:
        return None
    opt = opt.copy()
    opt["timestamp"] = pd.to_datetime(opt["timestamp"])
    opt = opt.set_index("timestamp").sort_index()
    opt = opt[~opt.index.duplicated(keep="last")]
    if sig["ts"] not in opt.index:
        return None
    entry = float(opt.loc[sig["ts"], "close"])
    if entry <= 0.05:                          # skip junk entries
        return None
    fwd = opt.loc[opt.index > sig["ts"]]
    if len(fwd) == 0:
        return None
    return entry, fwd["high"].astype(float).values, fwd["low"].astype(float).values, float(fwd["close"].values[-1])


def simulate(entry, hi, lo, last, T, L):
    """Premium-based TP/SL first-touch. Returns realized P&L % after slippage."""
    tp = entry * (1 + T / 100.0)
    sl = entry * (1 - L / 100.0)
    eff_entry = entry + SLIP_PER_SIDE
    for k in range(len(hi)):
        hit_tp = hi[k] >= tp
        hit_sl = lo[k] <= sl
        if hit_sl:                              # pessimistic: ties -> stop
            return (sl - SLIP_PER_SIDE - eff_entry) / eff_entry * 100.0, "sl"
        if hit_tp:
            return (tp - SLIP_PER_SIDE - eff_entry) / eff_entry * 100.0, "tp"
    return (max(last, 0.0) - SLIP_PER_SIDE - eff_entry) / eff_entry * 100.0, "time"

# ───────────────────────── run ─────────────────────────

def run(direction):
    spy = load_spy_5m()
    sigs = detect_signals(spy)
    print(f"\n[{direction.upper()}]  SPY 5m bars: {len(spy):,}  | signals: {len(sigs)} "
          f"({(sigs['dir']=='up').sum()} up / {(sigs['dir']=='dn').sum()} dn)")
    paths, raw = [], []
    for _, sig in sigs.iterrows():
        p = option_path_after_entry(sig, direction)
        if p is None:
            continue
        entry, hi, lo, last = p
        paths.append(p)
        mfe = (hi.max() - entry) / entry * 100.0
        mae = (entry - lo.min()) / entry * 100.0
        raw.append({"entry": entry, "mfe": mfe, "mae": mae,
                    "eod": (last - entry) / entry * 100.0})
    rawdf = pd.DataFrame(raw)
    print(f"  tradable options: {len(paths)}  | avg entry premium ${rawdf['entry'].mean():.2f}")
    print(f"  RAW premium excursions: max-gain p50/p75/p90 = "
          f"{rawdf['mfe'].median():.0f}/{rawdf['mfe'].quantile(.75):.0f}/{rawdf['mfe'].quantile(.90):.0f}%"
          f"  | max-loss p50/p90 = {rawdf['mae'].median():.0f}/{rawdf['mae'].quantile(.90):.0f}%")

    exp = pd.DataFrame(index=[f"TP{t}" for t in TARGETS], columns=[f"SL{l}" for l in STOPS], dtype=float)
    win = exp.copy()
    for T in TARGETS:
        for L in STOPS:
            outs = [simulate(e, hi, lo, last, T, L) for (e, hi, lo, last) in paths]
            pnls = np.array([o[0] for o in outs])
            exp.loc[f"TP{T}", f"SL{L}"] = round(pnls.mean(), 1)
            win.loc[f"TP{T}", f"SL{L}"] = round((pnls > 0).mean() * 100, 1)
    print("\n  Expectancy % per trade (after slippage)   rows=take-profit, cols=stop")
    print(exp.to_string())
    print("\n  Win rate %")
    print(win.to_string())
    best = exp.stack().idxmax(); bv = exp.stack().max()
    print(f"\n  Best cell: {best[0]}/{best[1]}  expectancy {bv:+.1f}% per trade")
    return exp


if __name__ == "__main__":
    print("ThetaData 0DTE backtest — SPY VolSpike signal")
    run("momentum")
    run("reversion")
    print("\nNOTE: entries/exits use 5m option OHLC with a flat ${:.2f}/side slippage; real "
          "0DTE spreads vary (tighter AM, wider near close). Upgrade to NBBO quotes for precision.".format(SLIP_PER_SIDE))
