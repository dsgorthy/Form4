#!/usr/bin/env python3
"""
VolSpike signal study — replays the TradingView VolSpike signal over local SPY
5-min bars and measures (a) trigger frequency and (b) the forward-move
distribution after each trigger, to inform a 0DTE price target / stop loss.

Signal (matches tradingview/volspike.pine exactly, on 5-min bars => winBars=1):
  % path : |move over 1 bar| >= PCT_THR  AND  bar_vol >= VOL_MULT   x session-avg-bar-vol
  $ path : |move over 1 bar| >= DOL_THR  AND  bar_vol >= VOL_MULT_LT x session-avg-bar-vol
  combined, fires on the first bar of a run (dedup), only inside the trade window
  (07:30-12:30 PT = 10:30-15:30 ET). Volume baseline = running session average
  per bar (includes the first hour), exactly like the indicator.

Entry = close of the signal bar. We then measure, in the SIGNED direction of the
signal (positive = price continued the way it spiked):
  - close-to-close return at 15 / 30 / 60 min and to the session close
  - MFE (max favorable excursion) and MAE (max adverse excursion) over each horizon
A positive mean continuation => momentum edge; negative => reversion (fade) edge.

Outputs a printed report and writes per-signal rows to volspike_signals.csv.
"""
from __future__ import annotations
import glob, os
import numpy as np
import pandas as pd

DATA_DIR   = "data/raw/SPY"
OUT_CSV    = "tradingview/volspike_signals.csv"

# --- signal params (mirror the pine defaults) ---
PCT_THR    = 0.5     # %
DOL_THR    = 2.0     # $
VOL_MULT   = 1.5     # % path volume gate
VOL_MULT_LT= 1.2     # $ path volume gate
WIN_START  = "10:30" # ET (= 07:30 PT)
WIN_END    = "15:30" # ET (= 12:30 PT), exclusive
HORIZONS   = {"15m": 3, "30m": 6, "60m": 12}   # in 5-min bars

_AGG = {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}


def resample_5m(df1: pd.DataFrame) -> pd.DataFrame:
    df5 = df1.resample("5min").agg(_AGG).dropna(subset=["close"])
    # keep regular session 09:30..15:55 (drop the lone 16:00 print)
    return df5.between_time("09:30", "15:55")


def day_signals(df5: pd.DataFrame) -> pd.DataFrame:
    c = df5["close"]
    pct = c.pct_change() * 100.0
    dol = c.diff()
    # running session-average bar volume (includes current bar), like the pine
    n = np.arange(1, len(df5) + 1)
    avg_bar = df5["volume"].cumsum() / n
    vrto = df5["volume"] / avg_bar

    up_pct = pct >= PCT_THR
    dn_pct = pct <= -PCT_THR
    up_dol = dol >= DOL_THR
    dn_dol = dol <= -DOL_THR
    vhit   = vrto >= VOL_MULT
    vhit_l = vrto >= VOL_MULT_LT

    up_hit = (up_pct & vhit) | (up_dol & vhit_l)
    dn_hit = (dn_pct & vhit) | (dn_dol & vhit_l)

    t = df5.index.time
    in_win = (t >= pd.to_datetime(WIN_START).time()) & (t < pd.to_datetime(WIN_END).time())

    gated = (up_hit | dn_hit) & in_win
    fresh = gated & ~gated.shift(1, fill_value=False)

    out = pd.DataFrame({
        "fresh": fresh, "up": up_hit, "dn": dn_hit,
        "pct": pct, "vrto": vrto, "close": c,
        "high": df5["high"], "low": df5["low"],
    }, index=df5.index)
    return out


def forward_stats(df5: pd.DataFrame, sig: pd.DataFrame) -> list:
    rows = []
    closes = df5["close"].values
    highs  = df5["high"].values
    lows   = df5["low"].values
    idx    = df5.index
    fresh_pos = np.where(sig["fresh"].values)[0]
    last = len(df5) - 1
    for i in fresh_pos:
        s = 1 if sig["up"].values[i] else -1           # signal direction
        entry = closes[i]
        rec = {
            "ts": idx[i], "dir": "up" if s == 1 else "dn",
            "entry": entry, "pct_move": sig["pct"].values[i], "vol_x": sig["vrto"].values[i],
        }
        for name, k in HORIZONS.items():
            j = min(i + k, last)
            if j <= i:
                continue
            seg_hi = highs[i+1:j+1]; seg_lo = lows[i+1:j+1]
            if len(seg_hi) == 0:
                continue
            ret = s * (closes[j] - entry) / entry * 100.0           # signed continuation %
            fav = (max(seg_hi) - entry) if s == 1 else (entry - min(seg_lo))
            adv = (entry - min(seg_lo)) if s == 1 else (max(seg_hi) - entry)
            rec[f"ret_{name}"] = ret
            rec[f"mfe_{name}"] = fav / entry * 100.0
            rec[f"mae_{name}"] = adv / entry * 100.0
        # to session close
        seg_hi = highs[i+1:]; seg_lo = lows[i+1:]
        if len(seg_hi):
            rec["ret_eod"] = s * (closes[last] - entry) / entry * 100.0
            rec["mfe_eod"] = ((max(seg_hi) - entry) if s == 1 else (entry - min(seg_lo))) / entry * 100.0
            rec["mae_eod"] = ((entry - min(seg_lo)) if s == 1 else (max(seg_hi) - entry)) / entry * 100.0
        rows.append(rec)
    return rows


def main():
    files = sorted(glob.glob(f"{DATA_DIR}/*.parquet"))
    print(f"SPY files: {len(files)}  ({os.path.basename(files[0])} .. {os.path.basename(files[-1])})")
    all_rows = []
    n_days = 0
    for f in files:
        df1 = pd.read_parquet(f)
        if df1.empty:
            continue
        n_days += 1
        df5 = resample_5m(df1)
        if len(df5) < 5:
            continue
        sig = day_signals(df5)
        all_rows.extend(forward_stats(df5, sig))

    res = pd.DataFrame(all_rows)
    if res.empty:
        print("No signals.")
        return
    res["date"] = pd.to_datetime(res["ts"]).dt.tz_convert("US/Eastern").dt.date
    res["year"] = pd.to_datetime(res["ts"]).dt.tz_convert("US/Eastern").dt.year
    res.to_csv(OUT_CSV, index=False)

    print("\n================ FREQUENCY ================")
    print(f"trading days analyzed : {n_days}")
    print(f"total signals         : {len(res)}")
    print(f"signals / day         : {len(res)/n_days:.2f}")
    print(f"up / down split       : {(res['dir']=='up').sum()} up / {(res['dir']=='dn').sum()} dn")
    print("\nper-year:")
    yr = res.groupby("year").agg(signals=("dir","size"),
                                 up=("dir", lambda x:(x=="up").sum()),
                                 dn=("dir", lambda x:(x=="dn").sum()))
    yr["days"] = res.groupby("year")["date"].nunique()
    yr["sig_per_day"] = (yr["signals"]/yr["days"]).round(2)
    print(yr.to_string())

    def block(title, sub):
        print(f"\n================ {title} (n={len(sub)}) ================")
        if len(sub) == 0:
            print("  (none)"); return
        for h in list(HORIZONS.keys()) + ["eod"]:
            rc, mf, ma = f"ret_{h}", f"mfe_{h}", f"mae_{h}"
            if rc not in sub: continue
            r = sub[rc].dropna()
            if len(r) == 0: continue
            win = (r > 0).mean() * 100
            print(f"  [{h:>3}] continuation%: mean {r.mean():+.3f}  median {r.median():+.3f} "
                  f"| P(continue)={win:4.1f}%  "
                  f"| MFE p50/p75/p90 = {sub[mf].median():.3f}/{sub[mf].quantile(.75):.3f}/{sub[mf].quantile(.90):.3f}  "
                  f"| MAE p50/p75/p90 = {sub[ma].median():.3f}/{sub[ma].quantile(.75):.3f}/{sub[ma].quantile(.90):.3f}")

    block("ALL SIGNALS — continuation (signed toward the spike)", res)
    block("UP signals", res[res["dir"]=="up"])
    block("DOWN signals", res[res["dir"]=="dn"])

    print("\nNote: continuation% = price move in the SAME direction as the spike.")
    print("Positive mean => momentum edge (trade with). Negative => reversion edge (fade).")
    print("MFE/MAE are % of SPY price; multiply by ~SPY level for $ points. CSV:", OUT_CSV)


if __name__ == "__main__":
    main()
