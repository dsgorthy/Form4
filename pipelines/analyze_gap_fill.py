"""
Empirical gap fill rate analysis for any symbol.

For each available trading day:
  1. Compute gap% = (open - prev_close) / prev_close × 100
  2. Compute F30 return (9:30 → 10:00 bar close vs open)
  3. Determine if F30 fades the gap
  4. Check if gap already filled during F30 (disqualifies clean entry)
  5. Check if price reaches prev_close by 15:30 (= "filled")

Reports fill rates by:
  - Gap bucket: small (0.05–0.15%), medium (0.15–0.30%)
  - F30 condition: all / fade only / continue only
  - Combined (small gap + F30 fade) — the strategy's entry gate

Gate threshold: ≥60% fill rate on small gaps + F30 fade to proceed.
Expected: 70–80% for large-cap ETFs (mean reversion is structural).

Usage:
    python pipelines/analyze_gap_fill.py --symbols SPY QQQ IWM
    python pipelines/analyze_gap_fill.py --symbols SPY --start 2020-01-01 --end 2023-12-31
    python pipelines/analyze_gap_fill.py --symbols QQQ IWM --data-dir /path/to/raw
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

FRAMEWORK_ROOT = Path(__file__).parent.parent
REPO_ROOT = FRAMEWORK_ROOT.parent

ENTRY_TIME = "10:00"
TIME_STOP = "15:30"
MIN_GAP = 0.05   # % — noise floor
MAX_GAP = 0.30   # % — beyond this fill rate drops sharply
SMALL_MAX = 0.15  # % — small gap bucket ceiling


def _build_storage(data_dir: Optional[Path]) -> "DataStorage":
    from framework.data.storage import DataStorage

    # Auto-detect multi-source dirs (spy-0dte + trading-framework)
    all_raw = [
        REPO_ROOT / "spy-0dte" / "data" / "raw",
        FRAMEWORK_ROOT / "data" / "raw",
    ]

    if data_dir:
        primary = data_dir
        extra = [d for d in all_raw if d.exists() and d != primary]
    else:
        primary = next((d for d in all_raw if d.exists()), FRAMEWORK_ROOT / "data" / "raw")
        extra = [d for d in all_raw if d.exists() and d != primary]

    return DataStorage(raw_dir=primary, extra_raw_dirs=extra or None)


def analyze_symbol(
    symbol: str,
    storage,
    start: Optional[str],
    end: Optional[str],
) -> pd.DataFrame:
    """
    Iterate all available dates for symbol and classify each gap day.

    Returns a DataFrame with one row per gap day:
      date, gap_pct, gap_dir, f30_ret, f30_fades, gap_filled_in_f30,
      filled, bucket
    """
    dates = storage.get_available_dates(symbol)
    if not dates:
        print(f"  [WARN] No data found for {symbol}")
        return pd.DataFrame()

    if start:
        dates = [d for d in dates if d >= start]
    if end:
        dates = [d for d in dates if d <= end]

    if len(dates) < 2:
        print(f"  [WARN] Fewer than 2 dates available for {symbol} in range")
        return pd.DataFrame()

    rows = []
    prev_close: Optional[float] = None
    prev_date: Optional[str] = None

    for date_str in dates:
        df = storage.load_minute_bars(symbol, date_str)

        if df is None or df.empty:
            prev_close = None
            prev_date = date_str
            continue

        # Ensure proper datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index, utc=True).tz_convert("US/Eastern")
            except Exception:
                prev_close = None
                prev_date = date_str
                continue

        # Extract session (9:30–15:30 for analysis window)
        try:
            session = df.between_time("09:30", "15:30")
            f30 = df.between_time("09:30", ENTRY_TIME)
        except Exception:
            prev_close = None
            prev_date = date_str
            continue

        # Update prev_close from this day's last bar (for next iteration)
        this_close = float(session.iloc[-1]["close"]) if not session.empty else None

        if prev_close is None or prev_close <= 0:
            prev_close = this_close
            prev_date = date_str
            continue

        if session.empty or len(f30) < 5:
            prev_close = this_close
            prev_date = date_str
            continue

        # Gap calculation
        open_price = float(f30.iloc[0]["open"])
        if open_price <= 0:
            prev_close = this_close
            prev_date = date_str
            continue

        gap_pct = (open_price - prev_close) / prev_close * 100.0
        abs_gap = abs(gap_pct)

        # Only analyze gaps within our strategy range
        if abs_gap < MIN_GAP or abs_gap > MAX_GAP:
            prev_close = this_close
            prev_date = date_str
            continue

        # F30 direction: 9:30 open → 10:00 close
        f30_close = float(f30.iloc[-1]["close"])
        f30_ret = (f30_close - open_price) / open_price * 100.0
        f30_fades = (gap_pct > 0 and f30_ret < 0) or (gap_pct < 0 and f30_ret > 0)

        # Was gap already filled during F30?
        gap_filled_in_f30 = False
        if gap_pct > 0:
            gap_filled_in_f30 = float(f30["low"].min()) <= prev_close
        else:
            gap_filled_in_f30 = float(f30["high"].max()) >= prev_close

        # Did price reach prev_close by 15:30?
        after_entry = session[session.index > f30.index[-1]]
        filled = False
        if not after_entry.empty:
            if gap_pct > 0:
                filled = float(after_entry["low"].min()) <= prev_close
            else:
                filled = float(after_entry["high"].max()) >= prev_close

        # Bucket classification
        bucket = "small" if abs_gap <= SMALL_MAX else "medium"

        rows.append({
            "date": date_str,
            "gap_pct": round(gap_pct, 4),
            "abs_gap": round(abs_gap, 4),
            "gap_dir": "up" if gap_pct > 0 else "down",
            "f30_ret": round(f30_ret, 4),
            "f30_fades": f30_fades,
            "gap_filled_in_f30": gap_filled_in_f30,
            "filled": filled,
            "bucket": bucket,
        })

        prev_close = this_close
        prev_date = date_str

    return pd.DataFrame(rows)


def _fill_rate(df: pd.DataFrame, mask: pd.Series) -> tuple[float, int]:
    """Return (fill_rate_pct, count) for a boolean mask."""
    sub = df[mask]
    n = len(sub)
    if n == 0:
        return float("nan"), 0
    return round(sub["filled"].mean() * 100, 1), n


def print_report(symbol: str, df: pd.DataFrame, gate_pct: float = 60.0) -> None:
    """Print a formatted fill-rate report for one symbol."""
    if df.empty:
        print(f"\n{'='*60}")
        print(f"  {symbol}: no data to analyze")
        return

    print(f"\n{'='*60}")
    print(f"  {symbol} — Gap Fill Analysis ({df['date'].min()} → {df['date'].max()})")
    print(f"  Total gap days (0.05–0.30%): {len(df)}")
    print(f"{'='*60}")

    # Overall table: rows = bucket, cols = all / fade / continue / fade+clean
    headers = ["Bucket", "N", "All", "F30 Fade", "F30 Cont.", "Fade+Clean"]
    row_fmt = "  {:<14} {:>5}  {:>7}  {:>8}  {:>9}  {:>10}"
    print(row_fmt.format(*headers))
    print("  " + "-" * 58)

    gate_met = False
    key_rate = float("nan")
    key_n = 0

    for bucket in ["small", "medium", "all"]:
        if bucket == "all":
            bmask = pd.Series([True] * len(df), index=df.index)
            label = "ALL (0.05–0.30)"
        elif bucket == "small":
            bmask = df["bucket"] == "small"
            label = "Small (0.05–0.15)"
        else:
            bmask = df["bucket"] == "medium"
            label = "Medium (0.15–0.30)"

        # Conditions
        all_rate, all_n = _fill_rate(df, bmask)
        fade_rate, fade_n = _fill_rate(df, bmask & df["f30_fades"])
        cont_rate, cont_n = _fill_rate(df, bmask & ~df["f30_fades"])
        clean_rate, clean_n = _fill_rate(
            df, bmask & df["f30_fades"] & ~df["gap_filled_in_f30"]
        )

        def fmt(rate, n):
            if n == 0:
                return "  —  "
            return f"{rate:.1f}% ({n})"

        print(row_fmt.format(
            label,
            all_n,
            fmt(all_rate, all_n),
            fmt(fade_rate, fade_n),
            fmt(cont_rate, cont_n),
            fmt(clean_rate, clean_n),
        ))

        # Gate check on small + clean (strategy's actual entry condition)
        if bucket == "small":
            key_rate = clean_rate
            key_n = clean_n
            gate_met = (not pd.isna(clean_rate)) and (clean_rate >= gate_pct) and clean_n >= 10

    print()

    # Gate verdict
    if pd.isna(key_rate) or key_n < 10:
        verdict = "INSUFFICIENT DATA (< 10 qualifying trades)"
    elif gate_met:
        verdict = f"PASS — {key_rate:.1f}% fill rate on {key_n} small+clean trades (threshold: {gate_pct:.0f}%)"
    else:
        verdict = f"FAIL — {key_rate:.1f}% fill rate on {key_n} small+clean trades (threshold: {gate_pct:.0f}%)"

    print(f"  Strategy gate [{symbol}]: {verdict}")

    # Gap direction breakdown (small + fade + clean)
    clean_mask = (df["bucket"] == "small") & df["f30_fades"] & ~df["gap_filled_in_f30"]
    if clean_mask.sum() > 0:
        up_mask = clean_mask & (df["gap_dir"] == "up")
        dn_mask = clean_mask & (df["gap_dir"] == "down")
        up_rate, up_n = _fill_rate(df, up_mask)
        dn_rate, dn_n = _fill_rate(df, dn_mask)
        print(f"  Direction split (small+clean): "
              f"gap-up {up_rate:.1f}% ({up_n}n)  |  gap-down {dn_rate:.1f}% ({dn_n}n)")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze empirical gap fill rates for one or more symbols."
    )
    parser.add_argument(
        "--symbols", nargs="+", default=["SPY", "QQQ", "IWM"],
        help="Symbols to analyze (default: SPY QQQ IWM)"
    )
    parser.add_argument("--start", help="Filter from date YYYY-MM-DD")
    parser.add_argument("--end", help="Filter to date YYYY-MM-DD")
    parser.add_argument("--data-dir", type=Path, help="Override raw data directory")
    parser.add_argument(
        "--gate", type=float, default=60.0,
        help="Minimum fill rate %% to pass the gate (default: 60)"
    )
    args = parser.parse_args()

    storage = _build_storage(args.data_dir)

    print(f"\nGap Fill Empirical Analysis")
    print(f"Entry gate: small gaps (0.05–0.15%) + F30 fade + not filled during F30")
    print(f"Gate threshold: ≥{args.gate:.0f}%")
    if args.start or args.end:
        print(f"Date filter: {args.start or 'all'} → {args.end or 'all'}")

    all_results: dict[str, pd.DataFrame] = {}

    for sym in args.symbols:
        sym = sym.upper()
        print(f"\nAnalyzing {sym}...", end="", flush=True)
        df = analyze_symbol(sym, storage, args.start, args.end)
        all_results[sym] = df
        print(f" {len(df)} gap days found.")
        print_report(sym, df, gate_pct=args.gate)

    # Cross-symbol co-occurrence (if ≥2 symbols analyzed)
    valid = {s: df for s, df in all_results.items() if not df.empty}
    if len(valid) >= 2:
        print(f"\n{'='*60}")
        print("  Cross-symbol co-occurrence (same-day gap triggers)")
        print(f"{'='*60}")
        # Build date sets for clean entries per symbol
        clean_dates: dict[str, set] = {}
        for sym, df in valid.items():
            clean_mask = (
                (df["bucket"] == "small") & df["f30_fades"] & ~df["gap_filled_in_f30"]
            )
            clean_dates[sym] = set(df.loc[clean_mask, "date"])

        syms = list(clean_dates.keys())
        for i, s1 in enumerate(syms):
            for s2 in syms[i + 1:]:
                overlap = clean_dates[s1] & clean_dates[s2]
                n1, n2 = len(clean_dates[s1]), len(clean_dates[s2])
                pct1 = len(overlap) / n1 * 100 if n1 else 0
                pct2 = len(overlap) / n2 * 100 if n2 else 0
                print(f"  {s1}∩{s2}: {len(overlap)} days overlap "
                      f"({pct1:.1f}% of {s1}, {pct2:.1f}% of {s2})")

        if len(syms) == 3:
            s1, s2, s3 = syms
            triple = clean_dates[s1] & clean_dates[s2] & clean_dates[s3]
            total = len(clean_dates[s1]) + len(clean_dates[s2]) + len(clean_dates[s3])
            print(f"  {s1}∩{s2}∩{s3}: {len(triple)} days (all 3 trigger same day)")


if __name__ == "__main__":
    main()
