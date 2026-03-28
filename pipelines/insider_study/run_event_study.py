"""
Run the insider buy event study with confidence scoring analysis.

For each event in the calendar:
  1. Entry: next trading day open after filing_date (T+1)
  2. Hold: N trading days (default 21 = ~1 month)
  3. Exit: close on day T+N
  4. Benchmark: same-period SPY return
  5. Abnormal return = trade return - SPY return

Analyses:
  - Overall summary statistics
  - Returns by confidence quintile (5 bins)
  - Returns by n_insiders (1, 2-3, 4+ insiders)
  - Cluster vs. single-insider comparison
  - Confidence score vs. abnormal return correlation (Pearson r + p-value if scipy available)
  - Top 10 events by confidence score
  - Holding period sensitivity sweep (7, 21, 63 days)

Usage:
    python run_event_study.py \\
        --events data/events.csv \\
        --prices-dir data/prices/ \\
        --hold-days 21

    # Cluster only:
    python run_event_study.py --events data/events.csv --prices-dir data/prices/ \\
        --cluster-only --hold-days 21

    # Sweep holding periods (7, 21, 63 days):
    python run_event_study.py --events data/events.csv --prices-dir data/prices/ --sweep
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

# Optional scipy import for p-values
try:
    from scipy import stats as _scipy_stats
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False


def load_prices(ticker: str, prices_dir: Path) -> Optional[pd.DataFrame]:
    """Load daily OHLCV from CSV. Returns None if not available."""
    path = prices_dir / f"{ticker.upper()}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        # Normalize timezone: strip tz info so we compare naive timestamps
        df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        logger.debug("Could not load %s: %s", ticker, e)
        return None


def get_trading_days_after(df: pd.DataFrame, date: pd.Timestamp, n: int) -> Optional[pd.Timestamp]:
    """
    Return the n-th trading day after `date` (strictly after) based on df's index.
    n=1 returns the very next trading day.
    """
    future = df.index[df.index > date]
    if len(future) < n:
        return None
    return future[n - 1]


def compute_trade_return(
    ticker: str,
    filing_date: pd.Timestamp,
    hold_days: int,
    prices_dir: Path,
    spy_df: pd.DataFrame,
) -> Optional[dict]:
    """
    Compute return for one event:
    - Entry: T+1 open (first trading day after filing_date)
    - Exit: close at T+hold_days trading days after entry

    Returns dict with trade metrics, or None if data unavailable.
    """
    prices = load_prices(ticker, prices_dir)
    if prices is None or prices.empty:
        return None

    # T+1: first trading day after filing
    entry_day = get_trading_days_after(prices, filing_date, 1)
    if entry_day is None:
        return None

    # Entry price: T+1 open
    try:
        entry_price = float(prices.loc[entry_day, "open"])
    except (KeyError, ValueError):
        return None
    if not np.isfinite(entry_price) or entry_price <= 0:
        return None

    # Exit: close at T+hold_days trading days after entry day
    exit_day = get_trading_days_after(prices, entry_day, hold_days)
    if exit_day is None:
        return None
    try:
        exit_price = float(prices.loc[exit_day, "close"])
    except (KeyError, ValueError):
        return None
    if not np.isfinite(exit_price) or exit_price <= 0:
        return None

    trade_return = (exit_price - entry_price) / entry_price

    # SPY benchmark return for the exact same period
    spy_entry_rows = spy_df.index[spy_df.index >= entry_day]
    spy_exit_rows = spy_df.index[spy_df.index >= exit_day]
    if len(spy_entry_rows) == 0 or len(spy_exit_rows) == 0:
        spy_return = 0.0
    else:
        try:
            spy_entry_price = float(spy_df.loc[spy_entry_rows[0], "open"])
            spy_exit_price = float(spy_df.loc[spy_exit_rows[0], "close"])
            spy_return = (
                (spy_exit_price - spy_entry_price) / spy_entry_price
                if spy_entry_price > 0
                else 0.0
            )
        except (KeyError, ValueError):
            spy_return = 0.0

    abnormal_return = trade_return - spy_return

    return {
        "ticker": ticker,
        "filing_date": filing_date.date() if hasattr(filing_date, "date") else filing_date,
        "entry_date": entry_day.date() if hasattr(entry_day, "date") else entry_day,
        "exit_date": exit_day.date() if hasattr(exit_day, "date") else exit_day,
        "entry_price": round(entry_price, 4),
        "exit_price": round(exit_price, 4),
        "trade_return": round(trade_return * 100, 3),   # in %
        "spy_return": round(spy_return * 100, 3),       # in %
        "abnormal_return": round(abnormal_return * 100, 3),  # in %
        "win": int(abnormal_return > 0),
        "hold_days": hold_days,
    }


def run_study(events: pd.DataFrame, prices_dir: Path, hold_days: int) -> pd.DataFrame:
    """
    Run the event study for all events at the given holding period.
    Passes through all columns from the events file (including confidence_score, n_insiders, etc.)
    """
    spy_df = load_prices("SPY", prices_dir)
    if spy_df is None:
        raise FileNotFoundError(
            "SPY price data not found in prices_dir. Run collect_prices.py first."
        )

    results = []
    skipped = 0

    # Columns to pass through from event row to result
    passthrough_cols = [
        "confidence_score", "value_score", "breadth_score", "quality_score",
        "concentration", "n_insiders", "cluster_size", "total_value",
        "max_single_value", "avg_value_per_insider", "is_cluster",
        "insider_names", "company", "event_start_date",
    ]

    for _, row in events.iterrows():
        ticker = row.get("ticker")
        # Entry trigger: use filing_date (last filing in the event window)
        filing_date_raw = row.get("filing_date")
        if not ticker or pd.isna(filing_date_raw):
            skipped += 1
            continue

        try:
            filing_date = pd.Timestamp(filing_date_raw)
        except Exception:
            skipped += 1
            continue

        result = compute_trade_return(
            str(ticker), filing_date, hold_days, prices_dir, spy_df
        )
        if result is None:
            skipped += 1
            continue

        # Attach event-level metadata
        for col in passthrough_cols:
            if col in row.index:
                val = row[col]
                # Coerce boolean columns
                if col == "is_cluster":
                    result[col] = bool(val) if not pd.isna(val) else False
                else:
                    result[col] = val

        # Ensure is_cluster is present even if not in events
        if "is_cluster" not in result:
            result["is_cluster"] = bool(row.get("is_cluster", False))

        # cluster_size fallback
        if "cluster_size" not in result:
            result["cluster_size"] = int(row.get("cluster_size", 1))

        results.append(result)

    logger.info("Study complete: %d trades computed, %d skipped (no price data)", len(results), skipped)
    return pd.DataFrame(results)


def print_summary(df: pd.DataFrame, label: str = "All events"):
    """Print key statistics for a results dataframe."""
    if df.empty:
        print(f"\n{label}: No results")
        return

    ar = df["abnormal_return"].dropna()
    n = len(ar)
    if n == 0:
        print(f"\n{label}: No valid returns")
        return

    hold = int(df["hold_days"].iloc[0]) if "hold_days" in df.columns else 21
    wr = float(df["win"].mean()) if "win" in df.columns else float((ar > 0).mean())
    mean_ar = float(ar.mean())
    median_ar = float(ar.median())
    std_ar = float(ar.std())
    sharpe = mean_ar / std_ar * (252 / hold) ** 0.5 if std_ar > 0 else 0.0
    t_stat = mean_ar / (std_ar / n ** 0.5) if std_ar > 0 and n > 1 else 0.0

    print(f"\n{'=' * 56}")
    print(f"  {label}")
    print(f"{'=' * 56}")
    print(f"  N events:              {n:>8}")
    print(f"  Win rate (vs SPY):     {wr:>8.1%}")
    print(f"  Mean abnormal return:  {mean_ar:>8.2f}%")
    print(f"  Median abnormal ret:   {median_ar:>8.2f}%")
    print(f"  Std dev:               {std_ar:>8.2f}%")
    print(f"  Sharpe (annualized):   {sharpe:>8.2f}")
    print(f"  % positive:            {(ar > 0).mean():>8.1%}")
    print(f"  90th pctile:           {ar.quantile(0.9):>8.2f}%")
    print(f"  10th pctile:           {ar.quantile(0.1):>8.2f}%")
    print(f"  t-statistic:           {t_stat:>8.2f}")
    print(f"{'=' * 56}")


def print_quintile_analysis(df: pd.DataFrame, hold_days: int):
    """
    Break events into 5 confidence quintiles and show returns per quintile.
    Requires 'confidence_score' in the dataframe.
    """
    if "confidence_score" not in df.columns or df["confidence_score"].isna().all():
        print("\n[Quintile analysis skipped: no confidence_score data]")
        return

    df = df.copy()
    df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce")
    valid = df.dropna(subset=["confidence_score", "abnormal_return"])

    if len(valid) < 5:
        print("\n[Quintile analysis skipped: fewer than 5 events with valid confidence scores]")
        return

    try:
        valid["quintile"] = pd.qcut(
            valid["confidence_score"], q=5, labels=["Q1 (Low)", "Q2", "Q3", "Q4", "Q5 (High)"]
        )
    except ValueError:
        # Fallback for ties: use cut with quantile edges
        edges = valid["confidence_score"].quantile([0, 0.2, 0.4, 0.6, 0.8, 1.0]).unique()
        if len(edges) < 3:
            print("\n[Quintile analysis skipped: not enough distinct confidence score values]")
            return
        valid["quintile"] = pd.cut(
            valid["confidence_score"],
            bins=edges,
            labels=[f"Q{i+1}" for i in range(len(edges) - 1)],
            include_lowest=True,
        )

    print(f"\n{'=' * 74}")
    print(f"  Returns by Confidence Quintile — {hold_days}d hold")
    print(f"{'=' * 74}")
    print(
        f"  {'Quintile':<12} {'N':>6} {'Mean AR':>10} {'Win Rate':>10} {'Mean Conf':>10}"
    )
    print(f"  {'-' * 12} {'-' * 6} {'-' * 10} {'-' * 10} {'-' * 10}")

    for q_label in valid["quintile"].cat.categories:
        subset = valid[valid["quintile"] == q_label]
        if subset.empty:
            continue
        n = len(subset)
        mean_ar = subset["abnormal_return"].mean()
        win_rate = subset["win"].mean() if "win" in subset.columns else (subset["abnormal_return"] > 0).mean()
        mean_conf = subset["confidence_score"].mean()
        print(
            f"  {str(q_label):<12} {n:>6} {mean_ar:>9.2f}% {win_rate:>9.1%} {mean_conf:>10.1f}"
        )

    print(f"{'=' * 74}")


def print_insiders_analysis(df: pd.DataFrame, hold_days: int):
    """
    Break events by n_insiders (1, 2-3, 4+) and show returns per group.
    """
    if "n_insiders" not in df.columns and "cluster_size" not in df.columns:
        print("\n[Insider count analysis skipped: no n_insiders/cluster_size data]")
        return

    df = df.copy()
    n_col = "n_insiders" if "n_insiders" in df.columns else "cluster_size"
    df[n_col] = pd.to_numeric(df[n_col], errors="coerce").fillna(1).astype(int)

    def bucket(n):
        if n <= 1:
            return "1 insider"
        elif n <= 3:
            return "2-3 insiders"
        else:
            return "4+ insiders"

    df["_n_bucket"] = df[n_col].apply(bucket)
    bucket_order = ["1 insider", "2-3 insiders", "4+ insiders"]

    print(f"\n{'=' * 70}")
    print(f"  Returns by Insider Count — {hold_days}d hold")
    print(f"{'=' * 70}")
    print(
        f"  {'Insiders':<16} {'N':>6} {'Mean AR':>10} {'Win Rate':>10} {'Mean Value':>12}"
    )
    print(f"  {'-' * 16} {'-' * 6} {'-' * 10} {'-' * 10} {'-' * 12}")

    for bucket_label in bucket_order:
        subset = df[df["_n_bucket"] == bucket_label]
        if subset.empty:
            continue
        n = len(subset)
        mean_ar = subset["abnormal_return"].mean()
        win_rate = subset["win"].mean() if "win" in subset.columns else (subset["abnormal_return"] > 0).mean()
        mean_val = subset["total_value"].mean() if "total_value" in subset.columns else float("nan")
        mean_val_str = f"${mean_val:,.0f}" if np.isfinite(mean_val) else "N/A"
        print(
            f"  {bucket_label:<16} {n:>6} {mean_ar:>9.2f}% {win_rate:>9.1%} {mean_val_str:>12}"
        )

    print(f"{'=' * 70}")


def print_correlation_table(df: pd.DataFrame, hold_days: int):
    """
    Print Pearson correlation between confidence_score and abnormal_return.
    Uses scipy for p-value if available; falls back to numpy otherwise.
    """
    if "confidence_score" not in df.columns:
        print("\n[Correlation analysis skipped: no confidence_score column]")
        return

    valid = df.dropna(subset=["confidence_score", "abnormal_return"])
    if len(valid) < 3:
        print("\n[Correlation analysis skipped: fewer than 3 valid rows]")
        return

    x = valid["confidence_score"].astype(float).values
    y = valid["abnormal_return"].astype(float).values

    r = float(np.corrcoef(x, y)[0, 1])

    print(f"\n{'=' * 56}")
    print(f"  Confidence Score vs. Abnormal Return — {hold_days}d hold")
    print(f"{'=' * 56}")
    print(f"  N observations: {len(valid)}")
    print(f"  Pearson r:      {r:.4f}")

    if _HAS_SCIPY:
        try:
            _, p_val = _scipy_stats.pearsonr(x, y)
            print(f"  p-value:        {p_val:.4f}")
            sig = "***" if p_val < 0.001 else ("**" if p_val < 0.01 else ("*" if p_val < 0.05 else "n.s."))
            print(f"  Significance:   {sig}  (*** p<0.001, ** p<0.01, * p<0.05)")
        except Exception:
            pass
    else:
        print("  p-value:        [install scipy for p-values]")

    # Also show correlations for sub-components if available
    component_cols = ["value_score", "breadth_score", "quality_score", "concentration"]
    available = [c for c in component_cols if c in valid.columns]
    if available:
        print(f"\n  Sub-component correlations with abnormal return:")
        for col in available:
            try:
                vals = valid[col].astype(float).values
                r_sub = float(np.corrcoef(vals, y)[0, 1])
                print(f"    {col:<22}: r = {r_sub:.4f}")
            except Exception:
                pass

    print(f"{'=' * 56}")


def print_title_type_analysis(df: pd.DataFrame, hold_days: int):
    """
    Break events into title-type categories using quality_score as a proxy
    for the dominant insider type in each event.

    quality_score is the mean title weight across all trades in the event:
        >= 2.5 → C-suite Officer (CEO, Chairman, CFO, President, 10% owner)
        >= 2.0 → Senior Officer (COO, SVP, EVP)
        >= 1.5 → VP / Director
        <  1.5 → Other / Unknown
    """
    if "quality_score" not in df.columns or df["quality_score"].isna().all():
        print("\n[Title type analysis skipped: no quality_score data]")
        return

    df = df.copy()
    df["quality_score"] = pd.to_numeric(df["quality_score"], errors="coerce")
    valid = df.dropna(subset=["quality_score", "abnormal_return"])

    if len(valid) < 2:
        print("\n[Title type analysis skipped: insufficient data]")
        return

    def _title_bucket(q):
        if q >= 2.5:
            return "C-suite Officer"
        elif q >= 2.0:
            return "Senior Officer"
        elif q >= 1.5:
            return "VP / Director"
        else:
            return "Other / Unknown"

    valid["_title_type"] = valid["quality_score"].apply(_title_bucket)
    bucket_order = ["C-suite Officer", "Senior Officer", "VP / Director", "Other / Unknown"]

    print(f"\n{'=' * 74}")
    print(f"  Returns by Title Type (quality_score proxy) — {hold_days}d hold")
    print(f"{'=' * 74}")
    print(
        f"  {'Title Type':<18} {'N':>6} {'Mean AR':>10} {'Win Rate':>10} {'Mean Conf':>10}"
    )
    print(f"  {'-' * 18} {'-' * 6} {'-' * 10} {'-' * 10} {'-' * 10}")

    for bkt in bucket_order:
        subset = valid[valid["_title_type"] == bkt]
        if subset.empty:
            continue
        n = len(subset)
        mean_ar = subset["abnormal_return"].mean()
        win_rate = subset["win"].mean() if "win" in subset.columns else (subset["abnormal_return"] > 0).mean()
        mean_conf = subset["confidence_score"].mean() if "confidence_score" in subset.columns else float("nan")
        conf_str = f"{mean_conf:.1f}" if np.isfinite(mean_conf) else "N/A"
        print(
            f"  {bkt:<18} {n:>6} {mean_ar:>9.2f}% {win_rate:>9.1%} {conf_str:>10}"
        )

    print(f"{'=' * 74}")


def print_top_events(df: pd.DataFrame, hold_days: int, n: int = 10):
    """
    Print the top N events by confidence score with their realized returns.
    """
    if "confidence_score" not in df.columns:
        print("\n[Top events skipped: no confidence_score column]")
        return

    valid = df.dropna(subset=["confidence_score", "abnormal_return"])
    if valid.empty:
        return

    top = valid.nlargest(n, "confidence_score")

    print(f"\n{'=' * 84}")
    print(f"  Top {n} Events by Confidence Score — {hold_days}d hold")
    print(f"{'=' * 84}")
    print(
        f"  {'Ticker':<8} {'Filing Date':<14} {'Conf':>6} {'N':>4} "
        f"{'Total $':>12} {'AR':>8} {'Win':>4}"
    )
    print(
        f"  {'-'*8} {'-'*14} {'-'*6} {'-'*4} "
        f"{'-'*12} {'-'*8} {'-'*4}"
    )

    for _, row in top.iterrows():
        ticker = str(row.get("ticker", ""))
        fdate = str(row.get("filing_date", ""))[:10]
        conf = row.get("confidence_score", float("nan"))
        n_ins = int(row.get("n_insiders", row.get("cluster_size", 1)))
        total_val = row.get("total_value", float("nan"))
        ar = row.get("abnormal_return", float("nan"))
        win = int(row.get("win", 0))

        conf_str = f"{conf:.1f}" if np.isfinite(conf) else "N/A"
        val_str = f"${total_val:,.0f}" if np.isfinite(total_val) else "N/A"
        ar_str = f"{ar:.2f}%" if np.isfinite(ar) else "N/A"

        print(
            f"  {ticker:<8} {fdate:<14} {conf_str:>6} {n_ins:>4} "
            f"{val_str:>12} {ar_str:>8} {'Y' if win else 'N':>4}"
        )

    print(f"{'=' * 84}")


def main():
    parser = argparse.ArgumentParser(
        description="Run insider buy event study with confidence scoring analysis"
    )
    parser.add_argument(
        "--events",
        type=Path,
        default=Path(__file__).parent / "data" / "events.csv",
    )
    parser.add_argument(
        "--prices-dir",
        type=Path,
        default=Path(__file__).parent / "data" / "prices",
    )
    parser.add_argument(
        "--hold-days", type=int, default=21, help="Holding period in trading days"
    )
    parser.add_argument(
        "--cluster-only", action="store_true", help="Only cluster events (n_insiders >= 2)"
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="Sweep 7, 21, and 63 day holding periods",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output CSV path (optional; defaults to data/results_<hold_days>d.csv)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum confidence score to include in study (0-100)",
    )
    parser.add_argument(
        "--exclude-dates",
        default="",
        metavar="START:END[,START:END,...]",
        help=(
            "Exclude events whose filing_date falls within these ranges. "
            "Comma-separated YYYY-MM-DD:YYYY-MM-DD pairs. "
            "Example: --exclude-dates 2020-01-01:2021-12-31"
        ),
    )
    args = parser.parse_args()

    # Load event calendar
    events = pd.read_csv(args.events)
    logger.info("Loaded %d events from %s", len(events), args.events)

    # Parse filing_date robustly
    if "filing_date" in events.columns:
        events["filing_date"] = pd.to_datetime(events["filing_date"], errors="coerce")

    # Exclude-dates filter (e.g. COVID period)
    if args.exclude_dates and "filing_date" in events.columns:
        exclude_ranges = []
        for part in args.exclude_dates.split(","):
            part = part.strip()
            if ":" not in part:
                continue
            s, e = part.split(":", 1)
            try:
                exclude_ranges.append((pd.Timestamp(s.strip()), pd.Timestamp(e.strip())))
            except Exception:
                logger.warning("Could not parse exclude-dates range: %s", part)

        if exclude_ranges:
            before = len(events)
            dates = pd.to_datetime(events["filing_date"], errors="coerce")
            mask = pd.Series(False, index=events.index)
            for start_dt, end_dt in exclude_ranges:
                mask |= (dates >= start_dt) & (dates <= end_dt)
            events = events[~mask]
            logger.info(
                "Exclude-dates filter: %d → %d events (removed %d in excluded ranges)",
                before, len(events), before - len(events),
            )

    # Cluster filter (n_insiders >= 2)
    if args.cluster_only:
        before = len(events)
        n_col = "n_insiders" if "n_insiders" in events.columns else "cluster_size"
        if n_col in events.columns:
            events = events[pd.to_numeric(events[n_col], errors="coerce").fillna(1) >= 2]
        elif "is_cluster" in events.columns:
            events = events[events["is_cluster"].astype(str).str.lower() == "true"]
        logger.info("Cluster filter: %d → %d events", before, len(events))

    # Minimum confidence filter
    if args.min_confidence > 0 and "confidence_score" in events.columns:
        before = len(events)
        events = events[
            pd.to_numeric(events["confidence_score"], errors="coerce").fillna(0) >= args.min_confidence
        ]
        logger.info(
            "Min confidence filter (%.1f): %d → %d events",
            args.min_confidence, before, len(events),
        )

    hold_periods = [7, 21, 63] if args.sweep else [args.hold_days]

    all_results = []
    for hold_days in hold_periods:
        logger.info("Running event study: hold_days=%d", hold_days)
        results = run_study(events, args.prices_dir, hold_days)

        if results.empty:
            logger.warning("No results for hold_days=%d", hold_days)
            continue

        all_results.append(results)

        # --- Core summary ---
        print_summary(results, f"All events — {hold_days}d hold")

        # --- Cluster vs. single insider ---
        if "is_cluster" in results.columns:
            cluster_mask = results["is_cluster"].astype(bool)
            if cluster_mask.sum() > 0:
                print_summary(results[cluster_mask], f"Cluster events (2+ insiders) — {hold_days}d hold")
            if (~cluster_mask).sum() > 0:
                print_summary(results[~cluster_mask], f"Single insider — {hold_days}d hold")

        # --- Confidence quintile analysis ---
        print_quintile_analysis(results, hold_days)

        # --- Insider count analysis ---
        print_insiders_analysis(results, hold_days)

        # --- Title type breakdown ---
        print_title_type_analysis(results, hold_days)

        # --- Correlation table ---
        print_correlation_table(results, hold_days)

        # --- Top 10 by confidence ---
        print_top_events(results, hold_days, n=10)

    # Save results
    if not all_results:
        logger.warning("No results to save.")
        return

    combined = pd.concat(all_results, ignore_index=True)

    if args.output:
        out_path = args.output
    elif args.sweep:
        out_path = Path(__file__).parent / "data" / "results_sweep.csv"
    else:
        out_path = Path(__file__).parent / "data" / f"results_{args.hold_days}d.csv"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_path, index=False)
    logger.info("Saved detailed results to %s (%d rows)", out_path, len(combined))
    print(f"\nResults saved to: {out_path}")


if __name__ == "__main__":
    main()
