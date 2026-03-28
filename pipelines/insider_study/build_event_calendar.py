"""
Build the insider buy event calendar from SEC EDGAR Form 4 data.

Reads EDGAR quarterly index files, locates Form 4 filings, fetches individual
filing details to extract transaction info, and applies academic filters to
produce a clean event table.

Supported input formats:
1. EDGAR quarterly index files (company.gz) — lists all filings
2. OpenInsider CSV export (free download: http://openinsider.com/?s=&o=&pl=1&ph=&ll=4&lh=&fd=1461&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=25000&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=100&page=1)
3. Quiver Quantitative insider API (paid)

The fastest path to a working backtest: download OpenInsider CSV.
This script handles all three formats.

Usage:
    # Using OpenInsider CSV (recommended for quick start):
    python build_event_calendar.py --input openinsider_export.csv --format openinsider

    # Using EDGAR quarterly index + individual filing fetch (slower but free):
    python build_event_calendar.py --input data/edgar_raw/ --format edgar-index

Event grouping algorithm (30-day rolling window per ticker):
    Sort trades by (ticker, filing_date). For each ticker, start at the earliest
    ungrouped trade, collect all trades within 30 calendar days as one "event".
    The event's trigger date = last filing_date in the group (i.e., the day
    you know all insiders in the cluster have filed). Repeat for remaining trades.

Confidence Score Components (all computed at event-group level):
    Title weights (case-insensitive keyword match):
        CEO / Chief Exec      : 3.0
        Chairman / Exec Chair : 3.0
        CFO / Chief Financial : 2.5
        President             : 2.5
        10% Owner             : 2.5
        COO / Chief Operating : 2.0
        SVP / EVP / Senior VP : 1.8
        VP / Vice President   : 1.5
        Director / Board      : 1.5
        Treasurer / Secretary : 1.2
        (default)             : 1.0

    A) value_score = min(5.0, log10(max(1, total_value) / 25000) + 1)
    B) breadth_score = min(5.0, 1 + log2(max(1, n_distinct_insiders)))
    C) quality_score = mean(title_weight for each trade in event)
    D) concentration = max_single_insider_total / total_event_value
       concentration_bonus = concentration * 0.5
    E) confidence_score = min(100, (A * B * C * (1 + D)) / 112.5 * 100)
       (112.5 = theoretical max: 5 * 5 * 3.0 * 1.5 * normalizer)
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

# Academic filter defaults
DEFAULTS = {
    "min_transaction_value": 50_000,    # $50K minimum open-market purchase per trade
    "min_market_cap": 100_000_000,      # $100M minimum
    "max_market_cap": 2_000_000_000,    # $2B maximum
    "event_window_days": 30,            # 30-day rolling window for event grouping
    "routine_consecutive_years": 3,     # Cohen et al.: 3+ = routine, exclude
    "exclude_10b51_plans": True,
}

# Confidence score normalization constant
# Theoretical max: value_score=5, breadth_score=5, quality_score=3.0, concentration_bonus multiplier=1.5
# 5 * 5 * 3.0 * 1.5 = 112.5
CONF_NORMALIZER = 112.5

# Title weight rules: list of (keywords_list, weight)
# Applied in order; first match wins
TITLE_WEIGHT_RULES = [
    (["ceo", "chief exec"],                           3.0),
    (["chairman", "exec chair", "executive chair"],   3.0),
    (["cfo", "chief financial"],                      2.5),
    (["president"],                                   2.5),
    (["10% owner", "10 percent owner", "10pct"],      2.5),
    (["coo", "chief operating"],                      2.0),
    (["svp", "evp", "senior vp", "senior vice",
      "exec vp", "executive vp", "executive vice president"],  1.8),
    (["vp", "vice president"],                        1.5),
    (["director", "board"],                           1.5),
    (["treasurer", "secretary"],                      1.2),
]
DEFAULT_TITLE_WEIGHT = 1.0

# C-suite keywords: at least one must be present for --csuite-only filter.
# Excludes pure 10% beneficial owners (hedge funds / PE) and board-only directors,
# which in the academic literature carry far weaker signal than operating officers.
CSUITE_KEYWORDS = [
    "ceo", "chief exec", "chief executive", "co-ceo",
    "cfo", "chief financial", "chief fin",
    "coo", "chief operating",
    "president", "pres",
    "chairman", "chairwoman", "chair", "cob",  # chair/cob = chairman of board
    "evp", "executive vp", "executive vice president",
    "svp", "senior vp", "senior vice president",
]


def is_csuite(title: str) -> bool:
    """
    Return True if the title contains at least one C-suite or senior officer keyword.
    Excludes pure "10%" (beneficial owner only) and "Dir" (board director only).

    The academic signal (Cohen et al., Lakonishok & Lee) comes from operating officers
    who have genuine informational advantage, not from institutional 10% owners or
    passive board members.
    """
    if not isinstance(title, str) or not title.strip():
        return False
    t = title.lower()
    return any(kw in t for kw in CSUITE_KEYWORDS)


def get_title_weight(title: str) -> float:
    """
    Return the insider's title weight based on keyword matching.
    Matching is case-insensitive; first rule that matches wins.
    """
    if not isinstance(title, str) or not title.strip():
        return DEFAULT_TITLE_WEIGHT
    t = title.lower()
    for keywords, weight in TITLE_WEIGHT_RULES:
        for kw in keywords:
            if kw in t:
                return weight
    return DEFAULT_TITLE_WEIGHT


def compute_confidence_score(
    total_value: float,
    n_distinct_insiders: int,
    title_weights: list,
    max_single_value: float,
) -> dict:
    """
    Compute the multi-factor confidence score for one event group.

    Returns a dict with all intermediate components plus the final score.
    """
    # A: Value score
    value_score = min(5.0, math.log10(max(1.0, total_value) / 25_000) + 1.0)

    # B: Breadth score
    breadth_score = min(5.0, 1.0 + math.log2(max(1, n_distinct_insiders)))

    # C: Quality score (mean of title weights across all individual trades in event)
    if title_weights:
        quality_score = float(np.mean(title_weights))
    else:
        quality_score = DEFAULT_TITLE_WEIGHT

    # D: Concentration bonus
    if total_value > 0:
        concentration = max_single_value / total_value
    else:
        concentration = 1.0
    concentration_bonus = concentration * 0.5

    # E: Final confidence score (0-100)
    raw = value_score * breadth_score * quality_score * (1.0 + concentration_bonus)
    confidence_score = min(100.0, raw / CONF_NORMALIZER * 100.0)

    return {
        "value_score": round(value_score, 4),
        "breadth_score": round(breadth_score, 4),
        "quality_score": round(quality_score, 4),
        "concentration": round(concentration, 4),
        "confidence_score": round(confidence_score, 4),
    }


def parse_openinsider_csv(path: Path, filters: dict) -> pd.DataFrame:
    """
    Parse OpenInsider CSV export.

    Download from: http://openinsider.com/ with these filters set:
    - Transaction type: P (purchases only)
    - Min value: $25K
    - Insider type: all
    - Date range: custom

    OpenInsider CSV columns (standard export):
    X, Filing Date, Trade Date, Ticker, Company Name, Insider Name, Title,
    Trade Type, Price, Qty, Owned, ΔOwn, Value

    Handles variations in column names across different OpenInsider export versions.
    """
    logger.info("Parsing OpenInsider CSV: %s", path)
    df = pd.read_csv(path)

    # Normalize column names: lowercase, replace spaces/special chars with underscore
    df.columns = [
        re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")
        for c in df.columns
    ]

    logger.debug("Columns after normalization: %s", list(df.columns))

    # Flexible column mapping: standard name -> candidate column names
    col_map = {
        "filing_date": ["filing_date", "filing date", "date_filed", "date filed", "filed"],
        "trade_date":  ["trade_date",  "trade date",  "trade_dt",   "date",       "traded"],
        "ticker":      ["ticker",      "symbol",      "tick"],
        "company":     ["company_name","company",     "issuer",     "company_nm"],
        "insider_name":["insider_name","insider name","owner",      "insider",    "name"],
        "title":       ["title",       "relationship","officer_title","role"],
        "trade_type":  ["trade_type",  "trade type",  "type",       "tran_type"],
        "price":       ["price",       "share_price", "price_per_share"],
        "qty":         ["qty",         "quantity",    "shares",     "num_shares", "quant"],
        "value":       ["value",       "total_value", "trade_value","trans_value"],
    }

    normalized = {}
    for standard, candidates in col_map.items():
        for c in candidates:
            c_norm = re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")
            if c_norm in df.columns:
                normalized[standard] = df[c_norm]
                break
            # Also try the raw candidate directly
            if c in df.columns:
                normalized[standard] = df[c]
                break

    result = pd.DataFrame(normalized)

    if result.empty or "ticker" not in result.columns:
        logger.error("Could not find required columns. Available: %s", list(df.columns))
        sys.exit(1)

    # Parse dates
    for col in ["filing_date", "trade_date"]:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors="coerce")

    # Parse numeric value — handle $, commas, +, spaces
    if "value" in result.columns:
        result["value"] = (
            result["value"]
            .astype(str)
            .str.replace(r"[$,+\s]", "", regex=True)
            .str.strip()
        )
        result["value"] = pd.to_numeric(result["value"], errors="coerce").abs()

    # Filter by trade type: P (purchases) and/or S (sales)
    if "trade_type" in result.columns:
        before = len(result)
        tt_upper = result["trade_type"].astype(str).str.strip().str.upper()
        allowed = filters.get("trade_types", ["P"])  # default: purchases only
        mask = tt_upper.str.startswith(tuple(allowed))
        result = result[mask]
        result["is_sale"] = tt_upper.str.startswith("S")
        logger.info("Trade type filter (%s): %d → %d", allowed, before, len(result))

    # Filter: minimum transaction value per individual trade
    if "value" in result.columns:
        before = len(result)
        result = result[result["value"] >= filters["min_transaction_value"]]
        logger.info("Min value filter ($%.0f): %d → %d", filters["min_transaction_value"], before, len(result))

    # Filter: C-suite only (exclude pure 10% owners and board-only directors)
    if filters.get("csuite_only", False) and "title" in result.columns:
        before = len(result)
        result = result[result["title"].apply(is_csuite)]
        logger.info("C-suite filter: %d → %d (removed %d non-operating-officer trades)",
                    before, len(result), before - len(result))

    result = result.dropna(subset=["filing_date", "ticker"])
    result["ticker"] = result["ticker"].astype(str).str.strip().str.upper()
    result = result.reset_index(drop=True)
    logger.info("After basic filters: %d individual trades", len(result))
    return result


def filter_routine_insiders(df: pd.DataFrame, n_years: int = 3) -> pd.DataFrame:
    """
    Remove "routine" insiders: those who traded in the same calendar month
    for 3+ consecutive prior years (Cohen, Malloy & Pomorski 2012).

    For each (ticker, insider_name, filing_month) tuple, check if the insider
    traded in the same month in each of the prior 3 years. If so, mark as routine
    and remove all their trades.
    """
    if "insider_name" not in df.columns or "filing_date" not in df.columns:
        logger.info("Skipping routine filter: missing insider_name or filing_date columns")
        return df

    df = df.copy()
    df["_year"] = pd.DatetimeIndex(df["filing_date"]).year
    df["_month"] = pd.DatetimeIndex(df["filing_date"]).month

    # Build lookup: {(ticker, insider, month): set of years}
    trade_history: dict = {}
    for _, row in df.iterrows():
        key = (
            str(row.get("ticker", "")),
            str(row.get("insider_name", "")),
            int(row["_month"]),
        )
        trade_history.setdefault(key, set()).add(int(row["_year"]))

    def is_routine(row) -> bool:
        key = (
            str(row.get("ticker", "")),
            str(row.get("insider_name", "")),
            int(row["_month"]),
        )
        current_year = int(row["_year"])
        # Only look at years strictly before the current row's year to avoid look-ahead bias
        years = sorted(y for y in trade_history.get(key, set()) if y < current_year)
        if len(years) < n_years:
            return False
        # Check for n_years consecutive years anywhere in the history
        for i in range(len(years) - n_years + 1):
            if years[i + n_years - 1] - years[i] == n_years - 1:
                return True
        return False

    before = len(df)
    df["_is_routine"] = df.apply(is_routine, axis=1)
    df = df[~df["_is_routine"]].drop(columns=["_is_routine", "_year", "_month"])
    logger.info("Routine filter: removed %d trades, %d remain", before - len(df), len(df))
    return df.reset_index(drop=True)


def group_events(df: pd.DataFrame, window_days: int = 30) -> pd.DataFrame:
    """
    Group individual trades into events using a 30-day rolling window per ticker.

    Algorithm:
      1. Sort trades by (ticker, filing_date).
      2. For each ticker, starting at the earliest ungrouped trade:
         - Collect all trades with filing_date <= anchor_date + window_days.
         - Record this group as one event.
         - Advance to the next ungrouped trade and repeat.
      3. For each event group, compute:
         - filing_date     = last filing_date in the group (entry trigger)
         - event_start_date = first filing_date in the group
         - n_insiders      = distinct insider names
         - total_value     = sum of trade values
         - max_single_value = max aggregate value from any single insider
         - avg_value_per_insider
         - insider_names   = semicolon-joined list (first 5)
         - title_weights   = per-trade list (for quality score)
         - all confidence score components

    Returns one row per event (not per trade).
    """
    df = df.sort_values(["ticker", "filing_date"]).reset_index(drop=True)

    event_rows = []
    tickers = df["ticker"].unique()

    for ticker in tickers:
        ticker_mask = df["ticker"] == ticker
        tick_df = df[ticker_mask].reset_index(drop=True)

        # Track which trades have been assigned to an event
        assigned = [False] * len(tick_df)
        i = 0

        while i < len(tick_df):
            # Skip already-assigned trades
            if assigned[i]:
                i += 1
                continue

            # Start a new event at trade i
            anchor_date = tick_df.loc[i, "filing_date"]
            if pd.isna(anchor_date):
                assigned[i] = True
                i += 1
                continue

            window_end = anchor_date + pd.Timedelta(days=window_days)

            # Collect all unassigned trades in [anchor_date, window_end]
            group_indices = []
            for j in range(i, len(tick_df)):
                d = tick_df.loc[j, "filing_date"]
                if pd.isna(d):
                    continue
                if d <= window_end:
                    group_indices.append(j)
                    assigned[j] = True
                else:
                    break  # Sorted, so no need to scan further

            if not group_indices:
                assigned[i] = True
                i += 1
                continue

            group = tick_df.loc[group_indices]

            # Aggregate event fields
            event_start_date = group["filing_date"].min()
            event_trigger_date = group["filing_date"].max()   # Use last filing as entry trigger

            # Insider aggregation
            has_insider_name = "insider_name" in group.columns
            if has_insider_name:
                # Per-insider aggregated values
                insider_totals = (
                    group.groupby("insider_name")["value"]
                    .sum()
                    if "value" in group.columns
                    else group.groupby("insider_name").size().rename("value").astype(float)
                )
                n_distinct_insiders = int(group["insider_name"].nunique())
                max_single_value = float(insider_totals.max()) if not insider_totals.empty else 0.0
                insider_names_list = list(group["insider_name"].dropna().unique())[:5]
                insider_names_str = ";".join(str(x) for x in insider_names_list)
            else:
                n_distinct_insiders = len(group)
                max_single_value = float(group["value"].max()) if "value" in group.columns else 0.0
                insider_names_str = ""

            total_value = float(group["value"].sum()) if "value" in group.columns else 0.0
            avg_value_per_insider = total_value / n_distinct_insiders if n_distinct_insiders > 0 else 0.0

            # Title weights per individual trade
            if "title" in group.columns:
                title_weights = [get_title_weight(t) for t in group["title"]]
            else:
                title_weights = [DEFAULT_TITLE_WEIGHT] * len(group)

            # Compute confidence score
            conf = compute_confidence_score(
                total_value=total_value,
                n_distinct_insiders=n_distinct_insiders,
                title_weights=title_weights,
                max_single_value=max_single_value,
            )

            # Company name (take first non-null value)
            company = ""
            if "company" in group.columns:
                company_vals = group["company"].dropna()
                company = str(company_vals.iloc[0]) if len(company_vals) > 0 else ""

            event_row = {
                "ticker": ticker,
                "filing_date": event_trigger_date,
                "event_start_date": event_start_date,
                "n_insiders": n_distinct_insiders,
                "cluster_size": n_distinct_insiders,   # Alias for compatibility
                "total_value": round(total_value, 2),
                "max_single_value": round(max_single_value, 2),
                "avg_value_per_insider": round(avg_value_per_insider, 2),
                "value_score": conf["value_score"],
                "breadth_score": conf["breadth_score"],
                "quality_score": conf["quality_score"],
                "concentration": conf["concentration"],
                "confidence_score": conf["confidence_score"],
                "is_cluster": n_distinct_insiders >= 2,
                "insider_names": insider_names_str,
                "company": company,
            }
            event_rows.append(event_row)

            # Advance to the next unassigned trade
            i += 1

    result = pd.DataFrame(event_rows)
    if not result.empty:
        result = result.sort_values(["ticker", "filing_date"]).reset_index(drop=True)

    logger.info(
        "Event grouping complete: %d individual trades → %d events (%d clusters)",
        len(df),
        len(result),
        int(result["is_cluster"].sum()) if not result.empty else 0,
    )
    return result


def add_market_cap_filter(df: pd.DataFrame, min_cap: float, max_cap: float) -> pd.DataFrame:
    """
    Add market cap from yfinance and filter to the $100M-$2B alpha zone.
    Note: yfinance `info` calls are slow (~0.5-2s per ticker). Cached per run.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning(
            "yfinance not installed. Skipping market cap filter. Install: pip install yfinance"
        )
        return df

    tickers = df["ticker"].unique()
    cap_cache = {}
    logger.info("Fetching market caps for %d tickers via yfinance...", len(tickers))

    for i, ticker in enumerate(tickers):
        if i % 50 == 0 and i > 0:
            logger.info("  %d/%d tickers processed...", i, len(tickers))
        try:
            info = yf.Ticker(ticker).fast_info
            cap = getattr(info, "market_cap", None) or getattr(info, "marketCap", None)
            cap_cache[ticker] = float(cap) if cap else None
        except Exception:
            cap_cache[ticker] = None
        time.sleep(0.1)

    df = df.copy()
    df["market_cap"] = df["ticker"].map(cap_cache)
    before = len(df)
    df = df[
        df["market_cap"].notna()
        & (df["market_cap"] >= min_cap)
        & (df["market_cap"] <= max_cap)
    ]
    logger.info(
        "Market cap filter ($%.0fM-$%.0fB): removed %d, %d remain",
        min_cap / 1e6,
        max_cap / 1e9,
        before - len(df),
        len(df),
    )
    return df.reset_index(drop=True)


def build_calendar(input_path: Path, input_format: str, filters: dict) -> pd.DataFrame:
    """Build the event calendar from input data and apply all filters."""
    if input_format == "openinsider":
        df = parse_openinsider_csv(input_path, filters)
    else:
        logger.error(
            "Unsupported format: %s. Currently supported: openinsider", input_format
        )
        sys.exit(1)

    if df.empty:
        logger.error("No events after basic filters.")
        return df

    # Routine insider filter (Cohen et al. 2012)
    df = filter_routine_insiders(df, n_years=filters.get("routine_consecutive_years", 3))

    if df.empty:
        logger.error("No events after routine insider filter.")
        return df

    # Group individual trades into events (30-day rolling window)
    events = group_events(df, window_days=filters.get("event_window_days", 30))

    # Market cap filter on events (optional, slow)
    if filters.get("apply_market_cap_filter", False):
        events = add_market_cap_filter(
            events,
            min_cap=filters["min_market_cap"],
            max_cap=filters["max_market_cap"],
        )

    return events


def main():
    parser = argparse.ArgumentParser(
        description="Build insider buy event calendar with confidence scoring"
    )
    parser.add_argument("--input", type=Path, required=True, help="Input CSV file")
    parser.add_argument(
        "--format",
        default="openinsider",
        choices=["openinsider", "edgar-index"],
        help="Input format",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "data" / "events.csv",
    )
    parser.add_argument(
        "--cluster-only",
        action="store_true",
        help="Keep only events with 2+ insiders (n_insiders >= 2)",
    )
    parser.add_argument(
        "--csuite-only",
        action="store_true",
        help="Keep only trades from C-suite/senior officers (exclude pure 10%% owners and board directors)",
    )
    parser.add_argument(
        "--min-value",
        type=float,
        default=50_000,
        help="Min transaction value per individual trade ($)",
    )
    parser.add_argument("--min-cap", type=float, default=100e6, help="Min market cap $")
    parser.add_argument("--max-cap", type=float, default=2e9, help="Max market cap $")
    parser.add_argument(
        "--no-market-cap",
        action="store_true",
        help="Skip market cap filter (much faster)",
    )
    parser.add_argument(
        "--event-window",
        type=int,
        default=30,
        help="Rolling window (days) for grouping trades into events",
    )
    parser.add_argument(
        "--trade-type",
        choices=["buy", "sell", "both"],
        default="buy",
        help="Filter to P-code purchases, S-code sales, or both",
    )
    args = parser.parse_args()

    filters = {**DEFAULTS}
    filters["min_transaction_value"] = args.min_value
    filters["min_market_cap"] = args.min_cap
    filters["max_market_cap"] = args.max_cap
    filters["apply_market_cap_filter"] = not args.no_market_cap
    filters["event_window_days"] = args.event_window
    filters["csuite_only"] = args.csuite_only
    # Map trade-type to allowed codes
    type_map = {"buy": ["P"], "sell": ["S"], "both": ["P", "S"]}
    filters["trade_types"] = type_map[args.trade_type]

    df = build_calendar(args.input, args.format, filters)

    if df.empty:
        logger.error("No events to save.")
        sys.exit(1)

    # Cluster-only filter: keep events with 2+ distinct insiders
    if args.cluster_only:
        before = len(df)
        df = df[df["n_insiders"] >= 2]
        logger.info("Cluster filter (n_insiders >= 2): %d → %d events", before, len(df))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)
    logger.info("Saved event calendar: %s (%d events)", args.output, len(df))

    # Terminal summary
    print("\n=== Event Calendar Summary ===")
    print(f"Total events:       {len(df)}")
    print(f"Cluster events:     {df['is_cluster'].sum()}  (n_insiders >= 2)")
    print(f"Single-insider:     {(~df['is_cluster']).sum()}")
    print(f"Unique tickers:     {df['ticker'].nunique()}")

    if "filing_date" in df.columns:
        print(f"Date range:         {df['filing_date'].min()} to {df['filing_date'].max()}")

    if "confidence_score" in df.columns:
        cs = df["confidence_score"]
        print(f"\nConfidence score stats:")
        print(f"  Mean:    {cs.mean():.2f}")
        print(f"  Median:  {cs.median():.2f}")
        print(f"  P25:     {cs.quantile(0.25):.2f}")
        print(f"  P75:     {cs.quantile(0.75):.2f}")
        print(f"  Max:     {cs.max():.2f}")

    if "n_insiders" in df.columns:
        print(f"\nInsider count distribution:")
        vc = df["n_insiders"].value_counts().sort_index()
        for n, count in vc.items():
            label = f"  {n} insider{'s' if n != 1 else '': <10}: {count}"
            print(label)

    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
