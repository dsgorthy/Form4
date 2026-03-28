#!/usr/bin/env python3
"""
Build V2 event calendars from insiders.db for both buy and sell legs.

Buy leg:  1+ insider, $1M+ total value in 30-day window (V2 loosened from V1's 2+/$5M+)
Sell leg: 2+ distinct insiders in 30-day window, $1M+ aggregate value

Outputs event CSVs compatible with run_event_study.py:
  - data/events_v2_buys.csv
  - data/events_v2_sells.csv

Usage:
    python build_v2_events.py
    python build_v2_events.py --buy-min-value 500000 --sell-min-insiders 3
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR.parent / "insider_catalog" / "insiders.db"
DATA_DIR = Path(os.environ.get(
    "V2_DATA_DIR",
    str(SCRIPT_DIR.parent.parent / "pipelines" / "insider_study" / "data"),
))

# V2 filter defaults
BUY_MIN_INSIDERS = 1
BUY_MIN_VALUE = 1_000_000
BUY_MIN_TRADE_VALUE = 50_000  # per-trade minimum

SELL_MIN_INSIDERS = 2
SELL_MIN_VALUE = 1_000_000
SELL_MIN_TRADE_VALUE = 100_000  # per-trade minimum for sells

EVENT_WINDOW_DAYS = 30

# Reuse title weight logic from build_event_calendar
TITLE_WEIGHT_RULES = [
    (["ceo", "chief exec"], 3.0),
    (["chairman", "exec chair", "executive chair"], 3.0),
    (["cfo", "chief financial"], 2.5),
    (["president"], 2.5),
    (["10% owner", "10 percent owner", "10pct"], 2.5),
    (["coo", "chief operating"], 2.0),
    (["svp", "evp", "senior vp", "senior vice", "exec vp", "executive vp"], 1.8),
    (["vp", "vice president"], 1.5),
    (["director", "board"], 1.5),
    (["treasurer", "secretary"], 1.2),
]
DEFAULT_TITLE_WEIGHT = 1.0
CONF_NORMALIZER = 112.5


def get_title_weight(title: str) -> float:
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
    value_score = min(5.0, math.log10(max(1.0, total_value) / 25_000) + 1.0)
    breadth_score = min(5.0, 1.0 + math.log2(max(1, n_distinct_insiders)))
    quality_score = float(np.mean(title_weights)) if title_weights else DEFAULT_TITLE_WEIGHT
    concentration = max_single_value / total_value if total_value > 0 else 1.0
    concentration_bonus = concentration * 0.5
    raw = value_score * breadth_score * quality_score * (1.0 + concentration_bonus)
    confidence_score = min(100.0, raw / CONF_NORMALIZER * 100.0)
    return {
        "value_score": round(value_score, 4),
        "breadth_score": round(breadth_score, 4),
        "quality_score": round(quality_score, 4),
        "concentration": round(concentration, 4),
        "confidence_score": round(confidence_score, 4),
    }


def load_trades(db_path: Path, trade_type: str, min_trade_value: float) -> pd.DataFrame:
    """Load trades from insiders.db."""
    conn = sqlite3.connect(str(db_path))
    query = """
        SELECT
            t.trade_id,
            t.ticker,
            t.filing_date,
            t.trade_date,
            t.value,
            t.title,
            t.is_csuite,
            t.title_weight,
            COALESCE(t.effective_insider_id, t.insider_id) as eid,
            i.name as insider_name,
            t.company
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.trade_type = ?
          AND t.value >= ?
          AND t.filing_date IS NOT NULL
          AND t.ticker IS NOT NULL
        ORDER BY t.ticker, t.filing_date
    """
    df = pd.read_sql_query(query, conn, params=(trade_type, min_trade_value))
    conn.close()
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df = df.dropna(subset=["filing_date"]).reset_index(drop=True)
    logger.info("Loaded %d %s trades (min $%.0f)", len(df), trade_type, min_trade_value)
    return df


def group_events(
    df: pd.DataFrame,
    window_days: int,
    min_insiders: int,
    min_total_value: float,
) -> pd.DataFrame:
    """
    Group trades into events using 30-day rolling window per ticker.
    Then filter by min_insiders and min_total_value.
    """
    df = df.sort_values(["ticker", "filing_date"]).reset_index(drop=True)
    event_rows = []

    for ticker in df["ticker"].unique():
        tick_df = df[df["ticker"] == ticker].reset_index(drop=True)
        assigned = [False] * len(tick_df)
        i = 0

        while i < len(tick_df):
            if assigned[i]:
                i += 1
                continue

            anchor_date = tick_df.loc[i, "filing_date"]
            if pd.isna(anchor_date):
                assigned[i] = True
                i += 1
                continue

            window_end = anchor_date + pd.Timedelta(days=window_days)

            group_indices = []
            for j in range(i, len(tick_df)):
                d = tick_df.loc[j, "filing_date"]
                if pd.isna(d):
                    continue
                if d <= window_end:
                    group_indices.append(j)
                    assigned[j] = True
                else:
                    break

            if not group_indices:
                assigned[i] = True
                i += 1
                continue

            group = tick_df.loc[group_indices]

            # Aggregate
            event_start_date = group["filing_date"].min()
            event_trigger_date = group["filing_date"].max()

            # Per-insider aggregation using effective_insider_id
            insider_totals = group.groupby("eid")["value"].sum()
            n_distinct = int(group["eid"].nunique())
            max_single_value = float(insider_totals.max()) if not insider_totals.empty else 0.0
            total_value = float(group["value"].sum())

            # Check filters
            if n_distinct < min_insiders or total_value < min_total_value:
                i += 1
                continue

            avg_value = total_value / n_distinct if n_distinct > 0 else 0.0
            insider_names = ";".join(group["insider_name"].dropna().unique()[:5])
            title_weights = [get_title_weight(t) for t in group["title"]]
            conf = compute_confidence_score(total_value, n_distinct, title_weights, max_single_value)

            company = ""
            company_vals = group["company"].dropna()
            if len(company_vals) > 0:
                company = str(company_vals.iloc[0])

            event_rows.append({
                "ticker": ticker,
                "filing_date": event_trigger_date,
                "event_start_date": event_start_date,
                "n_insiders": n_distinct,
                "cluster_size": n_distinct,
                "total_value": round(total_value, 2),
                "max_single_value": round(max_single_value, 2),
                "avg_value_per_insider": round(avg_value, 2),
                **conf,
                "is_cluster": n_distinct >= 2,
                "insider_names": insider_names,
                "company": company,
            })

            i += 1

    result = pd.DataFrame(event_rows)
    if not result.empty:
        result = result.sort_values(["ticker", "filing_date"]).reset_index(drop=True)

    n_clusters = int(result["is_cluster"].sum()) if not result.empty else 0
    logger.info(
        "Event grouping: %d trades -> %d events (%d clusters), "
        "filter: %d+ insiders, $%.0f+ value",
        len(df), len(result), n_clusters, min_insiders, min_total_value,
    )
    return result


def main():
    parser = argparse.ArgumentParser(description="Build V2 event calendars from insiders.db")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--buy-min-insiders", type=int, default=BUY_MIN_INSIDERS)
    parser.add_argument("--buy-min-value", type=float, default=BUY_MIN_VALUE)
    parser.add_argument("--sell-min-insiders", type=int, default=SELL_MIN_INSIDERS)
    parser.add_argument("--sell-min-value", type=float, default=SELL_MIN_VALUE)
    parser.add_argument("--window-days", type=int, default=EVENT_WINDOW_DAYS)
    parser.add_argument("--side", choices=["buy", "sell", "both"], default="both")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.side in ("buy", "both"):
        buy_trades = load_trades(args.db, "buy", BUY_MIN_TRADE_VALUE)
        buy_events = group_events(
            buy_trades, args.window_days, args.buy_min_insiders, args.buy_min_value
        )
        buy_path = args.output_dir / "events_v2_buys.csv"
        buy_events.to_csv(buy_path, index=False)
        logger.info("Buy events saved: %s (%d events)", buy_path, len(buy_events))
        print(f"\nBuy events: {len(buy_events)} ({buy_events['is_cluster'].sum()} clusters)")
        if not buy_events.empty:
            print(f"  Date range: {buy_events['filing_date'].min()} to {buy_events['filing_date'].max()}")
            print(f"  Unique tickers: {buy_events['ticker'].nunique()}")

    if args.side in ("sell", "both"):
        sell_trades = load_trades(args.db, "sell", SELL_MIN_TRADE_VALUE)
        sell_events = group_events(
            sell_trades, args.window_days, args.sell_min_insiders, args.sell_min_value
        )
        sell_path = args.output_dir / "events_v2_sells.csv"
        sell_events.to_csv(sell_path, index=False)
        logger.info("Sell events saved: %s (%d events)", sell_path, len(sell_events))
        print(f"\nSell events: {len(sell_events)} ({sell_events['is_cluster'].sum()} clusters)")
        if not sell_events.empty:
            print(f"  Date range: {sell_events['filing_date'].min()} to {sell_events['filing_date'].max()}")
            print(f"  Unique tickers: {sell_events['ticker'].nunique()}")


if __name__ == "__main__":
    main()
