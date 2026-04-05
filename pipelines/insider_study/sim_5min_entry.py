#!/usr/bin/env python3
"""
Compare entry strategies: T+1 open (baseline) vs 5-min intraday entry.

For each qualifying trade signal:
  - Baseline: enter at next trading day's open (current approach)
  - 5-min: if filed_at is during market hours (9:30-16:00 ET),
    enter at the close of the 5-min bar immediately after filed_at.
    If filed after hours, enter at next day's open (same as baseline).

Both strategies use identical exit logic: 30d time exit or -15% stop.

Usage:
    python3 pipelines/insider_study/sim_5min_entry.py
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"
INTRADAY_DB = DB_PATH.parent / "intraday.db"

START = "2020-01-01"
END = "2026-03-31"
STOP_PCT = -0.15
TARGET_HOLD = 30


@dataclass
class Trade:
    trade_id: int
    ticker: str
    thesis: str
    filing_date: str
    filed_at: str | None
    entry_price_baseline: float | None  # T+1 open
    entry_price_5min: float | None      # 5-min bar close after filed_at
    entry_date_baseline: str | None
    entry_date_5min: str | None
    exit_price: float | None
    exit_date: str | None
    exit_reason: str | None


def _load_events(conn: sqlite3.Connection) -> list[dict]:
    """Load all qualifying buy events with filed_at."""
    rows = conn.execute("""
        SELECT t.trade_id, t.ticker, t.filing_date, t.filed_at, t.title,
               t.is_csuite, t.is_rare_reversal, t.consecutive_sells_before,
               t.dip_1mo, t.dip_3mo, t.above_sma50, t.above_sma200,
               t.is_largest_ever, t.is_recurring, t.is_tax_sale, t.cohen_routine
        FROM trades t
        WHERE t.trans_code = 'P'
          AND t.filing_date BETWEEN ? AND ?
          AND t.filed_at IS NOT NULL AND t.filed_at != ''
          AND t.price > 2.0
          AND (t.superseded_by IS NULL)
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
          AND COALESCE(t.is_recurring, 0) = 0
          AND COALESCE(t.is_tax_sale, 0) = 0
          AND COALESCE(t.cohen_routine, 0) = 0
        ORDER BY t.filing_date, t.trade_id
    """, (START, END)).fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM trades LIMIT 0").description]
    # Use the actual columns from the query
    cols = ["trade_id", "ticker", "filing_date", "filed_at", "title",
            "is_csuite", "is_rare_reversal", "consecutive_sells_before",
            "dip_1mo", "dip_3mo", "above_sma50", "above_sma200",
            "is_largest_ever", "is_recurring", "is_tax_sale", "cohen_routine"]
    return [dict(zip(cols, r)) for r in rows]


def _classify_thesis(e: dict) -> str | None:
    """Classify an event into a thesis (reversal, dip_cluster, momentum)."""
    if e.get("is_rare_reversal") == 1 and (e.get("consecutive_sells_before") or 0) >= 5:
        return "reversal"
    dip1 = e.get("dip_1mo") or 0
    dip3 = e.get("dip_3mo") or 0
    if dip1 <= -0.15 or dip3 <= -0.25:
        return "dip_cluster"
    if e.get("above_sma50") == 1 and e.get("above_sma200") == 1 and e.get("is_largest_ever") == 1:
        return "momentum_largest"
    return None


def _filed_during_market(filed_at: str) -> bool:
    """Check if filed_at is during regular market hours (9:30-16:00 ET)."""
    if not filed_at or len(filed_at) < 16:
        return False
    hhmm = filed_at[11:16]  # "HH:MM"
    return "09:30" <= hhmm < "16:00"


def _next_5min_bar_time(filed_at: str) -> str:
    """Get the timestamp of the next 5-min bar close after filed_at.

    If filed at 10:02, next bar closes at 10:05.
    If filed at 10:05, next bar closes at 10:10 (bar at 10:05 was already open).
    """
    hh = int(filed_at[11:13])
    mm = int(filed_at[14:16])
    ss = int(filed_at[17:19]) if len(filed_at) >= 19 else 0

    # Round up to next 5-min boundary
    total_min = hh * 60 + mm
    if ss > 0 or mm % 5 != 0:
        total_min = ((total_min // 5) + 1) * 5
    else:
        # Exactly on a 5-min mark — that bar is already open, take next
        total_min += 5

    # If past market close (16:00 = 960 min), return None
    if total_min >= 960:
        return None

    bar_hh = total_min // 60
    bar_mm = total_min % 60
    date_part = filed_at[:10]
    return f"{date_part}T{bar_hh:02d}:{bar_mm:02d}:00"


def main():
    print("Loading data...", flush=True)
    t0 = time.time()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA busy_timeout=30000")
    prices_conn = sqlite3.connect(str(PRICES_DB))
    intraday_conn = sqlite3.connect(str(INTRADAY_DB))

    # Build trading calendar
    cal = [r[0] for r in prices_conn.execute(
        "SELECT DISTINCT date FROM daily_prices WHERE ticker='SPY' AND date BETWEEN ? AND ? ORDER BY date",
        (START, END),
    ).fetchall()]
    cal_set = set(cal)

    def next_trading_day(date: str) -> str | None:
        """Find the next trading day after date."""
        for d in cal:
            if d > date:
                return d
        return None

    # Load events
    events = _load_events(conn)
    print(f"  {len(events):,} qualifying buy events with filed_at", flush=True)

    # Process each event
    results_baseline = []  # (trade_id, ticker, thesis, entry_price, exit_price, pnl_pct, exit_reason)
    results_5min = []

    skipped_no_thesis = 0
    skipped_no_entry = 0
    used_5min = 0
    used_next_open = 0

    for i, e in enumerate(events):
        thesis = _classify_thesis(e)
        if not thesis:
            skipped_no_thesis += 1
            continue

        ticker = e["ticker"]
        filing_date = e["filing_date"]
        filed_at = e["filed_at"]

        # --- Baseline: T+1 open ---
        entry_day = next_trading_day(filing_date)
        if not entry_day:
            skipped_no_entry += 1
            continue

        baseline_open = prices_conn.execute(
            "SELECT open FROM daily_prices WHERE ticker=? AND date=?",
            (ticker, entry_day),
        ).fetchone()
        if not baseline_open or not baseline_open[0]:
            skipped_no_entry += 1
            continue
        baseline_entry = baseline_open[0]

        # --- 5-min: enter at bar after filed_at if during market hours ---
        fivemin_entry = None
        fivemin_date = None

        if _filed_during_market(filed_at):
            bar_ts = _next_5min_bar_time(filed_at)
            if bar_ts:
                row = intraday_conn.execute(
                    "SELECT close, timestamp FROM intraday_bars WHERE ticker=? AND timestamp=? AND timeframe='5Min'",
                    (ticker, bar_ts),
                ).fetchone()
                if row and row[0]:
                    fivemin_entry = row[0]
                    fivemin_date = bar_ts[:10]
                    used_5min += 1

        if fivemin_entry is None:
            # Fall back to next day open (same as baseline)
            fivemin_entry = baseline_entry
            fivemin_date = entry_day
            used_next_open += 1

        # --- Simulate exit for both (using daily bars from entry_day forward) ---
        def _simulate_exit(entry_price: float, entry_date: str):
            days_held = 0
            peak = entry_price
            for d in cal:
                if d <= entry_date:
                    continue
                bar = prices_conn.execute(
                    "SELECT open, high, low, close FROM daily_prices WHERE ticker=? AND date=?",
                    (ticker, d),
                ).fetchone()
                if not bar or not bar[3]:
                    continue
                o, h, l, c = bar
                days_held += 1

                # Stop check on low
                if l and entry_price > 0:
                    drawdown = (l - entry_price) / entry_price
                    if drawdown <= STOP_PCT:
                        exit_price = entry_price * (1 + STOP_PCT)
                        pnl = STOP_PCT
                        return pnl, d, "stop_loss", days_held

                # Trailing stop: peak tracking
                if c > peak:
                    peak = c

                # Time exit
                if days_held >= TARGET_HOLD:
                    pnl = (c - entry_price) / entry_price
                    return pnl, d, "time_exit_30d", days_held

            # Still open — use last available close
            last_close = prices_conn.execute(
                "SELECT close FROM daily_prices WHERE ticker=? AND date<=? ORDER BY date DESC LIMIT 1",
                (ticker, END),
            ).fetchone()
            if last_close and last_close[0]:
                pnl = (last_close[0] - entry_price) / entry_price
                return pnl, END, "open", 0
            return None, None, None, 0

        pnl_b, exit_d_b, reason_b, hold_b = _simulate_exit(baseline_entry, entry_day)
        pnl_5, exit_d_5, reason_5, hold_5 = _simulate_exit(fivemin_entry, fivemin_date)

        if pnl_b is not None:
            results_baseline.append((e["trade_id"], ticker, thesis, baseline_entry, pnl_b, reason_b, hold_b))
        if pnl_5 is not None:
            results_5min.append((e["trade_id"], ticker, thesis, fivemin_entry, pnl_5, reason_5, hold_5))

        if (i + 1) % 5000 == 0:
            print(f"  {i+1}/{len(events)} events processed...", flush=True)

    elapsed = time.time() - t0
    print(f"\nProcessed in {elapsed:.0f}s", flush=True)
    print(f"  Classified: {len(results_baseline):,} trades", flush=True)
    print(f"  Skipped (no thesis): {skipped_no_thesis:,}", flush=True)
    print(f"  Skipped (no entry price): {skipped_no_entry:,}", flush=True)
    print(f"  5-min entries used: {used_5min:,}", flush=True)
    print(f"  Next-open fallback: {used_next_open:,}", flush=True)

    # --- Report ---
    def _report(label: str, trades: list):
        if not trades:
            print(f"\n{label}: No trades")
            return
        pnls = [t[4] for t in trades if t[4] is not None]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        stops = sum(1 for t in trades if t[5] == "stop_loss")
        avg = sum(pnls) / len(pnls) if pnls else 0
        med = sorted(pnls)[len(pnls) // 2] if pnls else 0
        wr = len(wins) / len(pnls) * 100 if pnls else 0
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0

        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"{'='*60}")
        print(f"  Trades:     {len(pnls):,}")
        print(f"  Win Rate:   {wr:.1f}%")
        print(f"  Avg Return: {avg*100:.2f}%")
        print(f"  Med Return: {med*100:.2f}%")
        print(f"  Avg Win:    {avg_win*100:.2f}%")
        print(f"  Avg Loss:   {avg_loss*100:.2f}%")
        print(f"  Stops Hit:  {stops:,} ({100*stops/len(pnls):.1f}%)")

        # By thesis
        for thesis in ["reversal", "dip_cluster", "momentum_largest"]:
            t_pnls = [t[4] for t in trades if t[2] == thesis and t[4] is not None]
            if not t_pnls:
                continue
            t_wins = [p for p in t_pnls if p > 0]
            t_wr = len(t_wins) / len(t_pnls) * 100
            t_avg = sum(t_pnls) / len(t_pnls)
            print(f"    {thesis:20s}: {len(t_pnls):5,} trades, {t_wr:.1f}% WR, {t_avg*100:+.2f}% avg")

    _report("BASELINE: T+1 Open Entry", results_baseline)
    _report("5-MIN: Intraday Entry (market hours) + T+1 Open (after hours)", results_5min)

    # Head-to-head comparison
    print(f"\n{'='*60}")
    print(f"  HEAD-TO-HEAD COMPARISON")
    print(f"{'='*60}")

    # Match by trade_id
    baseline_map = {t[0]: t[4] for t in results_baseline if t[4] is not None}
    fivemin_map = {t[0]: t[4] for t in results_5min if t[4] is not None}
    common = set(baseline_map.keys()) & set(fivemin_map.keys())

    if common:
        b_avg = sum(baseline_map[tid] for tid in common) / len(common)
        f_avg = sum(fivemin_map[tid] for tid in common) / len(common)
        delta = f_avg - b_avg

        better = sum(1 for tid in common if fivemin_map[tid] > baseline_map[tid])
        worse = sum(1 for tid in common if fivemin_map[tid] < baseline_map[tid])
        same = len(common) - better - worse

        # Only trades where 5-min was actually used (not fallback)
        fivemin_tids = {t[0] for t in results_5min if t[0] in {r[0] for r in results_baseline}}

        print(f"  Matched trades: {len(common):,}")
        print(f"  Baseline avg:   {b_avg*100:+.2f}%")
        print(f"  5-min avg:      {f_avg*100:+.2f}%")
        print(f"  Delta:          {delta*100:+.2f}%")
        print(f"  5-min better:   {better:,} ({100*better/len(common):.1f}%)")
        print(f"  5-min worse:    {worse:,} ({100*worse/len(common):.1f}%)")
        print(f"  Same:           {same:,}")

        # Trades that actually used 5-min entry (during market hours)
        intraday_tids = set()
        for t in results_5min:
            # Check if this trade used 5-min by comparing entry price to baseline
            b_trade = next((bt for bt in results_baseline if bt[0] == t[0]), None)
            if b_trade and t[3] != b_trade[3]:  # different entry price = used 5-min
                intraday_tids.add(t[0])

        if intraday_tids:
            b_intra = sum(baseline_map[tid] for tid in intraday_tids) / len(intraday_tids)
            f_intra = sum(fivemin_map[tid] for tid in intraday_tids) / len(intraday_tids)
            better_i = sum(1 for tid in intraday_tids if fivemin_map[tid] > baseline_map[tid])
            print(f"\n  --- Trades with actual 5-min entry (filed during market hours) ---")
            print(f"  Count:          {len(intraday_tids):,}")
            print(f"  Baseline avg:   {b_intra*100:+.2f}%")
            print(f"  5-min avg:      {f_intra*100:+.2f}%")
            print(f"  Delta:          {(f_intra-b_intra)*100:+.2f}%")
            print(f"  5-min better:   {better_i:,} ({100*better_i/len(intraday_tids):.1f}%)")

    conn.close()
    prices_conn.close()
    intraday_conn.close()


if __name__ == "__main__":
    main()
