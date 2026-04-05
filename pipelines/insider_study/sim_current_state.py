#!/usr/bin/env python3
"""Simulate 3 strategies from 2020-01-01 to today, report current portfolio state.

Outputs per strategy:
- Cash balance (for Alpaca account reset)
- Open positions (ticker, shares, entry date, entry price)
- Mark-to-market equity
"""

from __future__ import annotations

import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipelines.insider_study.grid_search_strategies import (
    GridConfig,
    OpenPos,
    PriceCache,
    _build_trading_calendar,
    _filed_during_market_hours,
    compute_entry_prices,
    dedup_events,
    filter_events_for_config,
    DB_PATH,
    PRICES_DB,
    INTRADAY_DB,
    STARTING_CAPITAL,
    load_quality_momentum_events,
    load_10b5_1_surprise_events,
    load_reversal_dip_events,
)

START = "2020-01-01"
END = date.today().isoformat()


def run_sim_keep_open(
    events: list[dict],
    prices: PriceCache,
    calendar: list[str],
    cfg: GridConfig,
) -> tuple[float, list[OpenPos], int]:
    """Day-by-day sim that keeps positions open at the end.

    Returns (equity, open_positions, n_closed).
    """
    equity = STARTING_CAPITAL
    open_positions: list[OpenPos] = []
    n_closed = 0
    max_dd = 0.0
    peak_equity = STARTING_CAPITAL
    halted = False

    events_by_date: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        ed = e.get("_entry_date")
        if ed:
            events_by_date[ed].append(e)

    for today in calendar:
        if today > END:
            break

        day_pnl = 0.0

        # Check exits
        to_close = []
        for pos in open_positions:
            pos.days_held += 1
            close_price = prices.get_close(pos.ticker, today)
            if not close_price or close_price <= 0:
                continue

            if close_price > pos.peak_price:
                pos.peak_price = close_price

            should_exit = False
            if pos.days_held >= cfg.hold_days:
                should_exit = True
            if cfg.stop_loss is not None:
                pnl_pct = (close_price - pos.entry_price) / pos.entry_price
                if pnl_pct <= cfg.stop_loss:
                    should_exit = True
            if cfg.exit_type == "trailing_stop" and pos.peak_price > 0:
                trail_pct = (close_price - pos.peak_price) / pos.peak_price
                if trail_pct <= -cfg.trailing_stop_pct:
                    should_exit = True

            if should_exit:
                to_close.append((pos, close_price))

        for pos, close_price in to_close:
            pnl_pct = (close_price - pos.entry_price) / pos.entry_price
            pnl_dollar = pos.dollar_amount * pnl_pct
            equity += pnl_dollar
            day_pnl += pnl_dollar
            n_closed += 1
            open_positions = [p for p in open_positions if p.trade_id != pos.trade_id]

        # Circuit breaker
        if equity / peak_equity - 1 < -cfg.circuit_breaker_dd:
            halted = True
        if equity > peak_equity:
            peak_equity = equity
            halted = False

        # Entries
        if not halted:
            held_tickers = {p.ticker for p in open_positions}
            entered_today = set()

            for event in sorted(events_by_date.get(today, []), key=lambda e: -e.get("_conviction", 0)):
                ticker = event["ticker"]
                if ticker in held_tickers or ticker in entered_today:
                    continue
                if len(open_positions) + len(entered_today) >= cfg.max_concurrent:
                    continue

                entry_price = event.get("_entry_price", 0)
                if not entry_price or entry_price <= 0 or entry_price < 2.0:
                    continue

                dollar_amount = equity * cfg.position_size
                pos = OpenPos(
                    trade_id=event["trade_id"],
                    ticker=ticker,
                    entry_date=today,
                    entry_price=entry_price,
                    dollar_amount=dollar_amount,
                    conviction=event.get("_conviction", 5.0),
                    peak_price=entry_price,
                )
                open_positions.append(pos)
                entered_today.add(ticker)
                held_tickers.add(ticker)

    return equity, open_positions, n_closed


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    prices_conn = sqlite3.connect(str(PRICES_DB))
    intraday_conn = sqlite3.connect(str(INTRADAY_DB)) if INTRADAY_DB.exists() else None

    # Build shared price cache
    print("Loading price data...", flush=True)
    t0 = time.time()

    all_tickers_row = conn.execute(
        "SELECT DISTINCT ticker FROM trades WHERE trans_code = 'P' AND filing_date >= ?",
        (START,),
    ).fetchall()
    all_tickers = {r[0] for r in all_tickers_row}

    cache_start = (datetime.strptime(START, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    cache_end = (datetime.strptime(END, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")

    prices = PriceCache(prices_conn, all_tickers, cache_start, cache_end)
    calendar = _build_trading_calendar(prices_conn, START, cache_end)
    prices_conn.close()

    print(f"  Price cache: {len(all_tickers)} tickers, {len(calendar)} trading days ({time.time()-t0:.1f}s)\n")

    strategies = [
        {
            "name": "quality_momentum",
            "config": GridConfig(
                strategy="quality", position_size=0.10, max_concurrent=10,
                at_capacity="skip", hold_days=30, stop_loss=None,
                exit_type="fixed_hold", trailing_stop_pct=0.0,
                circuit_breaker_dd=0.15,
                filters={"grade_filter": "A+/A"},
            ),
            "loader": load_quality_momentum_events,
        },
        {
            "name": "reversal_dip",
            "config": GridConfig(
                strategy="reversal", position_size=0.10, max_concurrent=10,
                at_capacity="skip", hold_days=21, stop_loss=None,
                exit_type="fixed_hold", trailing_stop_pct=0.0,
                circuit_breaker_dd=0.15,
                filters={
                    "min_consecutive_sells": 10,
                    "dip_threshold_3mo": -0.25,
                    "exclude_10b5_1": True,
                },
            ),
            "loader": load_reversal_dip_events,
        },
        {
            "name": "tenb51_surprise",
            "config": GridConfig(
                strategy="tenb", position_size=0.20, max_concurrent=5,
                at_capacity="skip", hold_days=60, stop_loss=-0.20,
                exit_type="trailing_stop", trailing_stop_pct=0.15,
                circuit_breaker_dd=0.15,
                filters={"min_10b5_1_sells": 5, "require_momentum": False},
            ),
            "loader": load_10b5_1_surprise_events,
        },
    ]

    for strat in strategies:
        name = strat["name"]
        cfg = strat["config"]

        print(f"{'='*70}")
        print(f"STRATEGY: {name}")
        print(f"{'='*70}")

        # Load and prep events (same as grid search)
        raw_events = strat["loader"](conn, START, END)
        events = dedup_events(raw_events)
        print(f"  {len(raw_events)} raw events -> {len(events)} after dedup")

        tagged = [(cfg.strategy, e) for e in events]
        compute_entry_prices(tagged, intraday_conn, prices, calendar)
        events = [e for e in events if e.get("_entry_price") and e["_entry_price"] > 0]

        # Apply strategy filters
        filtered = filter_events_for_config(events, cfg)
        print(f"  {len(filtered)} events after filtering")

        # Run sim
        equity, open_positions, n_closed = run_sim_keep_open(filtered, prices, calendar, cfg)

        # Mark-to-market open positions
        total_open_value = 0.0
        invested = sum(p.dollar_amount for p in open_positions)
        cash = equity - invested

        print(f"\n  Account Summary:")
        print(f"  Starting capital:  $100,000.00")
        print(f"  Realized equity:   ${equity:>12,.2f}  (from {n_closed} closed trades)")
        print(f"  Cash balance:      ${cash:>12,.2f}")
        print(f"  Open cost basis:   ${invested:>12,.2f}")

        if open_positions:
            print(f"\n  OPEN POSITIONS — buy these at market open:")
            print(f"  {'Ticker':<8} {'Entry Date':<12} {'Entry $':>10} {'Shares':>8} {'Cost Basis':>12} {'Days':>6}")
            print(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*8} {'-'*12} {'-'*6}")
            for pos in sorted(open_positions, key=lambda p: p.entry_date):
                shares = int(pos.dollar_amount / pos.entry_price)
                last_c = prices.get_close_on_or_before(pos.ticker, END)
                mtm = pos.dollar_amount * ((last_c / pos.entry_price) if last_c else 1.0)
                total_open_value += mtm
                print(f"  {pos.ticker:<8} {pos.entry_date:<12} ${pos.entry_price:>8,.2f} {shares:>8} ${pos.dollar_amount:>10,.2f} {pos.days_held:>6}")

        account_equity = cash + total_open_value
        print(f"\n  Mark-to-market:    ${account_equity:>12,.2f}")
        print(f"\n  >>> SET ALPACA BALANCE TO: ${account_equity:,.2f}")
        print()

    conn.close()
    if intraday_conn:
        intraday_conn.close()


if __name__ == "__main__":
    main()
