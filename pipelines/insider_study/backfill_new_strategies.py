#!/usr/bin/env python3
"""Backfill strategy_portfolio table with simulated trades for new strategies.

Runs the same simulation as sim_current_state.py but writes each closed trade
to the strategy_portfolio table so the portfolio API can display them.

Usage:
    python3 pipelines/insider_study/backfill_new_strategies.py
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipelines.insider_study.grid_search_strategies import (
    GridConfig,
    OpenPos,
    PriceCache,
    _build_trading_calendar,
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
    load_reversal_quality_events,
)

START = "2020-01-01"
END = "2026-04-05"

STRATEGIES = [
    {
        "name": "quality_momentum",
        "display_name": "Form4 Quality + Momentum",
        "description": "A+/A insiders buying in uptrends. Sharpe 1.18, ~50 trades/yr, 30d hold.",
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
        "display_name": "Form4 Deep Reversal",
        "description": "Persistent sellers reversing into depressed stocks. Sharpe 1.08, ~20 trades/yr, 21d hold.",
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
        "display_name": "Form4 10b5-1 Surprise",
        "description": "Scheduled sellers breaking pattern to buy. Experimental, ~40 trades/yr, 60d hold.",
        "config": GridConfig(
            strategy="tenb", position_size=0.20, max_concurrent=5,
            at_capacity="skip", hold_days=60, stop_loss=-0.20,
            exit_type="trailing_stop", trailing_stop_pct=0.15,
            circuit_breaker_dd=0.15,
            filters={"min_10b5_1_sells": 5, "require_momentum": False},
        ),
        "loader": load_10b5_1_surprise_events,
    },
    {
        "name": "reversal_quality",
        "display_name": "Form4 Reversal + Quality",
        "description": "Proven insiders (A-B grade) breaking sell patterns. +4.0% at 30d, 61% WR, ~70 trades/yr.",
        "config": GridConfig(
            strategy="reversal_quality", position_size=0.10, max_concurrent=10,
            at_capacity="skip", hold_days=30, stop_loss=None,
            exit_type="fixed_hold", trailing_stop_pct=0.0,
            circuit_breaker_dd=0.15,
            filters={"grade_filter": "A+/A/B"},
        ),
        "loader": load_reversal_quality_events,
    },
]


def simulate_and_collect(
    cfg: GridConfig,
    events: list[dict],
    prices: PriceCache,
    calendar: list[str],
) -> tuple[list[dict], list[dict]]:
    """Run day-by-day sim, return (closed_trades, open_positions) as dicts."""
    equity = STARTING_CAPITAL
    open_positions: list[OpenPos] = []
    closed_trades: list[dict] = []
    peak_equity = STARTING_CAPITAL
    halted = False

    events_by_date: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        ed = e.get("_entry_date")
        if ed:
            events_by_date[ed].append(e)

    # Build a lookup from trade_id to event for insider info
    event_by_tid = {e["trade_id"]: e for e in events}

    for today in calendar:
        if today > END:
            break

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
            exit_reason = ""

            if pos.days_held >= cfg.hold_days:
                should_exit = True
                exit_reason = "time_exit"
            if cfg.stop_loss is not None:
                pnl_pct = (close_price - pos.entry_price) / pos.entry_price
                if pnl_pct <= cfg.stop_loss:
                    should_exit = True
                    exit_reason = "stop_loss"
            if cfg.exit_type == "trailing_stop" and pos.peak_price > 0:
                trail_pct = (close_price - pos.peak_price) / pos.peak_price
                if trail_pct <= -cfg.trailing_stop_pct:
                    should_exit = True
                    exit_reason = "trailing_stop"

            if should_exit:
                to_close.append((pos, close_price, exit_reason))

        for pos, close_price, reason in to_close:
            pnl_pct = (close_price - pos.entry_price) / pos.entry_price
            pnl_dollar = pos.dollar_amount * pnl_pct
            equity += pnl_dollar

            peak_ret = (pos.peak_price - pos.entry_price) / pos.entry_price if pos.entry_price else 0

            evt = event_by_tid.get(pos.trade_id, {})
            closed_trades.append({
                "trade_id": pos.trade_id,
                "ticker": pos.ticker,
                "entry_date": pos.entry_date,
                "entry_price": pos.entry_price,
                "exit_date": today,
                "exit_price": close_price,
                "hold_days": pos.days_held,
                "pnl_pct": pnl_pct,
                "pnl_dollar": pnl_dollar,
                "dollar_amount": pos.dollar_amount,
                "position_size": cfg.position_size,
                "portfolio_value": equity - pnl_dollar,
                "equity_after": equity,
                "exit_reason": reason,
                "stop_hit": 1 if reason in ("stop_loss", "trailing_stop") else 0,
                "peak_return": peak_ret,
                "shares": int(pos.dollar_amount / pos.entry_price) if pos.entry_price else 0,
                "insider_name": evt.get("insider_name"),
                "company": evt.get("company"),
                "insider_title": evt.get("title"),
                "filing_date": evt.get("filing_date"),
                "trade_date": evt.get("trade_date"),
                "signal_grade": evt.get("pit_grade"),
                "is_csuite": evt.get("is_csuite"),
                "is_rare_reversal": evt.get("is_rare_reversal"),
                "conviction": pos.conviction,
            })
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

    # Return open positions as dicts too
    open_dicts = []
    for pos in open_positions:
        evt = event_by_tid.get(pos.trade_id, {})
        open_dicts.append({
            "trade_id": pos.trade_id,
            "ticker": pos.ticker,
            "entry_date": pos.entry_date,
            "entry_price": pos.entry_price,
            "dollar_amount": pos.dollar_amount,
            "position_size": cfg.position_size,
            "portfolio_value": equity,
            "insider_name": evt.get("insider_name"),
            "company": evt.get("company"),
            "filing_date": evt.get("filing_date"),
            "signal_grade": evt.get("pit_grade"),
            "shares": int(pos.dollar_amount / pos.entry_price) if pos.entry_price else 0,
        })

    return closed_trades, open_dicts


def main():
    db_path = str(DB_PATH)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    prices_conn = sqlite3.connect(str(PRICES_DB))
    intraday_conn = sqlite3.connect(str(INTRADAY_DB)) if INTRADAY_DB.exists() else None

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
    print(f"  Price cache loaded ({time.time()-t0:.1f}s)\n")

    # Switch to write connection
    conn.close()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    for strat in STRATEGIES:
        name = strat["name"]
        cfg = strat["config"]
        print(f"{'='*60}")
        print(f"STRATEGY: {name}")

        # Register portfolio
        conn.execute("""
            INSERT OR IGNORE INTO portfolios (name, display_name, description, config, starting_capital)
            VALUES (?, ?, ?, ?, ?)
        """, (name, strat["display_name"], strat["description"], json.dumps(vars(cfg), default=str), STARTING_CAPITAL))
        conn.commit()

        portfolio_id = conn.execute("SELECT id FROM portfolios WHERE name = ?", (name,)).fetchone()[0]

        # Clear any existing backfill for this strategy
        deleted = conn.execute("DELETE FROM strategy_portfolio WHERE strategy = ?", (name,)).rowcount
        conn.commit()
        if deleted:
            print(f"  Cleared {deleted} existing rows")

        # Load events
        raw_events = strat["loader"](conn, START, END)
        events = dedup_events(raw_events)
        print(f"  {len(events)} events after dedup")

        tagged = [(cfg.strategy, e) for e in events]
        compute_entry_prices(tagged, intraday_conn, prices, calendar)
        events = [e for e in events if e.get("_entry_price") and e["_entry_price"] > 0]

        filtered = filter_events_for_config(events, cfg)
        print(f"  {len(filtered)} events after filtering")

        # Run simulation
        closed, open_pos = simulate_and_collect(cfg, filtered, prices, calendar)
        print(f"  {len(closed)} closed trades, {len(open_pos)} open positions")

        # Insert closed trades
        for t in closed:
            conn.execute("""
                INSERT INTO strategy_portfolio (
                    strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                    entry_date, entry_price, exit_date, exit_price, hold_days,
                    target_hold, stop_pct, stop_hit, pnl_pct, pnl_dollar,
                    position_size, portfolio_value, equity_after, insider_name,
                    signal_quality, signal_grade, exit_reason, status,
                    execution_source, peak_return, shares, dollar_amount,
                    company, filing_date, trade_date, is_csuite, is_rare_reversal
                ) VALUES (
                    ?, ?, ?, ?, 'buy_stock', 'long',
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, 'closed',
                    'backtest', ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
            """, (
                name, portfolio_id, t["trade_id"], t["ticker"],
                t["entry_date"], t["entry_price"], t["exit_date"], t["exit_price"], t["hold_days"],
                cfg.hold_days, abs(cfg.stop_loss) if cfg.stop_loss else 0.0, t["stop_hit"], t["pnl_pct"], t["pnl_dollar"],
                t["position_size"], t["portfolio_value"], t["equity_after"], t["insider_name"],
                t.get("conviction"), t.get("signal_grade"), t["exit_reason"],
                t.get("peak_return"), t.get("shares"), t.get("dollar_amount"),
                t.get("company"), t.get("filing_date"), t.get("trade_date"),
                t.get("is_csuite"), t.get("is_rare_reversal"),
            ))

        # Insert open positions
        for t in open_pos:
            conn.execute("""
                INSERT INTO strategy_portfolio (
                    strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                    entry_date, entry_price, target_hold, stop_pct,
                    position_size, portfolio_value, insider_name,
                    signal_grade, status, execution_source, shares, dollar_amount,
                    company, filing_date
                ) VALUES (
                    ?, ?, ?, ?, 'buy_stock', 'long',
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, 'open', 'backtest', ?, ?,
                    ?, ?
                )
            """, (
                name, portfolio_id, t["trade_id"], t["ticker"],
                t["entry_date"], t["entry_price"], cfg.hold_days, abs(cfg.stop_loss) if cfg.stop_loss else 0.0,
                t["position_size"], t["portfolio_value"], t["insider_name"],
                t.get("signal_grade"), t.get("shares"), t.get("dollar_amount"),
                t.get("company"), t.get("filing_date"),
            ))

        conn.commit()
        print(f"  Inserted {len(closed)} closed + {len(open_pos)} open trades\n")

    conn.close()
    if intraday_conn:
        intraday_conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
