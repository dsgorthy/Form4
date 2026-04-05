#!/usr/bin/env python3
"""
Grid search over reversal strategy parameters.

Variables:
  - Position size %: determines max concurrent (100% / size%)
  - Minimum conviction threshold
  - Substitution logic when at capacity

Uses the same day-by-day simulation engine as backfill_cw_portfolio.py
but strips out DB writes — pure in-memory simulation.

Usage:
    python3 pipelines/insider_study/grid_search_reversal.py
"""

from __future__ import annotations

import csv
import json
import math
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipelines.insider_study.conviction_score import compute_conviction, pit_score_to_grade
from pipelines.insider_study.backfill_cw_portfolio import (
    PriceCache, _build_trading_calendar, _load_reversal_events,
    _filed_during_market, _get_5min_entry,
    DB_PATH, PRICES_DB, INTRADAY_DB,
)

START = "2020-01-01"
END = "2026-03-31"
TARGET_HOLD = 30
STOP_PCT = -0.15

# ---------------------------------------------------------------------------
# Grid parameters
# ---------------------------------------------------------------------------

POSITION_SIZES = [0.10, 0.125, 0.15, 0.167, 0.20, 0.25]
MIN_CONVICTIONS = [3.0, 4.0, 5.0, 6.0, 7.0]
REPLACE_RULES = ["skip", "replace_oldest", "replace_lowest_conv", "replace_worst_pnl"]


@dataclass
class Config:
    position_size: float
    max_concurrent: int
    min_conviction: float
    replace_rule: str

    @property
    def label(self) -> str:
        return f"{self.position_size:.1%}/{self.max_concurrent}pos/mc{self.min_conviction:.0f}/{self.replace_rule}"


@dataclass
class OpenPos:
    trade_id: int
    ticker: str
    entry_date: str
    entry_price: float
    dollar_amount: float
    conviction: float
    days_held: int = 0
    peak_price: float = 0.0


@dataclass
class Result:
    config: Config
    trades: int
    wins: int
    win_rate: float
    avg_return: float
    total_pnl: float
    final_equity: float
    cagr: float
    sharpe: float
    max_drawdown: float
    stops_hit: int
    replacements: int
    max_concurrent_seen: int
    avg_concurrent: float
    avg_deployment: float  # avg % of equity deployed


def run_sim(events: list[dict], prices: PriceCache, calendar: list[str],
            intraday_conn, cfg: Config) -> Result:
    """Day-by-day simulation with configurable parameters."""

    equity = 100_000.0
    open_positions: list[OpenPos] = []
    daily_returns: list[float] = []
    peak_equity = equity
    max_dd = 0.0

    completed_pnls: list[float] = []
    stops = 0
    replacements = 0
    max_conc = 0
    total_deployed_days = 0  # sum of positions * days for avg calc
    total_days = 0

    # Index events by filing_date
    events_by_fd: dict[str, list[dict]] = {}
    for e in events:
        if e["_conviction"] < cfg.min_conviction:
            continue
        fd = e["filing_date"]
        events_by_fd.setdefault(fd, []).append(e)

    pending: list[dict] = []
    last_fd_checked = ""

    for today in calendar:
        if today < START:
            continue

        # Gather new signals (T+1: filed before today)
        for fd in sorted(events_by_fd.keys()):
            if fd >= today:
                break
            if fd <= last_fd_checked:
                # Check same-day intraday entries
                if fd == today:
                    for e in events_by_fd[fd]:
                        if _filed_during_market(e.get("filed_at")) and e not in pending:
                            pending.append(e)
                continue
            if fd == today:
                for e in events_by_fd[fd]:
                    if _filed_during_market(e.get("filed_at")):
                        pending.append(e)
            else:
                pending.extend(events_by_fd[fd])
            last_fd_checked = fd

        # Check exits
        still_open: list[OpenPos] = []
        day_pnl = 0.0
        for pos in open_positions:
            bar = prices.get_bar(pos.ticker, today)
            if bar is None:
                still_open.append(pos)
                continue

            o, h, l, c = bar
            pos.days_held += 1
            if c > pos.peak_price:
                pos.peak_price = c

            exited = False

            # Stop loss
            if l and pos.entry_price > 0:
                dd = (l - pos.entry_price) / pos.entry_price
                if dd <= STOP_PCT:
                    exit_price = pos.entry_price * (1 + STOP_PCT)
                    pnl_pct = STOP_PCT
                    pnl_dollar = pos.dollar_amount * pnl_pct
                    equity += pnl_dollar
                    day_pnl += pnl_dollar
                    completed_pnls.append(pnl_pct)
                    stops += 1
                    exited = True

            # Time exit
            if not exited and pos.days_held >= TARGET_HOLD:
                pnl_pct = (c - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.dollar_amount * pnl_pct
                equity += pnl_dollar
                day_pnl += pnl_dollar
                completed_pnls.append(pnl_pct)
                exited = True

            if not exited:
                still_open.append(pos)

        open_positions = still_open

        # Process entries
        pending.sort(key=lambda e: -e["_conviction"])
        held_tickers = {p.ticker for p in open_positions}
        entered_today: set[str] = set()
        replaced_today = False

        for event in pending:
            ticker = event["ticker"]
            if ticker in held_tickers or ticker in entered_today:
                continue

            entry_price = event.get("_entry_price")
            if not entry_price or entry_price <= 0:
                entry_price = prices.get_open(ticker, today)
                if not entry_price or entry_price <= 0:
                    entry_price = prices.get_close(ticker, today)
            if not entry_price or entry_price <= 0:
                continue

            conv = event["_conviction"]

            # Capacity check — one replacement per day, no chaining
            if len(open_positions) >= cfg.max_concurrent:
                if cfg.replace_rule == "skip" or replaced_today:
                    continue

                # Find replacement candidate (must have held > 0 days)
                candidate = None
                if cfg.replace_rule == "replace_oldest":
                    candidates = [p for p in open_positions if p.days_held > 0]
                    candidate = max(candidates, key=lambda p: p.days_held) if candidates else None
                elif cfg.replace_rule == "replace_lowest_conv":
                    candidate = min(open_positions, key=lambda p: p.conviction)
                    if conv <= candidate.conviction:
                        continue  # incoming isn't better
                elif cfg.replace_rule == "replace_worst_pnl":
                    def _unrealized(p):
                        c = prices.get_close(p.ticker, today)
                        if c and p.entry_price > 0:
                            return (c - p.entry_price) / p.entry_price
                        return 0.0
                    candidate = min(open_positions, key=_unrealized)
                    if _unrealized(candidate) >= 0:
                        continue  # no losing positions to replace

                if candidate is None:
                    continue

                # Close the replacement
                rep_close = prices.get_close(candidate.ticker, today)
                if not rep_close or rep_close <= 0:
                    continue
                rep_pnl_pct = (rep_close - candidate.entry_price) / candidate.entry_price
                rep_pnl_dollar = candidate.dollar_amount * rep_pnl_pct
                equity += rep_pnl_dollar
                day_pnl += rep_pnl_dollar
                completed_pnls.append(rep_pnl_pct)
                if rep_pnl_pct <= STOP_PCT:
                    stops += 1
                open_positions = [p for p in open_positions if p.trade_id != candidate.trade_id]
                held_tickers = {p.ticker for p in open_positions}
                replacements += 1
                replaced_today = True

            # Hard guard: never exceed max_concurrent
            if len(open_positions) >= cfg.max_concurrent:
                continue

            # Enter
            dollar_amount = equity * cfg.position_size
            pos = OpenPos(
                trade_id=event["trade_id"],
                ticker=ticker,
                entry_date=today,
                entry_price=entry_price,
                dollar_amount=dollar_amount,
                conviction=conv,
                peak_price=entry_price,
            )
            open_positions.append(pos)
            entered_today.add(ticker)
            held_tickers.add(ticker)

        # Clear pending (T+1 only)
        pending = []

        # Track stats
        if len(open_positions) > max_conc:
            max_conc = len(open_positions)
        total_deployed_days += len(open_positions)
        total_days += 1

        # Daily return for Sharpe
        if equity > 0:
            dr = day_pnl / (equity - day_pnl) if (equity - day_pnl) > 0 else 0
            daily_returns.append(dr)

        # Drawdown
        if equity > peak_equity:
            peak_equity = equity
        dd = (peak_equity - equity) / peak_equity if peak_equity > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # Close remaining positions
    for pos in open_positions:
        last_c = prices.get_close_on_or_before(pos.ticker, calendar[-1])
        if last_c and last_c > 0:
            pnl_pct = (last_c - pos.entry_price) / pos.entry_price
            pnl_dollar = pos.dollar_amount * pnl_pct
            equity += pnl_dollar
            completed_pnls.append(pnl_pct)

    # Compute metrics
    n = len(completed_pnls)
    wins = sum(1 for p in completed_pnls if p > 0)
    wr = wins / n * 100 if n else 0
    avg_ret = sum(completed_pnls) / n * 100 if n else 0
    total_pnl = equity - 100_000

    years = max((datetime.strptime(END, "%Y-%m-%d") - datetime.strptime(START, "%Y-%m-%d")).days / 365.25, 1)
    cagr = ((equity / 100_000) ** (1 / years) - 1) * 100 if equity > 0 else 0

    if len(daily_returns) > 1:
        import statistics
        mean_r = statistics.mean(daily_returns)
        std_r = statistics.stdev(daily_returns)
        sharpe = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else 0
    else:
        sharpe = 0

    avg_conc = total_deployed_days / total_days if total_days else 0
    avg_deploy = (avg_conc * cfg.position_size) * 100

    return Result(
        config=cfg, trades=n, wins=wins, win_rate=wr, avg_return=avg_ret,
        total_pnl=total_pnl, final_equity=equity, cagr=cagr, sharpe=sharpe,
        max_drawdown=max_dd * 100, stops_hit=stops, replacements=replacements,
        max_concurrent_seen=max_conc, avg_concurrent=avg_conc, avg_deployment=avg_deploy,
    )


def main():
    print("Loading data...", flush=True)
    t0 = time.time()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    events = _load_reversal_events(conn, START, END)
    print(f"  {len(events)} reversal events loaded", flush=True)

    # Dedup
    seen_ids, seen_td = set(), set()
    deduped = []
    for e in events:
        tid, td = e["trade_id"], (e["ticker"], e["filing_date"])
        if tid in seen_ids or td in seen_td:
            continue
        seen_ids.add(tid)
        seen_td.add(td)
        deduped.append(e)
    events = deduped
    print(f"  {len(events)} after dedup", flush=True)

    # Compute conviction using single source of truth (ALL PIT-safe inputs)
    from pipelines.insider_study.compute_trade_conviction import compute_full_conviction, clear_cache
    clear_cache()
    for e in events:
        e["_conviction"] = compute_full_conviction(e, conn, "reversal"
        )

    # Skip penny stocks
    events = [e for e in events if not (e.get("entry_price", 0) and e["entry_price"] < 2.0)]
    print(f"  {len(events)} after penny filter", flush=True)

    # Load prices
    all_tickers = {e["ticker"] for e in events}
    prices_conn = sqlite3.connect(str(PRICES_DB))
    intraday_conn = sqlite3.connect(str(INTRADAY_DB)) if INTRADAY_DB.exists() else None

    cache_start = (datetime.strptime(START, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    cache_end = (datetime.strptime(END, "%Y-%m-%d") + timedelta(days=60)).strftime("%Y-%m-%d")
    prices = PriceCache(prices_conn, all_tickers, cache_start, cache_end)
    calendar = _build_trading_calendar(prices_conn, START, cache_end)
    prices_conn.close()

    load_time = time.time() - t0
    print(f"  Data loaded in {load_time:.1f}s\n", flush=True)

    # Build grid
    configs = []
    for ps in POSITION_SIZES:
        max_conc = max(1, int(1.0 / ps))
        for mc in MIN_CONVICTIONS:
            for rr in REPLACE_RULES:
                configs.append(Config(
                    position_size=ps,
                    max_concurrent=max_conc,
                    min_conviction=mc,
                    replace_rule=rr,
                ))

    print(f"Running {len(configs)} configurations...\n", flush=True)

    results: list[Result] = []
    for i, cfg in enumerate(configs):
        r = run_sim(events, prices, calendar, intraday_conn, cfg)
        results.append(r)
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(configs)}] ...", flush=True)

    elapsed = time.time() - t0
    print(f"\nGrid complete in {elapsed:.1f}s\n", flush=True)

    if intraday_conn:
        intraday_conn.close()
    conn.close()

    # Sort by Sharpe
    results.sort(key=lambda r: -r.sharpe)

    # Print top 20
    print(f"{'='*120}")
    print(f"  TOP 20 by Sharpe")
    print(f"{'='*120}")
    print(f"{'#':>3} {'Config':42s} {'Trades':>6} {'WR':>6} {'AvgRet':>7} {'CAGR':>6} {'Sharpe':>7} {'MaxDD':>6} {'Equity':>10} {'Repl':>5} {'AvgConc':>8} {'Deploy':>7}")
    print("-" * 120)
    for i, r in enumerate(results[:20]):
        c = r.config
        print(f"{i+1:>3} {c.label:42s} {r.trades:>6} {r.win_rate:>5.1f}% {r.avg_return:>6.2f}% {r.cagr:>5.1f}% {r.sharpe:>7.2f} {r.max_drawdown:>5.1f}% ${r.final_equity:>9,.0f} {r.replacements:>5} {r.avg_concurrent:>7.1f} {r.avg_deployment:>6.1f}%")

    # Sort by CAGR
    results.sort(key=lambda r: -r.cagr)
    print(f"\n{'='*120}")
    print(f"  TOP 20 by CAGR")
    print(f"{'='*120}")
    print(f"{'#':>3} {'Config':42s} {'Trades':>6} {'WR':>6} {'AvgRet':>7} {'CAGR':>6} {'Sharpe':>7} {'MaxDD':>6} {'Equity':>10} {'Repl':>5} {'AvgConc':>8} {'Deploy':>7}")
    print("-" * 120)
    for i, r in enumerate(results[:20]):
        c = r.config
        print(f"{i+1:>3} {c.label:42s} {r.trades:>6} {r.win_rate:>5.1f}% {r.avg_return:>6.2f}% {r.cagr:>5.1f}% {r.sharpe:>7.2f} {r.max_drawdown:>5.1f}% ${r.final_equity:>9,.0f} {r.replacements:>5} {r.avg_concurrent:>7.1f} {r.avg_deployment:>6.1f}%")

    # Write CSV
    out_dir = Path(__file__).resolve().parents[2] / "reports" / "grid_search"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "reversal_grid.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["position_size", "max_concurrent", "min_conviction", "replace_rule",
                     "trades", "wins", "win_rate", "avg_return", "cagr", "sharpe",
                     "max_drawdown", "final_equity", "total_pnl", "stops_hit",
                     "replacements", "max_concurrent_seen", "avg_concurrent", "avg_deployment"])
        for r in results:
            c = r.config
            w.writerow([c.position_size, c.max_concurrent, c.min_conviction, c.replace_rule,
                         r.trades, r.wins, round(r.win_rate, 2), round(r.avg_return, 2),
                         round(r.cagr, 2), round(r.sharpe, 2), round(r.max_drawdown, 2),
                         round(r.final_equity, 2), round(r.total_pnl, 2), r.stops_hit,
                         r.replacements, r.max_concurrent_seen, round(r.avg_concurrent, 2),
                         round(r.avg_deployment, 2)])
    print(f"\nCSV: {csv_path}", flush=True)


if __name__ == "__main__":
    main()
