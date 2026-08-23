#!/usr/bin/env python3
"""Unified strategy portfolio simulator — writes strategy_portfolio rows.

What this is:
  A single source of truth for each insider strategy's portfolio state,
  treating the strategy as a live $100K fund running since its start_date.
  Walks forward day-by-day applying current strategy logic (career_grade
  filter for QM, etc.), opening/closing positions with proper compounding
  sizing, marking positions still in their hold window as OPEN with no
  exit_date.

  Replaces the previous mess of:
    - Original backtest rows (variable sizing)
    - backfill_v3 rows (fixed $10K cap — the bug Derek caught)
    - Paper account rows scattered separately

  The dashboard reads from `strategy_portfolio` and sees ONE coherent
  track record per strategy. Open positions reflect what would currently
  be held under current strategy rules.

Modes:
  --rebuild    Wipe and re-simulate from scratch (one-shot, ~minutes per strategy)
  --extend     Walk forward from latest known state to today (incremental, daily job)

PIT compliance:
  - Uses `trades.career_grade` (rebuilt 2026-05-12 with patched scorer)
  - Uses pre-computed `trades.consecutive_sells_before / dip_3mo / above_sma50 /
    is_largest_ever / is_recurring / is_tax_sale / cohen_routine / is_10b5_1`
    (all PIT-clean per signal_registry memory)
  - Conviction calc receives `pit_grade` as signal_grade (mirrors live cw_runner)
  - Entry/exit prices use prices.daily_prices closes (no future leak)
  - -30% stop applied retroactively per Derek's directive

Usage:
    python3 -m pipelines.insider_study.simulate_strategy_portfolio \\
        --strategy quality_momentum --rebuild

    python3 -m pipelines.insider_study.simulate_strategy_portfolio --all --rebuild
    python3 -m pipelines.insider_study.simulate_strategy_portfolio --all --extend
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

import yaml

from config.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Strategy registry ───────────────────────────────────────────────────

STRATEGY_CONFIG = {
    "quality_momentum": {
        "yaml": REPO / "strategies/cw_strategies/configs/quality_momentum.yaml",
        "start_date": "2023-01-01",
    },
    # Runs alongside quality_momentum with the trend filter removed, to build
    # a forward record on the one question a backtest cannot settle.
    "quality_notrend": {
        "yaml": REPO / "strategies/cw_strategies/configs/quality_notrend.yaml",
        "start_date": "2023-01-01",
    },
    # Same signals as quality_momentum at 2x gross, with margin interest
    # charged daily on the borrowed half. Published as its own book so the
    # levered record is a real one rather than a curve recomputed from the
    # unlevered one.
    "quality_momentum_2x": {
        "yaml": REPO / "strategies/cw_strategies/configs/quality_momentum_2x.yaml",
        "start_date": "2023-01-01",
    },
    "reversal_dip": {
        "yaml": REPO / "strategies/cw_strategies/configs/reversal_dip.yaml",
        "start_date": "2023-01-01",
    },
    # tenb51_surprise retired 2026-08-18 — the nightly rebuild no longer runs
    # it, so its ~200 simulated rows are frozen where they stand. The yaml and
    # the PIT strategy class remain; re-add the entry here to resume.
}

#: Grade column the simulator gates on.
#:
#: Exists so a scoring change can be measured against the shipped grades before
#: it is adopted: write the candidate scores to a shadow column, point this at
#: it, and run identical rules over both. That is how the 2026-08-22 tranche
#: correction was evaluated — `career_grade_grouped` held the filing-grouped
#: scores while every published surface still read `career_grade`.
#:
#: The shadow was dropped once the correction was adopted, because a second
#: copy of a published grade is its own drift risk. Recreate one per experiment
#: rather than leaving it lying around.
GRADE_COLUMN = "career_grade"

STARTING_CAPITAL = 100_000.0
def resolve_stop_pct(config: dict) -> Optional[float]:
    """The stop comes from the strategy yaml, the same place cw_runner reads it.

    Until 2026-08-20 this module carried `STOP_LOSS_PCT = -0.30` as a global
    override, applied to every strategy regardless of its config. All three
    yamls said `stop_loss_pct: null`, so the published book was simulating a
    -30% stop that the live alert runner never applied — cw_runner.py reads the
    yaml and treats null as no stop. The two surfaces disagreed for three
    months.

    Measured on 2026-08-20 before removing it: the -30% stop cost 6.4 CAGR
    points on quality_notrend in-sample and 4.4 on quality_momentum, and bought
    no drawdown protection (23.5% either way). A -50% stop is indistinguishable
    from no stop over the whole sample — nothing ever closed below -50% — so it
    is free as catastrophic insurance while remaining inert in the figures.

    Normalisation follows cw_runner exactly: a stop must be negative, and
    None / 0 / positive all mean no stop.
    """
    exit_cfg = (config.get("theses") or [{}])[0].get("exit") or config.get("exit") or {}
    raw = exit_cfg.get("stop_loss_pct")
    if raw is None:
        return None
    raw = float(raw)
    if raw == 0:
        return None
    return -abs(raw)



# ── State types ─────────────────────────────────────────────────────────

@dataclass
class OpenPosition:
    """A currently-held position during the simulation."""
    trade_id: int
    ticker: str
    insider_id: int
    insider_name: Optional[str]
    insider_title: Optional[str]
    company: Optional[str]
    entry_date: str
    # The filing's own dates, kept distinct from entry_date. These three are
    # three different days: trade_date is when the insider dealt, filing_date
    # is when EDGAR received it, entry_date is the first close we could have
    # bought at. Stamping entry_date into all three (which this did until
    # 2026-08-18) makes the product contradict the SEC filing it links to.
    filing_date: Optional[str]
    trade_date: Optional[str]
    entry_price: float
    capital_at_entry: float       # $ allocated at entry (position_size_pct × equity_at_entry)
    target_exit_idx: int          # calendar index when hold_td expires
    stop_price: Optional[float]   # entry_price × (1 + stop_loss_pct); None = no stop
    pit_grade: Optional[str]
    career_grade: Optional[str]
    conviction: float
    is_csuite: bool
    is_rare_reversal: bool
    days_held: int = 0
    last_seen_close: Optional[float] = None  # for stale-exit fallback (set as we walk)
    last_seen_date: Optional[str] = None


@dataclass
class ClosedPosition:
    """A position that has exited."""
    trade_id: int
    ticker: str
    insider_id: int
    insider_name: Optional[str]
    insider_title: Optional[str]
    company: Optional[str]
    entry_date: str
    filing_date: Optional[str]
    trade_date: Optional[str]
    entry_price: float
    capital_at_entry: float
    exit_date: str
    exit_price: float
    exit_reason: str              # 'time' | 'stop'
    hold_days: int
    pnl_pct: float
    pnl_dollar: float
    pit_grade: Optional[str]
    career_grade: Optional[str]
    conviction: float
    is_csuite: bool
    is_rare_reversal: bool
    equity_after: float


# ── Filter evaluation ────────────────────────────────────────────────────
# Moved to framework.decision.filters as part of Stage 3 (shared engine).
# Re-exported here so callers that imported from this module keep working
# during the migration window.
from framework.decision.entry_timing import entry_fill
from framework.decision.filters import evaluate_filters  # noqa: F401


def count_prior_10b5_1_sells(conn, insider_id, ticker, as_of):
    """PIT count of 10b5-1 sells filed BEFORE this trade's filing_date."""
    row = conn.execute(
        """SELECT COUNT(*) FROM trades
            WHERE insider_id = ? AND ticker = ?
              AND trans_code = 'S' AND is_10b5_1 = 1
              AND filing_date < ?""",
        (insider_id, ticker, as_of),
    ).fetchone()
    return int(row[0]) if row else 0


# ── Price helpers ───────────────────────────────────────────────────────

def load_trading_calendar(conn, start: str, end: str) -> List[str]:
    rows = conn.execute(
        """SELECT DISTINCT date::text FROM prices.daily_prices
           WHERE date >= ? AND date <= ? ORDER BY date""",
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


def preload_prices(conn, tickers: set, start: str, end: str) -> dict:
    """Bulk-load prices into dict[(ticker, date)] = (open, close).

    Opens are needed because a filing accepted after the bell fills at the next
    session's OPEN, not its close.
    """
    out = {}
    BATCH = 200
    tickers_list = sorted(tickers)
    for i in range(0, len(tickers_list), BATCH):
        chunk = tickers_list[i:i + BATCH]
        placeholders = ",".join(["?"] * len(chunk))
        rows = conn.execute(
            f"""SELECT ticker, date::text, close, open FROM prices.daily_prices
                WHERE ticker IN ({placeholders}) AND date >= ? AND date <= ?""",
            tuple(chunk) + (start, end),
        ).fetchall()
        for r in rows:
            close, opn = r[2], r[3]
            if close and close > 0:
                # Fall back to the close when a session has no open recorded.
                out[(r[0], r[1])] = (float(opn) if opn and opn > 0 else float(close),
                                     float(close))
    return out


def find_first_price_on_or_after(prices, calendar, ticker, cal_idx,
                                 max_forward=5, field="close"):
    """First available price for ticker at or after calendar[cal_idx].

    `field` is "close" or "open" — an after-the-bell filing fills at the next
    session's open. Returns (cal_idx, price) or None.
    """
    slot = 0 if field == "open" else 1
    for off in range(max_forward + 1):
        i = cal_idx + off
        if i >= len(calendar):
            return None
        bar = prices.get((ticker, calendar[i]))
        if bar is not None:
            return i, bar[slot]
    return None


# ── Core simulation ─────────────────────────────────────────────────────

def simulate_one_strategy(
    conn,
    strategy_name: str,
    config: dict,
    start_date: str,
    end_date: str,
) -> Tuple[List[ClosedPosition], List[OpenPosition], float]:
    """Walk forward day-by-day. Returns (closed_positions, open_at_end, final_equity)."""
    thesis = (config.get("theses") or [{
        "name": strategy_name,
        "filters": config["filters"],
        "exit": config["exit"],
    }])[0]
    thesis_filters = thesis.get("filters", {})
    hold_td = int(thesis.get("exit", {}).get("hold_days", 30))
    position_size_pct = float(config["position_size_pct"])
    max_concurrent = int(config["max_concurrent"])

    # Leverage. Absent from a strategy yaml means 1.0, so every existing book
    # keeps its exact numbers.
    #
    # Gross exposure may reach equity x leverage, and the borrowed part accrues
    # margin interest every calendar day it is outstanding. Sizing alone does
    # NOT model leverage: doubling position_size_pct doubles the returns and
    # charges nothing, which is how a backtest reports a CAGR no broker would
    # have funded. The carry is the whole difference between a levered strategy
    # and a fictional one.
    leverage = float(config.get("leverage", 1.0))
    margin_rate = float(config.get("margin_rate", 0.06))
    financing_paid = 0.0
    min_conviction = float(config.get("min_conviction", 1.5))
    stop_pct = resolve_stop_pct(config)
    # See the circuit-breaker note in the entry loop.
    breaker_from_peak = bool(config.get("circuit_breaker_from_peak", False))
    breaker_dd = float(config.get("circuit_breaker_dd_pct", 0.15))
    peak_equity = STARTING_CAPITAL
    halted_days = 0

    logger.info(
        "[%s] config: hold_td=%d, pos=%.0f%%, max=%d, min_conv=%.1f, stop=%s",
        strategy_name, hold_td, position_size_pct * 100, max_concurrent,
        min_conviction,
        "none" if stop_pct is None else f"{stop_pct * 100:.0f}%",
    )

    # Load every P-trade in the window with all features needed for filter+conviction
    t0 = time.monotonic()
    rows = conn.execute(
        ("""SELECT t.trade_id, t.insider_id, t.ticker,
                  t.filing_date::text, t.trade_date::text,
                  t.title, COALESCE(i.display_name, i.name) AS insider_name,
                  t.company, t.is_csuite,
                  COALESCE(t.is_duplicate, 0) AS is_duplicate,
                  t.is_rare_reversal, t.consecutive_sells_before,
                  t.dip_1mo, t.dip_3mo,
                  t.above_sma50, t.above_sma200, t.is_largest_ever,
                  t.is_10b5_1, t.is_recurring, t.is_tax_sale, t.cohen_routine,
                  t.pit_grade,
                  -- Which grade column the run reads. Set by GRADE_COLUMN so
                  -- an A/B can score the identical rules against the current
                  -- grades and the filing-grouped ones without a second copy
                  -- of the simulator.
                  t.{grade_col} AS career_grade,
                  t.net_buyer_flow_90d, t.industry_buy_pct_90d,
                  -- Raw acceptance timestamp. The SQL used to decide
                  -- tradeability itself, converting UTC->ET inline, and that
                  -- copy of the rule drifted from the Python twice: once
                  -- inverted (fixed 2026-08-18) and once because the column
                  -- silently became Eastern in 2026 while the conversion
                  -- stayed (fixed 2026-08-19, 37 positions entered a day
                  -- early). The decision now lives only in
                  -- framework.decision.entry_timing, which is also where the
                  -- five-minute pickup model lives.
                  t.filed_at
           FROM trades t
           JOIN insiders i ON t.insider_id = i.insider_id
           WHERE t.trans_code = 'P'
             AND t.filing_date >= ? AND t.filing_date <= ?
           ORDER BY t.filing_date, t.trade_id""").format(grade_col=GRADE_COLUMN),
        (start_date, end_date),
    ).fetchall()
    logger.info("[%s] %d P-trades loaded in %.1fs",
                strategy_name, len(rows), time.monotonic() - t0)

    # Bucket by filing_date
    trades_by_date = defaultdict(list)
    for r in rows:
        d = {k: r[k] for k in r.keys()} if hasattr(r, "keys") else dict(r)
        trades_by_date[d["filing_date"]].append(d)

    # Pre-load prices (only for tickers that pass filter — most won't, but easier to load all)
    all_tickers = {r["ticker"] for r in rows if isinstance(r, dict) or r.get("ticker")}
    all_tickers = {(d if isinstance(d, dict) else dict(d))["ticker"]
                   for d in (trades_by_date[k] for k in trades_by_date) for d in d}
    if not all_tickers:
        return [], [], STARTING_CAPITAL
    cal = load_trading_calendar(conn,
                                (datetime.strptime(start_date, "%Y-%m-%d") -
                                 timedelta(days=10)).strftime("%Y-%m-%d"),
                                (datetime.strptime(end_date, "%Y-%m-%d") +
                                 timedelta(days=hold_td * 2 + 10)).strftime("%Y-%m-%d"))
    logger.info("[%s] calendar: %d days", strategy_name, len(cal))
    prices = preload_prices(conn, all_tickers, cal[0], cal[-1])
    logger.info("[%s] cached %d (ticker, date) prices", strategy_name, len(prices))

    cal_idx_of = {d: i for i, d in enumerate(cal)}

    # Find start index
    start_idx = None
    for i, d in enumerate(cal):
        if d >= start_date:
            start_idx = i
            break
    if start_idx is None:
        return [], [], STARTING_CAPITAL

    # State
    equity = STARTING_CAPITAL
    held: List[OpenPosition] = []
    closed: List[ClosedPosition] = []

    # Import conviction once
    from pipelines.insider_study.conviction_score import (
        compute_conviction, _categorize_insider,
    )

    today_str = date.today().isoformat()

    prev_day = None
    for cal_idx, d in enumerate(cal[start_idx:], start=start_idx):
        # Carry on anything borrowed, before the day's exits and entries.
        # No-op at 1x, where gross exposure never exceeds equity.
        if leverage > 1.0:
            gross = sum(p.capital_at_entry for p in held)
            borrowed = max(0.0, gross - equity)
            if borrowed > 0 and prev_day is not None:
                days = max((date.fromisoformat(d) - date.fromisoformat(prev_day)).days, 0)
                cost = borrowed * margin_rate * days / 365.0
                equity -= cost
                financing_paid += cost
        prev_day = d
        if d > today_str:
            break
        if d > end_date:
            break

        # ── 1) Exit checks for held positions ──────────────────────────
        kept = []
        for pos in held:
            # prices maps to (open, close); exits mark at the close.
            _bar = prices.get((pos.ticker, d))
            close_today = _bar[1] if _bar is not None else None
            exit_was_stale = False
            if close_today is not None:
                pos.last_seen_close = close_today
                pos.last_seen_date = d
            else:
                # Today's close is missing. Prefer the most recent close we
                # have seen since entry; only as a last resort fall back to
                # entry_price (which silently zeroes the trade). Flag stale
                # exits so the UI can mark them.
                if pos.last_seen_close is not None:
                    close_today = pos.last_seen_close
                    exit_was_stale = True
                else:
                    close_today = pos.entry_price
                    exit_was_stale = True

            # Stop hit? `None` means the yaml declares no stop.
            if pos.stop_price is not None and close_today <= pos.stop_price:
                pnl_pct = (close_today - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.capital_at_entry * pnl_pct
                equity += pnl_dollar
                hold_days_actual = (datetime.strptime(d, "%Y-%m-%d") -
                                    datetime.strptime(pos.entry_date, "%Y-%m-%d")).days
                closed.append(ClosedPosition(
                    trade_id=pos.trade_id, ticker=pos.ticker,
                    insider_id=pos.insider_id, insider_name=pos.insider_name,
                    insider_title=pos.insider_title, company=pos.company,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    capital_at_entry=pos.capital_at_entry,
                    exit_date=d, exit_price=close_today,
                    exit_reason=("stop_stale" if exit_was_stale else "stop"),
                    hold_days=hold_days_actual,
                    pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                    filing_date=pos.filing_date, trade_date=pos.trade_date,
                    pit_grade=pos.pit_grade, career_grade=pos.career_grade,
                    conviction=pos.conviction,
                    is_csuite=pos.is_csuite, is_rare_reversal=pos.is_rare_reversal,
                    equity_after=equity,
                ))
                continue

            # Time exit?
            if cal_idx >= pos.target_exit_idx:
                pnl_pct = (close_today - pos.entry_price) / pos.entry_price
                pnl_dollar = pos.capital_at_entry * pnl_pct
                equity += pnl_dollar
                hold_days_actual = (datetime.strptime(d, "%Y-%m-%d") -
                                    datetime.strptime(pos.entry_date, "%Y-%m-%d")).days
                closed.append(ClosedPosition(
                    trade_id=pos.trade_id, ticker=pos.ticker,
                    insider_id=pos.insider_id, insider_name=pos.insider_name,
                    insider_title=pos.insider_title, company=pos.company,
                    entry_date=pos.entry_date, entry_price=pos.entry_price,
                    capital_at_entry=pos.capital_at_entry,
                    exit_date=d, exit_price=close_today,
                    exit_reason=("time_stale" if exit_was_stale else "time"),
                    hold_days=hold_days_actual,
                    pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                    filing_date=pos.filing_date, trade_date=pos.trade_date,
                    pit_grade=pos.pit_grade, career_grade=pos.career_grade,
                    conviction=pos.conviction,
                    is_csuite=pos.is_csuite, is_rare_reversal=pos.is_rare_reversal,
                    equity_after=equity,
                ))
                if exit_was_stale:
                    logger.warning(
                        "[%s] STALE exit for %s on %s — used last-seen close %.4f from %s (data gap)",
                        strategy_name, pos.ticker, d, close_today,
                        pos.last_seen_date or "ENTRY",
                    )
                continue

            pos.days_held += 1
            kept.append(pos)
        held = kept

        # ── 2) Today's filings → filter + conviction + capacity ─────────
        candidates_today = trades_by_date.get(d, [])
        if not candidates_today:
            continue

        passing = []
        for t in candidates_today:
            ok, _ = evaluate_filters(thesis_filters, t)
            if not ok:
                continue

            # min_prior_10b5_1_sells (for tenb51_surprise)
            min_10b5 = thesis_filters.get("min_prior_10b5_1_sells")
            if min_10b5:
                n = count_prior_10b5_1_sells(conn, t["insider_id"], t["ticker"], d)
                if n < int(min_10b5):
                    continue

            # Conviction
            conv = compute_conviction(
                thesis=strategy_name,
                signal_grade=t.get("pit_grade") or "C",
                consecutive_sells=t.get("consecutive_sells_before"),
                dip_1mo=t.get("dip_1mo"),
                is_largest_ever=bool(t.get("is_largest_ever")),
                above_sma50=bool(t.get("above_sma50")),
                above_sma200=bool(t.get("above_sma200")),
                insider_title=t.get("title"),
                is_csuite=bool(t.get("is_csuite")),
            )
            if conv < min_conviction:
                continue
            t["_conviction"] = conv
            passing.append(t)

        # Sort by conviction DESC for capacity allocation
        passing.sort(key=lambda x: -x["_conviction"])

        # ── 3) Capacity check + entry ───────────────────────────────────
        #
        # CIRCUIT BREAKER. Every strategy yaml declares circuit_breaker_dd_pct
        # and cw_runner enforces it; the simulator did not, so the published
        # backtest ran a risk control the live alerts apply. Same class as the
        # 2026-08-20 stop divergence, inverted.
        #
        # Measured from the PEAK, which is what "a 15% drawdown" means to a
        # reader. cw_runner computes 1 - equity/starting_capital instead, so its
        # breaker can only fire while the book is below its original stake —
        # inert for any book that has grown. Insider Breakout fell from $628k to
        # $315k in this sample without ever tripping it, because its minimum
        # equity ($89,360) stayed above the $85,000 line.
        #
        # Halts NEW entries only; open positions run to their time exit and
        # entries resume once the book recovers. Off unless the yaml opts in via
        # circuit_breaker_from_peak, so no existing figure moves.
        if breaker_from_peak:
            peak_equity = max(peak_equity, equity)
            if peak_equity > 0 and (peak_equity - equity) / peak_equity >= breaker_dd:
                halted_days += 1
                held_tickers = {p.ticker for p in held}
                continue

        held_tickers = {p.ticker for p in held}
        entered_today = set()
        for t in passing:
            ticker = t["ticker"]
            if ticker in held_tickers or ticker in entered_today:
                continue
            if len(held) + len(entered_today) >= max_concurrent:
                # at_capacity rule: 'skip' for QM/10b5, 'replace_weakest' for RD
                # For the simulator we use 'skip' uniformly to keep it simple
                # and matching the historical default. Rotation logic can be
                # layered in later if backtests show it's meaningful.
                break

            # When could we actually have filled, and at what?
            #
            #   picked up before 16:00 ET -> that session's CLOSE
            #   picked up after           -> the NEXT session's OPEN
            #
            # Not the next close. A filing accepted after the bell is
            # actionable at the next open, and using that session's close
            # instead hands the model a free day of drift in whichever
            # direction the news pushed the stock. CDNL: accepted 17:37,
            # booked at the $39.34 close, first real price $41.74 the next
            # morning.
            offset, price_field = entry_fill(t.get("filed_at"), t.get("trade_id"))
            first_idx = cal_idx + offset
            entry_lookup = find_first_price_on_or_after(
                prices, cal, ticker, first_idx, field=price_field)
            if entry_lookup is None:
                continue
            entry_idx, entry_price = entry_lookup
            if entry_price < 2.0:
                continue   # min price floor

            capital = position_size_pct * equity

            # Never exceed the mandate. max_concurrent x position_size_pct
            # happens to cap an unlevered book at 100%, but that is arithmetic
            # rather than a rule and stops being true the moment either moves.
            room = equity * leverage - sum(p.capital_at_entry for p in held)
            if room <= 0:
                continue
            capital = min(capital, room)

            target_exit_idx = entry_idx + hold_td

            held.append(OpenPosition(
                trade_id=t["trade_id"], ticker=ticker,
                insider_id=t["insider_id"], insider_name=t.get("insider_name"),
                insider_title=t.get("title"), company=t.get("company"),
                entry_date=cal[entry_idx], entry_price=entry_price,
                capital_at_entry=capital,
                target_exit_idx=target_exit_idx,
                stop_price=None if stop_pct is None else entry_price * (1 + stop_pct),
                filing_date=t.get("filing_date"), trade_date=t.get("trade_date"),
                pit_grade=t.get("pit_grade"), career_grade=t.get("career_grade"),
                conviction=t["_conviction"],
                is_csuite=bool(t.get("is_csuite")),
                is_rare_reversal=bool(t.get("is_rare_reversal")),
            ))
            entered_today.add(ticker)
            held_tickers.add(ticker)

    if leverage > 1.0:
        logger.info("[%s] %.1fx leverage — financing paid $%.0f", strategy_name,
                    leverage, financing_paid)
    return closed, held, equity


# ── Persistence ─────────────────────────────────────────────────────────

def wipe_strategy(conn, strategy_name: str) -> int:
    n = conn.execute(
        "DELETE FROM strategy_portfolio WHERE strategy = ?",
        (strategy_name,),
    ).rowcount
    conn.commit()
    return n or 0


def ensure_portfolio_row(conn, strategy_name: str) -> int:
    """Get or create the portfolios row id."""
    row = conn.execute(
        "SELECT id FROM portfolios WHERE name = ?", (strategy_name,)
    ).fetchone()
    if row:
        return int(row["id"] if hasattr(row, "keys") else row[0])
    conn.execute(
        """INSERT INTO portfolios (name, display_name, description, starting_capital)
           VALUES (?, ?, ?, ?)""",
        (strategy_name, strategy_name, "", STARTING_CAPITAL),
    )
    conn.commit()
    return ensure_portfolio_row(conn, strategy_name)


def persist_positions(
    conn,
    strategy_name: str,
    closed: List[ClosedPosition],
    open_at_end: List[OpenPosition],
    final_equity: float,
    stop_pct: Optional[float] = None,
    hold_td: int = 0,
):
    """Write all positions to strategy_portfolio."""
    portfolio_id = ensure_portfolio_row(conn, strategy_name)
    # stop_pct is NOT NULL in strategy_portfolio; 0 is how "no stop" is stored.
    stop_col = 0.0 if stop_pct is None else abs(stop_pct)

    # Closed
    for c in closed:
        reasoning = json.dumps({
            "thesis": strategy_name,
            "filing_date": c.entry_date,
            "conviction": c.conviction,
            "career_grade": c.career_grade,
            "pit_grade": c.pit_grade,
            "is_csuite": c.is_csuite,
            "is_rare_reversal": c.is_rare_reversal,
        }, default=str)
        conn.execute(
            """INSERT INTO strategy_portfolio (
                  strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                  entry_date, entry_price, exit_date, exit_price,
                  hold_days, target_hold, stop_pct, stop_hit,
                  pnl_pct, pnl_dollar, position_size, dollar_amount,
                  portfolio_value, equity_after,
                  insider_name, insider_title, is_csuite,
                  company, filing_date, trade_date,
                  signal_grade, signal_quality, is_rare_reversal,
                  exit_reason, status,
                  execution_source, is_estimated, is_live,
                  entry_reasoning, instrument, shares
              ) VALUES (
                  ?, ?, ?, ?, 'buy_stock', 'long',
                  ?, ?, ?, ?,
                  ?, ?, ?, ?,
                  ?, ?, ?, ?,
                  ?, ?,
                  ?, ?, ?,
                  ?, ?, ?,
                  ?, ?, ?,
                  ?, 'closed',
                  'simulated', 1, FALSE,
                  ?, 'stock', ?
              )""",
            (
                strategy_name, portfolio_id, c.trade_id, c.ticker,
                c.entry_date, c.entry_price, c.exit_date, c.exit_price,
                c.hold_days, c.hold_days,
                stop_col, 1 if c.exit_reason == "stop" else 0,
                c.pnl_pct, c.pnl_dollar,
                c.capital_at_entry / max(STARTING_CAPITAL, c.equity_after - c.pnl_dollar),
                c.capital_at_entry,
                c.equity_after - c.pnl_dollar, c.equity_after,
                c.insider_name, c.insider_title, int(bool(c.is_csuite)),
                c.company, c.filing_date, c.trade_date,
                c.career_grade, c.conviction, 1 if c.is_rare_reversal else 0,
                c.exit_reason,
                reasoning,
                int(c.capital_at_entry / c.entry_price) if c.entry_price else 0,
            ),
        )

    # Open (still in flight)
    for o in open_at_end:
        reasoning = json.dumps({
            "thesis": strategy_name,
            "filing_date": o.entry_date,
            "conviction": o.conviction,
            "career_grade": o.career_grade,
            "pit_grade": o.pit_grade,
            "is_csuite": o.is_csuite,
            "is_rare_reversal": o.is_rare_reversal,
        }, default=str)
        conn.execute(
            """INSERT INTO strategy_portfolio (
                  strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                  entry_date, entry_price, target_hold, stop_pct,
                  position_size, dollar_amount, portfolio_value,
                  insider_name, insider_title, is_csuite,
                  company, filing_date, trade_date,
                  signal_grade, signal_quality, is_rare_reversal,
                  status, execution_source, is_estimated, is_live,
                  entry_reasoning, instrument, shares
              ) VALUES (
                  ?, ?, ?, ?, 'buy_stock', 'long',
                  ?, ?, ?, ?,
                  ?, ?, ?,
                  ?, ?, ?,
                  ?, ?, ?,
                  ?, ?, ?,
                  'open', 'simulated', 1, FALSE,
                  ?, 'stock', ?
              )""",
            (
                strategy_name, portfolio_id, o.trade_id, o.ticker,
                o.entry_date, o.entry_price,
                # The HOLD LENGTH, not the exit index.
                #
                # This wrote target_exit_idx, which is a position in the
                # trading-day calendar array — so every open position reported
                # a target hold of 917-958 days while closed ones correctly
                # showed 29-92. On the trade detail page that rendered as
                # "Target Hold 947 days" for a book whose thesis holds 42.
                hold_td,
                stop_col,
                o.capital_at_entry / max(STARTING_CAPITAL, final_equity),
                o.capital_at_entry, final_equity,
                o.insider_name, o.insider_title, int(bool(o.is_csuite)),
                o.company, o.filing_date, o.trade_date,
                o.career_grade, o.conviction, 1 if o.is_rare_reversal else 0,
                reasoning,
                int(o.capital_at_entry / o.entry_price) if o.entry_price else 0,
            ),
        )

    conn.commit()


# ── Main ────────────────────────────────────────────────────────────────

def run(strategy_name: str, mode: str, end_date: str) -> Dict[str, int]:
    conn = get_connection()
    sc = STRATEGY_CONFIG[strategy_name]
    cfg = yaml.safe_load(sc["yaml"].read_text())

    if mode == "rebuild":
        n_deleted = wipe_strategy(conn, strategy_name)
        logger.info("[%s] wiped %d existing rows", strategy_name, n_deleted)
        start = sc["start_date"]
    elif mode == "extend":
        # Full wipe of every simulated row for this strategy, then re-run from
        # start_date. The previous 90d-window DELETE left pre-cutoff rows in
        # place and the re-sim re-inserted them, causing daily duplicate
        # accumulation (audited 2026-05-22). Full wipe costs ~30s/strategy
        # which is fine for a daily job.
        n_deleted = conn.execute(
            """DELETE FROM strategy_portfolio
               WHERE strategy = ? AND execution_source = 'simulated'""",
            (strategy_name,),
        ).rowcount
        conn.commit()
        logger.info("[%s] extend mode: wiped %d simulated rows",
                    strategy_name, n_deleted or 0)
        start = sc["start_date"]
    else:
        raise ValueError(mode)

    t0 = time.monotonic()
    closed, open_at_end, final_equity = simulate_one_strategy(
        conn, strategy_name, cfg, start, end_date,
    )
    elapsed = time.monotonic() - t0
    logger.info("[%s] sim done in %.1fs — closed=%d, open=%d, final_equity=$%.0f",
                strategy_name, elapsed, len(closed), len(open_at_end), final_equity)

    thesis_exit = (cfg.get("theses") or [{}])[0].get("exit") or cfg.get("exit") or {}
    persist_positions(conn, strategy_name, closed, open_at_end, final_equity,
                      stop_pct=resolve_stop_pct(cfg),
                      hold_td=int(thesis_exit.get("hold_days", 30)))
    logger.info("[%s] persisted %d closed + %d open positions",
                strategy_name, len(closed), len(open_at_end))

    conn.close()
    return {
        "n_closed": len(closed),
        "n_open": len(open_at_end),
        "final_equity": round(final_equity, 0),
        "total_return_pct": round((final_equity / STARTING_CAPITAL - 1) * 100, 1),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=list(STRATEGY_CONFIG),
                   help="Single strategy to run")
    p.add_argument("--all", action="store_true", help="Run all 3 strategies")
    p.add_argument("--rebuild", action="store_true",
                   help="Wipe + re-simulate from scratch (one-shot)")
    p.add_argument("--extend", action="store_true",
                   help="Daily incremental: wipe all simulated rows for the strategy and re-run from start_date")
    p.add_argument("--end", default=None,
                   help="End date (default: today)")
    args = p.parse_args()

    if not args.rebuild and not args.extend:
        p.error("specify --rebuild or --extend")
    if not args.strategy and not args.all:
        p.error("specify --strategy or --all")

    mode = "rebuild" if args.rebuild else "extend"
    end_date = args.end or date.today().isoformat()

    strategies = list(STRATEGY_CONFIG) if args.all else [args.strategy]

    from framework.observability import pipeline_run

    with pipeline_run(
        "strategy_simulator",
        log_path="/Users/derekg/trading-framework/logs/strategy-simulator.log",
    ) as prun:
        results = {}
        for s in strategies:
            logger.info("=" * 60)
            results[s] = run(s, mode, end_date)
        logger.info("=" * 60)
        logger.info("Summary:")
        for s, r in results.items():
            logger.info(
                "  %s: closed=%d, open=%d, final=$%s (%s%% total return)",
                s, r["n_closed"], r["n_open"], f"{r['final_equity']:,.0f}",
                f"{'+' if r['total_return_pct'] >= 0 else ''}{r['total_return_pct']:.1f}",
            )

        # Record telemetry: per-strategy closed/open counts + sum of rows touched.
        total_rows = sum(r["n_closed"] + r["n_open"] for r in results.values())
        prun.set_rows_written(total_rows)
        prun.set_metadata({
            "mode": mode,
            "end_date": end_date,
            "strategies": list(strategies),
            "results": results,
        })


if __name__ == "__main__":
    main()
