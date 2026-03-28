#!/usr/bin/env python3
"""Portfolio Simulator — offline backtester that populates strategy_portfolio.

Replicates the portfolio_runner logic using historical daily_prices data
instead of Alpaca. Every trade gets full entry_reasoning and exit_reasoning
JSON blobs for the trade detail UI.

Usage:
    # Fill the gap period (Feb 11 - Mar 25, 2026)
    python3 pipelines/portfolio_simulator.py --start 2026-02-11 --end 2026-03-25

    # Full simulation from scratch (wipes existing backtest trades first)
    python3 pipelines/portfolio_simulator.py --start 2016-01-01 --end 2026-03-26 --fresh

    # Dry run — log trades without writing to DB
    python3 pipelines/portfolio_simulator.py --start 2026-01-01 --end 2026-03-25 --dry-run

    # Specify a different portfolio config
    python3 pipelines/portfolio_simulator.py --portfolio form4_insider --start 2016-01-01

    # Backfill reasoning JSON for existing trades (no new trades)
    python3 pipelines/portfolio_simulator.py --backfill-reasoning
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = DB_PATH.parent / "prices.db"

# ---------------------------------------------------------------------------
# C-suite detection (same as portfolio_runner)
# ---------------------------------------------------------------------------

CSUITE_KEYWORDS = [
    "ceo", "chief exec", "president", "pres", "chairman", "chair",
    "cfo", "chief financial", "coo", "chief operating",
    "evp", "executive vp", "svp", "senior vp",
]


def _is_csuite(title: str | None) -> bool:
    if not title:
        return False
    t = title.lower()
    return any(kw in t for kw in CSUITE_KEYWORDS)


def _is_10pct_owner(title: str | None) -> bool:
    if not title:
        return False
    t = title.lower()
    return "10%" in t or "10 percent" in t or "ten percent" in t


# ---------------------------------------------------------------------------
# Signal quality scoring (identical to portfolio_runner)
# ---------------------------------------------------------------------------

def compute_signal_quality(
    pit_wr: float | None,
    pit_n: int | None,
    is_csuite: bool,
    holdings_pct_change: float | None,
    is_10pct_owner: bool,
    title: str | None = None,
    is_rare_reversal: bool = False,
    switch_rate: float | None = None,
) -> tuple[float, dict]:
    """V4 signal quality: role-weighted, rare reversal, switch rate, validated holdings.
    Returns (score, breakdown_dict)."""
    breakdown = {"baseline": 5.0}
    score = 5.0

    # PIT win rate
    pit_bonus = 0.0
    if pit_n and pit_n >= 3 and pit_wr is not None:
        if pit_wr >= 0.7:
            pit_bonus = 2.0
        elif pit_wr >= 0.6:
            pit_bonus = 1.0
        elif pit_wr < 0.4:
            pit_bonus = -1.0
    breakdown["pit_win_rate_bonus"] = pit_bonus
    score += pit_bonus

    # Role-weighted bonus (CFO/VP > CEO > Director)
    role_bonus = 0.0
    t = (title or "").lower()
    if "cfo" in t or "chief financial" in t:
        role_bonus = 1.5
    elif "vp" in t or "vice pres" in t:
        role_bonus = 1.0
    elif is_csuite:
        role_bonus = 0.5
    breakdown["csuite_bonus"] = role_bonus
    score += role_bonus

    # Rare reversal bonus
    reversal_bonus = 1.5 if is_rare_reversal else 0.0
    breakdown["rare_reversal_bonus"] = reversal_bonus
    score += reversal_bonus

    # Switch rate bonus (V4: moderate switchers 10-30% are best)
    switch_bonus = 0.0
    if switch_rate is not None:
        if 0.10 <= switch_rate <= 0.30:
            switch_bonus = 0.5  # moderate switcher — good signal
    breakdown["switch_rate_bonus"] = switch_bonus
    score += switch_bonus

    # Holdings % change (V4: tiered — big increases get more credit)
    holdings_bonus = 0.0
    if holdings_pct_change is not None:
        if holdings_pct_change >= 1.0:
            holdings_bonus = 1.5   # >100% increase — very strong signal
        elif holdings_pct_change >= 0.50:
            holdings_bonus = 1.0   # 50-100% — strong
        elif holdings_pct_change >= 0.10:
            holdings_bonus = 0.5   # 10-50% — moderate (was 1.0 in V3)
        elif holdings_pct_change < 0.01:
            holdings_bonus = -0.5  # <1% — noise
    breakdown["holdings_bonus"] = holdings_bonus
    score += holdings_bonus

    # 10% owner penalty
    owner_penalty = -2.0 if is_10pct_owner else 0.0
    breakdown["owner_10pct_penalty"] = owner_penalty
    score += owner_penalty

    return max(0.0, min(10.0, score)), breakdown


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=rw", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=wal")
    if PRICES_DB.exists():
        conn.execute(f"ATTACH DATABASE 'file:{PRICES_DB}?mode=ro' AS prices")
    return conn


def get_portfolio_config(conn: sqlite3.Connection, name: str) -> dict:
    row = conn.execute("SELECT * FROM portfolios WHERE name = ?", (name,)).fetchone()
    if not row:
        raise ValueError(f"Portfolio '{name}' not found in portfolios table")
    cfg = dict(row)
    cfg["params"] = json.loads(cfg["config"]) if cfg["config"] else {}
    return cfg


def get_price(conn: sqlite3.Connection, ticker: str, date: str) -> dict | None:
    """Get daily OHLCV for a ticker on a specific date."""
    row = conn.execute("""
        SELECT open, high, low, close, volume
        FROM daily_prices
        WHERE ticker = ? AND date = ?
    """, (ticker, date)).fetchone()
    return dict(row) if row else None


def get_next_trading_day(conn: sqlite3.Connection, ticker: str, after_date: str) -> str | None:
    """Find the next date with price data for this ticker after the given date."""
    row = conn.execute("""
        SELECT date FROM daily_prices
        WHERE ticker = ? AND date > ?
        ORDER BY date ASC LIMIT 1
    """, (ticker, after_date)).fetchone()
    return row["date"] if row else None


def get_trading_dates(conn: sqlite3.Connection, start: str, end: str) -> list[str]:
    """Get all distinct trading dates (from SPY) in range."""
    rows = conn.execute("""
        SELECT DISTINCT date FROM daily_prices
        WHERE ticker = 'SPY' AND date >= ? AND date <= ?
        ORDER BY date
    """, (start, end)).fetchall()
    return [r["date"] for r in rows]


# ---------------------------------------------------------------------------
# Filing scanner — same logic as portfolio_runner
# ---------------------------------------------------------------------------

def scan_filings_for_date(conn: sqlite3.Connection, filing_date: str) -> list[dict]:
    """Find qualifying buy filings on a specific date."""
    rows = conn.execute("""
        SELECT
            t.filing_key,
            MIN(t.trade_id) AS trade_id,
            t.ticker,
            MAX(t.company) AS company,
            MAX(COALESCE(i.display_name, i.name)) AS insider_name,
            MAX(t.title) AS title,
            t.insider_id,
            t.filing_date,
            MAX(t.trade_date) AS trade_date,
            SUM(t.value) AS total_value,
            SUM(t.qty) AS total_qty,
            -- PIT scoring columns
            t.pit_n_trades,
            t.pit_win_rate_7d,
            t.pit_avg_abnormal_7d,
            t.pit_win_rate_30d,
            t.signal_grade,
            t.is_rare_reversal,
            t.insider_switch_rate,
            t.week52_proximity,
            t.is_10b5_1,
            -- Holdings (direct only — indirect inflates %)
            MAX(CASE WHEN t.direct_indirect = 'D' OR t.direct_indirect IS NULL
                THEN t.shares_owned_after ELSE NULL END) AS shares_after,
            -- Transaction group for deduplication
            MIN(t.txn_group_id) AS txn_group_id
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.filing_date = ?
          AND t.trans_code = 'P'
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
          AND t.title NOT LIKE '%10%%owner%'
          AND t.title NOT LIKE '%10 percent%'
        GROUP BY t.filing_key
        ORDER BY t.filing_date
    """, (filing_date,)).fetchall()

    candidates = []
    for r in rows:
        r = dict(r)
        title = r["title"] or ""

        if _is_10pct_owner(title):
            continue

        # Holdings % change
        shares_after = r["shares_after"] or 0
        total_qty = r["total_qty"] or 0
        shares_before = shares_after - total_qty if shares_after > 0 else 0
        holdings_pct = (
            (shares_after - shares_before) / shares_before
            if shares_before > 0 else None
        )

        csuite = _is_csuite(title)
        quality, breakdown = compute_signal_quality(
            pit_wr=r["pit_win_rate_7d"],
            pit_n=r["pit_n_trades"],
            is_csuite=csuite,
            holdings_pct_change=holdings_pct,
            is_10pct_owner=False,
            title=title,
            is_rare_reversal=bool(r["is_rare_reversal"]),
            switch_rate=r.get("insider_switch_rate"),
        )

        if quality < 2:  # very low floor — real filtering happens in simulator/runner
            continue

        candidates.append({
            "trade_id": r["trade_id"],
            "filing_key": r["filing_key"],
            "ticker": r["ticker"],
            "company": r["company"],
            "insider_name": r["insider_name"],
            "insider_id": r["insider_id"],
            "title": title,
            "filing_date": r["filing_date"],
            "trade_date": r["trade_date"],
            "total_value": r["total_value"] or 0,
            "total_qty": r["total_qty"] or 0,
            "shares_after": shares_after,
            "holdings_pct_change": holdings_pct,
            "pit_n": r["pit_n_trades"],
            "pit_wr": r["pit_win_rate_7d"],
            "pit_avg_abnormal_7d": r["pit_avg_abnormal_7d"],
            "pit_wr_30d": r["pit_win_rate_30d"],
            "signal_quality": quality,
            "signal_quality_breakdown": breakdown,
            "signal_grade": r["signal_grade"],
            "is_csuite": csuite,
            "is_rare_reversal": bool(r["is_rare_reversal"]),
            "txn_group_id": r.get("txn_group_id"),
            "insider_switch_rate": r["insider_switch_rate"],
            "week52_proximity": r["week52_proximity"],
            "is_10b5_1": bool(r["is_10b5_1"]),
        })

    return candidates


# ---------------------------------------------------------------------------
# Cluster detection
# ---------------------------------------------------------------------------

def detect_clusters(candidates: list[dict]) -> dict[str, list[dict]]:
    """Group candidates by ticker to detect clusters (multiple insiders same day).
    Deduplicates by txn_group_id — filings reporting the same economic event
    count as one cluster member, not multiple."""
    by_ticker: dict[str, list[dict]] = {}
    for c in candidates:
        by_ticker.setdefault(c["ticker"], []).append(c)

    # Deduplicate: within each ticker group, collapse entries with the same txn_group_id
    # Keep the one with the highest signal quality (or first if tied)
    for ticker, group in by_ticker.items():
        seen_groups: dict[int | None, dict] = {}
        for c in group:
            gid = c.get("txn_group_id")
            if gid is None:
                # No group ID — treat as unique
                seen_groups[id(c)] = c
            elif gid not in seen_groups or (c["signal_quality"] > seen_groups[gid]["signal_quality"]):
                seen_groups[gid] = c
        by_ticker[ticker] = list(seen_groups.values())

    return by_ticker


# ---------------------------------------------------------------------------
# Build reasoning JSON
# ---------------------------------------------------------------------------

def build_entry_reasoning(
    candidate: dict,
    cluster_size: int,
    cluster_insiders: list[str],
    entry_price: float,
    position_size: float,
    dollar_amount: float,
    shares: int,
    portfolio_equity: float,
    params: dict,
) -> str:
    """Build the entry_reasoning JSON blob."""
    reasoning = {
        "signal": {
            "quality": candidate["signal_quality"],
            "grade": candidate.get("signal_grade"),
            "breakdown": candidate["signal_quality_breakdown"],
        },
        "insider": {
            "name": candidate["insider_name"],
            "title": candidate["title"],
            "is_csuite": candidate["is_csuite"],
            "pit_win_rate_7d": candidate["pit_wr"],
            "pit_n_trades": candidate["pit_n"],
            "pit_avg_abnormal_7d": candidate.get("pit_avg_abnormal_7d"),
            "pit_win_rate_30d": candidate.get("pit_wr_30d"),
            "switch_rate": candidate.get("insider_switch_rate"),
            "is_rare_reversal": candidate["is_rare_reversal"],
        },
        "filing": {
            "filing_date": candidate["filing_date"],
            "trade_date": candidate.get("trade_date"),
            "trade_value": candidate["total_value"],
            "shares_bought": candidate["total_qty"],
            "shares_after": candidate.get("shares_after"),
            "holdings_pct_change": candidate["holdings_pct_change"],
            "is_10b5_1": candidate.get("is_10b5_1", False),
        },
        "company": {
            "ticker": candidate["ticker"],
            "name": candidate["company"],
        },
        "market_context": {
            "entry_price": entry_price,
            "week52_proximity": candidate.get("week52_proximity"),
        },
        "position": {
            "size_pct": position_size,
            "dollar_amount": dollar_amount,
            "shares": shares,
            "portfolio_equity": portfolio_equity,
            "target_hold_days": params.get("target_hold_days", 30),
            "hard_stop_pct": params.get("hard_stop_pct", -0.15),
            "trailing_stop_drop": params.get("trailing_stop_drop", 0.10),
        },
        "cluster": {
            "is_cluster": cluster_size > 1,
            "cluster_size": cluster_size,
            "other_insiders": cluster_insiders,
        },
    }
    return json.dumps(reasoning, default=str)


def build_exit_reasoning(
    exit_reason: str,
    exit_price: float,
    entry_price: float,
    peak_return: float,
    peak_date: str | None,
    hold_days: int,
    spy_entry: float | None,
    spy_exit: float | None,
) -> str:
    """Build the exit_reasoning JSON blob."""
    pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
    spy_return = None
    if spy_entry and spy_exit and spy_entry > 0:
        spy_return = (spy_exit - spy_entry) / spy_entry

    reasoning = {
        "exit_reason": exit_reason,
        "exit_price": exit_price,
        "pnl_pct": round(pnl_pct, 6),
        "peak_return": round(peak_return, 6),
        "peak_date": peak_date,
        "hold_days": hold_days,
        "spy_return_during_hold": round(spy_return, 6) if spy_return is not None else None,
    }
    return json.dumps(reasoning, default=str)


# ---------------------------------------------------------------------------
# Simulator core
# ---------------------------------------------------------------------------

class PortfolioSimulator:
    """Day-by-day portfolio simulator using historical daily prices."""

    def __init__(self, conn: sqlite3.Connection, portfolio_name: str, dry_run: bool = False):
        self.conn = conn
        self.dry_run = dry_run
        self.portfolio_cfg = get_portfolio_config(conn, portfolio_name)
        self.portfolio_id = self.portfolio_cfg["id"]
        self.params = self.portfolio_cfg["params"]
        self.starting_capital = self.portfolio_cfg["starting_capital"]

        # Strategy parameters
        self.target_hold = self.params.get("target_hold_days", 30)
        self.hard_stop = self.params.get("hard_stop_pct", -0.15)
        self.trailing_drop = self.params.get("trailing_stop_drop", 0.10)
        self.target_gain = self.params.get("target_gain_pct", None)  # e.g. 0.08 = +8%
        self.max_concurrent = self.params.get("max_concurrent", 15)
        self.min_quality = self.params.get("min_signal_quality", 4.0)

        # V2 filters
        self.require_pit = self.params.get("require_pit_history", False)
        self.min_pit_n = self.params.get("min_pit_n", 3)
        self.min_pit_wr = self.params.get("min_pit_wr", 0.0)
        self.exclude_roles = [r.lower() for r in self.params.get("exclude_roles", [])]

        # Position sizing — variable (quality-based) or flat
        self.size_min = self.params.get("position_size_min", self.params.get("position_size", 0.05))
        self.size_max = self.params.get("position_size_max", self.params.get("position_size", 0.05))

        # Hybrid options: use calls for high-conviction trades
        self.use_options = self.params.get("use_options", True)
        self.option_min_quality = self.params.get("option_min_quality", 8.0)
        self.option_otm_pct = self.params.get("option_otm_pct", 0.07)  # target 7% OTM
        self.option_dte_range = tuple(self.params.get("option_dte_range", [30, 75]))

        # Portfolio state
        self.equity = self.starting_capital
        self.open_positions: list[dict] = []  # list of position dicts
        self.peak_returns: dict[int, tuple[float, str]] = {}  # pos_id -> (peak_return, peak_date)

        # Stats
        self.total_entered = 0
        self.total_exited = 0
        self.total_pnl = 0.0

    def get_position_size(self, signal_quality: float) -> float:
        """Compute position size based on signal quality. Linear interpolation
        from size_min (at min_quality) to size_max (at quality 10)."""
        if self.size_min == self.size_max:
            return self.size_min
        q_range = 10.0 - self.min_quality  # e.g. 10 - 6 = 4
        if q_range <= 0:
            return self.size_min
        t = min(1.0, max(0.0, (signal_quality - self.min_quality) / q_range))
        return self.size_min + t * (self.size_max - self.size_min)

    def find_call_option(self, ticker: str, trade_date: str, stock_price: float) -> dict | None:
        """Find a suitable OTM call for a high-conviction trade.
        Returns dict with expiration, strike, entry_price or None."""
        if stock_price <= 5:
            return None

        target_strike = stock_price * (1 + self.option_otm_pct)
        strike_lo = stock_price * 1.02
        strike_hi = stock_price * 1.15
        dte_min, dte_max = self.option_dte_range

        opt = self.conn.execute("""
            SELECT expiration, strike, close as opt_price, bid, ask
            FROM option_prices
            WHERE ticker = ? AND right = 'C' AND trade_date = ?
              AND strike BETWEEN ? AND ?
              AND julianday(expiration) - julianday(trade_date) BETWEEN ? AND ?
              AND close > 0.10
            ORDER BY ABS(strike - ?) ASC
            LIMIT 1
        """, (ticker, trade_date, strike_lo, strike_hi,
              dte_min, dte_max, target_strike)).fetchone()

        if not opt or not opt["opt_price"]:
            return None

        return {
            "expiration": opt["expiration"],
            "strike": opt["strike"],
            "entry_price": opt["opt_price"],
            "right": "C",
        }

    def get_option_exit_price(self, ticker: str, expiration: str, strike: float,
                               trade_date: str) -> float | None:
        """Get the option close price on a specific date."""
        row = self.conn.execute("""
            SELECT close FROM option_prices
            WHERE ticker = ? AND expiration = ? AND strike = ? AND right = 'C'
              AND trade_date = ?
            LIMIT 1
        """, (ticker, expiration, strike, trade_date)).fetchone()
        return row["close"] if row and row["close"] else None

    def _get_spy_close(self, date: str) -> float | None:
        row = self.conn.execute(
            "SELECT close FROM daily_prices WHERE ticker='SPY' AND date=?",
            (date,),
        ).fetchone()
        return row["close"] if row else None

    def run(self, start: str, end: str) -> dict:
        """Run simulation over date range."""
        trading_dates = get_trading_dates(self.conn, start, end)
        logger.info(
            "Simulating %s: %s to %s (%d trading days), starting equity $%.0f",
            self.portfolio_cfg["name"], start, end, len(trading_dates), self.equity,
        )

        # Load any existing open positions from DB (for gap-fill mode)
        self._load_existing_open_positions()

        for i, date in enumerate(trading_dates):
            # 1. Check exits on today's close
            self._check_exits(date)

            # 2. Scan filings for this date and enter on next day's open
            #    We look at filings from the PREVIOUS day (filed after market,
            #    enter next morning). But for simplicity and matching the backfill,
            #    we scan filings dated `date` and enter on next trading day's open.
            candidates = scan_filings_for_date(self.conn, date)
            if candidates:
                next_day = get_next_trading_day(self.conn, "SPY", date)
                if next_day and next_day <= end:
                    self._enter_positions(candidates, next_day)

            if (i + 1) % 100 == 0:
                logger.info(
                    "  Day %d/%d (%s): %d open, equity $%.0f",
                    i + 1, len(trading_dates), date,
                    len(self.open_positions), self.equity,
                )

        # Force-close any remaining open positions at end of sim
        if trading_dates:
            last_date = trading_dates[-1]
            self._close_remaining(last_date)

        logger.info(
            "Simulation complete: %d entries, %d exits, final equity $%.0f (%.1f%%)",
            self.total_entered, self.total_exited,
            self.equity, ((self.equity / self.starting_capital) - 1) * 100,
        )

        return {
            "entries": self.total_entered,
            "exits": self.total_exited,
            "final_equity": round(self.equity, 2),
            "total_return_pct": round(((self.equity / self.starting_capital) - 1) * 100, 2),
        }

    def _load_existing_open_positions(self):
        """Load open positions from DB for gap-fill continuity."""
        rows = self.conn.execute("""
            SELECT * FROM strategy_portfolio
            WHERE portfolio_id = ? AND status = 'open'
            ORDER BY entry_date
        """, (self.portfolio_id,)).fetchall()
        for r in rows:
            self.open_positions.append(dict(r))
        if self.open_positions:
            logger.info("Loaded %d existing open positions", len(self.open_positions))

    def _check_exits(self, date: str):
        """Check all open positions for exit conditions using daily close."""
        still_open = []

        for pos in self.open_positions:
            ticker = pos["ticker"]
            entry_price = pos["entry_price"]  # stock entry price
            entry_date = pos["entry_date"]
            is_option = pos.get("instrument") == "call_option"

            price_data = get_price(self.conn, ticker, date)
            if not price_data or not price_data["close"]:
                still_open.append(pos)
                continue

            # Stock-based P&L (used for stop/target/trailing decisions)
            current_close = price_data["close"]
            current_low = price_data["low"] or current_close
            current_high = price_data["high"] or current_close
            stock_pnl_pct = (current_close - entry_price) / entry_price if entry_price > 0 else 0

            # For option positions, also get actual option price for P&L calc
            opt_exit_price = None
            if is_option and pos.get("option_expiration") and pos.get("option_strike"):
                opt_exit_price = self.get_option_exit_price(
                    ticker, pos["option_expiration"], pos["option_strike"], date
                )

            # Hold days
            try:
                d_entry = datetime.strptime(entry_date[:10], "%Y-%m-%d")
                d_today = datetime.strptime(date[:10], "%Y-%m-%d")
                hold_days = (d_today - d_entry).days
            except Exception:
                hold_days = 0

            # Track peak return (based on stock movement)
            pos_id = pos.get("id", id(pos))
            prev_peak, prev_peak_date = self.peak_returns.get(pos_id, (0.0, entry_date))
            if stock_pnl_pct > prev_peak:
                self.peak_returns[pos_id] = (stock_pnl_pct, date)
                prev_peak = stock_pnl_pct
                prev_peak_date = date

            # Check exits based on STOCK price movement
            intraday_pnl = (current_low - entry_price) / entry_price if entry_price > 0 else 0
            intraday_high_pnl = (current_high - entry_price) / entry_price if entry_price > 0 else 0

            exit_reason = None

            # 1. Hard stop (stock drops X%)
            if intraday_pnl <= self.hard_stop:
                exit_reason = "stop_loss"
                current_close = entry_price * (1 + self.hard_stop)

            # 2. Target gain (stock rises X%)
            elif self.target_gain and intraday_high_pnl >= self.target_gain:
                exit_reason = "target_gain"
                current_close = entry_price * (1 + self.target_gain)

            # 3. Trailing stop
            elif prev_peak > 0.03 and (prev_peak - stock_pnl_pct) >= self.trailing_drop:
                exit_reason = "trailing_stop"

            # 4. Time exit
            elif hold_days >= self.target_hold:
                exit_reason = "time_exit"

            # For option positions: check expiration
            if is_option and not exit_reason:
                exp_date = pos.get("option_expiration", "")
                if exp_date and date >= exp_date:
                    exit_reason = "option_expiry"

            if exit_reason:
                self._close_position(
                    pos, date, current_close, exit_reason, hold_days,
                    prev_peak, prev_peak_date, opt_exit_price=opt_exit_price,
                )
            else:
                still_open.append(pos)

        self.open_positions = still_open

    def _enter_positions(self, candidates: list[dict], entry_date: str):
        """Enter qualifying positions on entry_date's open."""
        slots = self.max_concurrent - len(self.open_positions)
        if slots <= 0:
            return

        # Detect clusters (deduplicates by txn_group_id within each ticker)
        clusters = detect_clusters(candidates)

        # Flatten deduplicated candidates from clusters
        deduped: list[dict] = []
        seen_groups: set[int] = set()
        for ticker_group in clusters.values():
            for c in ticker_group:
                gid = c.get("txn_group_id")
                if gid and gid in seen_groups:
                    continue
                if gid:
                    seen_groups.add(gid)
                deduped.append(c)

        # Held tickers
        held = {p["ticker"] for p in self.open_positions}

        for c in deduped:
            if slots <= 0:
                break
            if c["signal_quality"] < self.min_quality:
                continue
            if c["ticker"] in held:
                continue
            if not c["ticker"] or c["ticker"] in ("NONE", ""):
                continue

            # V2 filters
            if self.require_pit:
                pit_n = c.get("pit_n") or 0
                pit_wr = c.get("pit_wr") or 0
                if pit_n < self.min_pit_n:
                    continue
                if pit_wr < self.min_pit_wr:
                    continue

            if self.exclude_roles:
                title = (c.get("title") or "").lower()
                if any(role in title for role in self.exclude_roles):
                    # Skip if the ONLY role is an excluded one (e.g. pure "Chairman")
                    has_good_role = any(kw in title for kw in ["ceo", "cfo", "president", "vp", "dir"])
                    if not has_good_role:
                        continue

            # Get entry price (next day's open)
            price_data = get_price(self.conn, c["ticker"], entry_date)
            if not price_data or not price_data["open"] or price_data["open"] <= 0:
                continue

            entry_price = price_data["open"]

            # Position sizing — variable by signal quality
            position_size = self.get_position_size(c["signal_quality"])
            dollar_amount = self.equity * position_size
            shares = int(dollar_amount / entry_price)
            if shares <= 0:
                continue

            actual_dollar = shares * entry_price

            # Instrument selection: options for high-conviction trades
            instrument = "stock"
            opt_contract = None
            if self.use_options and c["signal_quality"] >= self.option_min_quality:
                opt_contract = self.find_call_option(c["ticker"], entry_date, entry_price)
                if opt_contract:
                    instrument = "call_option"

            # Cluster info
            ticker_cluster = clusters.get(c["ticker"], [])
            cluster_size = len(ticker_cluster)
            cluster_insiders = [
                x["insider_name"] for x in ticker_cluster
                if x["insider_name"] != c["insider_name"]
            ]

            # Build reasoning
            entry_reasoning = build_entry_reasoning(
                candidate=c,
                cluster_size=cluster_size,
                cluster_insiders=cluster_insiders,
                entry_price=entry_price,
                position_size=position_size,
                dollar_amount=actual_dollar,
                shares=shares,
                portfolio_equity=self.equity,
                params=self.params,
            )

            if self.dry_run:
                logger.info(
                    "  [DRY] ENTER %s @ $%.2f (quality=%.1f, insider=%s)",
                    c["ticker"], entry_price, c["signal_quality"], c["insider_name"],
                )
                slots -= 1
                self.total_entered += 1
                continue

            # Insert into DB
            trade_type = "buy_call" if instrument == "call_option" else "buy_stock"
            cur = self.conn.execute("""
                INSERT INTO strategy_portfolio (
                    strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                    entry_date, entry_price, target_hold, stop_pct,
                    position_size, portfolio_value,
                    insider_name, insider_pit_n, insider_pit_wr,
                    signal_quality, status,
                    execution_source, is_estimated,
                    entry_reasoning,
                    company, insider_title, filing_date, trade_date, trade_value,
                    signal_grade, is_csuite, holdings_pct_change,
                    is_rare_reversal, is_cluster, cluster_size,
                    shares, dollar_amount,
                    instrument, option_expiration, option_strike, option_right,
                    option_entry_price
                ) VALUES (
                    ?, ?, ?, ?, ?, 'long',
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, 'open',
                    'backtest', 1,
                    ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?,
                    ?
                )
            """, (
                self.portfolio_cfg["name"], self.portfolio_id,
                c["trade_id"], c["ticker"], trade_type,
                entry_date, round(entry_price, 4), self.target_hold, self.hard_stop,
                position_size, round(self.equity, 2),
                c["insider_name"], c["pit_n"], c["pit_wr"],
                c["signal_quality"],
                entry_reasoning,
                c["company"], c["title"], c["filing_date"], c.get("trade_date"),
                c["total_value"],
                c.get("signal_grade"), 1 if c["is_csuite"] else 0,
                c["holdings_pct_change"],
                1 if c["is_rare_reversal"] else 0,
                1 if cluster_size > 1 else 0, cluster_size,
                shares, round(actual_dollar, 2),
                instrument,
                opt_contract["expiration"] if opt_contract else None,
                opt_contract["strike"] if opt_contract else None,
                opt_contract["right"] if opt_contract else None,
                opt_contract["entry_price"] if opt_contract else None,
            ))
            self.conn.commit()

            pos = {
                "id": cur.lastrowid,
                "ticker": c["ticker"],
                "entry_date": entry_date,
                "entry_price": entry_price,
                "position_size": position_size,
                "portfolio_value": self.equity,
                "target_hold": self.target_hold,
                "instrument": instrument,
                "option_expiration": opt_contract["expiration"] if opt_contract else None,
                "option_strike": opt_contract["strike"] if opt_contract else None,
                "option_entry_price": opt_contract["entry_price"] if opt_contract else None,
            }
            self.open_positions.append(pos)
            held.add(c["ticker"])
            slots -= 1
            self.total_entered += 1

    def _close_position(
        self,
        pos: dict,
        exit_date: str,
        exit_price: float,
        exit_reason: str,
        hold_days: int,
        peak_return: float,
        peak_date: str | None,
        opt_exit_price: float | None = None,
    ):
        """Close a position and update DB."""
        entry_price = pos["entry_price"]  # stock entry price
        is_option = pos.get("instrument") == "call_option"
        dollar_alloc = pos["position_size"] * pos["portfolio_value"]

        if is_option and pos.get("option_entry_price") and opt_exit_price is not None:
            # Option P&L: return on premium
            opt_entry = pos["option_entry_price"]
            opt_return = (opt_exit_price - opt_entry) / opt_entry if opt_entry > 0 else 0
            opt_return = max(opt_return, -1.0)  # can't lose more than premium
            pnl_pct = opt_return
            pnl_dollar = dollar_alloc * opt_return
        elif is_option and pos.get("option_entry_price") and opt_exit_price is None:
            # No option exit price — use stock return as fallback
            stock_pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
            # Approximate option return as ~3x leveraged stock return (rough delta*leverage)
            pnl_pct = stock_pnl_pct * 3.0
            pnl_pct = max(pnl_pct, -1.0)
            pnl_dollar = dollar_alloc * pnl_pct
        else:
            # Stock P&L
            pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
            pnl_dollar = dollar_alloc * pnl_pct

        # SPY return during hold
        spy_entry = self._get_spy_close(pos["entry_date"])
        spy_exit = self._get_spy_close(exit_date)

        exit_reasoning = build_exit_reasoning(
            exit_reason=exit_reason,
            exit_price=exit_price,
            entry_price=entry_price,
            peak_return=peak_return,
            peak_date=peak_date,
            hold_days=hold_days,
            spy_entry=spy_entry,
            spy_exit=spy_exit,
        )

        self.equity += pnl_dollar
        self.total_exited += 1
        self.total_pnl += pnl_dollar

        if not self.dry_run and pos.get("id"):
            self.conn.execute("""
                UPDATE strategy_portfolio SET
                    exit_date = ?,
                    exit_price = ?,
                    hold_days = ?,
                    pnl_pct = ?,
                    pnl_dollar = ?,
                    stop_hit = ?,
                    exit_reason = ?,
                    status = 'closed',
                    equity_after = ?,
                    exit_reasoning = ?,
                    peak_return = ?,
                    option_exit_price = ?
                WHERE id = ?
            """, (
                exit_date,
                round(exit_price, 4),
                hold_days,
                round(pnl_pct, 6),
                round(pnl_dollar, 2),
                1 if exit_reason == "stop_loss" else 0,
                exit_reason,
                round(self.equity, 2),
                exit_reasoning,
                round(peak_return, 6),
                round(opt_exit_price, 4) if opt_exit_price is not None else None,
                pos["id"],
            ))
            self.conn.commit()

        # Cleanup peak tracking
        pos_id = pos.get("id", id(pos))
        self.peak_returns.pop(pos_id, None)

    def _close_remaining(self, last_date: str):
        """Close any positions still open at the end of simulation (not at end of data)."""
        # Don't force-close — leave them open for the live runner to pick up
        # Only log them
        if self.open_positions:
            logger.info(
                "%d positions still open at end of simulation:",
                len(self.open_positions),
            )
            for pos in self.open_positions:
                price_data = get_price(self.conn, pos["ticker"], last_date)
                current = price_data["close"] if price_data else pos["entry_price"]
                pnl_pct = (current - pos["entry_price"]) / pos["entry_price"] if pos["entry_price"] > 0 else 0
                logger.info(
                    "  %s: entered %s @ $%.2f, current $%.2f (%.1f%%)",
                    pos["ticker"], pos["entry_date"], pos["entry_price"],
                    current, pnl_pct * 100,
                )


# ---------------------------------------------------------------------------
# Backfill reasoning for existing trades
# ---------------------------------------------------------------------------

def backfill_reasoning(conn: sqlite3.Connection, portfolio_name: str):
    """Add entry_reasoning and exit_reasoning JSON to existing trades that lack it."""
    portfolio = get_portfolio_config(conn, portfolio_name)
    params = portfolio["params"]

    rows = conn.execute("""
        SELECT sp.id, sp.trade_id, sp.ticker, sp.entry_date, sp.entry_price,
               sp.exit_date, sp.exit_price, sp.exit_reason, sp.hold_days,
               sp.insider_name, sp.insider_pit_n, sp.insider_pit_wr,
               sp.signal_quality, sp.position_size, sp.portfolio_value,
               sp.pnl_pct, sp.peak_return, sp.status
        FROM strategy_portfolio sp
        WHERE sp.strategy = ? AND sp.entry_reasoning IS NULL
        ORDER BY sp.entry_date
    """, (portfolio_name,)).fetchall()

    logger.info("Backfilling reasoning for %d trades", len(rows))
    updated = 0

    for r in rows:
        r = dict(r)
        trade_id = r["trade_id"]

        # Look up the original trade for context
        trade = None
        if trade_id:
            trade = conn.execute("""
                SELECT t.*, COALESCE(i.display_name, i.name) AS full_name, i.is_entity
                FROM trades t
                JOIN insiders i ON t.insider_id = i.insider_id
                WHERE t.trade_id = ?
            """, (trade_id,)).fetchone()

        if trade:
            trade = dict(trade)
            title = trade.get("title") or ""
            csuite = _is_csuite(title)

            # Holdings % change
            shares_after = trade.get("shares_owned_after") or 0
            qty = trade.get("qty") or 0
            shares_before = shares_after - qty if shares_after > 0 else 0
            holdings_pct = (
                (shares_after - shares_before) / shares_before
                if shares_before > 0 else None
            )

            _, breakdown = compute_signal_quality(
                pit_wr=r["insider_pit_wr"],
                pit_n=r["insider_pit_n"],
                is_csuite=csuite,
                holdings_pct_change=holdings_pct,
                is_10pct_owner=False,
            )

            entry_reasoning = json.dumps({
                "signal": {
                    "quality": r["signal_quality"],
                    "grade": trade.get("signal_grade"),
                    "breakdown": breakdown,
                },
                "insider": {
                    "name": r["insider_name"],
                    "title": title,
                    "is_csuite": csuite,
                    "pit_win_rate_7d": r["insider_pit_wr"],
                    "pit_n_trades": r["insider_pit_n"],
                    "pit_avg_abnormal_7d": trade.get("pit_avg_abnormal_7d"),
                    "pit_win_rate_30d": trade.get("pit_win_rate_30d"),
                    "switch_rate": trade.get("insider_switch_rate"),
                    "is_rare_reversal": bool(trade.get("is_rare_reversal")),
                },
                "filing": {
                    "filing_date": trade.get("filing_date"),
                    "trade_date": trade.get("trade_date"),
                    "trade_value": trade.get("value"),
                    "shares_bought": qty,
                    "shares_after": shares_after,
                    "holdings_pct_change": holdings_pct,
                    "is_10b5_1": bool(trade.get("is_10b5_1")),
                },
                "company": {
                    "ticker": r["ticker"],
                    "name": trade.get("company"),
                },
                "market_context": {
                    "entry_price": r["entry_price"],
                    "week52_proximity": trade.get("week52_proximity"),
                },
                "position": {
                    "size_pct": r["position_size"],
                    "dollar_amount": round((r["position_size"] or 0) * (r["portfolio_value"] or 0), 2),
                    "shares": None,  # not tracked in old schema
                    "portfolio_equity": r["portfolio_value"],
                    "target_hold_days": params.get("target_hold_days", 30),
                    "hard_stop_pct": params.get("hard_stop_pct", -0.15),
                    "trailing_stop_drop": params.get("trailing_stop_drop", 0.10),
                },
                "cluster": {
                    "is_cluster": False,
                    "cluster_size": 1,
                    "other_insiders": [],
                },
            }, default=str)

            # Also backfill context columns
            conn.execute("""
                UPDATE strategy_portfolio SET
                    entry_reasoning = ?,
                    company = ?,
                    insider_title = ?,
                    filing_date = ?,
                    trade_date = ?,
                    trade_value = ?,
                    signal_grade = ?,
                    is_csuite = ?,
                    holdings_pct_change = ?,
                    is_rare_reversal = ?,
                    is_cluster = 0,
                    cluster_size = 1
                WHERE id = ?
            """, (
                entry_reasoning,
                trade.get("company"),
                title,
                trade.get("filing_date"),
                trade.get("trade_date"),
                trade.get("value"),
                trade.get("signal_grade"),
                1 if csuite else 0,
                holdings_pct,
                1 if trade.get("is_rare_reversal") else 0,
                r["id"],
            ))
        else:
            # No trade found — minimal reasoning
            _, breakdown = compute_signal_quality(
                pit_wr=r["insider_pit_wr"],
                pit_n=r["insider_pit_n"],
                is_csuite=False,
                holdings_pct_change=None,
                is_10pct_owner=False,
            )
            entry_reasoning = json.dumps({
                "signal": {
                    "quality": r["signal_quality"],
                    "grade": None,
                    "breakdown": breakdown,
                },
                "insider": {
                    "name": r["insider_name"],
                    "title": None,
                    "is_csuite": False,
                    "pit_win_rate_7d": r["insider_pit_wr"],
                    "pit_n_trades": r["insider_pit_n"],
                },
                "filing": {},
                "company": {"ticker": r["ticker"], "name": None},
                "market_context": {"entry_price": r["entry_price"]},
                "position": {
                    "size_pct": r["position_size"],
                    "portfolio_equity": r["portfolio_value"],
                },
                "cluster": {"is_cluster": False, "cluster_size": 1},
            }, default=str)

            conn.execute(
                "UPDATE strategy_portfolio SET entry_reasoning = ? WHERE id = ?",
                (entry_reasoning, r["id"]),
            )

        # Build exit reasoning for closed trades
        if r["status"] == "closed" and r["exit_date"] and r["exit_price"]:
            spy_entry = conn.execute(
                "SELECT close FROM daily_prices WHERE ticker='SPY' AND date=?",
                (r["entry_date"],),
            ).fetchone()
            spy_exit = conn.execute(
                "SELECT close FROM daily_prices WHERE ticker='SPY' AND date=?",
                (r["exit_date"],),
            ).fetchone()

            exit_reasoning = build_exit_reasoning(
                exit_reason=r["exit_reason"] or "unknown",
                exit_price=r["exit_price"],
                entry_price=r["entry_price"],
                peak_return=r["peak_return"] or (r["pnl_pct"] if r["pnl_pct"] and r["pnl_pct"] > 0 else 0),
                peak_date=None,
                hold_days=r["hold_days"] or 0,
                spy_entry=spy_entry["close"] if spy_entry else None,
                spy_exit=spy_exit["close"] if spy_exit else None,
            )
            conn.execute(
                "UPDATE strategy_portfolio SET exit_reasoning = ? WHERE id = ?",
                (exit_reasoning, r["id"]),
            )

        updated += 1
        if updated % 200 == 0:
            conn.commit()
            logger.info("  Backfilled %d/%d", updated, len(rows))

    conn.commit()
    logger.info("Backfill complete: %d trades updated", updated)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Portfolio Simulator")
    parser.add_argument("--portfolio", default="form4_insider", help="Portfolio name")
    parser.add_argument("--start", default="2016-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-03-26", help="End date (YYYY-MM-DD)")
    parser.add_argument("--fresh", action="store_true",
                        help="Delete existing backtest trades before running")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log trades without writing to DB")
    parser.add_argument("--backfill-reasoning", action="store_true",
                        help="Backfill reasoning JSON for existing trades only")
    parser.add_argument("--equity", type=float, default=None,
                        help="Override starting equity (default: from portfolio config)")
    args = parser.parse_args()

    conn = get_db()

    if args.backfill_reasoning:
        backfill_reasoning(conn, args.portfolio)
        conn.close()
        return

    if args.fresh:
        count = conn.execute("""
            SELECT COUNT(*) FROM strategy_portfolio
            WHERE strategy = ? AND execution_source = 'backtest'
        """, (args.portfolio,)).fetchone()[0]
        logger.warning("Deleting %d existing backtest trades for %s", count, args.portfolio)
        conn.execute("""
            DELETE FROM strategy_portfolio
            WHERE strategy = ? AND execution_source = 'backtest'
        """, (args.portfolio,))
        conn.commit()

    sim = PortfolioSimulator(conn, args.portfolio, dry_run=args.dry_run)
    if args.equity is not None:
        sim.equity = args.equity

    result = sim.run(args.start, args.end)
    logger.info("Result: %s", json.dumps(result, indent=2))

    conn.close()


if __name__ == "__main__":
    main()
