#!/usr/bin/env python3
"""Form4 Insider Portfolio — live runner with Alpaca paper trading.

Replicates the backfilled portfolio strategy in real-time:
  - 30 calendar day hold
  - Dynamic 5/7/10% sizing by conviction
  - Max 20 concurrent positions
  - -15% hard stop, 10% trailing stop
  - No 10% owners, min $500K daily dollar volume
  - PIT scoring (no look-ahead bias)

Runs as a launchd daemon. Three phases per day:
  1. Pre-market (9:25 ET): Scan for new qualifying filings
  2. Market open (9:31 ET): Submit entry orders
  3. Intraday (every 5 min): Monitor positions, check exits

Usage:
    python3 pipelines/portfolio_runner.py              # run daemon
    python3 pipelines/portfolio_runner.py --backfill   # catch up missed days
    python3 pipelines/portfolio_runner.py --dry-run    # scan + log, no orders
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.database import get_connection
from framework.execution.paper import PaperBackend

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parent.parent / "strategies" / "insider_catalog" / "insiders.db"
STATE_PATH = Path(__file__).resolve().parent / "data" / "portfolio_runner_state.json"

# Import reasoning builders from simulator (shared logic)
try:
    from pipelines.portfolio_simulator import build_entry_reasoning, build_exit_reasoning, detect_clusters
except ImportError:
    from portfolio_simulator import build_entry_reasoning, build_exit_reasoning, detect_clusters

STRATEGY = "form4_insider"

# === V3 Strategy Config ===
MAX_CONCURRENT = 20
TARGET_HOLD_DAYS = 14
HARD_STOP_PCT = -0.10
TRAILING_STOP_DROP = 0.05
TARGET_GAIN_PCT = 0.08  # exit at +8%

# Variable sizing: 8% at quality 6, 15% at quality 10
POSITION_SIZE_MIN = 0.08
POSITION_SIZE_MAX = 0.15
MIN_SIGNAL_QUALITY = 6.0

# PIT filter
REQUIRE_PIT_HISTORY = True
MIN_PIT_N = 2
MIN_PIT_WR = 0.40

# Role exclusions
EXCLUDE_ROLES = ["chairman"]


def get_position_size(signal_quality: float) -> float:
    """Variable sizing: linear interpolation from 8% (quality 6) to 15% (quality 10)."""
    q_range = 10.0 - MIN_SIGNAL_QUALITY
    if q_range <= 0:
        return POSITION_SIZE_MIN
    t = min(1.0, max(0.0, (signal_quality - MIN_SIGNAL_QUALITY) / q_range))
    return POSITION_SIZE_MIN + t * (POSITION_SIZE_MAX - POSITION_SIZE_MIN)

MIN_DAILY_DOLLAR_VOL = 500_000
MAX_ADV_PCT = 0.05  # max 5% of 20-day average daily dollar volume

# V3: simulation and display both use $100K. No scaling needed.
DISPLAY_STARTING = 100_000
SCALE = 1.0  # V3: no scaling needed


# ---------------------------------------------------------------------------
# Alpaca setup
# ---------------------------------------------------------------------------

def get_alpaca() -> PaperBackend:
    # Use portfolio-specific keys (separate paper account from cluster buy strategy)
    api_key = os.getenv("PORTFOLIO_ALPACA_API_KEY", "") or os.getenv("ALPACA_API_KEY", "")
    api_secret = os.getenv("PORTFOLIO_ALPACA_API_SECRET", "") or os.getenv("ALPACA_API_SECRET", "")
    if not api_key or not api_secret:
        raise RuntimeError("PORTFOLIO_ALPACA_API_KEY/SECRET (or ALPACA_API_KEY/SECRET) required")
    return PaperBackend(api_key, api_secret)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

PRICES_DB = DB_PATH.parent / "prices.db"

def get_db(readonly: bool = False):
    return get_connection(readonly=readonly)


# Pending orders file — tracks orders submitted but not yet filled
PENDING_ORDERS_FILE = Path(__file__).resolve().parent / "data" / "pending_orders.json"


def _save_pending_order(conn, candidate, order_id, qty, size_pct, equity):
    """Save a pending order for later reconciliation."""
    import json as _json
    pending = []
    if PENDING_ORDERS_FILE.exists():
        try:
            pending = _json.loads(PENDING_ORDERS_FILE.read_text())
        except Exception:
            pass
    pending.append({
        "order_id": order_id,
        "ticker": candidate["ticker"],
        "trade_id": candidate["trade_id"],
        "insider_name": candidate["insider_name"],
        "pit_n": candidate.get("pit_n"),
        "pit_wr": candidate.get("pit_wr"),
        "signal_quality": candidate["signal_quality"],
        "qty": qty,
        "size_pct": size_pct,
        "equity": equity,
        "submitted_at": datetime.utcnow().isoformat(),
    })
    PENDING_ORDERS_FILE.write_text(_json.dumps(pending, indent=2))


def reconcile_pending_orders(conn, alpaca):
    """Check if any pending orders have been filled on Alpaca and record them."""
    import json as _json
    if not PENDING_ORDERS_FILE.exists():
        return
    try:
        pending = _json.loads(PENDING_ORDERS_FILE.read_text())
    except Exception:
        return
    if not pending:
        return

    remaining = []
    for p in pending:
        try:
            import requests as _req
            _headers = {
                "APCA-API-KEY-ID": os.getenv("PORTFOLIO_ALPACA_API_KEY", "") or os.getenv("ALPACA_API_KEY", ""),
                "APCA-API-SECRET-KEY": os.getenv("PORTFOLIO_ALPACA_API_SECRET", "") or os.getenv("ALPACA_API_SECRET", ""),
            }
            resp = _req.get(f"https://paper-api.alpaca.markets/v2/orders/{p['order_id']}", headers=_headers, timeout=10)
            order = resp.json() if resp.status_code == 200 else None
            if order and order.get("status") == "filled":
                entry_price = float(order.get("filled_avg_price", 0))
                if entry_price <= 0:
                    remaining.append(p)
                    continue
                today = p.get("submitted_at", "")[:10]
                portfolio_row = conn.execute(
                    "SELECT id FROM portfolios WHERE name = ?", (STRATEGY,)
                ).fetchone()
                _pid = portfolio_row["id"] if portfolio_row else None
                conn.execute("""
                    INSERT INTO strategy_portfolio (
                        strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                        entry_date, entry_price, target_hold, stop_pct,
                        position_size, portfolio_value,
                        insider_name, insider_pit_n, insider_pit_wr,
                        signal_quality, status,
                        execution_source, is_estimated, actual_fill_price
                    ) VALUES (?, ?, ?, ?, 'buy_stock', 'long',
                              ?, ?, ?, ?,
                              ?, ?,
                              ?, ?, ?,
                              ?, 'open',
                              'paper', 0, ?)
                """, (
                    STRATEGY, _pid, p["trade_id"], p["ticker"],
                    today, entry_price, TARGET_HOLD_DAYS, HARD_STOP_PCT,
                    p["size_pct"], p["equity"],
                    p["insider_name"], p.get("pit_n"), p.get("pit_wr"),
                    p["signal_quality"],
                    entry_price,
                ))
                conn.commit()
                logger.info("Reconciled pending order: %s filled @ $%.2f", p["ticker"], entry_price)
            elif order and order.get("status") in ("canceled", "expired", "rejected"):
                logger.info("Pending order %s for %s was %s — removing", p["order_id"], p["ticker"], order["status"])
            else:
                remaining.append(p)  # still pending
        except Exception as exc:
            logger.warning("Error reconciling order %s: %s", p.get("order_id"), exc)
            remaining.append(p)

    PENDING_ORDERS_FILE.write_text(_json.dumps(remaining, indent=2))


def reconcile_alpaca_positions(conn, alpaca):
    """Catch orphaned Alpaca positions not tracked in strategy_portfolio."""
    try:
        import requests as _req
        headers = {
            "APCA-API-KEY-ID": os.getenv("PORTFOLIO_ALPACA_API_KEY", "") or os.getenv("ALPACA_API_KEY", ""),
            "APCA-API-SECRET-KEY": os.getenv("PORTFOLIO_ALPACA_API_SECRET", "") or os.getenv("ALPACA_API_SECRET", ""),
        }
        resp = _req.get("https://paper-api.alpaca.markets/v2/positions", headers=headers, timeout=10)
        if resp.status_code != 200:
            return

        positions = resp.json()
        db_open = {r["ticker"] for r in get_open_positions(conn)}

        for pos in positions:
            symbol = pos["symbol"]
            if symbol in db_open:
                continue
            # Orphaned position — log it
            entry_price = float(pos.get("avg_entry_price", 0))
            qty = int(pos.get("qty", 0))
            logger.warning(
                "ORPHANED Alpaca position: %s qty=%d entry=$%.2f — not in strategy_portfolio",
                symbol, qty, entry_price
            )
    except Exception as exc:
        logger.debug("Alpaca position reconciliation error: %s", exc)


def get_open_positions(conn: object) -> list[dict]:
    """Get all open positions from strategy_portfolio."""
    rows = conn.execute("""
        SELECT * FROM strategy_portfolio
        WHERE strategy = ? AND status = 'open'
        ORDER BY entry_date
    """, (STRATEGY,)).fetchall()
    return [dict(r) for r in rows]


def count_open_positions(conn: object) -> int:
    return conn.execute("""
        SELECT COUNT(*) FROM strategy_portfolio
        WHERE strategy = ? AND status = 'open'
    """, (STRATEGY,)).fetchone()[0]


def get_theoretical_equity(conn: object) -> float:
    """Compute current portfolio equity from all closed trades.

    Rebuilds equity from starting capital + cumulative scaled P&L,
    matching the API display logic exactly.
    """
    rows = conn.execute("""
        SELECT pnl_dollar
        FROM strategy_portfolio
        WHERE strategy = ? AND status = 'closed' AND exit_date IS NOT NULL
        ORDER BY exit_date
    """, (STRATEGY,)).fetchall()

    equity = DISPLAY_STARTING
    for r in rows:
        equity += (r["pnl_dollar"] or 0) * SCALE
    return equity


# ---------------------------------------------------------------------------
# Signal quality scoring (same as backfill)
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
    """V4 signal quality: role-weighted, rare reversal, switch rate, validated holdings."""
    breakdown = {"baseline": 5.0}
    score = 5.0

    pit_bonus = 0.0
    if pit_n and pit_n >= 3 and pit_wr is not None:
        if pit_wr >= 0.7: pit_bonus = 2.0
        elif pit_wr >= 0.6: pit_bonus = 1.0
        elif pit_wr < 0.4: pit_bonus = -1.0
    breakdown["pit_win_rate_bonus"] = pit_bonus
    score += pit_bonus

    role_bonus = 0.0
    t = (title or "").lower()
    if "cfo" in t or "chief financial" in t: role_bonus = 1.5
    elif "vp" in t or "vice pres" in t: role_bonus = 1.0
    elif is_csuite: role_bonus = 0.5
    breakdown["csuite_bonus"] = role_bonus
    score += role_bonus

    reversal_bonus = 1.5 if is_rare_reversal else 0.0
    breakdown["rare_reversal_bonus"] = reversal_bonus
    score += reversal_bonus

    switch_bonus = 0.0
    if switch_rate is not None:
        if 0.10 <= switch_rate <= 0.30: switch_bonus = 0.5
    breakdown["switch_rate_bonus"] = switch_bonus
    score += switch_bonus

    holdings_bonus = 0.0
    if holdings_pct_change is not None:
        if holdings_pct_change >= 1.0: holdings_bonus = 1.5
        elif holdings_pct_change >= 0.50: holdings_bonus = 1.0
        elif holdings_pct_change >= 0.10: holdings_bonus = 0.5
        elif holdings_pct_change < 0.01: holdings_bonus = -0.5
    breakdown["holdings_bonus"] = holdings_bonus
    score += holdings_bonus

    owner_penalty = -2.0 if is_10pct_owner else 0.0
    breakdown["owner_10pct_penalty"] = owner_penalty
    score += owner_penalty

    return max(0.0, min(10.0, score)), breakdown


def get_conviction(quality: float) -> str:
    """Legacy label — kept for logging."""
    if quality >= 8:
        return "high"
    elif quality >= 7:
        return "medium"
    else:
        return "standard"


# ---------------------------------------------------------------------------
# Filing scanner — find new qualifying trades
# ---------------------------------------------------------------------------

CSUITE_KEYWORDS = [
    "ceo", "chief exec", "president", "pres", "chairman", "chair",
    "cfo", "chief financial", "coo", "chief operating",
    "evp", "executive vp", "svp", "senior vp",
]


def is_csuite(title: str | None) -> bool:
    if not title:
        return False
    t = title.lower()
    return any(kw in t for kw in CSUITE_KEYWORDS)


def is_10pct_owner(title: str | None) -> bool:
    if not title:
        return False
    t = title.lower()
    return "10%" in t or "10 percent" in t or "ten percent" in t


def scan_new_filings(conn: object, since_date: str) -> list[dict]:
    """Find qualifying buy filings since given date.

    Replicates backfill entry criteria:
    - Buys only (trans_code = 'P')
    - Not 10% owners
    - Not duplicates
    - Has price data available
    - Grouped by filing (filing_key)
    """
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
            -- PIT scoring
            t.pit_n_trades,
            t.pit_win_rate_7d,
            t.pit_avg_abnormal_7d,
            t.pit_win_rate_30d,
            t.signal_grade,
            t.is_rare_reversal,
            t.insider_switch_rate,
            t.week52_proximity,
            t.is_10b5_1,
            -- Holdings change (shares_before = shares_after - qty for buys)
            MAX(CASE WHEN t.direct_indirect = 'D' OR t.direct_indirect IS NULL
                THEN t.shares_owned_after ELSE NULL END) AS shares_after,
            SUM(t.qty) AS total_qty_filed,
            MIN(t.txn_group_id) AS txn_group_id
        FROM trades t
        JOIN insiders i ON t.insider_id = i.insider_id
        WHERE t.filing_date > ?
          AND t.trans_code = 'P'
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
          AND t.title NOT LIKE '%10%%owner%'
          AND t.title NOT LIKE '%10 percent%'
        GROUP BY t.filing_key
        ORDER BY t.filing_date
    """, (since_date,)).fetchall()

    candidates = []
    for r in rows:
        r = dict(r)
        title = r["title"] or ""

        # Skip 10% owners
        if is_10pct_owner(title):
            continue

        # Compute holdings % change (shares_before = shares_after - qty)
        shares_after = r["shares_after"] or 0
        total_qty = r["total_qty_filed"] or 0
        shares_before = shares_after - total_qty if shares_after > 0 else 0
        holdings_pct = (
            (shares_after - shares_before) / shares_before
            if shares_before > 0 else None
        )

        # V3 filters: PIT history required
        pit_n = r["pit_n_trades"] or 0
        pit_wr = r["pit_win_rate_7d"]
        if REQUIRE_PIT_HISTORY:
            if pit_n < MIN_PIT_N:
                continue
            if pit_wr is None or pit_wr < MIN_PIT_WR:
                continue

        # V3: exclude roles
        if EXCLUDE_ROLES:
            if any(role in title.lower() for role in EXCLUDE_ROLES):
                has_good_role = any(kw in title.lower() for kw in ["ceo", "cfo", "president", "vp", "dir"])
                if not has_good_role:
                    continue

        # Signal quality (V3: role-weighted)
        csuite_flag = is_csuite(title)
        quality, breakdown = compute_signal_quality(
            pit_wr=r["pit_win_rate_7d"],
            pit_n=r["pit_n_trades"],
            is_csuite=csuite_flag,
            holdings_pct_change=holdings_pct,
            is_10pct_owner=False,
            title=title,
            is_rare_reversal=bool(r.get("is_rare_reversal")),
            switch_rate=r.get("insider_switch_rate"),
        )

        if quality < MIN_SIGNAL_QUALITY:
            continue

        conviction = get_conviction(quality)

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
            "is_csuite": csuite_flag,
            "is_rare_reversal": bool(r["is_rare_reversal"]),
            "insider_switch_rate": r["insider_switch_rate"],
            "week52_proximity": r["week52_proximity"],
            "is_10b5_1": bool(r["is_10b5_1"]),
            "conviction": conviction,
            "position_size": get_position_size(quality),
            "txn_group_id": r.get("txn_group_id"),
        })

    return candidates


# ---------------------------------------------------------------------------
# Position monitoring — check stops and exits
# ---------------------------------------------------------------------------

def check_exits(
    conn: object,
    alpaca: PaperBackend,
    dry_run: bool = False,
) -> list[dict]:
    """Check all open positions for exit conditions. Returns list of closed trades."""
    open_positions = get_open_positions(conn)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    closed = []

    for pos in open_positions:
        ticker = pos["ticker"]
        entry_price = pos["entry_price"]
        entry_date = pos["entry_date"]
        target_hold = pos["target_hold"]
        stop_pct = pos["stop_pct"]

        # Get current price from Alpaca
        alpaca_pos = alpaca.get_position(ticker)
        if alpaca_pos is None:
            # Position might have been closed externally or doesn't exist
            logger.warning("No Alpaca position for %s — marking as closed", ticker)
            current_price = entry_price  # fallback
            exit_reason = "missing_position"
        else:
            current_price = alpaca_pos["current_price"]
            exit_reason = None

        pnl_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0

        # Calculate hold days
        try:
            d_entry = datetime.strptime(entry_date[:10], "%Y-%m-%d")
            d_today = datetime.strptime(today[:10], "%Y-%m-%d")
            hold_days = (d_today - d_entry).days
        except Exception:
            hold_days = 0

        # Check exit conditions
        should_exit = False

        # Track peak return
        peak_return = _get_peak_return(pos["id"], pnl_pct)

        # 1. Hard stop
        if pnl_pct <= stop_pct:
            exit_reason = "stop_loss"
            should_exit = True

        # 2. Target gain (V3: +8%)
        elif TARGET_GAIN_PCT and pnl_pct >= TARGET_GAIN_PCT:
            exit_reason = "target_gain"
            should_exit = True

        # 3. Trailing stop
        elif peak_return > 0.03 and (peak_return - pnl_pct) >= TRAILING_STOP_DROP:
            exit_reason = "trailing_stop"
            should_exit = True

        # 4. Time exit
        elif hold_days >= target_hold:
            exit_reason = "time_exit"
            should_exit = True

        if not should_exit:
            # Update peak return tracking
            _update_peak_return(pos["id"], pnl_pct)
            continue

        # Execute exit
        logger.info(
            "EXIT %s: reason=%s pnl=%.2f%% hold=%dd price=$%.2f",
            ticker, exit_reason, pnl_pct * 100, hold_days, current_price,
        )

        if not dry_run and alpaca_pos is not None:
            try:
                result = alpaca.close_position(ticker)
                if result.is_filled:
                    current_price = result.filled_price or current_price
                    logger.info("Sold %s at $%.2f", ticker, current_price)
                elif result.status == "pending":
                    fill = alpaca.wait_for_fill(result.order_id, timeout=60)
                    if fill.is_filled:
                        current_price = fill.filled_price or current_price
                    else:
                        logger.warning("Exit order for %s not filled, forcing", ticker)
                else:
                    logger.warning("Exit order for %s failed: %s", ticker, result.error)
            except Exception as exc:
                logger.error("Error closing %s: %s", ticker, exc)

        # Update DB
        pnl_pct_final = (current_price - entry_price) / entry_price
        pnl_dollar = pos["position_size"] * pos["portfolio_value"] * pnl_pct_final

        # Build exit reasoning
        exit_reasoning_json = build_exit_reasoning(
            exit_reason=exit_reason,
            exit_price=current_price,
            entry_price=entry_price,
            peak_return=peak_return,
            peak_date=None,
            hold_days=hold_days,
            spy_entry=None,
            spy_exit=None,
        )

        conn.execute("""
            UPDATE strategy_portfolio SET
                exit_date = ?,
                exit_price = ?,
                hold_days = ?,
                pnl_pct = ?,
                pnl_dollar = ?,
                stop_hit = ?,
                exit_reason = ?,
                status = 'closed',
                exit_reasoning = ?,
                peak_return = ?,
                actual_fill_price = ?
            WHERE id = ?
        """, (
            today,
            round(current_price, 4),
            hold_days,
            round(pnl_pct_final, 6),
            round(pnl_dollar, 2),
            1 if exit_reason == "stop_loss" else 0,
            exit_reason,
            exit_reasoning_json,
            round(peak_return, 6),
            round(current_price, 4),
            pos["id"],
        ))
        conn.commit()

        _clear_peak_return(pos["id"])

        closed.append({
            "ticker": ticker,
            "exit_reason": exit_reason,
            "pnl_pct": pnl_pct_final,
            "pnl_dollar": pnl_dollar,
            "hold_days": hold_days,
        })

    return closed


# ---------------------------------------------------------------------------
# Peak return tracking (for trailing stop)
# ---------------------------------------------------------------------------

_peak_returns: dict[int, float] = {}


def _load_state() -> None:
    global _peak_returns
    if STATE_PATH.exists():
        try:
            data = json.loads(STATE_PATH.read_text())
            _peak_returns = {int(k): v for k, v in data.get("peak_returns", {}).items()}
        except Exception:
            _peak_returns = {}


def _save_state() -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if STATE_PATH.exists():
        try:
            existing = json.loads(STATE_PATH.read_text())
        except Exception:
            pass
    existing.update({
        "peak_returns": {str(k): v for k, v in _peak_returns.items()},
        "updated_at": datetime.utcnow().isoformat(),
    })
    STATE_PATH.write_text(json.dumps(existing, indent=2))


HEARTBEAT_PATH = STATE_PATH.parent / "portfolio_runner_heartbeat.json"


def write_heartbeat(status: str = "ok", detail: str = "", **extra: any) -> None:
    """Write heartbeat file for monitoring. Called every loop iteration."""
    beat = {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "pid": os.getpid(),
        "detail": detail,
    }
    beat.update(extra)
    try:
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEARTBEAT_PATH.write_text(json.dumps(beat, indent=2))
    except Exception:
        pass


def get_scan_watermark() -> str | None:
    """Get the filing date watermark — only process filings after this date."""
    if STATE_PATH.exists():
        try:
            data = json.loads(STATE_PATH.read_text())
            return data.get("scan_watermark")
        except Exception:
            pass
    return None


def set_scan_watermark(date: str) -> None:
    """Set the filing date watermark."""
    existing = {}
    if STATE_PATH.exists():
        try:
            existing = json.loads(STATE_PATH.read_text())
        except Exception:
            pass
    existing["scan_watermark"] = date
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(existing, indent=2))


def _get_peak_return(pos_id: int, current_return: float) -> float:
    peak = _peak_returns.get(pos_id, 0.0)
    return max(peak, current_return)


def _update_peak_return(pos_id: int, current_return: float) -> None:
    _peak_returns[pos_id] = max(_peak_returns.get(pos_id, 0.0), current_return)
    _save_state()


def _clear_peak_return(pos_id: int) -> None:
    _peak_returns.pop(pos_id, None)
    _save_state()


# ---------------------------------------------------------------------------
# Entry execution
# ---------------------------------------------------------------------------

def execute_entries(
    conn: object,
    alpaca: PaperBackend,
    candidates: list[dict],
    dry_run: bool = False,
) -> list[dict]:
    """Submit entry orders for qualifying candidates. Returns list of opened positions."""
    # Detect clusters for reasoning
    all_clusters = detect_clusters(candidates)

    n_open = count_open_positions(conn)
    slots = MAX_CONCURRENT - n_open

    if slots <= 0:
        logger.info("Max concurrent positions reached (%d), skipping entries", MAX_CONCURRENT)
        return []

    # Don't enter tickers we already hold
    held_tickers = {
        r["ticker"]
        for r in conn.execute(
            "SELECT ticker FROM strategy_portfolio WHERE strategy = ? AND status = 'open'",
            (STRATEGY,),
        ).fetchall()
    }

    # Use theoretical equity (from backfill + live trades) for position sizing
    # Alpaca account balance is just the execution layer — our sizing is based
    # on the portfolio's tracked equity, not Alpaca's paper balance.
    equity = get_theoretical_equity(conn)
    logger.info("Theoretical equity: $%.2f, open slots: %d", equity, slots)

    entered = []
    for c in candidates:
        if slots <= 0:
            break
        if c["ticker"] in held_tickers:
            logger.info("Already holding %s, skip", c["ticker"])
            continue

        # Skip bad tickers
        if not c["ticker"] or c["ticker"] in ("NONE", ""):
            logger.warning("Invalid ticker '%s', skip", c["ticker"])
            continue

        size_pct = get_position_size(c["signal_quality"])
        dollar_amount = equity * size_pct

        # ADV cap: don't take more than 5% of 20-day avg daily dollar volume
        try:
            adv_row = conn.execute("""
                SELECT AVG(close * 1.0) as avg_close FROM daily_prices
                WHERE ticker = ? ORDER BY date DESC LIMIT 20
            """, (c["ticker"],)).fetchone()
            if adv_row and adv_row["avg_close"]:
                # Rough ADV estimate (close * typical volume ratio)
                # We only have close price, not volume, so use the MIN_DAILY_DOLLAR_VOL filter
                # and cap at MAX_ADV_PCT of that
                adv_cap = MIN_DAILY_DOLLAR_VOL * MAX_ADV_PCT  # $25K at current settings
                if dollar_amount > adv_cap and dollar_amount > 25_000:
                    old_amount = dollar_amount
                    dollar_amount = min(dollar_amount, adv_cap)
                    size_pct = dollar_amount / equity
                    logger.info("ADV cap: %s reduced $%.0f → $%.0f (%.0f%% of portfolio)",
                                c["ticker"], old_amount, dollar_amount, size_pct * 100)
        except Exception:
            pass  # proceed with original sizing

        # Get current price for qty calculation
        pos_check = alpaca.get_position(c["ticker"])
        if pos_check is not None:
            logger.info("Alpaca already holds %s, skip", c["ticker"])
            continue

        logger.info(
            "ENTRY %s: quality=%.1f size=%.0f%%  insider=%s",
            c["ticker"], c["signal_quality"],
            size_pct * 100, c["insider_name"],
        )

        if dry_run:
            entered.append(c)
            slots -= 1
            continue

        # Get latest price from Alpaca data API to compute quantity
        try:
            import requests as _req
            headers = {
                "APCA-API-KEY-ID": os.getenv("PORTFOLIO_ALPACA_API_KEY", "") or os.getenv("ALPACA_API_KEY", ""),
                "APCA-API-SECRET-KEY": os.getenv("PORTFOLIO_ALPACA_API_SECRET", "") or os.getenv("ALPACA_API_SECRET", ""),
            }
            resp = _req.get(
                f"https://data.alpaca.markets/v2/stocks/{c['ticker']}/trades/latest",
                headers=headers,
                timeout=10,
            )
            if resp.status_code == 200:
                latest_price = resp.json().get("trade", {}).get("p", 0)
            else:
                logger.warning("Could not get price for %s: HTTP %d", c["ticker"], resp.status_code)
                continue
        except Exception as exc:
            logger.error("Error fetching price for %s: %s", c["ticker"], exc)
            continue

        if latest_price <= 0:
            logger.warning("Invalid price for %s: $%.2f", c["ticker"], latest_price)
            continue

        qty = math.floor(dollar_amount / latest_price)
        if qty <= 0:
            logger.warning("Qty 0 for %s ($%.2f / $%.2f), skip", c["ticker"], dollar_amount, latest_price)
            continue

        # Only submit during market hours (9:31-15:55 ET)
        if not is_market_hours():
            logger.info("Skipping %s entry — outside market hours", c["ticker"])
            continue

        # Submit the actual order (day order — expires at market close)
        try:
            result = alpaca.submit_order(
                symbol=c["ticker"],
                qty=qty,
                side="buy",
                order_type="market",
                time_in_force="day",
            )

            if result.status == "pending":
                result = alpaca.wait_for_fill(result.order_id, timeout=120)

            if result.is_filled:
                entry_price = result.filled_price or latest_price
                logger.info("Filled %s: %d shares @ $%.2f", c["ticker"], qty, entry_price)
            else:
                # Order still pending (pre-market) — save for reconciliation
                logger.info("Order %s for %s pending — will reconcile later", result.order_id, c["ticker"])
                _save_pending_order(conn, c, result.order_id, qty, size_pct, equity)
                slots -= 1
                entered.append(c)
                continue

        except Exception as exc:
            logger.error("Order failed for %s: %s", c["ticker"], exc)
            continue

        # Build entry reasoning JSON
        ticker_cluster = all_clusters.get(c["ticker"], [])
        cluster_size = len(ticker_cluster)
        cluster_insiders = [
            x["insider_name"] for x in ticker_cluster
            if x["insider_name"] != c["insider_name"]
        ]
        reasoning = build_entry_reasoning(
            candidate=c,
            cluster_size=cluster_size,
            cluster_insiders=cluster_insiders,
            entry_price=entry_price,
            position_size=size_pct,
            dollar_amount=qty * entry_price,
            shares=qty,
            portfolio_equity=equity,
            params={"target_hold_days": TARGET_HOLD_DAYS, "hard_stop_pct": HARD_STOP_PCT,
                    "trailing_stop_drop": TRAILING_STOP_DROP},
        )

        # Record in DB
        today = datetime.utcnow().strftime("%Y-%m-%d")

        # Get portfolio_id
        portfolio_row = conn.execute(
            "SELECT id FROM portfolios WHERE name = ?", (STRATEGY,)
        ).fetchone()
        portfolio_id = portfolio_row["id"] if portfolio_row else None

        conn.execute("""
            INSERT INTO strategy_portfolio (
                strategy, portfolio_id, trade_id, ticker, trade_type, direction,
                entry_date, entry_price, target_hold, stop_pct,
                position_size, portfolio_value,
                insider_name, insider_pit_n, insider_pit_wr,
                signal_quality, status,
                execution_source, is_estimated, actual_fill_price,
                entry_reasoning,
                company, insider_title, filing_date, trade_date, trade_value,
                signal_grade, is_csuite, holdings_pct_change,
                is_rare_reversal, is_cluster, cluster_size,
                shares, dollar_amount
            ) VALUES (?, ?, ?, ?, 'buy_stock', 'long',
                      ?, ?, ?, ?,
                      ?, ?,
                      ?, ?, ?,
                      ?, 'open',
                      'paper', 0, ?,
                      ?,
                      ?, ?, ?, ?, ?,
                      ?, ?, ?,
                      ?, ?, ?,
                      ?, ?)
        """, (
            STRATEGY, portfolio_id,
            c["trade_id"], c["ticker"],
            today, round(entry_price, 4), TARGET_HOLD_DAYS, HARD_STOP_PCT,
            size_pct, equity,
            c["insider_name"], c["pit_n"], c["pit_wr"],
            c["signal_quality"],
            round(entry_price, 4),
            reasoning,
            c.get("company"), c.get("title"), c.get("filing_date"), c.get("trade_date"),
            c.get("total_value"),
            c.get("signal_grade"), 1 if c.get("is_csuite") else 0,
            c.get("holdings_pct_change"),
            1 if c.get("is_rare_reversal") else 0,
            1 if cluster_size > 1 else 0, cluster_size,
            qty, round(qty * entry_price, 2),
        ))
        conn.commit()

        entered.append({**c, "entry_price": entry_price, "qty": qty})
        held_tickers.add(c["ticker"])
        slots -= 1

    return entered


# ---------------------------------------------------------------------------
# Market hours check
# ---------------------------------------------------------------------------

def _now_et() -> datetime:
    """Current time in ET (UTC-5 or UTC-4 during DST)."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def is_market_day() -> bool:
    """Check if today is a weekday (basic check — doesn't handle holidays)."""
    now = _now_et()
    return now.weekday() < 5


def wait_until_market_open() -> None:
    """Sleep until 9:31 ET."""
    while True:
        now = _now_et()
        target = now.replace(hour=9, minute=31, second=0, microsecond=0)
        if now >= target:
            break
        delta = (target - now).total_seconds()
        if delta > 60:
            logger.info("Waiting %.0f min until market open", delta / 60)
            time.sleep(min(delta, 300))
        else:
            time.sleep(max(1, delta))


def is_market_hours() -> bool:
    now = _now_et()
    return 9 <= now.hour < 16 or (now.hour == 9 and now.minute >= 30)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_daily(dry_run: bool = False) -> dict:
    """Execute one daily cycle: scan → enter → monitor exits."""
    _load_state()

    alpaca = None if dry_run else get_alpaca()
    conn = get_db(readonly=dry_run)

    try:
        # 0. Reconcile any pending orders from previous cycles
        if alpaca:
            reconcile_pending_orders(conn, alpaca)
            reconcile_alpaca_positions(conn, alpaca)

        # 1. Determine scan watermark
        since = get_scan_watermark()
        if not since:
            # First run — only look at today's filings
            since = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info("No watermark set, starting from yesterday: %s", since)

        logger.info("Scanning filings since %s", since)

        # 2. Find new qualifying filings
        candidates = scan_new_filings(conn, since)
        logger.info("Found %d qualifying filings", len(candidates))
        for c in candidates:
            logger.info(
                "  %s: %s (quality=%.1f, conviction=%s, value=$%s)",
                c["ticker"], c["insider_name"],
                c["signal_quality"], c["conviction"],
                f"{c['total_value']:,.0f}" if c["total_value"] else "?",
            )

        # 3. Check exits first (free up slots)
        closed = check_exits(conn, alpaca, dry_run=dry_run) if alpaca else []
        if closed:
            logger.info("Closed %d positions", len(closed))
            for c in closed:
                logger.info(
                    "  %s: %s (%.1f%%, $%.0f, %dd)",
                    c["ticker"], c["exit_reason"],
                    c["pnl_pct"] * 100, c["pnl_dollar"], c["hold_days"],
                )

        # 4. Execute entries
        entered = execute_entries(conn, alpaca, candidates, dry_run=dry_run) if not dry_run else candidates
        if entered:
            logger.info("Opened %d new positions", len(entered))

        n_open = count_open_positions(conn)
        logger.info("Portfolio: %d open positions", n_open)

        # Advance watermark to yesterday — keeps today's filings scannable
        # for intraday re-scans (filings arrive throughout the day).
        # The scan query uses `filing_date > watermark`, so yesterday means
        # we'll always re-scan today's filings on each intraday pass.
        # Duplicate entries are prevented by the held_tickers check in execute_entries.
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        set_scan_watermark(yesterday)

        return {
            "scanned": len(candidates),
            "entered": len(entered),
            "closed": len(closed),
            "open": n_open,
        }

    finally:
        conn.close()


def run_daemon() -> None:
    """Main daemon loop — runs daily cycle then monitors positions intraday."""
    logger.info("Form4 Portfolio Runner starting")
    _load_state()

    while True:
        now = _now_et()

        if not is_market_day():
            write_heartbeat("sleeping", "Weekend")
            logger.info("Weekend — sleeping until Monday")
            time.sleep(3600)
            continue

        # Pre-market: scan and queue
        if now.hour < 9 or (now.hour == 9 and now.minute < 25):
            sleep_sec = ((9 - now.hour) * 3600 + (25 - now.minute) * 60)
            write_heartbeat("sleeping", f"Pre-market, {sleep_sec/60:.0f} min to open")
            logger.info("Pre-market — sleeping %.0f min", sleep_sec / 60)
            time.sleep(max(60, min(sleep_sec, 1800)))
            continue

        # 9:25-9:30: scan filings
        if now.hour == 9 and 25 <= now.minute < 31:
            logger.info("Scanning for new filings")
            # Just scan, don't enter yet
            conn = get_db(readonly=True)
            try:
                last_entry = conn.execute(
                    "SELECT MAX(entry_date) FROM strategy_portfolio WHERE strategy = ?",
                    (STRATEGY,),
                ).fetchone()[0]
                since = last_entry or (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
                candidates = scan_new_filings(conn, since)
                logger.info("Found %d candidates for today", len(candidates))
            finally:
                conn.close()
            wait_until_market_open()
            continue

        # 9:31+: execute daily cycle (entries + exit checks)
        if now.hour == 9 and 31 <= now.minute <= 35:
            try:
                result = run_daily()
                logger.info("Daily cycle: %s", result)
            except Exception as exc:
                logger.error("Daily cycle failed: %s", exc)

        # Intraday: re-scan for new filings every 30 min + check exits every 5 min
        if is_market_hours():
            # Re-scan for new filings every 30 min (filings arrive throughout the day)
            if now.minute in (0, 30) or (now.hour == 9 and 31 <= now.minute <= 35):
                try:
                    logger.info("Intraday re-scan for new filings")
                    result = run_daily()
                    if result["entered"] > 0:
                        logger.info("Intraday entries: %s", result)
                except Exception as exc:
                    logger.error("Intraday scan failed: %s", exc)

            # Check exits every cycle
            try:
                alpaca = get_alpaca()
                conn = get_db(readonly=False)
                try:
                    closed = check_exits(conn, alpaca)
                    if closed:
                        logger.info("Intraday exits: %d positions closed", len(closed))
                finally:
                    conn.close()
            except Exception as exc:
                logger.error("Intraday check failed: %s", exc)

            write_heartbeat("active", "Market hours",
                           open_positions=count_open_positions(get_db(readonly=True)))
            time.sleep(300)  # 5 min
            continue

        # After hours
        if now.hour >= 16:
            # Log daily summary
            conn = get_db(readonly=True)
            try:
                n_open = count_open_positions(conn)
                today = datetime.utcnow().strftime("%Y-%m-%d")
                today_entries = conn.execute(
                    "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND entry_date = ?",
                    (STRATEGY, today),
                ).fetchone()[0]
                today_exits = conn.execute(
                    "SELECT COUNT(*) FROM strategy_portfolio WHERE strategy = ? AND exit_date = ?",
                    (STRATEGY, today),
                ).fetchone()[0]
                logger.info(
                    "EOD Summary: %d open, %d entries today, %d exits today",
                    n_open, today_entries, today_exits,
                )
            finally:
                conn.close()

            # Sleep until next morning
            write_heartbeat("sleeping", "After hours",
                           open_positions=n_open, entries_today=today_entries, exits_today=today_exits)
            logger.info("After hours — sleeping until 9:25 ET tomorrow")
            time.sleep(3600)
            continue

        time.sleep(60)


# ---------------------------------------------------------------------------
# Backfill mode — catch up missed days
# ---------------------------------------------------------------------------

def run_backfill() -> None:
    """Process any filings that occurred since last entry but weren't traded."""
    logger.info("Running backfill mode — catching up missed filings")
    result = run_daily(dry_run=False)
    logger.info("Backfill complete: %s", result)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Form4 Insider Portfolio Runner")
    parser.add_argument("--dry-run", action="store_true", help="Scan and log without placing orders")
    parser.add_argument("--backfill", action="store_true", help="Catch up missed days")
    parser.add_argument("--once", action="store_true", help="Run one daily cycle and exit")
    args = parser.parse_args()

    if args.backfill:
        run_backfill()
    elif args.once or args.dry_run:
        result = run_daily(dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
    else:
        run_daemon()


if __name__ == "__main__":
    main()
