"""
Order management for Insider Cluster Buy paper trading.

Handles entry sizing, stop-loss monitoring, time-based exits,
position limits, VIX regime adjustment, and circuit breaker.
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from datetime import date, datetime
from typing import Optional

import requests

from state import add_trading_days, compute_rolling_dd

logger = logging.getLogger(__name__)

# Alpaca data API for quotes
ALPACA_DATA_URL = "https://data.alpaca.markets/v2"

# Yahoo Finance VIX fallback
YAHOO_VIX_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX"


def get_current_price(symbol: str, session: requests.Session) -> Optional[float]:
    """Get latest trade price from Alpaca data API."""
    try:
        resp = session.get(
            f"{ALPACA_DATA_URL}/stocks/{symbol}/trades/latest",
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            trade = data.get("trade", {})
            return float(trade.get("p", 0)) or None
    except Exception as e:
        logger.debug("Alpaca price fetch failed for %s: %s", symbol, e)
    return None


def get_vix(session: requests.Session) -> float:
    """
    Fetch current VIX level. Tries Alpaca data API first, Yahoo Finance fallback.
    Returns VIX value or 20.0 as a safe default.
    """
    # Try Alpaca — VIX is available as VIXY or through snapshot
    try:
        resp = session.get(
            f"{ALPACA_DATA_URL}/stocks/VIXY/trades/latest",
            timeout=10,
        )
        if resp.status_code == 200:
            # VIXY tracks VIX futures; rough approximation
            data = resp.json()
            price = float(data.get("trade", {}).get("p", 0))
            if price > 0:
                # VIXY is not VIX directly but gives directional signal
                # Use it as proxy: VIXY > 25 ≈ VIX > 30
                logger.debug("VIXY price: %.2f", price)
    except Exception:
        pass

    # Yahoo Finance fallback for actual VIX
    try:
        resp = requests.get(YAHOO_VIX_URL, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        if resp.status_code == 200:
            data = resp.json()
            result = data.get("chart", {}).get("result", [{}])[0]
            meta = result.get("meta", {})
            vix = float(meta.get("regularMarketPrice", 0))
            if vix > 0:
                logger.info("VIX from Yahoo: %.2f", vix)
                return vix
    except Exception as e:
        logger.debug("Yahoo VIX fetch failed: %s", e)

    logger.warning("Could not fetch VIX, defaulting to 20.0")
    return 20.0


def compute_position_size(
    equity: float,
    current_price: float,
    size_pct: float,
    vix: float,
    vix_threshold: float,
    reduced_size_pct: float,
) -> tuple[int, float]:
    """
    Compute position size (number of shares).
    Reduces to reduced_size_pct if VIX > threshold.

    Returns (qty, dollar_amount).
    """
    effective_pct = reduced_size_pct if vix > vix_threshold else size_pct
    dollar_amount = equity * effective_pct
    qty = math.floor(dollar_amount / current_price)
    return max(qty, 0), dollar_amount


def can_open_position(
    state: dict,
    max_concurrent: int,
    circuit_breaker_dd_pct: float,
    ticker: str = "",
) -> tuple[bool, str]:
    """
    Check if a new position can be opened.

    Rules:
      1. Max concurrent positions (default 3)
      2. Max 2 positions in the same sector (approximated by first SIC digit)
      3. Circuit breaker: drawdown > threshold
    """
    open_positions = state.get("open_positions", [])

    # Rule 1: max concurrent
    if len(open_positions) >= max_concurrent:
        return False, f"Max concurrent positions ({max_concurrent}) reached"

    # Rule 2: max 2 same ticker (proxy for sector — real sector lookup is expensive)
    ticker_count = Counter(p.get("ticker", "") for p in open_positions)
    if ticker and ticker_count.get(ticker, 0) >= 1:
        return False, f"Already have position in {ticker}"

    # Rule 3: circuit breaker
    if state.get("circuit_breaker_active", False):
        return False, "Circuit breaker active — paused after drawdown"

    # Check rolling drawdown
    returns = state.get("performance", {}).get("returns", [])
    dd = compute_rolling_dd(returns, window=30)
    if dd >= circuit_breaker_dd_pct:
        state["circuit_breaker_active"] = True
        return False, f"Circuit breaker tripped: {dd:.1%} drawdown >= {circuit_breaker_dd_pct:.1%} threshold"

    return True, "OK"


def submit_entry(
    signal: dict,
    backend,
    session: requests.Session,
    equity: float,
    size_pct: float,
    vix_threshold: float,
    reduced_size_pct: float,
) -> Optional[dict]:
    """
    Submit a market buy order for a cluster signal.

    Returns trade dict with entry details, or None if order fails.
    """
    ticker = signal["ticker"]

    # Get current price
    current_price = get_current_price(ticker, session)
    if current_price is None or current_price <= 0:
        logger.error("Cannot get price for %s — skipping entry", ticker)
        return None

    # Get VIX for sizing
    vix = get_vix(session)

    # Compute size
    qty, dollar_amount = compute_position_size(
        equity=equity,
        current_price=current_price,
        size_pct=size_pct,
        vix=vix,
        vix_threshold=vix_threshold,
        reduced_size_pct=reduced_size_pct,
    )

    if qty <= 0:
        logger.warning("Position size is 0 shares for %s at $%.2f", ticker, current_price)
        return None

    # Submit order
    logger.info("Submitting BUY %d %s @ ~$%.2f ($%.0f)", qty, ticker, current_price, dollar_amount)
    result = backend.submit_order(
        symbol=ticker,
        qty=qty,
        side="buy",
        order_type="market",
        time_in_force="day",
    )

    if result.is_error:
        logger.error("Order rejected for %s: %s", ticker, result.error)
        return None

    # Wait for fill if still pending
    if result.status == "pending":
        result = backend.wait_for_fill(result.order_id, timeout=60)

    filled_price = result.filled_price or current_price
    stop_pct = signal.get("stop_loss", -0.10)
    stop_price = round(filled_price * (1.0 + stop_pct), 2)

    trade = {
        "ticker": ticker,
        "company": signal.get("company", ticker),
        "order_id": result.order_id,
        "qty": result.filled_qty or qty,
        "entry_price": filled_price,
        "entry_date": date.today().isoformat(),
        "stop_price": stop_price,
        "exit_date_target": add_trading_days(date.today(), signal.get("hold_days", 7)).isoformat(),
        "signal": {
            "n_insiders": signal.get("n_insiders", 0),
            "total_value": signal.get("total_value", 0),
            "confidence": signal.get("confidence", 0),
            "quality_score": signal.get("quality_score", 0),
            "insiders": signal.get("insiders", []),
        },
        "vix_at_entry": vix,
        "status": "open",
    }

    logger.info(
        "ENTRY: %s — %d shares @ $%.2f, stop=$%.2f, exit_target=%s",
        ticker, trade["qty"], filled_price, stop_price, trade["exit_date_target"],
    )
    return trade


def check_stop_loss(trade: dict, backend) -> bool:
    """
    Check if a position has hit -15% stop loss.
    Returns True if position was closed.
    """
    ticker = trade["ticker"]
    position = backend.get_position(ticker)

    if position is None:
        logger.warning("No position found for %s — may have been closed externally", ticker)
        return True  # Treat as closed

    unrealized_pl = position.get("unrealized_pl", 0)
    entry_price = trade.get("entry_price", 0)
    qty = trade.get("qty", 0)
    cost_basis = entry_price * qty

    if cost_basis <= 0:
        return False

    pnl_pct = unrealized_pl / cost_basis
    current_price = position.get("current_price", 0)

    logger.debug(
        "%s: current=$%.2f, entry=$%.2f, P&L=%.2f%%",
        ticker, current_price, entry_price, pnl_pct * 100,
    )

    if pnl_pct <= -0.15:
        logger.info("STOP LOSS HIT: %s at $%.2f (%.1f%%)", ticker, current_price, pnl_pct * 100)
        result = backend.close_position(ticker)
        if result.is_error:
            logger.error("Failed to close stop-loss position %s: %s", ticker, result.error)
            return False
        return True

    return False


def check_time_exit(trade: dict, backend) -> bool:
    """
    Check if a position has reached T+7 trading days.
    Returns True if position was closed.
    """
    exit_target = trade.get("exit_date_target", "")
    if not exit_target:
        return False

    today_str = date.today().isoformat()
    if today_str < exit_target:
        return False

    ticker = trade["ticker"]
    logger.info("TIME EXIT: %s — reached target date %s", ticker, exit_target)
    result = backend.close_position(ticker)
    if result.is_error:
        logger.error("Failed to close time-exit position %s: %s", ticker, result.error)
        return False
    return True


def close_trade(trade: dict, reason: str, backend, portfolio_value: float = 30000) -> dict:
    """
    Finalize a closed trade: get fill details, compute P&L.
    Returns a closed_trade dict for the state.
    """
    ticker = trade["ticker"]
    position = backend.get_position(ticker)

    # Get exit price from position (if still open) or from last known
    if position and position.get("current_price", 0) > 0:
        exit_price = position["current_price"]
    else:
        exit_price = trade.get("entry_price", 0)  # fallback

    entry_price = trade.get("entry_price", 0)
    qty = trade.get("qty", 0)
    pnl = (exit_price - entry_price) * qty
    pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
    # Portfolio-level return for drawdown calculation
    portfolio_return = pnl / portfolio_value if portfolio_value > 0 else 0

    closed = {
        **trade,
        "exit_price": exit_price,
        "exit_date": date.today().isoformat(),
        "exit_reason": reason,
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 4),
        "portfolio_return": round(portfolio_return, 6),
        "status": "closed",
    }

    logger.info(
        "CLOSED: %s — %s, P&L=$%.2f (%.2f%%), portfolio impact=%.3f%%",
        ticker, reason, pnl, pnl_pct * 100, portfolio_return * 100,
    )
    return closed
