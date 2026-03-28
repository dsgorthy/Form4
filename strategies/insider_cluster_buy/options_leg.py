"""
Options overlay for Insider Cluster Buy paper trading.

Adds a parallel 5% OTM ~90 DTE call leg alongside each equity entry.
Queries Alpaca's options contracts API to find real listed contracts.

Exit rules:
  - 14 trading day hold (independent of equity leg's 7-day hold)
  - 50% profit target (close early if option doubles in value by 50%)
"""

from __future__ import annotations

import logging
import math
import os
from datetime import date, datetime, timedelta
from typing import Optional

import requests

from state import add_trading_days

logger = logging.getLogger(__name__)

# Alpaca options contracts endpoint (same for paper and live)
ALPACA_OPTIONS_URL = "https://paper-api.alpaca.markets/v2/options/contracts"


# ── Strike / Expiry Helpers ─────────────────────────────────────────────


def _get_strike_interval(price: float) -> float:
    """Standard option strike intervals based on stock price."""
    if price < 5:
        return 0.5
    elif price < 25:
        return 1.0
    elif price < 200:
        return 2.5
    else:
        return 5.0


def _nearest_strike(price: float, target_mult: float) -> float:
    """Find nearest standard strike to price * target_mult."""
    raw = price * target_mult
    interval = _get_strike_interval(price)
    return round(raw / interval) * interval


def _build_occ_symbol(ticker: str, expiry_date: date, strike: float) -> str:
    """
    Build OCC option symbol for a call.
    Format: {UNDERLYING}{YYMMDD}C{STRIKE*1000 zero-padded to 8}
    """
    exp_str = expiry_date.strftime("%y%m%d")
    strike_int = int(round(strike * 1000))
    return f"{ticker}{exp_str}C{strike_int:08d}"


def _lookup_contract_alpaca(
    ticker: str,
    target_strike: float,
    entry_date: date,
    target_dte: int,
    session: Optional[requests.Session] = None,
) -> Optional[dict]:
    """
    Query Alpaca /v2/options/contracts for a real listed call contract
    closest to our target strike and DTE.

    Returns dict with occ_symbol, strike, expiry_date, dte or None.
    """
    # Search window: target_dte ± 30 days
    min_expiry = entry_date + timedelta(days=max(target_dte - 30, 30))
    max_expiry = entry_date + timedelta(days=target_dte + 30)
    # Strike search: ± 20% from target
    strike_lo = round(target_strike * 0.80, 2)
    strike_hi = round(target_strike * 1.20, 2)

    params = {
        "underlying_symbols": ticker,
        "expiration_date_gte": min_expiry.isoformat(),
        "expiration_date_lte": max_expiry.isoformat(),
        "type": "call",
        "strike_price_gte": str(strike_lo),
        "strike_price_lte": str(strike_hi),
        "status": "active",
    }

    try:
        if session is None:
            session = requests.Session()
            session.headers.update({
                "APCA-API-KEY-ID": os.environ.get("ALPACA_API_KEY", ""),
                "APCA-API-SECRET-KEY": os.environ.get("ALPACA_API_SECRET", ""),
            })
        resp = session.get(ALPACA_OPTIONS_URL, params=params, timeout=15)
        if resp.status_code != 200:
            logger.warning("Alpaca options lookup failed: %d %s", resp.status_code, resp.text[:200])
            return None

        data = resp.json()
        contracts = data if isinstance(data, list) else data.get("option_contracts", [])
        if not contracts:
            logger.warning("No options contracts found for %s (strike ~$%.0f, DTE ~%d)", ticker, target_strike, target_dte)
            return None

        # Score each contract: minimize |strike - target| + |dte - target_dte|/3
        # Weight DTE difference less since we're flexible on timing
        target_date = entry_date + timedelta(days=target_dte)
        best = None
        best_score = float("inf")

        for c in contracts:
            c_strike = float(c.get("strike_price", 0))
            c_expiry_str = c.get("expiration_date", "")
            if not c_expiry_str or c_strike <= 0:
                continue
            c_expiry = date.fromisoformat(c_expiry_str)
            c_dte = (c_expiry - entry_date).days

            strike_diff = abs(c_strike - target_strike)
            dte_diff = abs(c_dte - target_dte) / 3.0
            score = strike_diff + dte_diff

            if score < best_score:
                best_score = score
                best = {
                    "occ_symbol": c.get("symbol", _build_occ_symbol(ticker, c_expiry, c_strike)),
                    "strike": c_strike,
                    "expiry_date": c_expiry.isoformat(),
                    "dte": c_dte,
                }

        if best:
            logger.info(
                "Alpaca contract found: %s (strike=$%.2f, DTE=%d, score=%.1f)",
                best["occ_symbol"], best["strike"], best["dte"], best_score,
            )
        return best

    except Exception as e:
        logger.warning("Alpaca options contract lookup error: %s", e)
        return None


# ── Core Functions ──────────────────────────────────────────────────────


def select_strike_and_expiry(
    ticker: str,
    current_price: float,
    entry_date: date,
    strike_mult: float = 1.05,
    target_dte: int = 90,
    session: Optional[requests.Session] = None,
) -> dict:
    """
    Select 5% OTM call with nearest available expiry ~target_dte.

    Queries Alpaca for real listed contracts first. Falls back to
    static OCC symbol construction if lookup fails.

    Returns dict with occ_symbol, strike, expiry_date, dte.
    """
    target_strike = _nearest_strike(current_price, strike_mult)

    # Try live contract lookup
    result = _lookup_contract_alpaca(ticker, target_strike, entry_date, target_dte, session)
    if result:
        return result

    # Fallback: use 3rd Friday of target month (standard monthly expiry)
    logger.warning("Falling back to static expiry for %s", ticker)
    target_date = entry_date + timedelta(days=target_dte)
    expiry = _third_friday(target_date.year, target_date.month)
    # If 3rd Friday is too close, use next month
    if (expiry - entry_date).days < target_dte - 14:
        m = target_date.month + 1
        y = target_date.year + (m > 12)
        m = m if m <= 12 else m - 12
        expiry = _third_friday(y, m)

    dte = (expiry - entry_date).days
    occ_symbol = _build_occ_symbol(ticker, expiry, target_strike)
    return {
        "occ_symbol": occ_symbol,
        "strike": target_strike,
        "expiry_date": expiry.isoformat(),
        "dte": dte,
    }


def _third_friday(year: int, month: int) -> date:
    """Return the 3rd Friday of the given month (standard monthly options expiry)."""
    # Find first day of month
    first = date(year, month, 1)
    # Days until first Friday: (4 - weekday) % 7
    days_to_fri = (4 - first.weekday()) % 7
    first_friday = first + timedelta(days=days_to_fri)
    return first_friday + timedelta(weeks=2)  # 3rd Friday


def compute_options_size(
    portfolio_value: float,
    option_price: float,
    size_pct: float = 0.01,
    max_contracts: int = 2,
) -> int:
    """
    Size options leg: size_pct of portfolio per contract, capped at max_contracts.

    option_price is per-share premium (multiply by 100 for contract cost).
    Returns number of contracts (0 if option is too expensive).
    """
    if option_price <= 0:
        return 0
    contract_cost = option_price * 100  # 1 contract = 100 shares
    budget = portfolio_value * size_pct
    qty = int(budget / contract_cost)
    return min(max(qty, 0), max_contracts)


def submit_options_entry(
    signal: dict,
    backend,
    current_price: float,
    portfolio_value: float,
    strike_mult: float = 1.05,
    target_dte: int = 90,
    hold_days: int = 14,
    size_pct: float = 0.01,
    max_contracts: int = 2,
    session: Optional[requests.Session] = None,
) -> Optional[dict]:
    """
    Build OCC symbol and submit market buy for call contract(s).

    Returns options_leg dict on success, None on failure.
    """
    ticker = signal["ticker"]
    entry_date = date.today()

    # Select strike and expiry (queries Alpaca for real contracts)
    selection = select_strike_and_expiry(
        ticker, current_price, entry_date, strike_mult, target_dte, session,
    )
    occ_symbol = selection["occ_symbol"]

    logger.info(
        "Options leg: %s — strike=$%.2f, expiry=%s (%dd), price=$%.2f",
        occ_symbol, selection["strike"], selection["expiry_date"],
        selection["dte"], current_price,
    )

    # Submit order to get a fill price, then compute sizing
    # First try with 1 contract to discover fill price
    try:
        result = backend.submit_order(
            symbol=occ_symbol,
            qty=1,
            side="buy",
            order_type="market",
            time_in_force="day",
        )
    except Exception as e:
        logger.warning("Options order submission failed for %s: %s", occ_symbol, e)
        return None

    if result.is_error:
        logger.warning(
            "Options order rejected for %s: %s", occ_symbol, result.error
        )
        return None

    # Wait for fill
    try:
        result = backend.wait_for_fill(result.order_id, timeout=30, poll_interval=2)
    except Exception as e:
        logger.warning("Options fill timeout for %s: %s", occ_symbol, e)
        return None

    if not result.is_filled or not result.filled_price:
        logger.warning("Options order not filled for %s: status=%s", occ_symbol, result.status)
        return None

    fill_price = result.filled_price

    # Now determine if we want more contracts
    qty = compute_options_size(portfolio_value, fill_price, size_pct, max_contracts)
    if qty == 0:
        logger.warning(
            "Options too expensive for %s: $%.2f/contract vs $%.2f budget",
            occ_symbol, fill_price * 100, portfolio_value * size_pct,
        )
        # We already bought 1 — keep it
        qty = 1

    # If we need more than 1, buy the remainder
    if qty > 1:
        try:
            extra = backend.submit_order(
                symbol=occ_symbol,
                qty=qty - 1,
                side="buy",
                order_type="market",
                time_in_force="day",
            )
            if extra.is_filled or not extra.is_error:
                backend.wait_for_fill(extra.order_id, timeout=30, poll_interval=2)
        except Exception as e:
            logger.warning("Extra contracts order failed for %s: %s", occ_symbol, e)
            # Keep the 1 we already have

    exit_target_date = add_trading_days(entry_date, hold_days)

    return {
        "occ_symbol": occ_symbol,
        "order_id": result.order_id,
        "qty": qty,
        "entry_price": fill_price,
        "entry_date": entry_date.isoformat(),
        "strike": selection["strike"],
        "expiry": selection["expiry_date"],
        "dte_at_entry": selection["dte"],
        "exit_date_target": exit_target_date.isoformat(),
        "status": "open",
        "exit_price": None,
        "exit_date": None,
        "exit_reason": None,
        "pnl": None,
        "pnl_pct": None,
    }


def check_options_exit(
    options_leg: dict,
    backend,
    profit_target: float = 0.50,
) -> tuple[bool, str]:
    """
    Check if options leg should be closed.

    Exit conditions:
      1. 14-day hold reached
      2. 50% profit target hit
      3. Option position no longer exists (expired/assigned)

    Returns (should_exit, reason).
    """
    if options_leg.get("status") != "open":
        return False, ""

    occ_symbol = options_leg["occ_symbol"]

    # Check if position still exists
    try:
        position = backend.get_position(occ_symbol)
    except Exception:
        position = None

    if position is None:
        # Position gone — expired or assigned
        return True, "position_gone"

    # Check time exit
    today_str = date.today().isoformat()
    exit_target = options_leg.get("exit_date_target", "")
    if exit_target and today_str >= exit_target:
        return True, "time_exit"

    # Check profit target
    entry_price = options_leg.get("entry_price", 0)
    if entry_price > 0:
        current_price = position.get("current_price") or position.get("avg_entry_price", 0)
        if isinstance(current_price, str):
            current_price = float(current_price)
        pnl_pct = (current_price - entry_price) / entry_price
        if pnl_pct >= profit_target:
            return True, "profit_target"

    return False, ""


def close_options_leg(
    options_leg: dict,
    backend,
    portfolio_value: float = 30000.0,
) -> dict:
    """
    Close options position and compute P&L.

    Returns updated options_leg dict with exit details.
    """
    occ_symbol = options_leg["occ_symbol"]
    entry_price = options_leg.get("entry_price", 0)
    qty = options_leg.get("qty", 1)

    exit_price = 0.0
    reason = options_leg.get("exit_reason", "manual")

    # Try to close the position
    try:
        position = backend.get_position(occ_symbol)
        if position is not None:
            result = backend.close_position(occ_symbol)
            if result.is_filled and result.filled_price:
                exit_price = result.filled_price
            elif position.get("current_price"):
                exit_price = float(position["current_price"])
        else:
            # Position already gone (expired worthless or assigned)
            exit_price = 0.0
            if not reason or reason == "manual":
                reason = "expired_worthless"
    except Exception as e:
        logger.warning("Failed to close options position %s: %s", occ_symbol, e)
        exit_price = 0.0

    # Compute P&L
    pnl_per_contract = (exit_price - entry_price) * 100  # 100 shares per contract
    total_pnl = pnl_per_contract * qty
    pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0

    options_leg.update({
        "status": "closed",
        "exit_price": exit_price,
        "exit_date": date.today().isoformat(),
        "exit_reason": reason,
        "pnl": round(total_pnl, 2),
        "pnl_pct": round(pnl_pct, 4),
    })

    logger.info(
        "Options closed: %s — %s, P&L=$%.2f (%.1f%%)",
        occ_symbol, reason, total_pnl, pnl_pct * 100,
    )

    return options_leg
