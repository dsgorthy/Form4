"""
Put execution leg for Insider V3.3 sell signals.

When the sell cluster trigger fires, buy ITM puts as a defined-risk short bet.

V3.3 changes from V2:
  - 5% ITM puts (strike_mult=1.05) instead of 5% OTM (0.95)
  - Tight DTE (7-21 days) instead of 30+ DTE
  - -25% stop loss instead of -50%
  - No profit target (hold to expiry or time exit)
  - Spread filter: reject if bid-ask spread > 10%
  - Min open interest: OI >= 100
  - Limit orders at ASK instead of market orders
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


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


def _find_expiry_date(entry_date: date, min_dte: int, max_dte: int = 21) -> date:
    """
    Find nearest Friday (standard options expiry) within DTE range.

    V3.3: tight DTE window (default 7-21 days). Prefers closest expiry
    within range. Returns None if no valid expiry found.
    """
    # Search for Fridays in range [min_dte, max_dte]
    candidates = []
    for offset in range(min_dte, max_dte + 7):  # slight buffer to find Fridays
        d = entry_date + timedelta(days=offset)
        if d.weekday() == 4:  # Friday
            dte = (d - entry_date).days
            if min_dte <= dte <= max_dte:
                candidates.append(d)

    if candidates:
        return candidates[0]  # Nearest valid Friday

    # Fallback: nearest Friday just outside range
    d = entry_date + timedelta(days=min_dte)
    weekday = d.weekday()
    days_until_friday = (4 - weekday) % 7
    return d + timedelta(days=days_until_friday)


def _build_occ_symbol(ticker: str, expiry_date: date, strike: float) -> str:
    """
    Build OCC option symbol for a PUT.
    Format: {UNDERLYING}{YYMMDD}P{STRIKE*1000 zero-padded to 8}
    """
    exp_str = expiry_date.strftime("%y%m%d")
    strike_int = int(round(strike * 1000))
    return f"{ticker}{exp_str}P{strike_int:08d}"


def _add_trading_days(start: date, n: int) -> date:
    """Add n trading days (skip weekends)."""
    d = start
    added = 0
    while added < n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


# ── Core Functions ──────────────────────────────────────────────────────


def select_put_strike_and_expiry(
    ticker: str,
    current_price: float,
    entry_date: date,
    strike_mult: float = 1.05,     # 5% ITM put (V3.3)
    min_dte: int = 7,              # Tight DTE lower bound (V3.3)
    max_dte: int = 21,             # Tight DTE upper bound (V3.3)
) -> dict:
    """
    Select ITM put with nearest expiry in tight DTE window.

    V3.3: 5% ITM (strike_mult=1.05), 7-21 DTE.
    """
    strike = _nearest_strike(current_price, strike_mult)
    expiry = _find_expiry_date(entry_date, min_dte, max_dte)
    dte = (expiry - entry_date).days
    occ_symbol = _build_occ_symbol(ticker, expiry, strike)

    return {
        "occ_symbol": occ_symbol,
        "strike": strike,
        "expiry_date": expiry.isoformat(),
        "dte": dte,
    }


def check_spread_and_oi(
    backend,
    occ_symbol: str,
    max_spread_pct: float = 0.10,
    min_oi: int = 100,
) -> tuple[bool, float, float, int]:
    """
    Check if option meets spread and open interest filters.

    Returns (passes, bid, ask, open_interest).
    Rejects if:
      - Spread > max_spread_pct (default 10%)
      - Open interest < min_oi (default 100)
    """
    try:
        quote = backend.get_option_quote(occ_symbol)
        if quote is None:
            logger.warning("No quote available for %s", occ_symbol)
            return False, 0, 0, 0

        bid = float(quote.get("bid", 0) or 0)
        ask = float(quote.get("ask", 0) or 0)
        oi = int(quote.get("open_interest", 0) or 0)

        if ask <= 0:
            logger.warning("Invalid ask price for %s: %.2f", occ_symbol, ask)
            return False, bid, ask, oi

        spread_pct = (ask - bid) / ask if ask > 0 else 1.0

        if spread_pct > max_spread_pct:
            logger.info(
                "SPREAD REJECT: %s — spread=%.1f%% (max=%.0f%%), bid=%.2f, ask=%.2f",
                occ_symbol, spread_pct * 100, max_spread_pct * 100, bid, ask,
            )
            return False, bid, ask, oi

        if oi < min_oi:
            logger.info(
                "OI REJECT: %s — OI=%d (min=%d)", occ_symbol, oi, min_oi,
            )
            return False, bid, ask, oi

        return True, bid, ask, oi

    except Exception as e:
        logger.warning("Failed to check spread/OI for %s: %s", occ_symbol, e)
        return False, 0, 0, 0


def compute_put_size(
    portfolio_value: float,
    option_price: float,
    size_pct: float = 0.005,       # 0.5% of portfolio per trade (V3.3)
    max_contracts: int = 3,
) -> int:
    """
    Size put leg: size_pct of portfolio, capped at max_contracts.

    option_price is per-share premium (multiply by 100 for contract cost).
    Returns number of contracts (0 if too expensive).
    """
    if option_price <= 0:
        return 0
    contract_cost = option_price * 100
    budget = portfolio_value * size_pct
    qty = int(budget / contract_cost)
    return min(max(qty, 0), max_contracts)


def submit_put_entry(
    signal: dict,
    backend,
    current_price: float,
    portfolio_value: float,
    strike_mult: float = 1.05,
    min_dte: int = 7,
    max_dte: int = 21,
    hold_days: int = 7,
    size_pct: float = 0.005,
    max_contracts: int = 3,
    max_spread_pct: float = 0.10,
    min_oi: int = 100,
) -> Optional[dict]:
    """
    Build OCC put symbol and submit limit buy at ASK for put contract(s).

    V3.3 changes:
      - Limit orders at ASK (not market orders)
      - Spread filter rejects trades with spread > 10%
      - Min OI filter rejects options with OI < 100
      - ITM puts (strike_mult=1.05)
      - Tight DTE (7-21 days)
    """
    ticker = signal["ticker"]
    entry_date = date.today()

    selection = select_put_strike_and_expiry(
        ticker, current_price, entry_date, strike_mult, min_dte, max_dte,
    )
    occ_symbol = selection["occ_symbol"]

    logger.info(
        "Put leg: %s — strike=$%.2f, expiry=%s (%dd), stock=$%.2f",
        occ_symbol, selection["strike"], selection["expiry_date"],
        selection["dte"], current_price,
    )

    # Check spread and OI filters before attempting entry
    passes, bid, ask, oi = check_spread_and_oi(
        backend, occ_symbol, max_spread_pct, min_oi,
    )
    if not passes:
        logger.info("Put entry REJECTED for %s: failed spread/OI filter", occ_symbol)
        return None

    # Size based on ask price
    qty = compute_put_size(portfolio_value, ask, size_pct, max_contracts)
    if qty == 0:
        logger.info("Put entry REJECTED for %s: too expensive (ask=$%.2f)", occ_symbol, ask)
        return None

    # Submit limit order at ASK price (conservative — guarantees fill)
    try:
        result = backend.submit_order(
            symbol=occ_symbol,
            qty=qty,
            side="buy",
            order_type="limit",
            limit_price=ask,
            time_in_force="day",
        )
    except Exception as e:
        logger.warning("Put order failed for %s: %s", occ_symbol, e)
        return None

    if result.is_error:
        logger.warning("Put order rejected for %s: %s", occ_symbol, result.error)
        return None

    try:
        result = backend.wait_for_fill(result.order_id, timeout=30, poll_interval=2)
    except Exception as e:
        logger.warning("Put fill timeout for %s: %s", occ_symbol, e)
        return None

    if not result.is_filled or not result.filled_price:
        logger.warning("Put order not filled for %s: status=%s", occ_symbol, result.status)
        return None

    fill_price = result.filled_price

    exit_target_date = _add_trading_days(entry_date, hold_days)

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
        "direction": "sell",  # This is a bearish bet
        "spread_at_entry": round((ask - bid) / ask, 4) if ask > 0 else 0,
        "ask_at_entry": ask,
        "oi_at_entry": oi,
        "exit_price": None,
        "exit_date": None,
        "exit_reason": None,
        "pnl": None,
        "pnl_pct": None,
    }


def check_put_exit(
    put_leg: dict,
    backend,
    stop_loss: float = -0.25,       # -25% of premium (V3.3)
) -> tuple[bool, str]:
    """
    Check if put leg should be closed.

    V3.3 exit conditions (no profit target):
      1. 7-day hold reached
      2. -25% stop loss (cut losses at 25% of premium)
      3. Position gone (expired/assigned)
    """
    if put_leg.get("status") != "open":
        return False, ""

    occ_symbol = put_leg["occ_symbol"]

    try:
        position = backend.get_position(occ_symbol)
    except Exception:
        position = None

    if position is None:
        return True, "position_gone"

    # Time exit
    today_str = date.today().isoformat()
    exit_target = put_leg.get("exit_date_target", "")
    if exit_target and today_str >= exit_target:
        return True, "time_exit"

    # Stop loss check (no profit target in V3.3)
    entry_price = put_leg.get("entry_price", 0)
    if entry_price > 0:
        current_price = position.get("current_price") or position.get("avg_entry_price", 0)
        if isinstance(current_price, str):
            current_price = float(current_price)
        pnl_pct = (current_price - entry_price) / entry_price

        if pnl_pct <= stop_loss:
            return True, "stop_loss"

    return False, ""


def close_put_leg(
    put_leg: dict,
    backend,
    portfolio_value: float = 30000.0,
) -> dict:
    """
    Close put position and compute P&L.

    Returns updated put_leg dict with exit details.
    """
    occ_symbol = put_leg["occ_symbol"]
    entry_price = put_leg.get("entry_price", 0)
    qty = put_leg.get("qty", 1)

    exit_price = 0.0
    reason = put_leg.get("exit_reason", "manual")

    try:
        position = backend.get_position(occ_symbol)
        if position is not None:
            result = backend.close_position(occ_symbol)
            if result.is_filled and result.filled_price:
                exit_price = result.filled_price
            elif position.get("current_price"):
                exit_price = float(position["current_price"])
        else:
            exit_price = 0.0
            if not reason or reason == "manual":
                reason = "expired_worthless"
    except Exception as e:
        logger.warning("Failed to close put position %s: %s", occ_symbol, e)
        exit_price = 0.0

    pnl_per_contract = (exit_price - entry_price) * 100
    total_pnl = pnl_per_contract * qty
    pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0

    put_leg.update({
        "status": "closed",
        "exit_price": exit_price,
        "exit_date": date.today().isoformat(),
        "exit_reason": reason,
        "pnl": round(total_pnl, 2),
        "pnl_pct": round(pnl_pct, 4),
    })

    logger.info(
        "Put closed: %s — %s, P&L=$%.2f (%.1f%%)",
        occ_symbol, reason, total_pnl, pnl_pct * 100,
    )

    return put_leg
