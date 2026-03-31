#!/usr/bin/env python3
"""
Exit strategy functions for thesis-based insider trade exits.

Each exit function takes daily prices and returns (exit_date, exit_price, exit_reason).
Designed for use with cw_simulation.py — the CEO Watcher-inspired simulation runner.

Usage:
    from cw_exit_strategies import thesis_based_exit, fixed_hold_exit

    prices = load_prices("AAPL")  # {date_str: close_price}
    exit_date, exit_price, reason = fixed_hold_exit(prices, "2024-01-15", 150.0, 30)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _get_sorted_dates_after(prices: dict[str, float], entry_date: str) -> list[str]:
    """Return sorted list of dates with prices strictly after entry_date."""
    return sorted(d for d in prices if d > entry_date)


def _find_nearest_date(prices: dict[str, float], target_date: str,
                       max_offset_days: int = 5) -> str | None:
    """Find the nearest date in prices within max_offset_days of target_date."""
    if target_date in prices:
        return target_date
    try:
        td = datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError:
        return None
    for offset in range(1, max_offset_days + 1):
        fwd = (td + timedelta(days=offset)).strftime("%Y-%m-%d")
        if fwd in prices:
            return fwd
        bwd = (td - timedelta(days=offset)).strftime("%Y-%m-%d")
        if bwd in prices:
            return bwd
    return None


# ---------------------------------------------------------------------------
# Exit 1: Fixed hold period
# ---------------------------------------------------------------------------

def fixed_hold_exit(prices: dict[str, float], entry_date: str, entry_price: float,
                    hold_days: int) -> tuple[str | None, float | None, str]:
    """
    Hold for exactly N calendar days, exit at close of that day.

    If no price on the exact target day, find nearest within 5 calendar days.

    Parameters
    ----------
    prices : dict mapping date strings to close prices
    entry_date : trade entry date (YYYY-MM-DD)
    entry_price : entry price (used for context, not exit calc)
    hold_days : number of calendar days to hold

    Returns
    -------
    (exit_date, exit_price, exit_reason)
    """
    try:
        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
    except ValueError:
        return None, None, f"fixed_hold_{hold_days}d"

    target_dt = entry_dt + timedelta(days=hold_days)
    target_str = target_dt.strftime("%Y-%m-%d")

    exit_date = _find_nearest_date(prices, target_str, max_offset_days=5)
    if exit_date is None:
        return None, None, f"fixed_hold_{hold_days}d"

    return exit_date, prices[exit_date], f"fixed_hold_{hold_days}d"


# ---------------------------------------------------------------------------
# Exit 2: Fair value recovery (for dip_buy thesis)
# ---------------------------------------------------------------------------

def fair_value_exit(prices: dict[str, float], entry_date: str, entry_price: float,
                    pre_dip_price: float, max_hold: int = 365) -> tuple[str | None, float | None, str]:
    """
    For dip_buy thesis: exit when price recovers to pre-dip level.

    The pre_dip_price is the price before the dip occurred. Typically:
        pre_dip_price = entry_price / (1 + dip_pct)
    where dip_pct is negative (e.g., -0.20 for a 20% dip).

    Scans daily from entry until price >= pre_dip_price or max_hold reached.

    Parameters
    ----------
    prices : dict mapping date strings to close prices
    entry_date : trade entry date
    entry_price : price at entry
    pre_dip_price : target recovery price (the level before the dip)
    max_hold : maximum calendar days to hold before forced exit

    Returns
    -------
    (exit_date, exit_price, exit_reason)
    """
    dates_after = _get_sorted_dates_after(prices, entry_date)
    if not dates_after:
        return None, None, f"max_hold_{max_hold}d"

    try:
        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
    except ValueError:
        return None, None, f"max_hold_{max_hold}d"

    max_date_str = (entry_dt + timedelta(days=max_hold)).strftime("%Y-%m-%d")

    for d in dates_after:
        if d > max_date_str:
            break
        close = prices[d]
        if close >= pre_dip_price:
            return d, close, "fair_value_hit"

    # Max hold reached -- exit at last available price within window
    last_date = None
    for d in dates_after:
        if d > max_date_str:
            break
        last_date = d

    if last_date is not None:
        return last_date, prices[last_date], f"max_hold_{max_hold}d"
    return None, None, f"max_hold_{max_hold}d"


# ---------------------------------------------------------------------------
# Exit 3: SMA50 break (for momentum thesis)
# ---------------------------------------------------------------------------

def sma50_break_exit(prices: dict[str, float], entry_date: str, entry_price: float,
                     max_hold: int = 365) -> tuple[str | None, float | None, str]:
    """
    For momentum thesis: exit when daily close drops below SMA50.

    Computes a rolling 50-day SMA as we scan forward from entry.
    Starts checking the SMA break condition after 5 trading days to give
    the trade room to develop.

    Parameters
    ----------
    prices : dict mapping date strings to close prices
    entry_date : trade entry date
    entry_price : price at entry
    max_hold : maximum calendar days before forced exit

    Returns
    -------
    (exit_date, exit_price, exit_reason)
    """
    # Build the full sorted price series up to max_hold after entry
    all_dates = sorted(prices.keys())
    if not all_dates:
        return None, None, f"max_hold_{max_hold}d"

    try:
        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
    except ValueError:
        return None, None, f"max_hold_{max_hold}d"

    max_date_str = (entry_dt + timedelta(days=max_hold)).strftime("%Y-%m-%d")

    # We need prices before entry to seed the SMA. Collect up to 50 prices
    # before entry_date, then prices after entry_date.
    pre_entry_prices = []
    post_entry_dates = []
    for d in all_dates:
        if d <= entry_date:
            pre_entry_prices.append(prices[d])
        elif d <= max_date_str:
            post_entry_dates.append(d)

    # Rolling window: last 50 close prices
    window = list(pre_entry_prices[-49:])  # up to 49 pre-entry prices

    days_since_entry = 0
    for d in post_entry_dates:
        close = prices[d]
        window.append(close)
        days_since_entry += 1

        # Keep window at most 50 elements
        if len(window) > 50:
            window = window[-50:]

        # Only check after 5 trading days and when we have a full 50-day window
        if days_since_entry <= 5:
            continue
        if len(window) < 50:
            continue

        sma50 = sum(window) / len(window)
        if close < sma50:
            return d, close, "sma50_break"

    # Max hold -- exit at last available price
    if post_entry_dates:
        last_d = post_entry_dates[-1]
        return last_d, prices[last_d], f"max_hold_{max_hold}d"
    return None, None, f"max_hold_{max_hold}d"


# ---------------------------------------------------------------------------
# Exit 4: Catalyst exit (event + buffer)
# ---------------------------------------------------------------------------

def catalyst_exit(prices: dict[str, float], entry_date: str, entry_price: float,
                  event_date: str, buffer_days: int = 5) -> tuple[str | None, float | None, str]:
    """
    Exit event_date + buffer_days trading days after the catalyst event.

    Trading days are approximated by counting dates that have prices in our
    price data (excludes weekends/holidays).

    Parameters
    ----------
    prices : dict mapping date strings to close prices
    entry_date : trade entry date
    entry_price : price at entry
    event_date : the catalyst event date (e.g., earnings, FDA date)
    buffer_days : number of trading days after event_date to exit

    Returns
    -------
    (exit_date, exit_price, exit_reason)
    """
    dates_after_event = sorted(d for d in prices if d > event_date)
    if not dates_after_event:
        # No prices after event -- try to find something near the event
        exit_d = _find_nearest_date(prices, event_date, max_offset_days=5)
        if exit_d:
            return exit_d, prices[exit_d], "catalyst_exit"
        return None, None, "catalyst_exit"

    # Count trading days (dates with prices)
    trading_days_counted = 0
    for d in dates_after_event:
        trading_days_counted += 1
        if trading_days_counted >= buffer_days:
            return d, prices[d], "catalyst_exit"

    # If we ran out of dates before reaching buffer_days, use last available
    last_d = dates_after_event[-1]
    return last_d, prices[last_d], "catalyst_exit"


# ---------------------------------------------------------------------------
# Exit 5: Trailing stop
# ---------------------------------------------------------------------------

def trailing_stop_exit(prices: dict[str, float], entry_date: str, entry_price: float,
                       stop_pct: float = 0.15, max_hold: int = 365) -> tuple[str | None, float | None, str]:
    """
    Track peak price from entry. Exit when price drops stop_pct below peak.

    Parameters
    ----------
    prices : dict mapping date strings to close prices
    entry_date : trade entry date
    entry_price : price at entry
    stop_pct : trailing stop percentage (e.g., 0.15 for 15% trailing stop)
    max_hold : maximum calendar days before forced exit

    Returns
    -------
    (exit_date, exit_price, exit_reason)
    """
    dates_after = _get_sorted_dates_after(prices, entry_date)
    if not dates_after:
        return None, None, f"max_hold_{max_hold}d"

    try:
        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
    except ValueError:
        return None, None, f"max_hold_{max_hold}d"

    max_date_str = (entry_dt + timedelta(days=max_hold)).strftime("%Y-%m-%d")
    peak_price = entry_price

    last_date = None
    for d in dates_after:
        if d > max_date_str:
            break
        last_date = d
        close = prices[d]

        if close > peak_price:
            peak_price = close

        # Check if price has dropped stop_pct below peak
        stop_level = peak_price * (1.0 - stop_pct)
        if close <= stop_level:
            return d, close, "trailing_stop"

    # Max hold reached
    if last_date is not None:
        return last_date, prices[last_date], f"max_hold_{max_hold}d"
    return None, None, f"max_hold_{max_hold}d"


# ---------------------------------------------------------------------------
# Exit 6: Thesis-based dispatcher
# ---------------------------------------------------------------------------

def thesis_based_exit(prices: dict[str, float], entry_date: str, entry_price: float,
                      thesis: str, **kwargs) -> tuple[str | None, float | None, str]:
    """
    Dispatch to the appropriate exit function based on the trade thesis.

    Thesis routing:
        dip_buy   -> fair_value_exit (requires pre_dip_price in kwargs)
        momentum  -> sma50_break_exit
        reversal  -> trailing_stop_exit (15% stop)
        catalyst  -> catalyst_exit (requires event_date in kwargs)
        cluster   -> fixed_hold_exit (90 days)
        value     -> trailing_stop_exit (20% stop)
        growth    -> sma50_break_exit

    Parameters
    ----------
    prices : dict mapping date strings to close prices
    entry_date : trade entry date
    entry_price : price at entry
    thesis : thesis type string
    **kwargs : additional parameters for specific exit strategies

    Returns
    -------
    (exit_date, exit_price, exit_reason)
    """
    thesis_lower = thesis.lower().strip() if thesis else ""

    if thesis_lower == "dip_buy":
        pre_dip_price = kwargs.get("pre_dip_price")
        if pre_dip_price is None:
            # Fall back: estimate pre-dip price from a 20% dip assumption
            pre_dip_price = entry_price / (1 - 0.20)
            logger.debug("dip_buy thesis: no pre_dip_price given, estimating %.2f", pre_dip_price)
        max_hold = kwargs.get("max_hold", 365)
        return fair_value_exit(prices, entry_date, entry_price, pre_dip_price, max_hold)

    elif thesis_lower == "momentum":
        max_hold = kwargs.get("max_hold", 365)
        return sma50_break_exit(prices, entry_date, entry_price, max_hold)

    elif thesis_lower == "reversal":
        max_hold = kwargs.get("max_hold", 365)
        return trailing_stop_exit(prices, entry_date, entry_price, stop_pct=0.15, max_hold=max_hold)

    elif thesis_lower == "catalyst":
        event_date = kwargs.get("event_date")
        if event_date is None:
            # No event date -- fall back to 30d fixed hold
            logger.debug("catalyst thesis: no event_date, falling back to 30d fixed hold")
            return fixed_hold_exit(prices, entry_date, entry_price, hold_days=30)
        buffer_days = kwargs.get("buffer_days", 5)
        return catalyst_exit(prices, entry_date, entry_price, event_date, buffer_days)

    elif thesis_lower == "cluster":
        hold_days = kwargs.get("hold_days", 90)
        return fixed_hold_exit(prices, entry_date, entry_price, hold_days)

    elif thesis_lower == "value":
        max_hold = kwargs.get("max_hold", 365)
        return trailing_stop_exit(prices, entry_date, entry_price, stop_pct=0.20, max_hold=max_hold)

    elif thesis_lower == "growth":
        max_hold = kwargs.get("max_hold", 365)
        return sma50_break_exit(prices, entry_date, entry_price, max_hold)

    else:
        # Unknown thesis -- default to 30d fixed hold
        logger.debug("Unknown thesis '%s', defaulting to 30d fixed hold", thesis)
        return fixed_hold_exit(prices, entry_date, entry_price, hold_days=30)


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

def main():
    """Quick smoke test with synthetic price data."""
    import sys

    # Generate synthetic prices: gradual uptrend then a drop
    base_date = datetime(2024, 1, 2)
    prices = {}
    price = 100.0
    for i in range(400):
        d = (base_date + timedelta(days=i))
        if d.weekday() >= 5:
            continue
        if i < 200:
            price *= 1.002  # uptrend
        elif i < 220:
            price *= 0.995  # pullback
        else:
            price *= 1.001  # slow recovery
        prices[d.strftime("%Y-%m-%d")] = round(price, 2)

    entry_date = "2024-01-03"
    entry_price = prices.get("2024-01-03", 100.0)

    print("=" * 60)
    print("  CW Exit Strategy Smoke Test")
    print("=" * 60)
    print(f"  Entry: {entry_date} @ ${entry_price:.2f}")
    print(f"  Price data: {len(prices)} trading days")
    print()

    tests = [
        ("Fixed 30d", lambda: fixed_hold_exit(prices, entry_date, entry_price, 30)),
        ("Fixed 90d", lambda: fixed_hold_exit(prices, entry_date, entry_price, 90)),
        ("Fair Value (target $110)", lambda: fair_value_exit(prices, entry_date, entry_price, 110.0)),
        ("SMA50 Break", lambda: sma50_break_exit(prices, entry_date, entry_price)),
        ("Trailing 15%", lambda: trailing_stop_exit(prices, entry_date, entry_price, 0.15)),
        ("Trailing 20%", lambda: trailing_stop_exit(prices, entry_date, entry_price, 0.20)),
        ("Thesis: cluster", lambda: thesis_based_exit(prices, entry_date, entry_price, "cluster")),
        ("Thesis: momentum", lambda: thesis_based_exit(prices, entry_date, entry_price, "momentum")),
        ("Thesis: value", lambda: thesis_based_exit(prices, entry_date, entry_price, "value")),
    ]

    for label, fn in tests:
        exit_d, exit_p, reason = fn()
        if exit_d and exit_p:
            ret = (exit_p - entry_price) / entry_price
            print(f"  {label:<30} Exit: {exit_d}  Price: ${exit_p:.2f}  Return: {ret:+.2%}  Reason: {reason}")
        else:
            print(f"  {label:<30} No exit found  Reason: {reason}")

    print()
    print("Smoke test complete.")


if __name__ == "__main__":
    main()
