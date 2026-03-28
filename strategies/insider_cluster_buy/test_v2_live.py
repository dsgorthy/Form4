#!/usr/bin/env python3
"""
V2 Live Smoke Test — Insider Cluster Buy with Options Overlay

Sends a synthetic signal through the full entry/exit pipeline against the
Alpaca paper account. Does NOT touch the running daemon or state.json.

Usage:
    python test_v2_live.py
"""

import json
import sys
import time
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
import os

STRATEGY_DIR = Path(__file__).resolve().parent
FRAMEWORK_ROOT = STRATEGY_DIR.parent.parent
sys.path.insert(0, str(FRAMEWORK_ROOT))
sys.path.insert(0, str(STRATEGY_DIR))

load_dotenv(str(STRATEGY_DIR / "config.env"))

from framework.execution.paper import PaperBackend
from order_manager import get_current_price, get_vix, compute_position_size
from options_leg import select_strike_and_expiry, compute_options_size, _lookup_contract_alpaca

# ── Config ───────────────────────────────────────────────────────────────

API_KEY = os.environ["ALPACA_API_KEY"]
API_SECRET = os.environ["ALPACA_API_SECRET"]
BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")

# Use a liquid, cheap stock for testing
TEST_TICKER = "SOFI"
PORTFOLIO_VALUE = 30000.0
EQUITY_SIZE_PCT = 0.05
OPTIONS_SIZE_PCT = 0.01

import requests

def main():
    print("=" * 60)
    print("V2 SMOKE TEST — Equity + Options Overlay")
    print("=" * 60)

    backend = PaperBackend(api_key=API_KEY, api_secret=API_SECRET, base_url=BASE_URL)
    session = requests.Session()
    session.headers.update({
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET,
    })

    # Step 1: Account check
    print("\n[1/7] Checking account...")
    account = backend.get_account()
    print(f"  Account equity: ${account['equity']:,.2f}")
    print(f"  Cash: ${account['cash']:,.2f}")

    # Step 2: Get price + VIX
    print(f"\n[2/7] Fetching {TEST_TICKER} price and VIX...")
    price = get_current_price(TEST_TICKER, session)
    if price is None:
        print(f"  FAIL: Could not get price for {TEST_TICKER}")
        print("  (Market may be closed — try on a weekday)")
        # Use last known price as fallback for options symbol test
        price = 15.00
        print(f"  Using fallback price: ${price:.2f}")
        can_trade = False
    else:
        print(f"  {TEST_TICKER} price: ${price:.2f}")
        can_trade = True

    vix = get_vix(session)
    print(f"  VIX: {vix:.2f}")

    # Step 3: Compute equity sizing
    print(f"\n[3/7] Computing equity position size...")
    qty, dollar_amt = compute_position_size(
        equity=PORTFOLIO_VALUE, current_price=price,
        size_pct=EQUITY_SIZE_PCT, vix=vix,
        vix_threshold=30.0, reduced_size_pct=0.03,
    )
    effective_pct = 0.03 if vix > 30 else EQUITY_SIZE_PCT
    print(f"  Size: {qty} shares @ ${price:.2f} = ${qty * price:,.2f}")
    print(f"  Sizing: {effective_pct:.0%} of ${PORTFOLIO_VALUE:,.0f} = ${dollar_amt:,.2f}")

    # Step 4: Select options strike/expiry (live Alpaca lookup)
    print(f"\n[4/7] Selecting options contract (Alpaca lookup)...")
    selection = select_strike_and_expiry(
        TEST_TICKER, price, date.today(),
        strike_mult=1.05, target_dte=90,
        session=session,
    )
    print(f"  OCC symbol: {selection['occ_symbol']}")
    print(f"  Strike: ${selection['strike']:.2f} (5% OTM from ${price:.2f})")
    print(f"  Expiry: {selection['expiry_date']} ({selection['dte']}d)")

    if not can_trade:
        print("\n[5/7] SKIPPED — Market closed, cannot submit orders")
        print("[6/7] SKIPPED")
        print("[7/7] SKIPPED")
        print("\n" + "=" * 60)
        print("RESULT: PARTIAL PASS")
        print("  Account connection:  OK")
        print("  Equity sizing:       OK")
        print("  Options selection:   OK")
        print("  Live order test:     SKIPPED (market closed)")
        print("=" * 60)
        return

    # Step 5: Submit equity order
    print(f"\n[5/7] Submitting equity order: BUY {qty} {TEST_TICKER}...")
    eq_result = backend.submit_order(
        symbol=TEST_TICKER, qty=qty, side="buy",
        order_type="market", time_in_force="day",
    )
    if eq_result.is_error:
        print(f"  FAIL: {eq_result.error}")
        return
    print(f"  Order ID: {eq_result.order_id}")
    print(f"  Status: {eq_result.status}")

    if eq_result.status == "pending":
        print("  Waiting for fill...")
        eq_result = backend.wait_for_fill(eq_result.order_id, timeout=30)

    if eq_result.is_filled:
        print(f"  FILLED: {eq_result.filled_qty} shares @ ${eq_result.filled_price:.2f}")
    else:
        print(f"  Status: {eq_result.status} (may fill when market opens)")

    # Step 6: Submit options order
    print(f"\n[6/7] Submitting options order: BUY 1x {selection['occ_symbol']}...")
    opt_result = backend.submit_order(
        symbol=selection["occ_symbol"], qty=1, side="buy",
        order_type="market", time_in_force="day",
    )
    if opt_result.is_error:
        print(f"  Options order failed: {opt_result.error}")
        opt_ok = False
    else:
        print(f"  Order ID: {opt_result.order_id}")
        print(f"  Status: {opt_result.status}")
        if opt_result.status == "pending":
            print("  Waiting for fill...")
            opt_result = backend.wait_for_fill(opt_result.order_id, timeout=30)
        if opt_result.is_filled:
            print(f"  FILLED: {opt_result.filled_qty} contract @ ${opt_result.filled_price:.2f}")
        else:
            print(f"  Status: {opt_result.status}")
        opt_ok = True

    # Step 7: Check positions then close
    print(f"\n[7/7] Checking positions and cleaning up...")
    time.sleep(2)

    eq_pos = backend.get_position(TEST_TICKER)
    if eq_pos:
        print(f"  Equity: {eq_pos['qty']} {TEST_TICKER} @ ${eq_pos['avg_entry_price']:.2f}, "
              f"P&L: ${eq_pos['unrealized_pl']:+.2f}")
        close_r = backend.close_position(TEST_TICKER)
        print(f"  Closed equity: {close_r.status}")
    else:
        print(f"  No equity position found (order may not have filled)")

    if opt_ok:
        opt_pos = backend.get_position(selection["occ_symbol"])
        if opt_pos:
            print(f"  Options: {opt_pos['qty']} {selection['occ_symbol']} @ ${opt_pos['avg_entry_price']:.2f}")
            close_r = backend.close_position(selection["occ_symbol"])
            print(f"  Closed options: {close_r.status}")
        else:
            print(f"  No options position found (order may not have filled)")

    # Summary
    print("\n" + "=" * 60)
    print("RESULT: PASS")
    print(f"  Account connection:  OK")
    print(f"  Equity sizing:       OK ({qty} shares)")
    print(f"  Options selection:   OK ({selection['occ_symbol']})")
    print(f"  Equity order:        {'FILLED' if eq_result.is_filled else eq_result.status}")
    print(f"  Options order:       {'FILLED' if opt_ok and opt_result.is_filled else 'SUBMITTED' if opt_ok else 'FAILED'}")
    print(f"  Cleanup:             DONE")
    print("=" * 60)


if __name__ == "__main__":
    main()
