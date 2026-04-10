#!/usr/bin/env python3
"""Re-seed open positions on the NEW quality_momentum and tenb51_surprise
Alpaca paper accounts after they were re-created and funded to theoretical
equity on 2026-04-10.

These positions exist in strategy_portfolio (status='open') but the new
Alpaca accounts are empty because they're literally new accounts. We need to
re-buy the shares so the runner picks up where it left off.

Run this at or just after market open on Monday. It waits for the market to
be open, submits market buys, waits for fills, and reports the final state.

Usage:
    python3 scripts/seed_rebalanced_positions.py              # submit orders
    python3 scripts/seed_rebalanced_positions.py --dry-run    # show without submitting
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import requests

BASE_URL = "https://paper-api.alpaca.markets/v2"

# Open positions to re-establish. Shares match strategy_portfolio.
STRATEGIES = [
    {
        "name": "quality_momentum",
        "api_key": "PK744YJCVAFDXSHRLY5C7EFI46",
        "api_secret": "CS7cUVF6Cz3b6GJhbffNtk4qpncJL3DqwFt7MWUmY42M",
        "positions": [
            {"ticker": "BW", "shares": 1961},
            {"ticker": "KOS", "shares": 12814},
        ],
    },
    {
        "name": "tenb51_surprise",
        "api_key": "PKXCDOBGX6UFBH7ZBIZUHXES7D",
        "api_secret": "NTFW3vSerhM6eBfWNzSvzRUzE8GkqPZ1GuaTFfJX81W",
        "positions": [
            {"ticker": "KOS", "shares": 12618},
        ],
    },
    # reversal_dip is unchanged (no positions, old account still in use)
]


def headers(strat: dict) -> dict:
    return {
        "APCA-API-KEY-ID": strat["api_key"],
        "APCA-API-SECRET-KEY": strat["api_secret"],
    }


def is_market_open(strat: dict) -> bool:
    r = requests.get(f"{BASE_URL}/clock", headers=headers(strat), timeout=10)
    r.raise_for_status()
    return r.json()["is_open"]


def wait_for_market_open(strat: dict) -> None:
    clock = requests.get(f"{BASE_URL}/clock", headers=headers(strat), timeout=10).json()
    if clock["is_open"]:
        print("Market is OPEN.")
        return
    print(f"Market CLOSED. Next open: {clock.get('next_open')}")
    print("Waiting for market open...", flush=True)
    while True:
        time.sleep(30)
        try:
            clock = requests.get(f"{BASE_URL}/clock", headers=headers(strat), timeout=10).json()
            if clock["is_open"]:
                print("Market is OPEN. Proceeding.")
                return
        except Exception as e:
            print(f"  Clock check failed: {e}", flush=True)


def submit_order(strat: dict, ticker: str, shares: int, dry_run: bool) -> dict | None:
    if dry_run:
        print(f"    [DRY RUN] buy {shares} {ticker}")
        return None
    r = requests.post(
        f"{BASE_URL}/orders",
        headers=headers(strat),
        json={
            "symbol": ticker,
            "qty": str(shares),
            "side": "buy",
            "type": "market",
            "time_in_force": "day",
        },
        timeout=15,
    )
    if r.status_code != 200:
        print(f"    ERROR: {ticker} {r.status_code} {r.text}")
        return None
    order = r.json()
    print(f"    submitted: {ticker} x{shares} -> {order['id'][:8]}")
    return order


def wait_for_fill(strat: dict, order_id: str, timeout_s: int = 60) -> dict | None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = requests.get(f"{BASE_URL}/orders/{order_id}", headers=headers(strat), timeout=10)
        if r.status_code != 200:
            time.sleep(2)
            continue
        order = r.json()
        if order["status"] == "filled":
            return order
        if order["status"] in ("canceled", "expired", "rejected"):
            print(f"    order {order_id[:8]} {order['status']}")
            return order
        time.sleep(2)
    print(f"    order {order_id[:8]} timed out")
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run:
        wait_for_market_open(STRATEGIES[0])
        print("Waiting 30s for opening liquidity...")
        time.sleep(30)

    total_orders = 0
    total_filled = 0
    total_skipped = 0

    for strat in STRATEGIES:
        name = strat["name"]
        print(f"\n{'='*60}")
        print(f"STRATEGY: {name}")
        print(f"{'='*60}")

        # Idempotency check: if the account already holds ALL the target
        # positions (by ticker), skip this strategy. Safe to re-run any time.
        existing_tickers: set = set()
        if not args.dry_run:
            acct = requests.get(f"{BASE_URL}/account", headers=headers(strat), timeout=10).json()
            print(f"  before: equity=${float(acct['equity']):,.2f}  cash=${float(acct['cash']):,.2f}")
            pos_list = requests.get(f"{BASE_URL}/positions", headers=headers(strat), timeout=10).json()
            existing_tickers = {p["symbol"] for p in pos_list}

            target_tickers = {p["ticker"] for p in strat["positions"]}
            if target_tickers.issubset(existing_tickers):
                print(f"  SKIP: all target positions already present ({sorted(target_tickers)})")
                total_skipped += len(strat["positions"])
                continue

        orders = []
        for pos in strat["positions"]:
            if pos["ticker"] in existing_tickers:
                print(f"    SKIP {pos['ticker']}: already held")
                total_skipped += 1
                continue
            o = submit_order(strat, pos["ticker"], pos["shares"], args.dry_run)
            if o:
                orders.append(o)
            total_orders += 1

        if orders:
            print(f"\n  waiting for {len(orders)} fills...")
            for o in orders:
                f = wait_for_fill(strat, o["id"])
                if f and f["status"] == "filled":
                    print(f"    FILLED: {f['symbol']} x{int(f['filled_qty'])} @ ${float(f['filled_avg_price']):.2f}")
                    total_filled += 1

        if not args.dry_run:
            time.sleep(2)
            acct = requests.get(f"{BASE_URL}/account", headers=headers(strat), timeout=10).json()
            print(f"\n  after: equity=${float(acct['equity']):,.2f}  cash=${float(acct['cash']):,.2f}")

    print(f"\n{total_orders} orders submitted, {total_filled} filled, {total_skipped} skipped")


if __name__ == "__main__":
    main()
