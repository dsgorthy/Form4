#!/usr/bin/env python3
"""
Seed Alpaca paper accounts with open positions from backfill simulation.

Places market orders for all positions that the backfill says should be open.
Run AFTER funding accounts to theoretical equity in the Alpaca dashboard.

Must be run during market hours (9:30 AM - 4:00 PM ET).

Usage:
    python3 scripts/seed_open_positions.py              # dry run (default)
    python3 scripts/seed_open_positions.py --execute     # place real orders
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "strategies" / "insider_catalog" / "insiders.db"
PRICES_DB = ROOT / "strategies" / "insider_catalog" / "prices.db"

ACCOUNTS = {
    "quality_momentum": {
        "key": "PKMBLEJVRXC6Q6HUAOIPOZYWIG",
        "secret": "34Z19Z9HGa9oeZYuiZXhgeLGdjXJfHQ7av6QjWmyc5qG",
    },
    "reversal_dip": {
        "key": "PKUHEOVC6U53AWPPKMMJYNENVZ",
        "secret": "Cniiiz3AHqYRqnsKYXSdJopEC9UbPpqBKwsdZinJUoxR",
    },
    "tenb51_surprise": {
        "key": "PKRSN7GPVFC2REMWMAVVVWCGWP",
        "secret": "ApMTd6QjFVZMSGCJsGE3Y3dp1K1VUbLZiJnM1W8i6ynG",
    },
}

BASE_URL = "https://paper-api.alpaca.markets/v2"


def get_account(key: str, secret: str) -> dict:
    r = requests.get(f"{BASE_URL}/account", headers={
        "APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret,
    })
    r.raise_for_status()
    return r.json()


def place_order(key: str, secret: str, ticker: str, shares: int, dry_run: bool) -> dict | None:
    if dry_run:
        print(f"    [DRY RUN] Would buy {shares} sh {ticker}")
        return None

    r = requests.post(f"{BASE_URL}/orders", headers={
        "APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret,
        "Content-Type": "application/json",
    }, json={
        "symbol": ticker,
        "qty": str(shares),
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
    })

    if r.status_code == 200:
        order = r.json()
        print(f"    ORDER {ticker}: {shares} sh — {order['status']} (id={order['id'][:8]})")
        return order
    else:
        print(f"    FAILED {ticker}: {r.status_code} {r.text[:200]}")
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Actually place orders (default: dry run)")
    parser.add_argument("--strategy", help="Only seed this strategy")
    args = parser.parse_args()

    dry_run = not args.execute

    conn = sqlite3.connect(str(DB_PATH))
    prices = sqlite3.connect(str(PRICES_DB))

    if dry_run:
        print("=== DRY RUN (add --execute to place orders) ===\n")

    for strat, creds in ACCOUNTS.items():
        if args.strategy and args.strategy != strat:
            continue

        # Get open positions from backfill
        opens = conn.execute("""
            SELECT ticker, shares, entry_price, dollar_amount
            FROM strategy_portfolio
            WHERE strategy = ? AND status = 'open' AND execution_source = 'backtest'
            ORDER BY entry_date
        """, (strat,)).fetchall()

        if not opens:
            print(f"{strat}: no open positions to seed")
            continue

        # Check account
        try:
            acct = get_account(creds["key"], creds["secret"])
            cash = float(acct["cash"])
            equity = float(acct["equity"])
        except Exception as e:
            print(f"{strat}: account error — {e}")
            continue

        print(f"=== {strat.upper()} ===")
        print(f"  Account equity: ${equity:,.0f}  cash: ${cash:,.0f}")

        total_cost = 0
        for ticker, shares, entry_price, dollar_amt in opens:
            cur = prices.execute(
                "SELECT close FROM daily_prices WHERE ticker=? ORDER BY date DESC LIMIT 1",
                (ticker,),
            ).fetchone()
            est_price = cur[0] if cur else entry_price
            est_cost = shares * est_price
            total_cost += est_cost

        if total_cost > cash:
            print(f"  WARNING: positions cost ~${total_cost:,.0f} but only ${cash:,.0f} cash available")
            print(f"  Fund account to at least ${total_cost + 1000:,.0f} before running with --execute")
            if not dry_run:
                print(f"  SKIPPING {strat} — insufficient funds")
                continue

        for ticker, shares, entry_price, dollar_amt in opens:
            if shares <= 0:
                continue
            result = place_order(creds["key"], creds["secret"], ticker, shares, dry_run)
            if not dry_run and result:
                time.sleep(0.5)  # rate limit

        print()

    conn.close()
    prices.close()

    if dry_run:
        print("Run with --execute to place real orders during market hours.")


if __name__ == "__main__":
    main()
