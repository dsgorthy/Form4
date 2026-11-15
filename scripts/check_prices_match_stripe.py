#!/usr/bin/env python3
"""Do the prices Form4 SHOWS equal the prices checkout CHARGES?

api/pricing.py is what every page and email shows. Checkout charges the Stripe
prices named in the root .env. A unit test cannot see Stripe, and a price can
be changed in the Stripe dashboard at any time, so this reads both and exits
non-zero on any difference. Never prints a key.

    python3 scripts/check_prices_match_stripe.py
"""
from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from api import pricing  # noqa: E402

PAIRS = [  # (.env variable naming the Stripe price, shown amount, interval)
    ("STRIPE_PRO_MONTHLY_PRICE_ID", pricing.PRO_MONTHLY_USD, "month"),
    ("STRIPE_PRO_YEARLY_PRICE_ID", pricing.PRO_YEARLY_USD, "year"),
    ("STRIPE_PRO_PLUS_MONTHLY_PRICE_ID", pricing.PRO_PLUS_MONTHLY_USD, "month"),
    ("STRIPE_PRO_PLUS_YEARLY_PRICE_ID", pricing.PRO_PLUS_YEARLY_USD, "year"),
]


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def main() -> int:
    env = load_env(REPO / ".env")
    key = env["STRIPE_SECRET_KEY"]
    bad = 0
    for var, shown, interval in PAIRS:
        pid = env.get(var)
        price = json.load(urllib.request.urlopen(urllib.request.Request(
            f"https://api.stripe.com/v1/prices/{pid}", headers={"Authorization": f"Bearer {key}"}), timeout=30))
        charged = price["unit_amount"] / 100
        charged_interval = (price.get("recurring") or {}).get("interval")
        ok = charged == shown and charged_interval == interval and price["active"]
        bad += not ok
        print(f"{'OK  ' if ok else 'DIFF'} {var:<34} shown ${shown}/{interval}  "
              f"charged ${charged:g}/{charged_interval}{'' if price['active'] else ' (INACTIVE)'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
