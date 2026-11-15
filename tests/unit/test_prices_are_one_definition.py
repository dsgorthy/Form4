"""Every price Form4 shows comes from one definition, and matches checkout.

On 2026-11-14 the pricing page, the homepage and the day-10 email said Pro was
$25 a month and $250 a year (the old InsiderEdge prices) while checkout
charged $19 and $149. Each place had typed its own number. api/pricing.py is
the one definition and frontend/src/lib/pricing.ts mirrors it; nothing else
may type a price. Whether the definition matches Stripe is a runtime fact:
scripts/check_prices_match_stripe.py checks it.
"""
from __future__ import annotations

import re
from pathlib import Path

from api import pricing
from api.email_templates import pro_once_email

REPO = Path(__file__).resolve().parents[2]
TS = REPO / "frontend/src/lib/pricing.ts"
PRICING_PAGE = REPO / "frontend/src/app/pricing/page.tsx"
HOME = REPO / "frontend/src/app/page.tsx"
NAMES = ("PRO_MONTHLY_USD", "PRO_YEARLY_USD", "PRO_PLUS_MONTHLY_USD", "PRO_PLUS_YEARLY_USD")


def test_the_frontend_mirror_matches_the_definition():
    ts = dict(re.findall(r"export const (\w+_USD) = (\d+);", TS.read_text()))
    for name in NAMES:
        assert int(ts[name]) == getattr(pricing, name), f"{name}: pricing.ts says {ts[name]}, api/pricing.py says {getattr(pricing, name)}"


def test_checkout_prices_are_the_ones_derek_confirmed():
    """Changing a price is a decision, not an edit: it must move Stripe too."""
    assert (pricing.PRO_MONTHLY_USD, pricing.PRO_YEARLY_USD) == (19, 149)
    assert (pricing.PRO_PLUS_MONTHLY_USD, pricing.PRO_PLUS_YEARLY_USD) == (75, 750)


def test_the_pricing_page_types_no_price():
    src = PRICING_PAGE.read_text()
    typed = sorted(set(re.findall(r"\$\d[\d,]*", src)) - {"$0"})
    assert not typed, f"the pricing page types its own prices {typed}; import them from @/lib/pricing"
    assert '@/lib/pricing"' in src


def test_the_homepage_cards_type_no_price():
    src = HOME.read_text()
    typed = sorted(set(re.findall(r'price:\s*"(\$\d[\d,]*)"', src)) - {"$0"})
    assert not typed, f"the homepage pricing cards type {typed}; import them from @/lib/pricing"


def test_the_pro_pitch_quotes_the_real_price():
    _, html = pro_once_email()
    assert f"It is ${pricing.PRO_MONTHLY_USD} a month" in html
    assert "$25" not in html
