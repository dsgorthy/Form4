"""The prices Form4 shows. ONE definition.

Mirrored in frontend/src/lib/pricing.ts; tests/unit/test_prices_are_one_definition.py
fails the build if the two drift or a page or email types a price itself.

These must equal what checkout charges: the Stripe prices named by
STRIPE_PRO_MONTHLY_PRICE_ID etc. in the root .env. A test cannot see Stripe,
so scripts/check_prices_match_stripe.py does that at runtime.

On 2026-11-14 the pricing page, the homepage and the day-10 email all said Pro
was $25 a month and $250 a year, the old InsiderEdge prices, while checkout
charged $19 and $149. Checkout was right (Derek, 2026-11-14).
"""
PRO_MONTHLY_USD = 19
PRO_YEARLY_USD = 149
PRO_PLUS_MONTHLY_USD = 75
PRO_PLUS_YEARLY_USD = 750
