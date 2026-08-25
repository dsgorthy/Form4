from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# THE REPO-ROOT .env IS AUTHORITATIVE. api/.env only fills gaps.
#
# This was the other way round until 2026-08-24, and it caused a total outage
# of the alert product that ran undetected for months.
#
# docker-compose gives the API container `env_file: .env` (the root one) and
# the image never copies api/.env, so precedence is irrelevant in production
# containers. It only bites code run on the HOST -- which is every pipeline
# and every script.
#
# On Studio, api/.env was a leftover from March holding a complete set of
# sk_test_ Clerk and Stripe keys. All seven of its keys also exist in the root
# .env with live values, so it added nothing and shadowed everything.
# notification_scanner imports api.email -> api.config, so it resolved
# production user IDs against Clerk's TEST instance: every lookup 404d,
# _account_exists returned False, and _get_subscribed_users returned ZERO
# subscribers for all six event types. 6,887 notifications were created and
# not one was ever emailed.
#
# A 404 is indistinguishable from a deleted account, which is why nothing
# caught it -- the scanner even cached live subscribers as __deleted__.
#
# Root-first means a stale local file can no longer shadow deployed
# credentials. It can still SUPPLY a key the root .env omits, which is the
# legitimate use. notification_scanner additionally refuses to send mail on a
# test key, as defence in depth.
_api_env = Path(__file__).resolve().parent / ".env"
_root_env = Path(__file__).resolve().parent.parent / ".env"

if _root_env.exists():
    load_dotenv(_root_env)
if _api_env.exists():
    load_dotenv(_api_env, override=False)

# Clerk
CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY", "")
CLERK_JWKS_URL = os.getenv("CLERK_JWKS_URL", "")

# Stripe
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRO_MONTHLY_PRICE_ID = os.getenv("STRIPE_PRO_MONTHLY_PRICE_ID", "")
STRIPE_PRO_YEARLY_PRICE_ID = os.getenv("STRIPE_PRO_YEARLY_PRICE_ID", "")
STRIPE_API_MONTHLY_PRICE_ID = os.getenv("STRIPE_API_MONTHLY_PRICE_ID", "")
STRIPE_PRO_PLUS_MONTHLY_PRICE_ID = os.getenv("STRIPE_PRO_PLUS_MONTHLY_PRICE_ID", "")
STRIPE_PRO_PLUS_YEARLY_PRICE_ID = os.getenv("STRIPE_PRO_PLUS_YEARLY_PRICE_ID", "")

PRO_PRICE_IDS = {STRIPE_PRO_MONTHLY_PRICE_ID, STRIPE_PRO_YEARLY_PRICE_ID} - {""}
PRO_PLUS_PRICE_IDS = {STRIPE_PRO_PLUS_MONTHLY_PRICE_ID, STRIPE_PRO_PLUS_YEARLY_PRICE_ID} - {""}
API_PRICE_IDS = {STRIPE_API_MONTHLY_PRICE_ID} - {""}

# Resend (email)
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
