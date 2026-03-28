from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from api/ dir, then fall back to project root
_api_env = Path(__file__).resolve().parent / ".env"
_root_env = Path(__file__).resolve().parent.parent / ".env"

if _api_env.exists():
    load_dotenv(_api_env)
if _root_env.exists():
    load_dotenv(_root_env, override=False)

# Clerk
CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY", "")
CLERK_JWKS_URL = os.getenv("CLERK_JWKS_URL", "")

# Stripe
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRO_MONTHLY_PRICE_ID = os.getenv("STRIPE_PRO_MONTHLY_PRICE_ID", "")
STRIPE_PRO_YEARLY_PRICE_ID = os.getenv("STRIPE_PRO_YEARLY_PRICE_ID", "")
STRIPE_API_MONTHLY_PRICE_ID = os.getenv("STRIPE_API_MONTHLY_PRICE_ID", "")

PRO_PRICE_IDS = {STRIPE_PRO_MONTHLY_PRICE_ID, STRIPE_PRO_YEARLY_PRICE_ID} - {""}
API_PRICE_IDS = {STRIPE_API_MONTHLY_PRICE_ID} - {""}

# Resend (email)
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
