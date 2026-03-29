"""Rate limiting for the Form4 API.

Tiers:
    Anonymous / free:       30 req/min (by IP)
    Authenticated (pro):   120 req/min (by user ID)
    API key:               300 req/min (by user ID)
    Expensive endpoints:    10 req/min (export, leaderboard full-scan, search)
"""
from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.requests import Request

# Tier-specific rate strings
ANON_RATE = "30/minute"
AUTH_RATE = "120/minute"
API_KEY_RATE = "300/minute"
EXPENSIVE_RATE = "10/minute"


def _key_func(request: Request) -> str:
    """Rate limit key: user_id if authenticated, else IP address."""
    user_id = getattr(request.state, "rate_limit_key", None)
    if user_id:
        return user_id
    return get_remote_address(request)


def get_dynamic_rate(request: Request) -> str:
    """Return the rate limit string based on the request's auth context."""
    tier = getattr(request.state, "rate_limit_tier", "anon")
    if tier == "api_key":
        return API_KEY_RATE
    elif tier == "auth":
        return AUTH_RATE
    return ANON_RATE


limiter = Limiter(key_func=_key_func, default_limits=[get_dynamic_rate])
