from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Optional

import jwt
from jwt import PyJWKClient
from fastapi import Header, Request

from api.config import CLERK_JWKS_URL

# Comma-separated Clerk user IDs that can access /admin/* endpoints
_ADMIN_USER_IDS = set(
    uid.strip() for uid in os.environ.get("ADMIN_USER_IDS", "").split(",") if uid.strip()
)

logger = logging.getLogger(__name__)

_jwks_client: Optional[PyJWKClient] = None


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        _jwks_client = PyJWKClient(CLERK_JWKS_URL, cache_keys=True)
    return _jwks_client


TRIAL_DAYS = 7
GRACE_DAYS = 7  # 7 more days after trial ends


@dataclass
class UserContext:
    user_id: Optional[str] = None
    tier: str = "free"  # "free" | "pro" | "trial" | "grace"
    api_access: bool = False
    trial_days_left: int = 0
    grace_days_left: int = 0

    @property
    def is_pro(self) -> bool:
        return self.tier in ("pro", "pro_plus", "trial")

    @property
    def is_pro_plus(self) -> bool:
        return self.tier == "pro_plus"

    @property
    def is_grace(self) -> bool:
        return self.tier == "grace"

    @property
    def has_full_feed(self) -> bool:
        """Pro, Pro+, trial, and grace users see the full feed (no 90-day cutoff, no gated items)."""
        return self.tier in ("pro", "pro_plus", "trial", "grace")

    @property
    def is_admin(self) -> bool:
        """True if this user's Clerk ID is in the ADMIN_USER_IDS env var."""
        return self.user_id is not None and self.user_id in _ADMIN_USER_IDS


ANONYMOUS = UserContext()

# Cache Clerk metadata for 60s to avoid hitting the API on every request
_metadata_cache: dict = {}  # {user_id: (timestamp, metadata)}
_CACHE_TTL = 60


async def _fetch_clerk_metadata(user_id: str) -> dict:
    """Fetch a user's public_metadata + created_at from Clerk, with caching."""
    import time
    now = time.time()

    cached = _metadata_cache.get(user_id)
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1]

    try:
        import httpx
        from api.config import CLERK_SECRET_KEY

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.clerk.com/v1/users/%s" % user_id,
                headers={"Authorization": "Bearer %s" % CLERK_SECRET_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                metadata = data.get("public_metadata", {})
                # Include created_at for trial computation (ms timestamp from Clerk)
                metadata["_created_at"] = data.get("created_at")
                _metadata_cache[user_id] = (now, metadata)
                return metadata
    except Exception as e:
        logger.debug("Clerk metadata fetch failed: %s", e)

    return {}


def decode_clerk_jwt(token: str) -> dict:
    """Decode and verify a Clerk JWT using JWKS."""
    client = _get_jwks_client()
    signing_key = client.get_signing_key_from_jwt(token)
    return jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256"],
        options={"verify_aud": False},
    )


async def get_current_user(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> UserContext:
    """FastAPI dependency: extract user context from JWT or API key.

    Returns anonymous/free context when no credentials are provided.
    Never raises — unauthenticated users get free tier.
    """
    # Try API key first
    if x_api_key:
        return await _resolve_api_key(x_api_key)

    # Try JWT from Authorization header
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]

    if not token:
        return ANONYMOUS

    try:
        claims = decode_clerk_jwt(token)
        user_id = claims.get("sub")

        # Check JWT claims first (works if Clerk JWT template includes metadata)
        metadata = claims.get("metadata", {}) or claims.get("public_metadata", {})
        tier = metadata.get("tier", "")
        api_access = metadata.get("api_access", False)

        # If no tier in JWT claims, fetch from Clerk API
        if not tier and user_id:
            metadata = await _fetch_clerk_metadata(user_id)
            tier = metadata.get("tier", "free")
            api_access = metadata.get("api_access", False)

        # If user is already Pro (paid), skip trial logic
        if tier == "pro":
            return UserContext(user_id=user_id, tier="pro", api_access=api_access)

        # Check for free trial / grace period based on account creation date
        created_at = metadata.get("_created_at")
        if created_at:
            import time as _time
            # Clerk returns created_at as milliseconds since epoch
            created_ts = created_at / 1000 if created_at > 1e12 else created_at
            age_days = (_time.time() - created_ts) / 86400
            if age_days <= TRIAL_DAYS:
                trial_days_left = max(1, int(TRIAL_DAYS - age_days + 0.5))
                return UserContext(
                    user_id=user_id, tier="trial",
                    api_access=api_access, trial_days_left=trial_days_left,
                )
            elif age_days <= TRIAL_DAYS + GRACE_DAYS:
                grace_days_left = max(1, int(TRIAL_DAYS + GRACE_DAYS - age_days + 0.5))
                return UserContext(
                    user_id=user_id, tier="grace",
                    api_access=api_access, grace_days_left=grace_days_left,
                )

        return UserContext(user_id=user_id, tier=tier or "free", api_access=api_access)
    except Exception as e:
        logger.debug("JWT decode failed: %s", e)
        return ANONYMOUS


async def _resolve_api_key(api_key: str) -> UserContext:
    """Validate an API key against Clerk user metadata."""
    try:
        import httpx
        from api.config import CLERK_SECRET_KEY

        key_hash = hashlib.sha256(api_key.encode()).hexdigest()

        # API keys are formatted as ie_<user_id>_<32-char hex>
        # Clerk user IDs contain underscores, so split from the right
        if not api_key.startswith("ie_"):
            return ANONYMOUS

        # Last 32 chars after the final underscore are the random hex
        last_underscore = api_key.rfind("_")
        if last_underscore <= 3:
            return ANONYMOUS

        user_id = api_key[3:last_underscore]

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.clerk.com/v1/users/%s" % user_id,
                headers={"Authorization": "Bearer %s" % CLERK_SECRET_KEY},
            )
            if resp.status_code != 200:
                return ANONYMOUS

            user_data = resp.json()
            private_meta = user_data.get("private_metadata", {})
            public_meta = user_data.get("public_metadata", {})

            # Check key hash against both new (api_keys) and legacy (api_key_hashes) formats
            valid = False
            for k in private_meta.get("api_keys", []):
                if k.get("hash") == key_hash:
                    valid = True
                    break
            if not valid:
                # Fallback to legacy flat list
                if key_hash not in private_meta.get("api_key_hashes", []):
                    return ANONYMOUS

            if not public_meta.get("api_access", False):
                return ANONYMOUS

            return UserContext(
                user_id=user_id,
                tier=public_meta.get("tier", "free"),
                api_access=True,
            )
    except Exception as e:
        logger.debug("API key validation failed: %s", e)
        return ANONYMOUS
