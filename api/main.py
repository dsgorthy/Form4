from __future__ import annotations

import os

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from api.auth import UserContext, get_current_user
from api.rate_limit import limiter

from api.notifications_db import init_db as init_notifications_db
from api.routers import (
    api_keys,
    clusters,
    companies,
    congress,
    dashboard,
    data_quality,
    portfolio,
    export,
    filings,
    insiders,
    leaderboard,
    notifications,
    onboarding,
    private_companies,
    search,
    signals,
    sitemap,
    webhooks,
)

app = FastAPI(
    title="Form4 API",
    description="Insider trading intelligence from SEC Form 4 filings",
    version="0.2.0",
)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Set rate limit key and tier based on auth context before route handlers run."""
    # Default to anonymous
    request.state.rate_limit_key = None
    request.state.rate_limit_tier = "anon"

    # Check for API key first
    api_key = request.headers.get("x-api-key")
    if api_key and api_key.startswith("ie_"):
        # Extract user_id from key format ie_{user_id}_{hex}
        last_underscore = api_key.rfind("_")
        if last_underscore > 3:
            request.state.rate_limit_key = f"apikey:{api_key[3:last_underscore]}"
            request.state.rate_limit_tier = "api_key"
    elif request.headers.get("authorization", "").startswith("Bearer "):
        # JWT auth — verify signature via JWKS before trusting claims
        # (prevents forged JWTs from getting authenticated rate limits)
        try:
            from api.auth import decode_clerk_jwt
            token = request.headers["authorization"][7:]
            claims = decode_clerk_jwt(token)
            user_id = claims.get("sub", "")
            if user_id:
                request.state.rate_limit_key = f"user:{user_id}"
                request.state.rate_limit_tier = "auth"
        except Exception:
            # Invalid/expired/forged JWT — fall back to anonymous rate limits
            pass

    response = await call_next(request)
    return response

app.include_router(clusters.router)
app.include_router(congress.router)
app.include_router(data_quality.router)
app.include_router(dashboard.router)
app.include_router(filings.router)
app.include_router(insiders.router)
app.include_router(companies.router)
app.include_router(leaderboard.router)
app.include_router(search.router)
app.include_router(signals.router)
app.include_router(webhooks.router)
app.include_router(api_keys.router)
app.include_router(export.router)
app.include_router(notifications.router)
app.include_router(onboarding.router)
app.include_router(portfolio.router)
app.include_router(private_companies.router)
app.include_router(sitemap.router)


@app.on_event("startup")
def startup() -> None:
    init_notifications_db()


@app.get("/api/v1/health")
@limiter.exempt
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/v1/portfolio/runner-status")
def portfolio_runner_status(strategy: str = "form4_insider", user: UserContext = Depends(get_current_user)) -> dict:
    """Check portfolio runner health via heartbeat file.
    Supports multiple strategies with per-strategy heartbeat files."""
    import json as _json
    from pathlib import Path
    from datetime import datetime

    # Strategy -> heartbeat file mapping
    heartbeat_paths = {
        "form4_insider": [
            Path("/data/runner/portfolio_runner_heartbeat.json"),
            Path("/Users/openclaw/trading-framework/pipelines/data/portfolio_runner_heartbeat.json"),
        ],
        "cw_reversal": [
            Path("/data/runner/cw_reversal_heartbeat.json"),
            Path("/Users/openclaw/trading-framework/strategies/cw_strategies/data/cw_reversal_heartbeat.json"),
        ],
        "cw_composite": [
            Path("/data/runner/cw_composite_heartbeat.json"),
            Path("/Users/openclaw/trading-framework/strategies/cw_strategies/data/cw_composite_heartbeat.json"),
        ],
    }

    candidates = heartbeat_paths.get(strategy, heartbeat_paths["form4_insider"])
    heartbeat_path = None
    for p in candidates:
        if p.exists():
            heartbeat_path = p
            break

    if heartbeat_path is None:
        # Check if it's a weekend — daemons sleep on weekends, that's normal
        now = datetime.utcnow()
        if now.weekday() >= 5:
            return {"status": "weekend", "healthy": True, "detail": "Daemons sleep on weekends"}
        return {"status": "unknown", "detail": "No heartbeat file found"}

    try:
        beat = _json.loads(heartbeat_path.read_text())
        ts = beat.get("timestamp", "")
        if ts:
            age_sec = (datetime.utcnow() - datetime.fromisoformat(ts)).total_seconds()
            beat["age_seconds"] = int(age_sec)
            beat["healthy"] = age_sec < 3600  # stale if > 1 hour
        return beat
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/api/v1/me")
async def me(user: "UserContext" = Depends(get_current_user)) -> dict:
    """Return current user status including trial info."""
    from api.auth import UserContext
    return {
        "user_id": user.user_id,
        "tier": user.tier,
        "is_pro": user.is_pro,
        "trial_days_left": user.trial_days_left,
        "grace_days_left": user.grace_days_left,
    }
