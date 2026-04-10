from __future__ import annotations

import os

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from api.auth import UserContext, get_current_user
from api.rate_limit import limiter
from config.database import get_db

from api.notifications_db import init_db as init_notifications_db
from api.routers import (
    api_keys,
    clusters,
    companies,
    congress,
    dashboard,
    data_quality,
    paper_trading,
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

ERROR_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    422: "validation_error",
    429: "rate_limited",
    500: "server_error",
}

app = FastAPI(
    title="Form4 API",
    description=(
        "Insider trading intelligence from SEC Form 4 filings.\n\n"
        "## Authentication\n"
        "Pass a Clerk JWT in `Authorization: Bearer <token>` "
        "or an API key in `X-Api-Key: ie_...` header.\n\n"
        "## Rate Limits\n"
        "| Tier | Limit |\n"
        "|------|-------|\n"
        "| Anonymous | 30 req/min |\n"
        "| Authenticated (Pro) | 120 req/min |\n"
        "| API Key | 300 req/min |\n\n"
        "## Error Responses\n"
        "All errors return a standard envelope:\n"
        "```json\n"
        '{"error": {"code": "not_found", "message": "Filing not found"}}\n'
        "```\n"
        "Codes: `bad_request` (400), `unauthorized` (401), `forbidden` (403), "
        "`not_found` (404), `rate_limited` (429), `server_error` (500)"
    ),
    version="0.2.0",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    openapi_url="/api/v1/openapi.json",
    openapi_tags=[
        {"name": "filings", "description": "SEC Form 4 insider trading filings"},
        {"name": "insiders", "description": "Insider profiles, track records, and scores"},
        {"name": "companies", "description": "Company filings and insider activity"},
        {"name": "clusters", "description": "Insider cluster detection (multiple insiders buying together)"},
        {"name": "dashboard", "description": "Market-wide insider activity summaries"},
        {"name": "portfolio", "description": "Strategy portfolios, backtests, and performance"},
        {"name": "signals", "description": "Trade signals and scoring breakdowns"},
        {"name": "congress", "description": "Congressional trading data"},
        {"name": "leaderboard", "description": "Top insiders and companies ranked by performance"},
        {"name": "search", "description": "Full-text search across insiders and companies"},
        {"name": "notifications", "description": "User notifications and watchlist management"},
        {"name": "export", "description": "CSV/JSON data exports"},
        {"name": "api-keys", "description": "API key management (create, list, revoke)"},
    ],
)


# ── Error envelope: all errors return {error: {code, message}} ───────────

@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException):
    """Wrap all HTTP errors in a standard envelope."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": ERROR_CODES.get(exc.status_code, "error"),
                "message": exc.detail or "An error occurred",
            }
        },
    )


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Rate limit errors in the standard envelope."""
    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "code": "rate_limited",
                "message": f"Rate limit exceeded: {exc.detail}",
            }
        },
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception):
    """Catch-all for unhandled exceptions."""
    import logging
    logging.getLogger("api").exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "server_error",
                "message": "Internal server error",
            }
        },
    )


# Rate limiting
app.state.limiter = limiter
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
app.include_router(paper_trading.router)
app.include_router(portfolio.router)
app.include_router(private_companies.router)
app.include_router(sitemap.router)


@app.on_event("startup")
def startup() -> None:
    init_notifications_db()


@app.get("/api/v1/health")
@limiter.exempt
def health() -> dict:
    """Shallow health check — proves the API process is alive and can hit the DB.

    Returns 200 only if a SELECT 1 against PG succeeds. This is what uptime
    monitors and load balancers should hit.
    """
    from time import perf_counter
    from fastapi import HTTPException
    t0 = perf_counter()
    try:
        with get_db(readonly=True) as conn:
            row = conn.execute("SELECT 1 AS ok").fetchone()
            if not row or row["ok"] != 1:
                raise HTTPException(status_code=503, detail="DB returned unexpected result")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unreachable: {type(e).__name__}: {e}")
    return {
        "status": "ok",
        "db_roundtrip_ms": round((perf_counter() - t0) * 1000, 1),
    }


@app.get("/api/v1/health/deep")
@limiter.exempt
def health_deep() -> dict:
    """Deep health check — proves we can read real data from the trades table.

    Slower than /health (real query) but catches issues that SELECT 1 misses,
    like search_path problems, table corruption, or schema drift.
    """
    from time import perf_counter
    from fastapi import HTTPException
    t0 = perf_counter()
    try:
        with get_db(readonly=True) as conn:
            row = conn.execute("SELECT MAX(filing_date) AS latest FROM trades").fetchone()
            latest = row["latest"] if row else None
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB query failed: {type(e).__name__}: {e}")
    return {
        "status": "ok",
        "latest_filing": latest,
        "query_ms": round((perf_counter() - t0) * 1000, 1),
    }


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
