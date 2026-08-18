from __future__ import annotations

from datetime import datetime, timedelta

from fastapi import Depends, HTTPException

from api.auth import UserContext, get_current_user

# Field lists live in api.public_fields, which imports nothing, so the
# public/Pro split can be unit-tested without standing up FastAPI.
from api.public_fields import (  # noqa: E402
    PUBLIC_VOLUME_FIELDS,
    TRACK_RECORD_FIELDS,
)

# 90-day free window
FREE_TIER_DAYS = 90


def null_track_record_fields(item: dict) -> dict:
    """Null out Pro-only track record fields in a dict, preserving score_tier."""
    for field in TRACK_RECORD_FIELDS:
        if field in item:
            item[field] = None
    # Null nested track_record if present
    if "track_record" in item and isinstance(item["track_record"], dict):
        for field in TRACK_RECORD_FIELDS:
            if field in item["track_record"]:
                item["track_record"][field] = None
    return item


def null_items_track_records(items: list[dict]) -> list[dict]:
    """Null track record fields across a list of items."""
    return [null_track_record_fields(item) for item in items]


def get_free_cutoff_date() -> str:
    """Return the earliest date free users can access (90 days ago)."""
    return (datetime.utcnow() - timedelta(days=FREE_TIER_DAYS)).strftime("%Y-%m-%d")


def get_grace_cutoff_datetime() -> str:
    """Return the cutoff datetime for grace users (24h ago). Filings filed before this are visible."""
    return (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")


# Fields redacted on gated items (identifying data hidden, but structural fields kept)
GATED_REDACT_FIELDS = {
    "insider_name": "Insider ••••",
    "name": "Insider ••••",
    "title": "••••",
    "company": "••••",
    "filing_date": None,
    "filed_at": None,
    "price": None,
    "qty": None,
    "value": None,
    "accession": None,
    "cik": None,
    "insider_id": None,
}

# Fields preserved on gated items (needed for chart rendering & row structure)
# trade_date, trade_type, ticker, trade_id, gated, score_tier


def redact_gated_item(item: dict) -> dict:
    """Redact identifying fields on a gated item. Keeps structural fields for rendering."""
    if not item.get("gated"):
        return item
    for field, placeholder in GATED_REDACT_FIELDS.items():
        if field in item:
            item[field] = placeholder
    return item


def redact_gated_items(items: list[dict]) -> list[dict]:
    """Redact identifying fields across a list of items."""
    return [redact_gated_item(item) for item in items]


# ── where the alert line sits ───────────────────────────────────────────────
#
# The event is free. The judgment is paid.
#
# Being told that someone filed on a company you follow is a fact about the
# world, and it is what brings a visitor back, so it costs nothing. Everything
# that requires Form4 to have an opinion — a grade, a cluster, a spike, a
# convergence, a strategy entry — is the product, and so is the ability to
# filter alerts by any of it. "Tell me when an insider trades NVDA" is free;
# "tell me when an A+ insider trades NVDA" is not.
#
# min_trade_value and high_value_filing stay free deliberately: a dollar
# threshold is a fact about the filing, not a view about it, and a user
# narrowing their own alerts costs us less mail rather than more.

#: Alert event types any signed-in account may enable.
FREE_ALERT_EVENTS = ("watchlist_activity", "high_value_filing")

#: Alert event types that exist only because we computed something.
PRO_ALERT_EVENTS = (
    "cluster_formation",      # our clustering
    "activity_spike",         # our baseline and threshold
    "congress_convergence",   # our cross-source join
    "portfolio_alert",        # our strategies
)

#: Preference fields that filter alerts by our own scoring.
PRO_ALERT_FILTERS = ("min_insider_tier",)

#: Everything a free account may set, so the check is an allowlist and a new
#: field is Pro until someone deliberately says otherwise.
FREE_ALERT_FIELDS = frozenset(
    FREE_ALERT_EVENTS
    + ("email_enabled", "in_app_enabled", "email_frequency", "min_trade_value")
)

PRO_ALERT_FIELDS = frozenset(PRO_ALERT_EVENTS + PRO_ALERT_FILTERS)


def require_auth(user: UserContext = Depends(get_current_user)) -> UserContext:
    """Dependency that rejects anonymous callers, but not free accounts.

    For features that need somewhere to hang per-person state rather than a
    subscription: a watchlist, an alert, a read receipt. Anonymous callers have
    no user_id to key any of it on, so they are refused — but a free account is
    a real account and gets in.

    This exists because watchlists and alerts were `require_pro`, which meant
    the one feature that brings a visitor back was the one feature they could
    not have. Measured 2026-08-18: 47,000 entity page views a week, every one
    ending in "follow this company and hear about the next filing", against six
    users and one watchlist. Signing up started a 7-day Pro trial, so the
    promise also expired.

    Pro still buys the analytical layer — track records, grades, the
    leaderboard, congress, export. Sell the analysis, give away the alert.
    """
    if not user.user_id:
        raise HTTPException(
            status_code=401,
            detail="Sign in to follow companies and insiders.",
        )
    return user


def require_pro(user: UserContext = Depends(get_current_user)) -> UserContext:
    """Dependency that rejects non-Pro users with 403."""
    if not user.is_pro:
        raise HTTPException(
            status_code=403,
            detail="Pro subscription required. Upgrade at /pricing",
        )
    return user


def require_pro_plus(user: UserContext = Depends(get_current_user)) -> UserContext:
    """Dependency that rejects non-Pro+ users with 403.

    Pro+ is a superset of Pro — includes research tools (screener,
    leaderboard, congress, export, API keys). Tier stored as
    publicMetadata.tier = "pro_plus" in Clerk. Pro users who aren't
    Pro+ get a 403 pointing them to the upgrade path.

    TODO: Wire to Stripe product/price in Phase 4.
    """
    if not user.is_pro_plus and not user.is_admin:
        raise HTTPException(
            status_code=403,
            detail="Pro+ subscription required for research tools. Upgrade at /pricing",
        )
    return user
