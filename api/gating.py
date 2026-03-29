from __future__ import annotations

from datetime import datetime, timedelta

from fastapi import Depends, HTTPException

from api.auth import UserContext, get_current_user

# Fields nulled for free-tier users. score_tier stays visible as a teaser.
TRACK_RECORD_FIELDS = [
    "score",
    "percentile",
    "buy_win_rate_7d",
    "buy_avg_return_7d",
    "buy_avg_abnormal_7d",
    "sell_win_rate_7d",
    "sell_avg_return_7d",
    "sell_avg_abnormal_7d",
    "return_7d",
    "return_30d",
    "return_90d",
    "abnormal_7d",
    "abnormal_30d",
    "abnormal_90d",
    "score_recency_weighted",
    "tier_recency",
]

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


def require_pro(user: UserContext = Depends(get_current_user)) -> UserContext:
    """Dependency that rejects non-Pro users with 403."""
    if not user.is_pro:
        raise HTTPException(
            status_code=403,
            detail="Pro subscription required. Upgrade at /pricing",
        )
    return user
