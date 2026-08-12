"""Comped account expiry.

Clerk is the system of record for a user's tier — there is no subscriptions
table. Tier normally arrives via the Stripe webhook (api/routers/webhooks.py),
which writes ``public_metadata.tier`` and nothing else. Comping an account by
hand means setting that same ``tier`` plus a ``pro_until`` end date, so this
module is what makes the comp stop on its own.

Stdlib only, on purpose: this is imported both by the API (inside Docker) and
by pipelines/trial_emails.py (a launchd job on Studio running bare python3,
which has neither fastapi nor PyJWT installed). Keep it that way.

Mirrored client-side by ``compLapsed`` in frontend/src/lib/subscription.ts.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def comp_lapsed(metadata: dict) -> bool:
    """True if this account carries a comped tier whose end date has passed.

    ``pro_until`` is only ever written by hand when comping an account —
    Stripe never sets it — so its absence means "no expiry" and paid
    subscribers are untouched. Accepts a bare ``YYYY-MM-DD`` (access runs
    through the end of that day, UTC) or a full ISO 8601 timestamp.

    A value we can't parse leaves access in place and logs. A typo in a comp
    date should not silently revoke access we promised somebody.
    """
    raw = metadata.get("pro_until")
    if not raw:
        return False

    text = str(raw).strip()
    try:
        if len(text) == 10:
            expires = datetime.strptime(text, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
        else:
            expires = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        logger.warning("Unparseable pro_until %r — leaving access in place", raw)
        return False

    return datetime.now(timezone.utc) > expires
