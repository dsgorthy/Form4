from __future__ import annotations

from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth import UserContext, get_current_user
from api.notifications_db import get_notifications_db
from api.db import get_db
from api.public_fields import ACTIVE_STRATEGIES

router = APIRouter(prefix="/api/v1/onboarding", tags=["onboarding"])

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class UserType(str, Enum):
    individual = "individual"
    advisor = "advisor"
    quant = "quant"
    fund_manager = "fund_manager"
    journalist = "journalist"
    student = "student"


class PrimaryUseCase(str, Enum):
    trading_signals = "trading_signals"
    research = "research"
    portfolio = "portfolio"
    compliance = "compliance"
    academic = "academic"
    tracking = "tracking"


class ExperienceLevel(str, Enum):
    beginner = "beginner"
    intermediate = "intermediate"
    expert = "expert"


class ReferralSource(str, Enum):
    search = "search"
    social = "social"
    referral = "referral"
    press = "press"
    community = "community"
    other = "other"
    not_specified = "not_specified"


# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------


class OnboardingRequest(BaseModel):
    """What the onboarding form actually sends.

    The form (frontend/src/app/onboarding/onboarding-form.tsx) asks one
    question -- which strategy book to open with -- and where you heard of
    us, and posts `{default_strategy, referral_source}`. This model demanded
    user_type / primary_use_case / experience_level, three questions the form
    stopped asking months ago, so every submit was a 422 the form swallowed
    and `user_profiles` kept its two rows from March. Found 2026-09-17 with
    four `onboarding_complete` events and no rows. The old fields stay
    optional so an older client is not rejected either.
    """
    default_strategy: Optional[str] = None
    referral_source: Optional[ReferralSource] = None
    # "Skip for now". The form still sends the book it is about to open as
    # default_strategy, because that is the book the account lands on.
    skipped: bool = False
    user_type: Optional[UserType] = None
    primary_use_case: Optional[PrimaryUseCase] = None
    experience_level: Optional[ExperienceLevel] = None


# How many of the chosen book's positions a new account follows by default.
# Well under the free cap of 10 companies, so the account still has room.
SEED_FOLLOWS = 5


def pick_seed_tickers(positions: list[dict], limit: int = SEED_FOLLOWS) -> list[str]:
    """The tickers a new account should follow because of the book it chose:
    the book's open positions, newest first, then its latest entries if it
    holds nothing (Insider Dip Buys goes dark for months at a time). Pure.

    Rows: {ticker, status, entry_date}, any order, duplicates allowed (a
    position appears once per execution source)."""
    def newest(rows):
        seen, out = set(), []
        for r in sorted(rows, key=lambda r: r.get("entry_date") or "", reverse=True):
            t = (r.get("ticker") or "").upper()
            if t and t not in seen:
                seen.add(t); out.append(t)
        return out
    open_now = newest([r for r in positions if r.get("status") == "open"])
    if open_now:
        return open_now[:limit]
    return newest(positions)[:min(limit, 3)]


def ensure_alert_settings(nconn, user_id: str) -> None:
    """Give the account its default alert settings row, if it has none.

    notification_scanner selects recipients FROM notification_preferences, so
    an account without a row hears about nothing, whatever it follows. Following
    from a company or insider page already creates the row; onboarding has to
    as well, including a skip."""
    nconn.execute(
        "INSERT INTO notification_preferences (user_id) VALUES (?) ON CONFLICT (user_id) DO NOTHING",
        (user_id,),
    )
    nconn.commit()


def seed_follows(nconn, iconn, user_id: str, strategy: str | None) -> list[str]:
    """Follow the chosen book's positions for an account that follows nothing.

    Signups come for the strategy books (every one since August landed on
    /portfolio), choose one in onboarding, and then heard nothing from it: the
    books' entry/exit alerts are Pro, and the account followed nobody, so no
    email ever arrived. Following the book's open positions gives the free
    account the alert it can have -- an insider at one of those companies
    filing -- and ties it to the choice they just made. Never touches an
    account that already follows something; that was their choice."""
    if not strategy:
        return []
    already = nconn.execute(
        "SELECT COUNT(*) AS n FROM watchlist WHERE user_id = ?", (user_id,)
    ).fetchone()["n"]
    if already:
        return []
    rows = [dict(r) for r in iconn.execute(
        """SELECT ticker, status, entry_date FROM strategy_portfolio
            WHERE strategy = ? AND execution_source IN ('alert', 'simulated')
              AND is_live = FALSE""",
        (strategy,),
    ).fetchall()]
    tickers = pick_seed_tickers(rows)
    if not tickers:
        return []
    nconn.execute("INSERT INTO notification_preferences (user_id) VALUES (?) ON CONFLICT (user_id) DO NOTHING", (user_id,))
    for t in tickers:
        nconn.execute("INSERT OR IGNORE INTO watchlist (user_id, ticker) VALUES (?, ?)", (user_id, t))
    nconn.commit()
    return tickers


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.post("")
def submit_onboarding(
    body: OnboardingRequest,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Save onboarding answers for the authenticated user."""
    if not user.user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    referral = body.referral_source.value if body.referral_source else "not_specified"
    strategy = body.default_strategy if body.default_strategy in ACTIVE_STRATEGIES else None
    # Until 2026-11-16 a skip never reached this endpoint: no profile row, no
    # alert settings, no follows, so the account could not be told anything.
    # Every signup tracked since October skipped. A skip records no choice of
    # book, but it still follows the one the form is about to open.
    chosen = None if body.skipped else strategy

    with get_notifications_db() as conn:
        conn.execute(
            """INSERT INTO user_profiles
                   (user_id, user_type, primary_use_case, experience_level, referral_source,
                    default_strategy, onboarding_skipped)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                   user_type = COALESCE(excluded.user_type, user_profiles.user_type),
                   primary_use_case = COALESCE(excluded.primary_use_case, user_profiles.primary_use_case),
                   experience_level = COALESCE(excluded.experience_level, user_profiles.experience_level),
                   referral_source = CASE WHEN excluded.referral_source = 'not_specified'
                                          THEN COALESCE(user_profiles.referral_source, 'not_specified')
                                          ELSE excluded.referral_source END,
                   default_strategy = COALESCE(excluded.default_strategy, user_profiles.default_strategy),
                   onboarding_skipped = excluded.onboarding_skipped,
                   updated_at = datetime('now')""",
            (
                user.user_id,
                body.user_type.value if body.user_type else None,
                body.primary_use_case.value if body.primary_use_case else None,
                body.experience_level.value if body.experience_level else None,
                referral,
                chosen,
                1 if body.skipped else 0,
            ),
        )
        conn.commit()
        ensure_alert_settings(conn, user.user_id)
        with get_db() as iconn:
            followed = seed_follows(conn, iconn, user.user_id, strategy)

    return {"ok": True, "followed": followed}
