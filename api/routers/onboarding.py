from __future__ import annotations

from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth import UserContext, get_current_user
from api.notifications_db import get_notifications_db
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
    user_type: Optional[UserType] = None
    primary_use_case: Optional[PrimaryUseCase] = None
    experience_level: Optional[ExperienceLevel] = None


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

    with get_notifications_db() as conn:
        conn.execute(
            """INSERT INTO user_profiles
                   (user_id, user_type, primary_use_case, experience_level, referral_source, default_strategy)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                   user_type = COALESCE(excluded.user_type, user_profiles.user_type),
                   primary_use_case = COALESCE(excluded.primary_use_case, user_profiles.primary_use_case),
                   experience_level = COALESCE(excluded.experience_level, user_profiles.experience_level),
                   referral_source = excluded.referral_source,
                   default_strategy = COALESCE(excluded.default_strategy, user_profiles.default_strategy),
                   updated_at = datetime('now')""",
            (
                user.user_id,
                body.user_type.value if body.user_type else None,
                body.primary_use_case.value if body.primary_use_case else None,
                body.experience_level.value if body.experience_level else None,
                referral,
                strategy,
            ),
        )
        conn.commit()

    return {"ok": True}
