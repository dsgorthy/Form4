from __future__ import annotations

from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.auth import UserContext, get_current_user
from api.notifications_db import get_notifications_db

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
    user_type: UserType
    primary_use_case: PrimaryUseCase
    experience_level: ExperienceLevel
    referral_source: Optional[ReferralSource] = None


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

    with get_notifications_db() as conn:
        conn.execute(
            """INSERT INTO user_profiles (user_id, user_type, primary_use_case, experience_level, referral_source)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                   user_type = excluded.user_type,
                   primary_use_case = excluded.primary_use_case,
                   experience_level = excluded.experience_level,
                   referral_source = excluded.referral_source,
                   updated_at = datetime('now')""",
            (user.user_id, body.user_type.value, body.primary_use_case.value, body.experience_level.value, referral),
        )
        conn.commit()

    return {"ok": True}
