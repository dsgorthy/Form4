"""An account is free unless it pays. Nothing derives a tier from its age.

Until 2026-09-17 every new account was a Pro trial for seven days and in
"grace" for seven more, computed from Clerk `created_at` on every request in
two languages. Nobody chose it; the nav counted it down, a banner sold it on
every page, and a six-hourly job sent "your Pro access expires in 2 days" to
everyone who signed up -- including the people the search-landing pages had
just promised a free account. Derek, 2026-09-17: free users over anonymous
users. The Pro trial is now a choice made at checkout and run by Stripe.

These pin the model on both sides, the copy that sold the old one, and the
one place the trial is allowed to exist.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FE = ROOT / "frontend" / "src"
SUB = (FE / "lib" / "subscription.ts").read_text(encoding="utf-8")
AUTH = (ROOT / "api" / "auth.py").read_text(encoding="utf-8")
CHECKOUT = (FE / "app" / "api" / "checkout" / "route.ts").read_text(encoding="utf-8")
PRICING = (FE / "app" / "pricing" / "page.tsx").read_text(encoding="utf-8")


def _code(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    src = re.sub(r'"""(?:.|\n)*?"""', "", src)
    return re.sub(r"^\s*(//|#).*$", "", src, flags=re.M)


def _frontend_sources():
    for p in FE.rglob("*.ts*"):
        if ".next" in p.parts:
            continue
        yield p, p.read_text(encoding="utf-8")


def test_client_tier_comes_from_metadata_only():
    code = _code(SUB)
    assert 'export type Tier = "free" | "pro" | "pro_plus";' in SUB
    for banned in ("createdAt", "TRIAL_DAYS", "GRACE_DAYS", '"trial"', '"grace"', "getTrialDaysLeft", "getGraceDaysLeft"):
        assert banned not in code, f"{banned} is back in subscription.ts: the tier is age-derived again"


def test_server_tier_comes_from_metadata_only():
    code = _code(AUTH)
    for banned in ("TRIAL_DAYS", "GRACE_DAYS", "_created_at", "age_days", '"trial"', '"grace"', "trial_days_left", "grace_days_left", "is_grace"):
        assert banned not in code, f"{banned} is back in api/auth.py: the tier is age-derived again"
    assert 'PAID_TIERS = ("pro", "pro_plus")' in AUTH


def test_nothing_in_the_api_asks_for_a_grace_tier():
    for p in (ROOT / "api").rglob("*.py"):
        assert "is_grace" not in _code(p.read_text(encoding="utf-8")), f"{p} still branches on a grace tier"


def test_no_surface_sells_a_trial_as_the_way_in():
    """The words that made a free account read as a paid one."""
    banned = ("Start Free Trial", "Start a free trial", "Start 7-day trial", "7 days of full Pro access",
              "7 days of full access", "7-day free trial, no card", "trial, no card", "trial requires no card",
              "Full access for 7 days")
    hits = []
    for p, src in _frontend_sources():
        code = _code(src)
        for b in banned:
            if b.lower() in code.lower():
                hits.append(f"{p.relative_to(ROOT)}: {b!r}")
    assert not hits, "trial copy is back:\n" + "\n".join(hits)


def test_the_banner_is_gone():
    assert not (FE / "components" / "trial-banner.tsx").exists()
    assert "TrialBanner" not in (FE / "app" / "layout.tsx").read_text(encoding="utf-8")


def test_the_pro_trial_lives_in_checkout_once_per_customer():
    code = _code(CHECKOUT)
    assert "const PRO_TRIAL_DAYS = 7;" in code
    assert "trial_period_days: PRO_TRIAL_DAYS" in code
    assert "createdAt" not in code, "checkout derives the trial from account age again"
    assert 'status: "all"' in code and "hadASubscription" in code, (
        "a returning subscriber would get a second free trial"
    )


def test_pricing_states_the_same_trial_the_checkout_grants():
    assert "7-day free trial" in PRICING or "free for 7 days" in PRICING
    assert "nothing is charged until the trial ends" in PRICING.lower() or "Nothing is charged until the trial ends" in PRICING
    assert "requires no card" not in PRICING, "pricing still promises a no-card trial; the Stripe trial takes a card"


def test_onboarding_accepts_what_the_form_sends():
    src = (ROOT / "api" / "routers" / "onboarding.py").read_text(encoding="utf-8")
    form = (FE / "app" / "onboarding" / "onboarding-form.tsx").read_text(encoding="utf-8")
    assert "default_strategy" in form and "referral_source" in form
    assert "default_strategy: Optional[str] = None" in src
    assert "user_type: Optional[UserType] = None" in src, "the form does not send user_type; it must not be required"
    mig = ROOT / "migrations" / "2026-09-17_user_profiles_default_strategy.sql"
    assert mig.exists() and "ADD COLUMN IF NOT EXISTS default_strategy" in mig.read_text()
