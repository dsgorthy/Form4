"""The follow ask on search-landing pages is a free thing and must read as one.

Until 2026-09-16 the anonymous band said "7-day free trial, no credit card
required" under the follow button. That is the Pro trial's line, attached to a
feature a free account already has (10 insiders, 10 companies, an email when
they file). Search visitors read a paid product they'd be trialling; 0 of 27
who saw it clicked. Trial copy belongs on /pricing and nowhere near "follow".

Also pinned here: the ask now exists in the first viewport of both pages (the
band alone was seen by one visitor in five), the mount event waits for
PostHog to be initialised (it was reaching PostHog for 5% of landers because
it fired before init), and sign-up hands Clerk the way back with the follow
still attached (nothing honoured `next=` before, so every sign-up dropped the
follow on the home page).
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "frontend" / "src"
CTA = (ROOT / "components" / "follow-cta.tsx").read_text(encoding="utf-8")
INSIDER = (ROOT / "app" / "insider" / "[id]" / "page.tsx").read_text(encoding="utf-8")
COMPANY = (ROOT / "app" / "company" / "[ticker]" / "page.tsx").read_text(encoding="utf-8")
SIGNUP = (ROOT / "app" / "(auth)" / "sign-up" / "[[...sign-up]]" / "page.tsx").read_text(encoding="utf-8")
PRICING = (ROOT / "app" / "pricing" / "page.tsx").read_text(encoding="utf-8")


def _code(src: str) -> str:
    """Source with comments removed, so a docblock quoting the old copy does
    not count as the copy."""
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def test_the_anonymous_ask_says_free_and_never_trial():
    code = _code(CTA)
    assert "for free" in code, "the anonymous button no longer says it is free"
    assert "Free account" in code, "the anonymous sub-line no longer says the account is free"
    for banned in ("trial", "credit card", "Trial"):
        assert banned not in code, f"{banned!r} is back in the follow copy; that line belongs on /pricing"


def test_trial_copy_still_lives_on_pricing():
    # The trial exists; it just is not how following is sold.
    assert "free trial" in PRICING.lower()


def test_the_ask_is_in_the_first_viewport_on_both_pages():
    for name, page in (("insider", INSIDER), ("company", COMPANY)):
        top = page.find("<FollowInline")
        band = page.find("<FollowCta")
        assert top != -1, f"{name} page has no first-viewport follow ask"
        assert band != -1, f"{name} page lost the band"
        assert top < band, f"{name} page renders the inline ask below the band"
        h1 = page.find("<h1")
        assert h1 != -1 and top - h1 < 6000, f"{name} page: the inline ask is not near the header"


def test_events_carry_the_placement_so_the_two_asks_are_told_apart():
    for ev in ("follow_cta_shown", "follow_cta_viewed", "follow_cta_clicked"):
        i = CTA.index(f'"{ev}"')
        assert "placement" in CTA[i:i + 200], f"{ev} does not carry placement"


def test_the_mount_event_waits_for_posthog_to_be_initialised():
    assert "__loaded" in CTA, (
        "follow_cta_shown fires before posthog.init() and is dropped; the "
        "capture has to wait for the client (it reached PostHog for 5% of landers)"
    )


def test_signup_tells_clerk_the_way_back_with_the_follow_attached():
    assert "forceRedirectUrl" in SIGNUP, "sign-up does not send people back where they came from"
    assert "follow=" in SIGNUP, "the follow token is not on the return URL, so PendingFollow never runs"
    assert 'startsWith("/")' in SIGNUP and 'startsWith("//")' in SIGNUP, (
        "next= must be constrained to a same-site path"
    )


def test_signup_says_what_the_account_is_for():
    assert "Free account" in SIGNUP
    assert "name" in SIGNUP and "name=" in CTA, (
        "the CTA has to pass the display name so sign-up can say who they'll hear about"
    )


def test_copy_reads_like_a_person_wrote_it():
    """No em-dash sentence-joins, no 'Unlock', no 'seamless', no exclamation
    marks in the strings a visitor reads."""
    strings = re.findall(r'[`"]([^`"\n]{12,})[`"]', _code(CTA)) + re.findall(r'[`"]([^`"\n]{12,})[`"]', _code(SIGNUP))
    visible = [s for s in strings if re.search(r"[A-Z][a-z]+ [a-z]+", s) and "className" not in s and "/" not in s]
    assert visible, "no visible strings found; did the copy move?"
    for s in visible:
        assert " — " not in s, f"em-dash join in visible copy: {s!r}"
        assert "!" not in s, f"exclamation mark in visible copy: {s!r}"
        for word in ("Unlock", "seamless", "supercharge", "elevate", "journey"):
            assert word.lower() not in s.lower(), f"{word!r} in visible copy: {s!r}"
