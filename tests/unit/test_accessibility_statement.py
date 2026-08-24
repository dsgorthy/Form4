"""The accessibility statement exists, is reachable, and does not overclaim.

WHY IT MATTERS THAT IT IS HONEST

The European Accessibility Act has been enforceable since 28 June 2025 and
applies by where a service is CONSUMED, not where the company sits. Form4's
privacy policy already asserts UK GDPR / GDPR applicability, so by our own
stated position we serve EU users. The EAA requires a publicly available
accessibility statement.

A statement that claims conformance we do not have is a documented false claim
about a legally-relevant fact — worse than no statement at all, and the
specific failure mode the W3C guidance warns about. Measured 2026-08-24, the
tertiary text colour gives 2.37:1 where WCAG AA needs 4.5:1, in 618 places.
So the statement says "partially conformant" and names the barrier.

These tests fail the build if the statement disappears, drops out of the
footer, or starts claiming full conformance.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PAGE = REPO / "frontend/src/app/accessibility/page.tsx"
FOOTER = REPO / "frontend/src/components/footer.tsx"
LAYOUT = REPO / "frontend/src/app/layout.tsx"


def test_the_statement_exists():
    assert PAGE.exists(), "the accessibility statement page is gone"


def test_it_is_linked_from_the_footer_on_every_page():
    """'Publicly available' in practice means reachable without hunting."""
    assert '"/accessibility"' in FOOTER.read_text(), (
        "the accessibility statement is not linked in the footer"
    )


@pytest.mark.parametrize("required,why", [
    ("WCAG 2.1", "the statement must name the standard it targets"),
    ("partially conformant", "the conformance level must be stated plainly"),
    ("accessibility@form4.app", "there must be a way to report a barrier"),
    ("2026", "the statement must carry a date"),
])
def test_the_w3c_required_elements_are_present(required, why):
    """W3C WAI: commitment, standard applied, contact for reporting."""
    assert required in PAGE.read_text(), why


def test_it_does_not_claim_conformance_we_do_not_have():
    body = PAGE.read_text()
    # Strip the source comment block; it discusses the claim rather than making it.
    code = re.sub(r"/\*\*.*?\*/", "", body, flags=re.S)
    for overclaim in ("fully conformant", "fully accessible", "WCAG 2.1 AA compliant",
                      "is conformant with WCAG"):
        assert overclaim.lower() not in code.lower(), (
            f"the statement claims {overclaim!r}. Measured 2026-08-24 the site "
            "is not: tertiary text is 2.37:1 against 4.5:1 in 618 places. A "
            "false conformance claim is worse than no statement."
        )


def test_known_limitations_are_named_not_hand_waved():
    """W3C advises naming barriers in plain language so users are not
    surprised. 'We strive for accessibility' on its own is not a statement."""
    body = PAGE.read_text().lower()
    assert "contrast" in body or "too faint" in body, (
        "the contrast gap — the largest known barrier — is not disclosed"
    )


def test_the_skip_link_exists():
    """WCAG 2.4.1 Bypass Blocks, Level A. The cheapest real fix on the site."""
    layout = LAYOUT.read_text()
    assert 'href="#main"' in layout, "the skip-to-content link is gone"
    assert 'id="main"' in layout, (
        "the skip link has no target — it would jump nowhere, which is worse "
        "than not having one because it looks handled"
    )
