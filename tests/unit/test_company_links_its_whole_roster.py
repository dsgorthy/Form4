"""Every company->insider edge must be in the delivered HTML, not just the data.

A sitemap is a weak crawl signal next to a link. On 2026-09-20 Google held
59,022 URLs as "Discovered - currently not indexed" — found, queued, never
fetched — and 67% of its crawl budget went to Discovery rather than Refresh.

The company page is the best-performing page type (2.80 search views per 1,000
URLs, against 1.53 for insiders) and the insider page is the one being lifted,
so company -> insider is the edge that matters. It was mostly missing:
`InsiderRoster` paginates CLIENT-SIDE at PAGE_SIZE rows, so the delivered HTML
carried the first 10 links however long the roster was. Measured 2026-09-27
across the 10,684 company pages we submit:

    median roster        21 insiders
    p90                  49
    max                 183
    rosters over 10   8,553 of 10,684  (80%)
    links in the HTML 99,570 of 265,605 (37%)

Two thirds of the edges existed in the data and not in the page. The fix is a
collapsed `<details>` list of every insider — crawlable, no grades, nothing
gated, and not a second roster.

What is pinned here is the invariant, not the markup: the page must link the
WHOLE roster, and the cutoff must be one number rather than two.
"""
from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "frontend" / "src"
COMPANY = SRC / "app" / "company" / "[ticker]" / "page.tsx"
ROSTER = SRC / "components" / "insider-roster.tsx"
LIST = SRC / "components" / "entity-link-list.tsx"


def _code(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def test_the_company_page_links_every_insider_not_just_the_first_page():
    code = _code(COMPANY.read_text())
    # Anchored on the opening TAG WITH ITS BOUNDARY. Both "EntityLinkList" and
    # "<EntityLinkList" are substrings of "<EntityLinkListGone", so the first
    # two attempts at this assertion passed against a removed component. A
    # mutation test caught each one.
    tag = re.search(r"<EntityLinkList[\s>]", code)
    assert tag, (
        "the company page no longer renders the complete insider link list, so "
        "80% of its pages link only their first 10 insiders"
    )
    block = code[tag.start():]
    block = block[: block.index("/>") + 2]
    assert "overview.insiders" in block, (
        "the link list is not built from the full roster"
    )
    assert ".slice(" not in block, (
        "the complete link list slices its input, which is the exact truncation "
        "it exists to undo"
    )
    assert "insiderPath(" in block, (
        "the link list builds hrefs itself instead of using insiderPath, so it "
        "would publish non-canonical /insider/{cik} URLs"
    )


def test_the_page_size_is_one_number_shared_by_both():
    """Two copies of the cutoff would list the first 10 names twice, or skip
    some — and either is invisible without counting the rendered links."""
    roster = ROSTER.read_text()
    assert re.search(r"export const PAGE_SIZE = (\d+)", roster), (
        "PAGE_SIZE is no longer exported from insider-roster"
    )
    company = _code(COMPANY.read_text())
    assert "PAGE_SIZE as INSIDER_ROSTER_PAGE_SIZE" in company, (
        "the company page does not import the roster's PAGE_SIZE"
    )
    assert "alreadyShown={INSIDER_ROSTER_PAGE_SIZE}" in company, (
        "alreadyShown is a literal instead of the imported PAGE_SIZE"
    )


def test_the_list_renders_without_javascript():
    """Crawlers read the delivered HTML. A client component that hides its
    content behind a click puts the links out of reach."""
    src = LIST.read_text()
    assert '"use client"' not in src, (
        "entity-link-list became a client component; the links must be in the "
        "server-rendered HTML"
    )
    assert "<details" in src and "<summary" in src, (
        "the collapse is no longer a <details> element — a JS toggle would keep "
        "the links out of the delivered HTML"
    )
    assert "useState" not in src


def test_the_list_publishes_nothing_gated():
    """It is a link list, not a roster. Grades and scores are the gated part of
    this page and must not leak into an ungated block."""
    src = _code(LIST.read_text())
    for leaked in ("career_grade", "score", "ProGate", "InsiderGradeBadge",
                   "formatCurrency"):
        assert leaked not in src, (
            f"{leaked} appears in the link list; it publishes names and links "
            "only, and duplicating the roster's columns would both leak gated "
            "content and make the page worse"
        )


def test_it_renders_nothing_when_there_is_nothing_to_add():
    """A company with 6 insiders already links all 6 above. An empty
    'Every insider (6)' toggle is noise."""
    src = LIST.read_text()
    assert "items.length <= alreadyShown" in src, (
        "the list does not bail out when the page already links everything"
    )
