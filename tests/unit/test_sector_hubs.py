"""The sector hub pages, and the one thing they must not turn into.

WHY THEY EXIST

Measured 2026-09-03. Googlebot crawls ~4,300 pages a week here; organic search
returns 62 unique visitors per 90 days at 1.15 pages per visit. The crawl goes
almost entirely to leaves — one filing, one insider, one company — which
target queries with no volume. "Erez Chimovits" is not a search anyone runs.

The 17 static URLs in the core sitemap were /pricing, /privacy, /terms and
research notes. Not one targeted an informational query. These twelve do, and
each sector page links out to ~40 leaf pages that had no topical page pointing
at them.

WHAT THEY MUST NOT BECOME

A page headed "stocks insiders are buying" that ranks PEOPLE by past returns
is a tip sheet. Three separate experiments this month failed to show our
grades predict forward returns, so the buyer list is ranked by how much was
bought and says so on its face. Accuracy figures stay on the insider's own
page where they carry a denominator and a publishing floor.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROUTER = (REPO / "api" / "routers" / "sectors.py").read_text(encoding="utf-8")
HUB = (REPO / "frontend" / "src" / "app" / "insider-buying" / "page.tsx").read_text(encoding="utf-8")
DETAIL = (REPO / "frontend" / "src" / "app" / "insider-buying" / "[sector]" / "page.tsx").read_text(encoding="utf-8")


def _no_comments(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


# ── the population is the same one every other surface uses ────────────────

@pytest.mark.parametrize("sql_name", [
    "_SECTORS_SQL", "_TOP_BUYS_SQL", "_TOP_COMPANIES_SQL", "_TOP_INSIDERS_SQL",
])
def test_every_query_counts_decisions_only(sql_name: str):
    """discretionary_buy, never trans_code or trade_type.

    184k compensation grants and 221k option exercises carry
    trade_type='buy'. A hub page built on that would be mostly stock a board
    handed someone, presented as insiders buying.
    """
    i = ROUTER.index(sql_name)
    sql = ROUTER[i:ROUTER.index('"""', ROUTER.index('"""', i) + 3)]
    assert "signal_class = 'discretionary_buy'" in sql, (
        f"{sql_name} no longer filters on signal_class"
    )
    assert "trade_type" not in sql, (
        f"{sql_name} filters on trade_type, which does not mean bought"
    )


@pytest.mark.parametrize("sql_name", ["_TOP_BUYS_SQL", "_TOP_COMPANIES_SQL", "_TOP_INSIDERS_SQL"])
def test_derivative_and_suspect_rows_are_excluded(sql_name: str):
    """These pages rank by VALUE. Derivative rows carry notional value
    reaching $180 quadrillion, so without this the whole page is those."""
    i = ROUTER.index(sql_name)
    sql = ROUTER[i:ROUTER.index('"""', ROUTER.index('"""', i) + 3)]
    assert "is_derivative = 0" in sql, f"{sql_name} admits derivative rows"
    assert "value_suspect" in sql, f"{sql_name} admits value_suspect rows"


def test_filings_are_counted_not_lots():
    """A purchase filled in five tranches is one decision."""
    assert "COALESCE(t.filing_key, t.accession)" in ROUTER, (
        "the sector counts no longer group by filing; counting lots inflates "
        "every figure by the tranche count"
    )


# ── the buyer list is activity, not performance ────────────────────────────

def test_buyers_are_ranked_by_activity_not_returns():
    i = ROUTER.index("_TOP_INSIDERS_SQL")
    sql = ROUTER[i:ROUTER.index('"""', ROUTER.index('"""', i) + 3)]
    for banned in ("win_rate", "abnormal", "return_", "career_grade", "score"):
        assert banned not in sql, (
            f"the most-active-buyers query selects {banned!r}. Ranking people "
            "by past returns under a 'buying' headline is a tip sheet."
        )


def test_the_page_says_what_the_ranking_is():
    assert "not by past performance" in _no_comments(DETAIL), (
        "the buyer list no longer tells the reader it is ordered by value "
        "purchased rather than by results"
    )


# ── the exclusion that makes this different from a Form 4 dump ─────────────

def test_both_pages_disclose_the_exclusion():
    for name, src in (("hub", HUB), ("detail", DETAIL)):
        assert "10b5-1" in _no_comments(src), (
            f"the {name} page no longer says scheduled plan buys are excluded. "
            "They are the majority of Form 4 activity and return -2.22% "
            "abnormal at 30d against +1.71% for discretionary ones."
        )


# ── SEO plumbing these pages exist for ─────────────────────────────────────

def test_the_detail_page_is_canonical_and_described():
    assert "alternates:" in DETAIL and "canonical" in DETAIL
    assert "generateMetadata" in DETAIL
    assert "application/ld+json" in DETAIL, "no structured data on a hub page"


def test_the_slug_is_defined_once():
    """The frontend reads slugs from the API rather than keeping a copy, so a
    renamed sector cannot leave a route pointing at nothing."""
    assert "def slugify(" in ROUTER
    for src in (HUB, DETAIL):
        assert "Financial Services" not in src, (
            "a sector name is hard-coded in the frontend; the list comes from "
            "/api/v1/sectors"
        )


# ── the hubs need a route in, not just a sitemap entry ─────────────────────

def test_something_actually_links_to_the_hubs():
    """WHAT WENT WRONG, found 2026-09-04.

    The hubs shipped into the sitemap AND NOTHING ELSE. Nothing on the site
    linked to them. Googlebot crawls ~1,100 pages a day here — 552 filings,
    324 insiders, 240 companies in 24h — and had touched no hub, because its
    only route in was a sitemap it had not fetched in 48 hours.

    A page reachable only from a sitemap is the slowest discovery path there
    is, and distributing crawl to the leaves was the entire argument for
    building them.
    """
    src = REPO / "frontend" / "src"
    linkers = [
        p.relative_to(src).as_posix()
        for p in src.rglob("*.tsx")
        if ".next" not in p.parts
        and "app/insider-buying" not in p.as_posix()
        and "/insider-buying" in p.read_text(encoding="utf-8")
    ]
    assert linkers, (
        "nothing outside the hub pages links to /insider-buying. It is "
        "reachable only from the sitemap, which is how it went uncrawled."
    )


def test_the_hub_links_are_server_rendered():
    """The nav's More dropdown is a client component that mounts its list on
    open, so /explore, /clusters, /research and /insider-buying appeared in NO
    page's HTML. Putting the hub in that menu bought exactly nothing for
    crawling. The footer is real markup on every page."""
    footer = (REPO / "frontend" / "src" / "components" / "footer.tsx"
              ).read_text(encoding="utf-8")
    for href in ("/insider-buying", "/clusters", "/explore", "/research"):
        assert f'"{href}"' in footer, (
            f"{href} is not linked from the footer. The nav dropdown does not "
            "render server-side, so the footer is what a crawler can follow."
        )


def test_the_company_page_links_to_its_sector_hub():
    """The strongest topical link available: a Healthcare company pointing at
    Healthcare Insider Buying, from the second-most-crawled surface."""
    page = (REPO / "frontend" / "src" / "app" / "company" / "[ticker]" / "page.tsx"
            ).read_text(encoding="utf-8")
    assert "/insider-buying/${overview.sector_slug}" in page, (
        "company pages no longer link to their sector hub"
    )
    assert "toLowerCase().replace" not in page, (
        "the sector slug is being derived in the frontend again; it comes "
        "from the API, which uses the same slugify the routes use"
    )
