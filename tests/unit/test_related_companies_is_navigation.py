"""Related companies is topical navigation, not a comparison.

WHY IT EXISTS AT ALL

Measured 2026-09-03 from Caddy access logs, Googlebot's 7-day crawl:

    /filing/*    2,263
    /company/*   1,482
    /insider/*     508

Company pages are the second-most-crawled surface and carried TWENTY outbound
links to insiders and NOT ONE to another company. Nothing for a reader who
landed from search to click, and no sector signal for a crawler to read.
Organic search delivers 62 unique visitors per 90 days at 1.15 pages per
visit -- they land and leave.

WHAT IT MUST NOT BECOME

Two companies sharing a director tells you about one person's calendar. It is
not a price correlation, not a peer-comparison table, and not a signal. The
card may say how the two are connected and how much insider buying there has
been; it may not rank, score or compare them as holdings.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROUTER = (REPO / "api" / "routers" / "companies.py").read_text(encoding="utf-8")
COMPONENT = (REPO / "frontend" / "src" / "components" / "related-companies.tsx").read_text(encoding="utf-8")
SCRIPT = (REPO / "scripts" / "insider_similarity.py").read_text(encoding="utf-8")

RENDERED = re.sub(r"/\*.*?\*/", "", COMPONENT, flags=re.S)
RENDERED = re.sub(r"^\s*//.*$", "", RENDERED, flags=re.M)


def _endpoint() -> str:
    i = ROUTER.index("def get_related_companies(")
    j = ROUTER.index("\n@router.", i)
    return ROUTER[i:j]


def _endpoint_code() -> str:
    """The endpoint with its docstring removed.

    The docstring NAMES the things the endpoint must not do ("not a price
    correlation"), so scanning the whole function flags the documentation of a
    rule as a violation of it. Same trap the component tests fell into.
    """
    body = _endpoint()
    return re.sub(r'"""[\s\S]*?"""', "", body, count=1)


@pytest.mark.parametrize("word", ["Top ", "Best ", "outperform", "Correlated", "vs."])
def test_the_card_makes_no_comparison_claim(word: str):
    assert word not in RENDERED, (
        f"the related-companies card says {word!r}. It relates companies by "
        "who files on them, which says nothing about how either stock does."
    )


def test_the_disclaimer_names_the_actual_basis():
    assert "not by how the stocks move" in RENDERED, (
        "the visible note no longer tells the reader the relation is about "
        "filers rather than prices"
    )


def test_both_reasons_are_distinguished():
    # The values are written by the compute and passed through by the API, so
    # the literals live in the script, not the endpoint.
    assert '"shared_insiders"' in SCRIPT and '"sector_peer"' in SCRIPT
    assert 'r.reason === "shared_insiders"' in RENDERED, (
        "the card no longer says which relation put a company on it; two "
        "shared insiders and 'same sector' are not the same evidence"
    )


def test_no_price_or_return_field_reaches_the_card():
    body = _endpoint_code()
    for banned in ("return_", "abnormal", "price", "correlation", "beta"):
        assert banned not in body, (
            f"get_related_companies returns {banned!r}; this list is not a "
            "performance comparison"
        )


# ── the compute keeps the properties the list depends on ───────────────────

def test_peers_are_scored_on_one_scale():
    """The first version sorted shared-insider peers by count then
    ALPHABETICALLY. Sharing exactly one insider is extremely common, so AAPL's
    peers came out ADES, ADSK, AI, AMRS, AMZN -- an alphabet, three of them
    with no insider filing in a year."""
    assert "ONE SCALE FOR BOTH RELATIONS" in SCRIPT, (
        "the single-scale scoring note is gone; check whether shared-insider "
        "peers went back to being ordered alphabetically"
    )


def test_a_dead_peer_is_dropped():
    """One shared insider and no filings in a year is a dead link."""
    assert re.search(r"if n < 2 and not rb:", SCRIPT), (
        "peers with weak evidence and no recent activity are no longer dropped"
    )


def test_the_fund_cap_is_kept():
    """Past ~40 tickers an 'insider' is a fund spraying across a portfolio and
    'shares an insider' stops meaning the companies are related. It also bounds
    a pair count that is quadratic in the ticker set."""
    m = re.search(r"MAX_TICKERS_FOR_COMPANY_PAIRS\s*=\s*(\d+)", SCRIPT)
    assert m and 5 <= int(m.group(1)) <= 100, (
        "the fund cap on company pair generation is gone or implausible"
    )
