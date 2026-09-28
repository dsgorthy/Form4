"""Internal insider links must resolve to the canonical URL, not merely resolve.

`/insider/{cik}` and `/insider/{sqid}` both answer 200 and declare a canonical
pointing somewhere else. They do NOT redirect: the middleware's alias map holds
retired SLUGS only, so a CIK URL is a distinct 200-response URL that Google has
to crawl, read, and discard.

That was mostly theoretical while `insiders.cik` was NULL for 56% of the table.
On 2026-09-27 a backfill took CIK coverage from 44.4% to 92.1% (101,777 rows,
recovered from `trades.rptowner_cik` where a single CIK appeared across an
insider's own filings), which flipped ~100k roster links from
`/insider/{insider_id}` to `/insider/{cik}` — a fresh set of non-canonical URLs
handed to a crawler that is already sitting on 126,000 "crawled, currently not
indexed" pages. Each company page carries up to 39 of them.

`insiderPath()` is the one place that knows the answer: it prefers the stored
slug (99.3% of insiders hold one) and falls back to name+id. The rule pinned
here is that no component builds the path itself.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "frontend" / "src"

# Files allowed to name the path directly, each for a specific reason.
EXEMPT = {
    # Defines insiderPath. Naming the shape is its job.
    "lib/insider-url.ts",
    # Builds the 308 target for a retired slug; the alias map is already
    # canonical slugs, so there is nothing for insiderPath to decide.
    "middleware.ts",
    # Absolute JSON-LD @id / url built from the canonical slug directly. These
    # are not links and must not fall back to an id form.
    "lib/structured-data.ts",
    # Emits sitemap entries from the API's own slug list.
    "app/sitemaps/[section]/route.ts",
    # Declares its own canonical for the explore lander.
    "app/explore/page.tsx",
}


def _code(src: str) -> str:
    """Comments stripped. A comment that MENTIONS a URL is not a link, and
    three tests in this repo have already failed on prose instead of code."""
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def _files():
    for p in sorted(SRC.rglob("*.tsx")) + sorted(SRC.rglob("*.ts")):
        rel = str(p.relative_to(SRC))
        if rel.startswith(".next/") or rel in EXEMPT:
            continue
        yield rel, p


# An href built by interpolation: href={`/insider/${...}`} or href="/insider/x".
HREF_LITERAL = re.compile(r"""href\s*=\s*\{?\s*[`"']/insider/""")


@pytest.mark.parametrize("rel,path", list(_files()), ids=lambda v: v if isinstance(v, str) else "")
def test_no_component_builds_an_insider_href_itself(rel, path):
    hits = [
        m.start() for m in HREF_LITERAL.finditer(_code(path.read_text(encoding="utf-8")))
    ]
    assert not hits, (
        f"{rel} builds an insider href from a string. Use "
        "insiderPath(name, id, slug) — a hand-built /insider/{cik} or "
        "/insider/{sqid} answers 200 at a non-canonical URL and costs a crawl."
    )


def test_insider_path_prefers_the_stored_slug():
    src = (SRC / "lib" / "insider-url.ts").read_text()
    body = src[src.index("export function insiderPath") :]
    body = body[: body.index("export function", 20)]  # skip its own header
    slug_at = body.index("if (slug) return")
    derived_at = body.index("slugifyName(")
    assert slug_at < derived_at, (
        "insiderPath derives from the name before checking the stored slug; the "
        "stored slug is authoritative and the derived form disagrees with it "
        "the moment a name is normalised"
    )


def test_the_company_roster_api_returns_the_slug():
    """The roster is the highest-volume internal link into insider pages, and
    insiderPath can only prefer a slug the payload actually carries."""
    api = (Path(__file__).resolve().parents[2] / "api" / "routers" / "companies.py").read_text()
    start = api.index("# Insider roster for this company")
    query = api[start : start + 2500]
    assert re.search(r"^\s*i\.slug,", query, re.M), (
        "the company overview roster no longer selects i.slug, so every roster "
        "link falls back to a non-canonical /insider/{cik}"
    )


def test_the_roster_component_accepts_a_slug():
    src = (SRC / "components" / "insider-roster.tsx").read_text()
    assert re.search(r"slug\?:\s*string \| null", src), (
        "the Insider interface dropped slug, so the API's slug is discarded "
        "before it reaches insiderPath"
    )
    assert "insiderPath(" in _code(src)
