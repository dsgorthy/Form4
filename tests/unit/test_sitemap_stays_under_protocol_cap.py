"""No sitemap section may be able to exceed the 50,000-URL protocol cap.

Google REJECTS an oversized <urlset> rather than truncating it, so the whole
document stops being processed. form4.app learned this the expensive way: one
file holding every URL crossed the cap and nothing was indexed between
2026-04-03 and mid-August.

The fix chunked filings but left insiders as a single file, held under the cap
only by a hardcoded `limit_insiders=45000` — while a comment in
api/routers/sitemap.py asserted "the client chunks at CHUNK anyway", which was
true of filings and false of insiders. Eligibility (`buy_count >= 2`) then grew
from 42,195 on 2026-09-03 to 51,747 on 2026-09-10, so the section was one raised
constant away from repeating the outage.

These tests encode the invariant rather than the constants: every section that
is sourced from a growing population must be chunked, and the number the client
requests must fit in the files it will write it into.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DATA_TS = ROOT / "frontend" / "src" / "lib" / "sitemap-data.ts"
ROUTE_TS = ROOT / "frontend" / "src" / "app" / "sitemaps" / "[section]" / "route.ts"
API_PY = ROOT / "api" / "routers" / "sitemap.py"

PROTOCOL_CAP = 50_000


def _const(text: str, name: str) -> int:
    m = re.search(rf"export const {name}\s*=\s*(\d+)", text)
    assert m, f"{name} not found or no longer a literal"
    return int(m.group(1))


@pytest.fixture(scope="module")
def data_ts() -> str:
    return DATA_TS.read_text()


def test_chunk_size_is_under_the_protocol_cap(data_ts):
    chunk = _const(data_ts, "CHUNK")
    assert chunk <= PROTOCOL_CAP, (
        f"CHUNK={chunk} exceeds the {PROTOCOL_CAP}-URL cap on a single urlset; "
        "every generated file would be rejected outright"
    )


def test_insider_request_fits_in_the_files_it_is_written_into(data_ts):
    """The client must not ask the API for more rows than its chunks can hold.

    Any excess is silently dropped by the last .slice(), so the URLs would be
    fetched, paid for, and never published — invisible unless someone counts.
    """
    chunk = _const(data_ts, "CHUNK")
    chunks = _const(data_ts, "INSIDER_CHUNKS")
    m = re.search(r"export const INSIDER_LIMIT\s*=\s*([^;]+);", data_ts)
    assert m, "INSIDER_LIMIT not found"
    expr = m.group(1).strip()
    assert expr == "INSIDER_CHUNKS * CHUNK", (
        f"INSIDER_LIMIT is {expr!r}; derive it from INSIDER_CHUNKS * CHUNK so the "
        "request and the capacity cannot drift apart"
    )
    # 30,793 insiders clear the submission floor as of 2026-09-27 (>= 10
    # decision filings, or filed in the last 12 months with >= 5). That is down
    # from 51,747 under the old buy_count >= 2 rule, which counted execution
    # lots — see tests/unit/test_sitemap_submits_what_can_rank.py.
    #
    # Capacity is deliberately left well ABOVE eligibility rather than trimmed
    # to fit. INSIDER_CHUNKS = 3 means the third file currently renders an empty
    # urlset, which is valid and costs one crawler fetch. Trimming to 2 would
    # tidy that up and leave only 9,200 of headroom against a rule whose
    # recency arm admits every insider who files — and the failure mode on the
    # other side is silent: the excess is dropped by the last .slice() with
    # nothing to notice it. An empty file beats a missing one.
    eligible_2026_09_27 = 30_793
    assert chunks * chunk >= eligible_2026_09_27, (
        f"capacity {chunks * chunk} is below the {eligible_2026_09_27} insiders "
        "eligible as of 2026-09-27; raise INSIDER_CHUNKS"
    )


def test_no_growing_section_is_emitted_as_a_single_file(data_ts):
    """Sections sourced from a growing population must be chunked IF PUBLISHED.

    'insiders' and 'filings' both scale with the database. A bare entry in
    SECTIONS means one file for the whole population, which is the shape that
    caused the outage.

    Absent is also fine, and since 2026-09-27 filings ARE absent: they were a
    third of everything submitted and a third as productive per URL, and
    dropping them redirects crawl budget from Discovery to Refresh. The
    invariant being pinned is "never one file for an unbounded population",
    not "must be submitted" — the first version of this test conflated the two
    and failed on the retirement.
    """
    m = re.search(r"export const SECTIONS = \[(.*?)\];", data_ts, re.S)
    assert m, "SECTIONS not found"
    body = m.group(1)
    for name in ("insiders", "filings"):
        assert not re.search(rf'"{name}"', body), (
            f'SECTIONS contains a bare "{name}" entry — that is one file for a '
            "population that grows without bound. Emit it as "
            f'`{name}-${{i}}` chunks instead.'
        )
    # Insiders must be published and chunked; there is nothing else serving them.
    assert "insiders-$" in body or "INSIDER_SECTIONS" in body, (
        "no chunked insider entries in SECTIONS"
    )


def test_filings_are_retired_cleanly_rather_than_404ing(data_ts):
    """Unpublishing a section Google already read must not 404.

    Google holds /sitemaps/filings-0..3.xml from the index it read on
    2026-09-26. A 404 on those sits in the report as an error for weeks; an
    empty urlset is how the protocol says "nothing here now". The route must
    also answer them WITHOUT calling fetchSitemapData, or an unpublished
    section still costs the API its query.
    """
    if "PUBLISH_FILINGS = true" in data_ts:
        pytest.skip("filings are published again; nothing to retire")
    assert "export const RETIRED_SECTIONS" in data_ts, (
        "filings are unpublished but no RETIRED_SECTIONS list resolves the "
        "children Google already knows"
    )
    route = ROUTE_TS.read_text()
    assert "RETIRED_SECTIONS.includes(section)" in route, (
        "the section route does not resolve retired sections, so previously "
        "submitted filings sitemaps now 404"
    )
    retired_at = route.index("RETIRED_SECTIONS.includes(section)")
    fetch_at = route.index("await fetchSitemapData()")
    assert retired_at < fetch_at, (
        "the retired-section branch runs after fetchSitemapData, so an "
        "unpublished section still pays for the URL list"
    )
    not_found_at = route.index('status: 404')
    assert retired_at < not_found_at, (
        "the 404 branch precedes the retired branch, so retired sections 404"
    )


def test_filing_pages_still_declare_a_self_canonical(data_ts):
    """Out of the sitemap is not out of the index.

    Filing pages stay linked from every company and insider page, so they are
    still crawled. Thirty thousand structurally identical pages with no declared
    canonical is how Google picks its own and attributes the wrong URL.
    """
    page = (ROOT / "frontend" / "src" / "app" / "filing" / "[id]" / "page.tsx").read_text()
    assert "alternates:" in page and "canonical:" in page, (
        "the filing page no longer declares a canonical"
    )


def test_api_ceiling_admits_what_the_client_asks_for():
    """The API's Query(le=...) must not sit below the client's request.

    It was pinned at the 50,000 PROTOCOL cap, which is a per-file property and
    has no business bounding a query that feeds several files. With the client
    asking for 60,000 the validator would have rejected the request outright.
    """
    api = API_PY.read_text()
    m = re.search(r"limit_insiders:\s*int\s*=\s*Query\((.*?)\)", api, re.S)
    assert m, "limit_insiders Query() not found"
    le = re.search(r"le=(\d+)", m.group(1))
    assert le, "limit_insiders has no le= ceiling"
    ceiling = int(le.group(1))

    data = DATA_TS.read_text()
    requested = _const(data, "INSIDER_CHUNKS") * _const(data, "CHUNK")
    assert ceiling >= requested, (
        f"API ceiling le={ceiling} is below the {requested} the client requests; "
        "the request would 422 and the section would render empty"
    )


def test_chunked_sections_filter_before_slicing():
    """Dropping rows after slicing leaves a gap no file covers.

    The insider list is filtered for missing ids. If that filter runs per-chunk
    after .slice(), a dropped row shortens one file without shifting the next,
    so an insider on the boundary falls out of the sitemap entirely.
    """
    route = ROUTE_TS.read_text()
    m = re.search(r'section\.startsWith\("insiders-"\)(.*?)\}\s*else', route, re.S)
    assert m, "insiders- branch not found in the section route"
    branch = m.group(1)
    filter_at = branch.find(".filter(")
    # The CHUNK slice specifically. `section.slice("insiders-".length)` parses
    # the chunk index out of the section name and sits above both of these, so
    # a bare ".slice(" search matches the wrong call and always fails.
    slice_m = re.search(r"\.slice\(\s*n\s*\*\s*CHUNK", branch)
    assert filter_at != -1, "expected a .filter() on the insider list"
    assert slice_m, "expected a .slice(n * CHUNK, ...) chunk slice"
    slice_at = slice_m.start()
    assert filter_at < slice_at, (
        "the insiders branch slices before filtering; a filtered-out row would "
        "then leave a hole at the chunk boundary"
    )


def test_the_index_does_not_pull_the_url_list():
    """The index only names sections; pulling 5 MB to count filings chunks
    made every crawler's index fetch cost three queries (2026-09-20)."""
    index = (ROUTE_TS.parents[2] / "sitemap.xml" / "route.ts").read_text()
    assert "fetchSitemapData" not in index
    assert "renderIndex(SECTIONS" in index


def test_the_api_serves_the_url_list_from_a_one_hour_cache():
    api = API_PY.read_text()
    assert "_CACHE_TTL_S = 3600" in api
    assert "_sitemap_urls_uncached(" in api
    assert 'if result["counts"]["tickers"]:' in api, "an empty answer from a DB hiccup must not be cached for an hour"
