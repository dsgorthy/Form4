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
    assert chunks * chunk >= 51_747, (
        f"capacity {chunks * chunk} is below the {51_747} insiders eligible as of "
        "2026-09-10; raise INSIDER_CHUNKS"
    )


def test_no_growing_section_is_emitted_as_a_single_file(data_ts):
    """Sections sourced from a growing population must be chunked.

    'insiders' and 'filings' both scale with the database. A bare entry in
    SECTIONS means one file for the whole population, which is the shape that
    caused the outage.
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
        assert f"{name}-$" in body, f"no chunked {name} entries in SECTIONS"


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
