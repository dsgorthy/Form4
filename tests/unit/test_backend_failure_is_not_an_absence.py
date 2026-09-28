"""A broken backend must never be published to a crawler as a missing page.

THIS IS THE BUG THAT EMPTIED THE INDEX.

`fetchAPIAuth` threw one undifferentiated Error for every failure — a 404, a
502, a dropped connection, all the same. Every indexable page wrapped it in
`catch { notFound() }`. And the not-found route answers **HTTP 200** with
`<meta name="robots" content="noindex">`, because `generateMetadata` resolves and
flushes the document head before the page body runs, so `notFound()` can no
longer set a status.

So a backend blip made the page tell Googlebot: *200 OK, this page exists, do not
index it.* Google complies at once, and a successful fetch gives it no reason to
come back.

The data, from Search Console's own daily totals:

    08-24   149 impressions      09-14  4238
    09-03   809                  09-15  7040
    09-07  2634                  09-16  3035
    09-09  3469                  09-17   258   <- cliff
    09-13  2419                  09-20    45

Impressions were CLIMBING from 149 to ~3,000 a day, with 5-16 clicks a day, then
fell to 2% in one day and kept decaying. Crawl backoff cannot do that: indexed
pages keep ranking when crawling slows, only new discovery stops. Removal can.
The window is 09-16 to 09-20 — a 5.5h outage followed by four days in which a
launchd agent's 256-fd default dropped 302 connections, so the site looked up
while individual frontend-to-API fetches failed.

Confirmed from origin logs on 09-27, after eleven days: Googlebot 4 requests,
bingbot 0, while Applebot, GPTBot and meta-externalagent each pulled ~2,400.
Google and Bing honour noindex. The others do not rank anything.

THE INVARIANT: only a definite 404 may render a not-found response. Everything
else must propagate so Next serves a 500, which Google retries and which leaves
the URL in the index. A spurious 500 costs one retry. A spurious not-found costs
the page.

Full account: the memory `project_2026-09-27_noindex_deindexed_the_site`.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "frontend" / "src"
AUTH = SRC / "lib" / "auth.ts"
NOT_FOUND = SRC / "app" / "not-found.tsx"

#: Pages a crawler can reach that resolve an entity through the API. Each must
#: gate its notFound() on a definite 404.
INDEXABLE = [
    "app/company/[ticker]/page.tsx",
    "app/insider/[id]/page.tsx",
    "app/filing/[id]/page.tsx",
    "app/company/private/[slug]/page.tsx",
]


def _code(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def test_the_fetch_helper_distinguishes_missing_from_broken():
    code = _code(AUTH.read_text())
    assert "class ApiError" in code and "class ApiUnreachable" in code, (
        "fetchAPIAuth no longer has distinct error types, so every caller is "
        "back to treating a 502 as a missing page"
    )
    assert "export function isEntityMissing" in code, (
        "isEntityMissing is gone; callers have no way to tell a 404 from a 500"
    )
    assert re.search(r"isEntityMissing[\s\S]{0,200}status === 404", code), (
        "isEntityMissing no longer keys on a 404 specifically"
    )


def test_a_network_failure_is_not_reported_as_a_status():
    """`fetch` rejects on a refused or reset connection. Left unwrapped that
    surfaces as a TypeError, which `isEntityMissing` correctly rejects — but
    wrapping it makes the intent explicit and keeps the classification in one
    place."""
    code = _code(AUTH.read_text())
    # Scoped to the region between the declaration and the status check. A
    # looser regex matched the res.json() catch further down, which throws the
    # same error type — so the assertion passed with the fetch left unwrapped.
    # Mutation testing caught it.
    start = code.index("let res: Response;")
    region = code[start: code.index("if (!res.ok)", start)]
    assert "try {" in region and "catch" in region and "ApiUnreachable" in region, (
        "the fetch call is not wrapped, so a dropped connection escapes as an "
        "untyped error — which is exactly the 09-16 to 09-20 failure mode"
    )


def test_the_403_message_format_is_preserved():
    """The insider page renders its upgrade prompt on
    `e.message?.includes("403")`. Changing the message silently turns a gated
    profile into a not-found page."""
    code = _code(AUTH.read_text())
    assert "`API error: ${status}`" in code, (
        "the ApiError message format changed; the insider page matches on the "
        'literal "403" inside it'
    )
    insider = _code((SRC / "app" / "insider" / "[id]" / "page.tsx").read_text())
    assert '"403"' in insider, "the insider page lost its gated-profile branch"


@pytest.mark.parametrize("rel", INDEXABLE)
def test_only_a_definite_404_renders_not_found(rel):
    code = _code((SRC / rel).read_text())
    for m in re.finditer(r"notFound\(\);", code):
        # The guard must sit in the same catch block, above the call.
        window = code[max(0, m.start() - 700): m.start()]
        assert "isEntityMissing" in window, (
            f"{rel} calls notFound() without first checking isEntityMissing. A "
            "backend failure there answers 200 with noindex and asks Google to "
            "remove a page that exists."
        )


def test_the_insider_page_rethrows_server_errors_before_anything_else():
    """Its catch has three outcomes — gated, missing, broken — and the broken
    branch has to win, because a 5xx must never be rendered at all."""
    code = _code((SRC / "app" / "insider" / "[id]" / "page.tsx").read_text())
    at = code.index("} catch (e: any) {")
    block = code[at: at + 900]
    rethrow = block.find("throw e")
    gated = block.find('"403"')
    assert rethrow != -1, "the insider catch never rethrows a server error"
    assert rethrow < gated, (
        "the 403 branch is checked before the server-error rethrow, so a 502 "
        "could render the upgrade prompt instead of a retryable 500"
    )


def test_generate_metadata_does_not_publish_a_noindex_on_a_server_error():
    """Metadata resolves FIRST and flushes the head. A 5xx that returns a
    noindex title there stamps the instruction into a page that exists."""
    code = _code((SRC / "app" / "insider" / "[id]" / "page.tsx").read_text())
    head = code[: code.index("export default")]
    assert re.search(r"res\.status >= 500[\s\S]{0,200}throw", head), (
        "generateMetadata does not rethrow on a 5xx, so a backend error still "
        "produces a noindex document"
    )
    noindex_at = head.index("robots: { index: false")
    throw_at = head.index("res.status >= 500")
    assert throw_at < noindex_at, (
        "the 5xx rethrow sits below the noindex return, so the noindex wins"
    )


def test_the_not_found_route_keeps_its_noindex_while_it_answers_200():
    """The two must move together.

    A real 404 would let the noindex come off. It is NOT achievable cheaply:
    `generateMetadata` resolves and flushes the head before the body runs, so
    `notFound()` cannot set a status — the same constraint documented on the
    insider page's redirect. Until the status is genuinely 404, the noindex is
    the only thing keeping bogus URLs out of the index, and removing it while
    the status is still 200 would publish thin duplicates at scale.
    """
    src = NOT_FOUND.read_text()
    assert "index: false" in _code(src), (
        "the not-found route dropped its noindex. That is only safe once it "
        "answers a real 404 — verify with `curl -o /dev/null -w '%{http_code}' "
        "https://form4.app/company/ZZZZQQ` and update this test when it does"
    )
