"""No sitemap response may grow past the size Next.js will cache.

## What happened on 2026-10-01

Every sitemap file fetched the ENTIRE corpus from `/api/v1/sitemap/urls` and
sliced its own 20,000 URLs out of it client-side. Four live files, so four full
transfers and four full JSON parses per crawl, of a 2.33 MB payload holding
10,683 tickers, 30,764 insider objects and 28,828 filing IDs.

None of them were cached. **Next refuses to write a fetch response over 2 MB
into its data cache, and it does not raise — it logs one line and serves the
request.** So `next: { revalidate: 3600 }` looked like it was working while
every single crawler request re-fetched and re-parsed the whole corpus.

30,764 JSON objects inflate roughly tenfold as live JS objects. The frontend's
Node heap reached its 2,080 MB ceiling and the container was OOM-killed nine
times. 32% of all origin requests returned 502 and p95 on /filing/ reached
43 seconds — while `/api/v1/health` answered 200 the entire time, which is why
nothing paged.

## Why a test can cover it now, and could not before

The payload crossed the ceiling because the DATA grew. No code changed, so
there was no commit for a test to fail on: the only honest guard for that shape
of bug is a runtime check.

Slicing server-side changes the shape of the problem. A section now holds at
most `CHUNK` rows, so its size is bounded by a CONSTANT rather than by a
population that grows nightly. That is a thing a test can hold down, and these
tests hold it down at the worst case the type system allows rather than at
today's data.

Both guards are kept — the test for the bound, and the runtime check for
anything the bound does not anticipate.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

import api.routers.sitemap as sitemap  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
DATA_TS = ROOT / "frontend" / "src" / "lib" / "sitemap-data.ts"
ROUTE_TS = ROOT / "frontend" / "src" / "app" / "sitemaps" / "[section]" / "route.ts"

#: The longest slug `slugifyName` in frontend/src/lib/insider-url.ts can emit.
#: It hard-slices at 60 characters.
MAX_SLUG_LEN = 60


def _chunk() -> int:
    m = re.search(r"export const CHUNK\s*=\s*(\d+)", DATA_TS.read_text())
    assert m, "CHUNK is no longer a literal in sitemap-data.ts"
    return int(m.group(1))


def _size(payload: dict) -> int:
    return len(json.dumps(payload, separators=(",", ":")).encode())


def _worst_case_insiders(n: int) -> list[dict]:
    """`n` insiders, each as large as the URL rules permit one to be."""
    return [{"id": "abcdef", "slug": "x" * MAX_SLUG_LEN} for _ in range(n)]


def test_the_alarm_sits_below_the_ceiling_it_protects():
    assert sitemap.CACHEABLE_WARN_BYTES < sitemap.NEXT_DATA_CACHE_MAX_BYTES, (
        "the warn budget is at or above Next's hard ceiling, so the alarm "
        "fires only once caching has already stopped"
    )
    assert sitemap.NEXT_DATA_CACHE_MAX_BYTES == 2 * 1024 * 1024, (
        "Next's data-cache ceiling is 2 MB; if that changed, change it here "
        "deliberately and re-measure the budget below it"
    )


def test_a_full_insider_section_fits_the_budget_at_its_worst_case():
    """THE CENTRAL INVARIANT.

    Not "today's corpus fits" — today's corpus fitting is what was true right
    before this broke. A section holds at most CHUNK rows, so the question is
    whether CHUNK rows of the largest legal shape fit.
    """
    chunk = _chunk()
    full = {
        "tickers": [],
        "insiders": _worst_case_insiders(chunk * 3),
        "filings": [],
        "counts": {"tickers": 0, "insiders": chunk * 3, "filings": 0},
    }
    biggest = max(
        _size(sitemap.project_section(full, "insiders", n, chunk))
        for n in range(3)
    )
    assert biggest <= sitemap.CACHEABLE_WARN_BYTES, (
        f"a CHUNK={chunk} insider section reaches {biggest:,} bytes at worst "
        f"case, over the {sitemap.CACHEABLE_WARN_BYTES:,}-byte budget. Lower "
        f"CHUNK in sitemap-data.ts (and raise INSIDER_CHUNKS to cover the "
        f"population) rather than raising the budget — the budget exists to "
        f"keep the response inside Next's "
        f"{sitemap.NEXT_DATA_CACHE_MAX_BYTES:,}-byte data cache."
    )


def test_every_section_the_client_asks_for_reports_itself_cacheable():
    chunk = _chunk()
    full = {
        "tickers": [f"TICK{i}" for i in range(12_000)],
        "insiders": _worst_case_insiders(chunk),
        "filings": [f"fil{i}" for i in range(40_000)],
        "counts": {"tickers": 12_000, "insiders": chunk, "filings": 40_000},
    }
    for section in ("companies", "insiders", "filings"):
        out = sitemap.project_section(full, section, 0, chunk)
        assert _size(out) <= sitemap.CACHEABLE_WARN_BYTES, (
            f"section {section} is {_size(out):,} bytes, over budget"
        )


def test_the_whole_corpus_response_is_the_one_that_cannot_fit():
    """`section=all` is kept only for rolling-deploy skew, and is exempt.

    Pinned so nobody "fixes" the exemption by deleting it and then wonders why
    a mid-deploy frontend serves empty sitemaps — and so nobody mistakes the
    exemption for the whole-corpus shape being safe to use.
    """
    assert "all" in sitemap.SECTIONS
    chunk = _chunk()
    full = {
        "tickers": [f"TICK{i}" for i in range(12_000)],
        "insiders": _worst_case_insiders(chunk * 3),
        "filings": [f"fil{i}" for i in range(40_000)],
        "counts": {"tickers": 12_000, "insiders": chunk * 3, "filings": 40_000},
    }
    assert _size(sitemap.project_section(full, "all", 0, chunk)) > \
        sitemap.NEXT_DATA_CACHE_MAX_BYTES, (
        "the whole-corpus shape now fits the cache, which means the sections "
        "shrank enough that this test no longer describes anything — re-read "
        "the module docstring before deleting it"
    )


def test_the_client_never_asks_for_the_whole_corpus():
    """The regression that matters: one un-sectioned fetch undoes all of this."""
    data = DATA_TS.read_text()

    # Exactly one place may reach the endpoint. A second one is how an
    # un-sectioned fetch creeps back in.
    hits = re.findall(r"sitemap/urls", data)
    assert len(hits) == 1, (
        f"{len(hits)} places fetch sitemap/urls; there must be exactly one, "
        "inside fetchSection, or an un-sectioned whole-corpus request can "
        "return without this test noticing"
    )

    body = re.search(r"async function fetchSection\(.*?\n}\n", data, re.S)
    assert body, "fetchSection is gone from sitemap-data.ts"
    body = body.group(0)
    assert "sitemap/urls" in body, (
        "the sitemap/urls fetch moved out of fetchSection, which is the "
        "function that guarantees a section is named"
    )
    # The section must reach the query string, not merely be a parameter.
    assert re.search(r"section,|section:\s*section|section:\s*String\(", body), (
        "fetchSection takes a section but does not put it in the query, so "
        "every call still returns the whole corpus"
    )


def test_the_route_no_longer_slices_client_side():
    """Client-side slicing means the client holds the whole list to slice it."""
    route = ROUTE_TS.read_text()
    assert not re.search(r"\.slice\(\s*n\s*\*\s*CHUNK", route), (
        "the section route still slices by CHUNK, so it is still fetching "
        "every URL in order to keep 20,000 of them"
    )
    assert "fetchInsiderChunk(n)" in route, (
        "the insiders branch does not request its own chunk from the API"
    )


def test_the_name_is_not_sent_when_a_slug_exists():
    """What halved the insider payload, and the reason it was safe.

    `insiderPath` returns `/insider/{slug}` the moment a slug is present and
    never reads the name. Coverage measured 2026-10-01: 30,764 of 30,764.
    """
    class Row(dict):
        def __getitem__(self, k):
            return dict.get(self, k)

    shaped = sitemap._as_insider_list([
        Row(insider_id=1, name="Jensen Huang", slug="jensen-huang"),
        Row(insider_id=2, name="No Slug Here", slug=""),
    ])
    assert "name" not in shaped[0], (
        "the name is being sent alongside a slug; insiderPath ignores it and "
        "it is what pushed this payload past the cache ceiling"
    )
    assert shaped[0]["slug"] == "jensen-huang"
    assert shaped[1]["name"] == "No Slug Here", (
        "a row with no slug lost its name, so insiderPath cannot build the "
        "/insider/{derived-name}-{id} fallback"
    )


def test_counts_survive_projection_so_a_monitor_can_see_a_collapse():
    """A section carries the FULL totals, not just its own slice.

    An empty chunk is indistinguishable from a failed fetch unless something
    reports what the corpus holds — the failure mode the sitemap header has
    warned about since the silent-shrink bug.
    """
    chunk = _chunk()
    full = {
        "tickers": ["AAA"],
        "insiders": _worst_case_insiders(5),
        "filings": [],
        "counts": {"tickers": 1, "insiders": 5, "filings": 0},
    }
    far_past_the_end = sitemap.project_section(full, "insiders", 9, chunk)
    assert far_past_the_end["insiders"] == []
    assert far_past_the_end["counts"]["insiders"] == 5, (
        "a past-the-end chunk reports no corpus total, so an empty sitemap "
        "cannot be told from a broken one"
    )
    assert far_past_the_end["returned"]["insiders"] == 0
