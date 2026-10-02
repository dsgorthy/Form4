"""The watchdog must notice the frontend running out of heap, and an
uncacheable fetch long before it does.

## Why this exists

On 2026-10-01 form4.app spent roughly forty minutes serving 502 to a third of
its requests, with p95 on /filing/ at 43 seconds, and the alert came from Derek
looking at the site. Every monitor was green and each one was correct:

  * `/api/v1/health` returned 200 — the API was genuinely healthy
  * Postgres answered in 0.01s from inside the API container
  * Dagster had 50 successful runs in the hour
  * `launchctl list` showed every must-run agent with a live pid
  * TIME_WAIT was 12% of the ephemeral range

The failure was entirely inside the Next.js process: a 2.33 MB sitemap payload
that Next declines to put in its data cache. It does not raise on that — it
logs one line and serves the request — so every crawler request re-fetched and
re-parsed 30,764 insider objects into a 2,080 MB heap until the container was
OOM-killed, nine times.

The sitemap cause is fixed and bounded by
`test_sitemap_payload_stays_cacheable.py`. These tests cover the gap that made
it expensive: nothing was watching the heap, so no amount of fixing one cause
would have told us about the next one.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "offbox_watchdog",
    Path(__file__).resolve().parents[2] / "scripts" / "offbox_watchdog.py",
)
watchdog = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(watchdog)


# Real lines, copied from the container log during the incident.
UNCACHEABLE = (
    "Failed to set Next.js data cache for "
    "http://api:8000/api/v1/sitemap/urls?limit_insiders=60000&filing_days=90, "
    "items over 2MB can not be cached (3255845 bytes)"
)
GC_SPIRAL = (
    "[1:0xfbaf4e620000]  4455220 ms: Mark-Compact 1843.7 (2080.2) -> 1820.0 "
    "(2081.2) MB, pooled: 5 MB, 461.35 / 0.03 ms\n"
    "[1:0xfbaf4e620000]  4455788 ms: Mark-Compact 1967.1 (2080.6) -> 1945.8 "
    "(2076.1) MB, pooled: 10 MB, 537.28 / 0.02 ms\n"
)
OOM = "FATAL ERROR: Ineffective mark-compacts near heap limit Allocation failed - JavaScript heap out of memory"

HEALTHY = (
    "[1:0x118008000]   12033 ms: Mark-Compact 412.6 (524.1) -> 398.2 (501.3) MB\n"
    "  ▲ Next.js 16.1.6\n  ✓ Ready in 39ms\n"
)


def test_a_quiet_frontend_is_clean():
    assert watchdog.evaluate_frontend_heap(HEALTHY) == []
    assert watchdog.evaluate_frontend_heap("") == []


def test_the_uncacheable_fetch_is_caught_and_the_route_is_named():
    """This is the EARLY signal — it appeared for days before the OOM."""
    (problem,) = watchdog.evaluate_frontend_heap(UNCACHEABLE)
    assert "UNCACHEABLE" in problem
    # The reader has to know which response to shrink.
    assert "/api/v1/sitemap/urls" in problem
    # ...and not be handed the query string as if it mattered.
    assert "limit_insiders" not in problem


def test_the_gc_spiral_is_caught_before_the_process_dies():
    """1,967 MB of a 2,080 MB ceiling is a slow site, not yet a dead one."""
    (problem,) = watchdog.evaluate_frontend_heap(GC_SPIRAL)
    # The WORST cycle in the window, not the first one it happened to match.
    assert "1967 MB" in problem, problem
    assert "2081 MB" in problem, problem
    assert "2 GC cycles" in problem, problem
    assert "GC spiral" in problem


def test_the_oom_itself_is_caught():
    problems = watchdog.evaluate_frontend_heap(OOM)
    assert any("out of memory" in p for p in problems)


def test_the_whole_incident_reports_all_three_signals():
    problems = watchdog.evaluate_frontend_heap(UNCACHEABLE + "\n" + GC_SPIRAL + OOM)
    assert len(problems) == 3, problems


def test_a_healthy_heap_near_a_small_ceiling_does_not_alarm():
    """The alarm is a FRACTION of the ceiling the process reports, not a fixed
    megabyte count — a container started with a different --max-old-space-size
    must not read as either permanently sick or permanently fine."""
    small = "Mark-Compact 300.0 (1024.0) -> 280.0 (1020.0) MB"
    assert watchdog.evaluate_frontend_heap(small) == []
    big_but_fine = "Mark-Compact 2000.0 (8192.0) -> 1900.0 (8100.0) MB"
    assert watchdog.evaluate_frontend_heap(big_but_fine) == []


# ── the sitemap section self-report ─────────────────────────────────────────

def _ok(label="insiders-0"):
    return {label: {"counts": {"tickers": 10683, "insiders": 30764, "filings": 28828},
                    "returned": {"tickers": 0, "insiders": 20000, "filings": 0},
                    "payload_bytes": 836_967, "cacheable": True}}


def test_a_cacheable_section_is_clean():
    assert watchdog.evaluate_sitemap_sections(_ok()) == []


def test_a_section_that_reports_itself_uncacheable_pages():
    bad = _ok()
    bad["insiders-0"].update(cacheable=False, payload_bytes=2_442_014)
    (problem,) = watchdog.evaluate_sitemap_sections(bad)
    assert "NOT cacheable" in problem
    assert "2,442,014" in problem, "the reader needs the measured size"
    assert "CHUNK" in problem, "the problem must name the lever that fixes it"


def test_a_failed_fetch_is_not_silence():
    (problem,) = watchdog.evaluate_sitemap_sections({"insiders-0": {}})
    assert "fetch failed" in problem


def test_an_empty_corpus_pages_even_though_the_response_is_valid():
    """The silent-shrink failure. An empty <urlset> is valid XML and a valid
    sitemap; it is also what a collapsed query looks like. Only the full-corpus
    counts can tell them apart, which is why a section response carries them."""
    empty = _ok()
    empty["insiders-0"]["counts"] = {"tickers": 0, "insiders": 0, "filings": 0}
    problems = watchdog.evaluate_sitemap_sections(empty)
    assert any("empty urlset" in p for p in problems)


def test_the_watchdog_asks_about_both_the_bounded_and_unbounded_sections():
    """`companies` is the one with no chunking of its own, so it is the one
    that can grow past the budget without any constant changing."""
    assert "companies" in watchdog.SITEMAP_SECTIONS
    assert "insiders-0" in watchdog.SITEMAP_SECTIONS
    for qs in watchdog.SITEMAP_SECTIONS.values():
        assert "section=" in qs, "a probe that names no section fetches the whole corpus"


def test_the_heap_probe_reads_the_frontend_not_the_api():
    """The API was healthy throughout the incident; reading its log would have
    reported nothing wrong, which is exactly what every other monitor did."""
    assert "trading-framework-frontend-1" in watchdog.FRONTEND_LOG_CMD
    assert "trading-framework-api-1" not in watchdog.FRONTEND_LOG_CMD
    # Must not drag the whole log across ssh every cycle.
    assert "--tail" in watchdog.FRONTEND_LOG_CMD
    for marker in ("can not be cached", "heap out of memory", "Mark-Compact"):
        assert marker in watchdog.FRONTEND_LOG_CMD, (
            f"the probe does not grep for {marker!r}, so "
            f"evaluate_frontend_heap can never see it"
        )
