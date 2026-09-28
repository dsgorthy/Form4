"""The sitemap floor must be stated in filings, and must fail OPEN.

Page indexing on 2026-09-20: 44.6K indexed, 126K not — 59,022 "Discovered -
currently not indexed" plus 56,197 "Crawled - currently not indexed", which is
91% of the not-indexed total. Google was finding these pages and declining them.

The rule that produced ~70,000 submitted URLs was
`insider_track_records.buy_count >= 2`, and buy_count counts EXECUTION LOTS, not
filings. Measured 2026-09-27: insider 14368 has 1,167 decision filings against a
buy_count+sell_count of 24,994; insider 84 has 1,539 against 4,114. A purchase
filled in five tranches counted five times, so ">= 2" admitted insiders who had
made exactly one decision — the same lot-vs-filing defect that cost A-List its
headline, fixed in the published track record and never here.

Two invariants are pinned:

1. The floor is a DECISION-FILING count from sitemap_quality_*, never a lot
   count, and the recency arm carries MIN_SCORED_FILINGS — below which the track
   record is suppressed, so the page renders nothing that is ours.
2. Every path FAILS OPEN. A missing, empty or stale quality table falls back to
   submitting more, never less. An empty sitemap has no other reader to notice
   it, and the 2026-04 outage was four months of nothing being indexed.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

ROOT = Path(__file__).resolve().parents[2]
API = ROOT / "api" / "routers" / "sitemap.py"
REFRESH = ROOT / "pipelines" / "insider_study" / "refresh_sitemap_quality.py"


def _code(src: str) -> str:
    """Prose out, SQL kept.

    A first version stripped every triple-quoted block as a docstring — which
    also deleted every SQL query in the file, since those are triple-quoted
    too, and four assertions then passed or exploded against an empty string.
    So: drop the MODULE docstring (the only long prose block), drop Python `#`
    lines, and drop SQL `--` comments. Function docstrings are short and
    deliberately do not contain the strings asserted on below.
    """
    src = src[src.index('"""', src.index('"""') + 3) + 3:]  # past the module docstring
    out = []
    for ln in src.splitlines():
        if ln.lstrip().startswith("#"):
            continue
        ln = re.sub(r"\s*--.*$", "", ln)   # SQL comment
        out.append(ln)
    return "\n".join(out)


@pytest.fixture(scope="module")
def api_src() -> str:
    return API.read_text()


@pytest.fixture(scope="module")
def api_code(api_src) -> str:
    return _code(api_src)


def test_the_floor_is_imported_not_retyped(api_src):
    """MIN_SCORED_FILINGS is the suppression floor for the track record. If it
    moves, the set of pages worth submitting moves with it."""
    assert "from api.routers.insiders import MIN_SCORED_FILINGS" in api_src, (
        "the sitemap no longer imports MIN_SCORED_FILINGS; a second copy of "
        "that number is a second definition of what a thin page is"
    )
    code = _code(api_src)
    assert not re.search(r"decision_filings\s*>=\s*5\b", code), (
        "the recency floor is written as a literal 5 instead of "
        "MIN_SCORED_FILINGS"
    )


def test_the_insider_rule_counts_filings_not_lots(api_code):
    """buy_count is a lot count and must not gate the primary query."""
    primary = api_code[: api_code.index("if not insiders")]
    assert "sitemap_quality_insiders" in primary, (
        "the primary insider query no longer reads the quality table"
    )
    assert "decision_filings" in primary
    assert "buy_count" not in primary, (
        "buy_count (an EXECUTION-LOT count) is back in the primary insider "
        "query; 24,994 lots against 1,167 filings on one insider"
    )


def test_both_entity_types_have_a_floor(api_code):
    for table in ("sitemap_quality_insiders", "sitemap_quality_companies"):
        assert table in api_code, f"{table} is not consulted"
    # Companies were submitted in full (18,267) with no floor at all.
    at = api_code.index("FROM sitemap_quality_companies")
    co = api_code[at: at + 900]
    assert "decision_filings" in co, "the company query applies no floor"


@pytest.mark.parametrize("table", ["sitemap_quality_insiders",
                                   "sitemap_quality_companies"])
def test_each_quality_read_is_staleness_checked(api_code, table):
    """A frozen quality table does not error. It quietly stops admitting the
    pages that became eligible since it froze, which is invisible."""
    at = api_code.index(table)
    window = api_code[max(0, at - 600):at]
    assert "_quality_is_usable" in window, (
        f"{table} is queried without a _quality_is_usable() check above it"
    )


def test_every_path_fails_open(api_code):
    """Falling back must submit MORE, not nothing."""
    # Both entity types keep their previous query as the fallback.
    assert "insider_track_records" in api_code, (
        "the old insider rule was deleted rather than kept as the fallback; "
        "a missing quality table would then publish zero insider URLs"
    )
    assert api_code.count("if not tickers") == 1, (
        "the tickers path has no empty-result fallback"
    )
    assert api_code.count("if not insiders") == 1, (
        "the insiders path has no empty-result fallback"
    )


def test_the_staleness_window_is_wider_than_the_refresh_cadence(api_src):
    """A 1-day window on a daily job pages on the first missed run. See the
    memory `feedback_monitor_budgets_follow_schedules`."""
    m = re.search(r"QUALITY_MAX_AGE_DAYS\s*=\s*(\d+)", api_src)
    assert m, "QUALITY_MAX_AGE_DAYS not found"
    assert int(m.group(1)) >= 2, (
        f"a {m.group(1)}-day staleness window on a daily refresh falls back on "
        "the first missed run"
    )


def test_the_ordering_is_total_so_the_submitted_set_is_stable(api_code):
    """1,347 insider URLs (13%) churned in and out between two generations in
    2026-08 because the ORDER BY had a huge tied block and the LIMIT cut
    through it. A URL that appears and vanishes between crawls is a stability
    signal we do not want to send."""
    at = api_code.index("FROM sitemap_quality_insiders")
    q = api_code[at: at + 1200]
    order = q[q.index("ORDER BY"): q.index("LIMIT")]
    assert "insider_id" in order, (
        "the insider ORDER BY does not end in a unique column, so ties are "
        "broken by whatever Postgres returns and the set churns"
    )


def test_the_refresh_never_swaps_in_an_empty_table():
    src = REFRESH.read_text()
    assert "if n == 0" in src and "refusing the swap" in src, (
        "the refresh would swap in an empty table, which unpublishes the site"
    )
    code = _code(src)
    assert "lock_timeout" in code, (
        "no lock_timeout on a script that renames a table the API reads; a "
        "queued ALTER blocks every later read (form4.app, 2026-08-27)"
    )


def test_the_refresh_derives_the_classes_and_stamps_its_run():
    src = REFRESH.read_text()
    assert "from api.filters import MEANINGFUL_CLASSES" in src, (
        "the refresh types the signal classes instead of deriving them"
    )
    assert not re.search(r"'discretionary_buy'", _code(src)), (
        "a class name is typed out in code; derive from MEANINGFUL_CLASSES"
    )
    assert "refreshed_at=" in src, (
        "the refresh does not stamp the table, so _quality_is_usable can never "
        "tell a fresh table from a frozen one and always falls back"
    )
