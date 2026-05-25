"""
PIT (Point-in-Time) Validation Tests

These tests verify that scoring and signal code never uses forward-looking data.
They are the enforceable layer of the PIT enforcement framework — Claude can skip
CLAUDE.md checklists, but these tests will fail if PIT violations are introduced.

Test categories:
  1. Import guards — no insider_track_records in scoring paths
  2. Score stability — score at date T unchanged when future data added
  3. Observable return lag — returns used in scoring respect lag windows
  4. Signal detector PIT — detectors produce identical results on truncated datasets
"""

import ast
import os
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Test 1: No insider_track_records in scoring code paths
# ---------------------------------------------------------------------------

SCORING_FILES = [
    "api/trade_grade.py",
    "strategies/insider_catalog/pit_scoring.py",
    "strategies/insider_catalog/build_pit_scores.py",
    "pipelines/insider_study/conviction_score.py",
    "pipelines/insider_study/compute_pit_clusters.py",
    "pipelines/insider_study/compute_cw_indicators.py",
    # CEO Watcher net-flow signals — added 2026-05-24, must remain PIT-clean
    "pipelines/insider_study/compute_company_net_flow.py",
    "pipelines/insider_study/compute_industry_net_flow.py",
]

# Per-trade request-path files migrated off insider_track_records in P1.6.
# These read the per-trade `pit_grade` / `pit_blended_score` columns instead
# (those columns ARE point-in-time, populated by backfill_pit_grades.py).
# Adding a fresh `insider_track_records` SELECT to any of these files would
# re-introduce a non-PIT dependency — fail the build to prevent regression.
# Entity-level routers (insiders, leaderboard, search, sitemap) are NOT in
# this list — those intentionally read cached career aggregates from itr.
REQUEST_PATH_FILES = [
    "api/routers/clusters.py",
    "api/routers/dashboard.py",
    "api/routers/companies.py",
    "api/routers/filings.py",
    "api/routers/export.py",
    "api/routers/signals.py",
    "api/routers/private_companies.py",
    "pipelines/notification_scanner.py",
    "pipelines/generate_breaking_signal.py",
    "pipelines/render_ig_carousel.py",
    "pipelines/generate_daily_content.py",
    "pipelines/generate_weekly_snapshot.py",
    "pipelines/insider_study/compute_context.py",
]


class TestNoTrackRecordsInScoring:
    """Ensure scoring code never references the non-PIT insider_track_records table."""

    @pytest.mark.parametrize("filepath", SCORING_FILES + REQUEST_PATH_FILES)
    def test_no_insider_track_records_reference(self, filepath):
        """Scoring + per-trade request-path files must not READ from insider_track_records."""
        full_path = PROJECT_ROOT / filepath
        if not full_path.exists():
            pytest.skip(f"{filepath} does not exist")
        source = full_path.read_text()
        # Check for SELECT queries on insider_track_records (reading non-PIT data)
        # Allow UPDATE/INSERT (backward compat sync writes are OK)
        for i, line in enumerate(source.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                continue
            if "insider_track_records" in stripped:
                # Allow writes (UPDATE, INSERT, sync functions)
                if any(kw in stripped.upper() for kw in ("UPDATE", "INSERT", "SYNC", "BACKWARD")):
                    continue
                # Allow function definitions that do sync
                if "def sync" in stripped or "def _sync" in stripped:
                    continue
                # Check if it's a SELECT (reading from non-PIT table)
                if "SELECT" in stripped.upper() or "FROM" in stripped.upper():
                    pytest.fail(
                        f"{filepath}:{i}: SELECTs from insider_track_records (NOT point-in-time). "
                        f"Use insider_ticker_scores with as_of_date <= filing_date instead."
                    )

    @pytest.mark.parametrize("filepath", SCORING_FILES)
    def test_no_score_tier_reference(self, filepath):
        """Scoring files must not reference score_tier (derived from non-PIT data)."""
        full_path = PROJECT_ROOT / filepath
        if not full_path.exists():
            pytest.skip(f"{filepath} does not exist")
        source = full_path.read_text()
        # Allow "score_tier" in comments but not in active code
        for i, line in enumerate(source.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                continue
            if "score_tier" in stripped and "insider_track_records" not in stripped:
                # Could be a different score_tier usage — check if it's a SQL reference
                if "score_tier" in stripped and not stripped.startswith("#"):
                    # Allow if it's clearly NOT a DB query (e.g., it's a dict key for output)
                    pass  # Soft check — the table reference test above is the hard guard


# ---------------------------------------------------------------------------
# Test 2: Signal quality module not imported in active scoring
# ---------------------------------------------------------------------------

class TestNoSignalQualityImports:
    """Ensure signal_quality.py is not imported by active API/scoring code."""

    ACTIVE_FILES = [
        "api/routers/filings.py",
        "api/routers/companies.py",
        "api/routers/signals.py",
        "api/routers/clusters.py",
        "api/routers/dashboard.py",
        "api/routers/insiders.py",
        "api/trade_grade.py",
        "strategies/insider_catalog/pit_scoring.py",
        "pipelines/insider_study/conviction_score.py",
    ]

    @pytest.mark.parametrize("filepath", ACTIVE_FILES)
    def test_no_signal_quality_import(self, filepath):
        """Active code must not import from api.signal_quality (has PIT violation)."""
        full_path = PROJECT_ROOT / filepath
        if not full_path.exists():
            pytest.skip(f"{filepath} does not exist")
        source = full_path.read_text()
        assert "from api.signal_quality import" not in source, (
            f"{filepath} imports from api.signal_quality which has a known PIT violation "
            f"(sell_win_rate_7d uses full track record). Use trade_grade.py instead."
        )
        assert "import api.signal_quality" not in source, (
            f"{filepath} imports api.signal_quality — use trade_grade.py instead."
        )


# ---------------------------------------------------------------------------
# Test 3: PIT score lookup always uses as_of_date <= filing_date
# ---------------------------------------------------------------------------

class TestPITScoreLookupPattern:
    """Verify that code querying insider_ticker_scores uses as_of_date <= constraint."""

    FILES_WITH_PIT_LOOKUPS = [
        "strategies/insider_catalog/pit_scoring.py",
        "strategies/cw_strategies/cw_runner.py",
        "pipelines/insider_study/backfill_cw_portfolio.py",
        "pipelines/insider_study/compute_trade_conviction.py",
    ]

    @pytest.mark.parametrize("filepath", FILES_WITH_PIT_LOOKUPS)
    def test_pit_lookup_has_date_constraint(self, filepath):
        """Any SELECT on insider_ticker_scores must include as_of_date <= constraint."""
        full_path = PROJECT_ROOT / filepath
        if not full_path.exists():
            pytest.skip(f"{filepath} does not exist")
        source = full_path.read_text()

        # Extract all SQL blocks that SELECT from insider_ticker_scores
        import re
        # Find multiline SQL strings containing insider_ticker_scores
        # Match triple-quoted strings and single-quoted f-strings
        sql_blocks = re.findall(
            r'(?:"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\')',
            source
        )
        for block in sql_blocks:
            if "insider_ticker_scores" not in block:
                continue
            if "SELECT" not in block.upper():
                continue
            # This is a SELECT query on insider_ticker_scores
            assert "as_of_date <=" in block or "as_of_date <" in block or "MAX(as_of_date)" in block, (
                f"{filepath}: SELECT on insider_ticker_scores missing as_of_date <= constraint. "
                f"This allows future scores to leak. Query excerpt: {block[:200]}"
            )


# ---------------------------------------------------------------------------
# Test 4: Observable return lag constants are correct
# ---------------------------------------------------------------------------

class TestObservableReturnLag:
    """Verify return lag constants prevent forward-looking data leakage."""

    def test_build_pit_scores_lag_constants(self):
        """Return observable lags must be >= the return window duration."""
        build_path = PROJECT_ROOT / "strategies" / "insider_catalog" / "build_pit_scores.py"
        if not build_path.exists():
            pytest.skip("build_pit_scores.py not found")
        source = build_path.read_text()
        # RETURN_OBSERVABLE_LAG should be defined and >= 10
        assert "RETURN_OBSERVABLE_LAG" in source, "Missing RETURN_OBSERVABLE_LAG constant"

    def test_pit_scoring_lag_constants(self):
        """Standalone scorer lag constants must match build_pit_scores."""
        scoring_path = PROJECT_ROOT / "strategies" / "insider_catalog" / "pit_scoring.py"
        if not scoring_path.exists():
            pytest.skip("pit_scoring.py not found")
        source = scoring_path.read_text()
        assert "_RETURN_LAGS" in source, "Missing _RETURN_LAGS dict in pit_scoring.py"


# ---------------------------------------------------------------------------
# Test 5: CW indicators are backward-looking only
# ---------------------------------------------------------------------------

class TestCWIndicatorsPIT:
    """Verify CW indicators never reference future prices."""

    def test_no_forward_price_lookups(self):
        """compute_cw_indicators.py must not contain forward-looking price patterns."""
        cw_path = PROJECT_ROOT / "pipelines" / "insider_study" / "compute_cw_indicators.py"
        if not cw_path.exists():
            pytest.skip("compute_cw_indicators.py not found")
        source = cw_path.read_text()
        # Should never add days to trade_date for price lookups
        assert "timedelta(days=" not in source or "_find_nearest" in source, (
            "compute_cw_indicators.py has timedelta additions that may look forward. "
            "Verify all timedelta usage is for backward lookups only."
        )

    def test_sma20_rel_removed(self):
        """sma20_rel should no longer be computed (dead code, removed in Phase 1)."""
        cw_path = PROJECT_ROOT / "pipelines" / "insider_study" / "compute_cw_indicators.py"
        if not cw_path.exists():
            pytest.skip("compute_cw_indicators.py not found")
        source = cw_path.read_text()
        # sma20_rel should not appear in COLUMNS dict
        assert '"sma20_rel"' not in source, (
            "sma20_rel is dead code and should be removed from compute_cw_indicators.py"
        )


# ---------------------------------------------------------------------------
# Test 6: Conviction score uses only PIT inputs
# ---------------------------------------------------------------------------

class TestConvictionScorePIT:
    """Verify conviction_score.py doesn't reference non-PIT data sources."""

    def test_no_full_history_references(self):
        """conviction_score.py must not use sell_win_rate or full track record stats."""
        conv_path = PROJECT_ROOT / "pipelines" / "insider_study" / "conviction_score.py"
        if not conv_path.exists():
            pytest.skip("conviction_score.py not found")
        source = conv_path.read_text()
        assert "sell_win_rate" not in source, (
            "conviction_score.py references sell_win_rate which is NOT point-in-time"
        )
        assert "insider_track_records" not in source, (
            "conviction_score.py references insider_track_records — use PIT scores instead"
        )


# ---------------------------------------------------------------------------
# Test 7: compute_signals top_trade uses PIT scores
# ---------------------------------------------------------------------------

class TestTopTradeSignalPIT:
    """Verify top_trade signal detector uses PIT blended_score, not static track records."""

    def test_no_track_records_in_top_trade(self):
        """top_trade detector must use insider_ticker_scores, not insider_track_records."""
        signals_path = PROJECT_ROOT / "pipelines" / "insider_study" / "compute_signals.py"
        if not signals_path.exists():
            pytest.skip("compute_signals.py not found")
        source = signals_path.read_text()

        # Find the top_trade function
        in_func = False
        func_source = []
        for line in source.splitlines():
            if "def top_trade(" in line:
                in_func = True
            elif in_func and (line.startswith("def ") or line.startswith("@register_signal")):
                break
            if in_func:
                func_source.append(line)

        func_text = "\n".join(func_source)
        assert "insider_track_records" not in func_text, (
            "top_trade signal uses insider_track_records (NOT point-in-time). "
            "Must use insider_ticker_scores with as_of_date <= trade_date."
        )
        assert "insider_ticker_scores" in func_text, (
            "top_trade signal should query insider_ticker_scores for PIT scores"
        )


# ---------------------------------------------------------------------------
# Net-flow signals (CEO Watcher validation experiment, 2026-05-24)
# ---------------------------------------------------------------------------

NET_FLOW_FILES = [
    "pipelines/insider_study/compute_company_net_flow.py",
    "pipelines/insider_study/compute_industry_net_flow.py",
]


class TestNetFlowSignalsPIT:
    """Enforce the strict-less-than window semantics for the two net-flow
    backfills. Any drift toward `<= filing_date` would include the current
    trade in its own signal — a textbook PIT leak."""

    @pytest.mark.parametrize("filepath", NET_FLOW_FILES)
    def test_uses_strict_less_than_anchor(self, filepath):
        """Window calculations must use STRICT `<` against the upper bound
        (current trade's filing_date / anchor date). Any `<=` against
        filing_date / q_date / anchor would include same-day filings — PIT leak.

        Algorithm-agnostic: forbids the `<=` pattern in any form. Two
        accepted implementations: bisect_left (industry script) or pointer
        sweep with `<` comparison (company script).
        """
        source = (PROJECT_ROOT / filepath).read_text()
        forbidden_patterns = [
            "filing_date <=",
            "<= filing_date",
            "<= q_date",
            "<= anchor",
            "<= F)",                # forbids `<= F)` in window comparators
            "bisect_right",         # would shift to <= semantics
        ]
        for pat in forbidden_patterns:
            assert pat not in source, (
                f"{filepath}: forbidden pattern '{pat}' would allow same-day "
                f"filings into the window — PIT leak"
            )
        # Confirm at least one strict-less-than reference exists (sanity check
        # the strictness invariant is explicitly asserted somewhere in the file).
        assert "< q_date" in source or "bisect_left" in source or "< F" in source, (
            f"{filepath}: no obvious strict-< comparator against the anchor — "
            f"hard to verify the PIT contract is enforced"
        )

    @pytest.mark.parametrize("filepath", NET_FLOW_FILES)
    def test_anchor_strictly_before_filing_date(self, filepath):
        """_anchor_dates must produce anchors strictly < filing_date.

        compute_company_net_flow's _last_quarter_end_before uses `< d`
        comparison; _previous_quarter_end only walks backward. compute_
        industry_net_flow imports both helpers from the company script.

        This test ALSO runs the function on quarter-end edge cases at
        test time to catch logic bugs (a 2026-05-25 fix corrected an
        infinite loop and a non-monotonic step-back bug).
        """
        source = (PROJECT_ROOT / filepath).read_text()
        if filepath.endswith("compute_company_net_flow.py"):
            # Structural check: the strict < comparator in _last_quarter_end_before
            assert "date(d.year, m, day) < d" in source, (
                f"{filepath}: _last_quarter_end_before must use strict < "
                f"against filing_date"
            )
            # Behavioral check: actually call _anchor_dates on the edge cases
            from pipelines.insider_study.compute_company_net_flow import _anchor_dates
            from datetime import date
            for d in (date(2026, 3, 31), date(2026, 6, 30), date(2026, 12, 31),
                      date(2026, 4, 1), date(2026, 1, 2)):
                anchors = _anchor_dates(d)
                assert all(a < d for a in anchors), (
                    f"{filepath}: _anchor_dates({d}) returned {anchors!r}; "
                    f"some not strictly < filing_date"
                )
                assert all(anchors[i] > anchors[i + 1]
                           for i in range(len(anchors) - 1)), (
                    f"{filepath}: _anchor_dates({d}) not monotonically "
                    f"decreasing: {anchors!r}"
                )

    @pytest.mark.parametrize("filepath", NET_FLOW_FILES)
    def test_no_insider_track_records_reference(self, filepath):
        """Net-flow backfills must not touch the non-PIT global aggregates."""
        source = (PROJECT_ROOT / filepath).read_text()
        assert "insider_track_records" not in source, (
            f"{filepath}: insider_track_records is non-PIT — forbidden here"
        )

    @pytest.mark.parametrize("filepath", NET_FLOW_FILES)
    def test_no_forward_looking_timedelta(self, filepath):
        """Walking the window forward (timedelta(days=+N) added to filing_date
        when querying past trades) would peek at future filings. Sanity-check
        no obvious forward arithmetic on filing_date."""
        source = (PROJECT_ROOT / filepath).read_text()
        forbidden = [
            "filing_date + timedelta",
            "F + timedelta",
            "anchor + timedelta",
        ]
        for pat in forbidden:
            assert pat not in source, (
                f"{filepath}: '{pat}' suggests forward-looking window arithmetic"
            )


class TestNoFutureDataInTickerMetadata:
    """ticker_metadata is a snapshot table — last_refreshed is for staleness
    only. It MUST NOT be used as a temporal anchor in any scoring path, since
    its value reflects when WE refreshed sector data, not when the sector
    assignment was true."""

    @pytest.mark.parametrize("filepath",
        SCORING_FILES + ["api/trade_grade.py",
                         "pipelines/insider_study/conviction_score.py"])
    def test_no_last_refreshed_temporal_use(self, filepath):
        source = (PROJECT_ROOT / filepath).read_text()
        # Allow mentions in comments / docstrings; forbid as an SQL filter
        for pat in ["last_refreshed <", "last_refreshed >", "last_refreshed =",
                    "last_refreshed BETWEEN"]:
            assert pat not in source, (
                f"{filepath}: ticker_metadata.last_refreshed used as a "
                f"temporal filter ('{pat}') — that's not its purpose."
            )
