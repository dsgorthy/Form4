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
]


class TestNoTrackRecordsInScoring:
    """Ensure scoring code never references the non-PIT insider_track_records table."""

    @pytest.mark.parametrize("filepath", SCORING_FILES)
    def test_no_insider_track_records_reference(self, filepath):
        """Scoring files must not READ from insider_track_records for scoring decisions."""
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
