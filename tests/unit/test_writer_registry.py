"""Unit tests for the writer registry runtime check.

Verifies that `assert_writer_wired` raises WriterMismatchError exactly when
the most-recent `signal_freshness` row's `populated_by` differs from the
registry's declared `script`.

This is the runtime defense against the mislabel failure mode (the
`is_rare_reversal` 8-week silence): contract said script A, actual writer
was script B, contract reported green because script A's `signal_freshness`
rows on unrelated columns kept the named column "fresh".
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework.contracts.exceptions import WriterMismatchError
from framework.contracts.freshness import (
    WriterRegistry,
    WriterRegistryEntry,
    assert_writer_wired,
    assert_all_writers_wired_for_strategy,
)


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE signal_freshness (
            source TEXT, table_name TEXT, column_name TEXT,
            last_computed_at TEXT, n_rows_affected INTEGER,
            populated_by TEXT
        )
    """)
    return db


@pytest.fixture
def registry_yaml(tmp_path):
    """Write a one-entry registry yaml and reload the singleton against it."""
    def _make(entries: list[dict]) -> Path:
        p = tmp_path / "writer_registry.yaml"
        p.write_text(yaml.safe_dump({"writers": entries}))
        return p
    return _make


@pytest.fixture(autouse=True)
def reset_registry_singleton():
    """Force WriterRegistry.get() to re-read between tests."""
    WriterRegistry._instance = None
    yield
    WriterRegistry._instance = None


def _seed_fresh(conn, table, column, populated_by, ts="2026-05-16T06:00:00+00:00"):
    conn.execute(
        "INSERT INTO signal_freshness(source, table_name, column_name, last_computed_at, n_rows_affected, populated_by) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("public", table, column, ts, 100, populated_by),
    )
    conn.commit()


def _load_registry_from_path(path: Path):
    WriterRegistry._instance = WriterRegistry(path)


# ── Tests: assert_writer_wired ────────────────────────────────────────────


def test_no_op_when_column_not_in_registry(conn, registry_yaml):
    """A column the registry doesn't know about → no-op (rollout phase
    accepts partial coverage)."""
    _load_registry_from_path(registry_yaml([]))
    _seed_fresh(conn, "trades", "career_grade", "anything.py")
    # Must not raise
    assert_writer_wired(conn, table="trades", column="career_grade")


def test_no_op_when_no_signal_freshness_row(conn, registry_yaml):
    """No signal_freshness row at all → assert_fresh's FreshnessUnknownError
    territory; this check is silent."""
    _load_registry_from_path(registry_yaml([
        {"column": "trades.career_grade",
         "script": "pipelines/insider_study/compute_career_grades.py"},
    ]))
    # No row in signal_freshness — should not raise
    assert_writer_wired(conn, table="trades", column="career_grade")


def test_match_passes(conn, registry_yaml):
    _load_registry_from_path(registry_yaml([
        {"column": "trades.career_grade",
         "script": "pipelines/insider_study/compute_career_grades.py"},
    ]))
    _seed_fresh(conn, "trades", "career_grade",
                "pipelines/insider_study/compute_career_grades.py")
    assert_writer_wired(conn, table="trades", column="career_grade")  # no raise


def test_mismatch_raises(conn, registry_yaml):
    """The is_rare_reversal failure mode: contract/registry says script A,
    signal_freshness was actually populated by script B."""
    _load_registry_from_path(registry_yaml([
        {"column": "trades.is_rare_reversal",
         "script": "pipelines/insider_study/compute_switch_rate.py"},
    ]))
    _seed_fresh(conn, "trades", "is_rare_reversal",
                "pipelines/insider_study/compute_cw_indicators.py")  # wrong writer
    with pytest.raises(WriterMismatchError) as exc:
        assert_writer_wired(conn, table="trades", column="is_rare_reversal")
    assert exc.value.column == "is_rare_reversal"
    assert exc.value.registered_script == "pipelines/insider_study/compute_switch_rate.py"
    assert exc.value.observed_populated_by == "pipelines/insider_study/compute_cw_indicators.py"


def test_strategy_filter_skips_unrelated_columns(conn, registry_yaml):
    """If a registry entry's required_for doesn't include the strategy,
    no check runs."""
    _load_registry_from_path(registry_yaml([
        {"column": "trades.is_rare_reversal",
         "script": "pipelines/insider_study/compute_switch_rate.py",
         "required_for": ["reversal_dip"]},
    ]))
    _seed_fresh(conn, "trades", "is_rare_reversal", "wrong_script.py")
    # quality_momentum doesn't depend on is_rare_reversal — no raise
    assert_writer_wired(conn, table="trades", column="is_rare_reversal",
                        strategy="quality_momentum")


def test_strategy_wildcard_applies_to_all(conn, registry_yaml):
    """required_for: ['*'] applies to every strategy."""
    _load_registry_from_path(registry_yaml([
        {"column": "trades.filing_date",
         "script": "strategies/insider_catalog/fetch_latest.py",
         "required_for": ["*"]},
    ]))
    _seed_fresh(conn, "trades", "filing_date", "wrong_script.py")
    with pytest.raises(WriterMismatchError):
        assert_writer_wired(conn, table="trades", column="filing_date",
                            strategy="quality_momentum")


def test_most_recent_row_wins(conn, registry_yaml):
    """If signal_freshness has multiple rows for the same column, the most
    recent populated_by is checked (older rows from a deprecated writer
    must not save us)."""
    _load_registry_from_path(registry_yaml([
        {"column": "trades.is_rare_reversal",
         "script": "pipelines/insider_study/compute_switch_rate.py"},
    ]))
    # Older row with the correct writer
    _seed_fresh(conn, "trades", "is_rare_reversal",
                "pipelines/insider_study/compute_switch_rate.py",
                ts="2026-01-01T06:00:00+00:00")
    # Newer row with the wrong writer
    _seed_fresh(conn, "trades", "is_rare_reversal",
                "pipelines/insider_study/compute_cw_indicators.py",
                ts="2026-05-16T06:00:00+00:00")
    with pytest.raises(WriterMismatchError):
        assert_writer_wired(conn, table="trades", column="is_rare_reversal")


# ── Tests: assert_all_writers_wired_for_strategy ──────────────────────────


def test_assert_all_passes_when_all_match(conn, registry_yaml):
    _load_registry_from_path(registry_yaml([
        {"column": "trades.career_grade",
         "script": "pipelines/insider_study/compute_career_grades.py",
         "required_for": ["quality_momentum"]},
        {"column": "trades.above_sma50",
         "script": "pipelines/insider_study/compute_cw_indicators.py",
         "required_for": ["quality_momentum"]},
    ]))
    _seed_fresh(conn, "trades", "career_grade",
                "pipelines/insider_study/compute_career_grades.py")
    _seed_fresh(conn, "trades", "above_sma50",
                "pipelines/insider_study/compute_cw_indicators.py")
    assert_all_writers_wired_for_strategy(conn, "quality_momentum")  # no raise


def test_assert_all_raises_on_first_mismatch(conn, registry_yaml):
    _load_registry_from_path(registry_yaml([
        {"column": "trades.career_grade",
         "script": "pipelines/insider_study/compute_career_grades.py",
         "required_for": ["quality_momentum"]},
        {"column": "trades.above_sma50",
         "script": "pipelines/insider_study/compute_cw_indicators.py",
         "required_for": ["quality_momentum"]},
    ]))
    _seed_fresh(conn, "trades", "career_grade", "wrong.py")
    _seed_fresh(conn, "trades", "above_sma50",
                "pipelines/insider_study/compute_cw_indicators.py")
    with pytest.raises(WriterMismatchError):
        assert_all_writers_wired_for_strategy(conn, "quality_momentum")
