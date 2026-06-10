"""Test-time PIT enforcement.

Every Signal subclass should declare a non-empty ``test_cases()``. The
PITValidator runs each test case twice — once with full DB visibility,
once with rows hidden by ingestion timestamp — and asserts identical
results. Diff = PIT leak.

CI gate:

    from dataplane.testing import PITValidator
    from signals.insider.career_grade_v3 import CareerGradeV3

    def test_pit_clean():
        with get_connection() as conn:
            violations = PITValidator(CareerGradeV3, conn).run()
        assert violations == [], violations
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Type


@dataclass(frozen=True)
class PITTestCase:
    """One (ticker, as_of) tuple plus optional expected value.

    The PITValidator runs ``signal.compute(ticker, as_of)`` twice:
      - normal mode (full DB visible)
      - frozen mode (only ingested_at < as_of rows visible)
    If results differ, PIT validation fails.

    Optional ``expected_value`` adds a *value*-correctness assertion on top
    (a regression test). Use when the truth is known and stable.
    """

    ticker: str
    as_of: datetime
    expected_value: Any = None
    description: str = ""

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None:
            raise ValueError(
                f"PITTestCase.as_of must be timezone-aware: {self.as_of!r}"
            )


class PITValidator:
    """Runs each PITTestCase under normal + frozen modes; reports violations."""

    def __init__(self, signal_class: Type, conn):
        self.signal_class = signal_class
        self.conn = conn

    def run(self) -> list:
        """Return list of violation dicts. Empty list = PIT clean."""
        violations = []
        for tc in self.signal_class.test_cases():
            normal = self._compute(tc, mode="normal")
            frozen = self._compute(tc, mode="frozen")
            if self._materially_differ(normal, frozen):
                violations.append({
                    "test_case": tc,
                    "normal": self._dump(normal),
                    "frozen": self._dump(frozen),
                    "reason": "normal vs frozen mode disagreed — PIT leak",
                })
            if tc.expected_value is not None and not self._matches_expected(
                normal, tc.expected_value
            ):
                violations.append({
                    "test_case": tc,
                    "normal": self._dump(normal),
                    "expected": tc.expected_value,
                    "reason": "regression: computed value differs from expected",
                })
        return violations

    # ── internals ─────────────────────────────────────────────────────

    def _compute(self, tc: PITTestCase, mode: str):
        signal = self.signal_class(conn=self.conn)
        signal._pit_mode = mode
        try:
            return signal.compute(tc.ticker, tc.as_of)
        except Exception as exc:
            return {"error": repr(exc)}

    @staticmethod
    def _materially_differ(a, b) -> bool:
        """Compare two SignalObservation-or-error results for material difference.

        Ignores source_run_id and ingested_at (always differ between runs).
        Compares value, confidence, ticker, signal_id, as_of_date.
        """
        if isinstance(a, dict) or isinstance(b, dict):
            return a != b  # one (or both) is an error dict; compare directly
        return (
            a.value != b.value
            or a.confidence != b.confidence
            or a.ticker != b.ticker
            or a.signal_id != b.signal_id
            or a.as_of_date != b.as_of_date
        )

    @staticmethod
    def _matches_expected(observation, expected_value) -> bool:
        if isinstance(observation, dict):
            return False  # was an error
        return observation.value == expected_value

    @staticmethod
    def _dump(observation):
        if isinstance(observation, dict):
            return observation
        return {
            "signal_id": observation.signal_id,
            "ticker": observation.ticker,
            "as_of_date": observation.as_of_date.isoformat(),
            "value": observation.value,
            "confidence": observation.confidence,
        }
