"""Smoke + invariant tests for the dataplane Python package.

Run from /Users/derekg/trading-framework/dataplane:
    python3 -m pytest tests/ -v
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from dataplane import (
    PIT, PITTestCase, PITViolationError, Signal, SignalObservation, Upstream,
)


# ── SignalObservation ──────────────────────────────────────────────────

class TestSignalObservation:
    def test_constructs(self):
        obs = SignalObservation(
            signal_id="insider.career_grade.v3",
            ticker="AAPL",
            as_of_date=datetime.now(timezone.utc),
            value={"grade": "A+"},
            source_run_id=uuid4(),
        )
        assert obs.ticker == "AAPL"
        assert obs.signal_class == "insider"

    def test_rejects_naive_datetime(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            SignalObservation(
                signal_id="x.v1",
                ticker="AAPL",
                as_of_date=datetime.now(),  # no tz!
                value=1.0,
                source_run_id=uuid4(),
            )

    def test_rejects_out_of_range_confidence(self):
        for bad in (-0.1, 1.1, 2.0):
            with pytest.raises(ValueError, match="confidence"):
                SignalObservation(
                    signal_id="x.v1", ticker="AAPL",
                    as_of_date=datetime.now(timezone.utc),
                    value=1.0, source_run_id=uuid4(),
                    confidence=bad,
                )

    def test_accepts_boundary_confidence(self):
        for ok in (0.0, 0.5, 1.0, None):
            SignalObservation(
                signal_id="x.v1", ticker="AAPL",
                as_of_date=datetime.now(timezone.utc),
                value=1.0, source_run_id=uuid4(),
                confidence=ok,
            )

    def test_rejects_empty_identifiers(self):
        with pytest.raises(ValueError):
            SignalObservation(
                signal_id="", ticker="AAPL",
                as_of_date=datetime.now(timezone.utc),
                value=1.0, source_run_id=uuid4(),
            )


# ── Upstream ────────────────────────────────────────────────────────────

class TestUpstream:
    def test_default_lag_is_zero(self):
        u = Upstream("prices.daily.v1")
        assert u.pit_lag == timedelta(0)

    def test_json_roundtrip(self):
        u = Upstream("insider.trades.raw", pit_lag=timedelta(hours=24))
        d = u.to_json()
        assert d["signal_id"] == "insider.trades.raw"
        assert d["pit_lag_seconds"] == 24 * 3600
        assert Upstream.from_json(d) == u


# ── Signal subclass contract ───────────────────────────────────────────

class TestSignalSubclassValidation:
    def test_missing_signal_id_rejected(self):
        with pytest.raises(TypeError, match="must define"):
            class Bad(Signal):
                # signal_id missing
                version = "v1"
                owner = "x"
                sla_hours = 24

                def compute(self, ticker, as_of):
                    pass

    def test_complete_subclass_accepted(self):
        class Good(Signal):
            signal_id = "test.foo"
            version = "v1"
            owner = "x"
            sla_hours = 24
            output_schema = {"x": "real"}

            def compute(self, ticker, as_of):
                return self.observation(ticker, as_of, value=1.0)

        g = Good()
        assert g.signal_id == "test.foo"

    def test_upstream_must_be_list(self):
        with pytest.raises(TypeError, match="upstream"):
            class Bad(Signal):
                signal_id = "test.bar"
                version = "v1"
                owner = "x"
                sla_hours = 24
                upstream = "not a list"   # type: ignore

                def compute(self, ticker, as_of):
                    pass


# ── PIT decorator behavior ─────────────────────────────────────────────

class TestPITDecorator:
    def test_strict_marks_method(self):
        class S(Signal):
            signal_id = "test.x"
            version = "v1"
            owner = "x"
            sla_hours = 1

            @PIT.strict
            def compute(self, ticker, as_of):
                return self.observation(ticker, as_of, value=1.0)

        assert S.compute._pit_strict is True

    def test_strict_rejects_naive_as_of(self):
        class S(Signal):
            signal_id = "test.x"
            version = "v1"
            owner = "x"
            sla_hours = 1

            @PIT.strict
            def compute(self, ticker, as_of):
                return self.observation(ticker, as_of, value=1.0)

        with pytest.raises(PITViolationError, match="timezone-aware"):
            S().compute("AAPL", datetime.now())   # no tz!

    def test_strict_threads_as_of(self):
        """During compute, self._pit_as_of must be set to as_of."""
        captured = {}

        class S(Signal):
            signal_id = "test.x"
            version = "v1"
            owner = "x"
            sla_hours = 1

            @PIT.strict
            def compute(self, ticker, as_of):
                captured["pit_as_of"] = self._pit_as_of
                return self.observation(ticker, as_of, value=1.0)

        when = datetime(2026, 1, 1, tzinfo=timezone.utc)
        S().compute("AAPL", when)
        assert captured["pit_as_of"] == when


# ── Signal.read enforcement ────────────────────────────────────────────

class TestSignalRead:
    def test_read_rejects_undeclared_upstream(self):
        class S(Signal):
            signal_id = "test.x"
            version = "v1"
            owner = "x"
            sla_hours = 1
            upstream = [Upstream("declared.v1")]

            @PIT.strict
            def compute(self, ticker, as_of):
                self.read("not.declared.v1", ticker)  # should raise
                return self.observation(ticker, as_of, value=1.0)

        with pytest.raises(PITViolationError, match="not in the upstream"):
            S().compute("AAPL", datetime.now(timezone.utc))

    def test_read_outside_compute_raises(self):
        class S(Signal):
            signal_id = "test.x"
            version = "v1"
            owner = "x"
            sla_hours = 1
            upstream = [Upstream("u.v1")]

            def compute(self, ticker, as_of):
                pass

        with pytest.raises(PITViolationError, match="outside"):
            S().read("u.v1", "AAPL")


# ── PITTestCase ─────────────────────────────────────────────────────────

class TestPITTestCase:
    def test_rejects_naive_datetime(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            PITTestCase(ticker="AAPL", as_of=datetime(2026, 1, 1))


# ── Materialization modes ───────────────────────────────────────────────

class TestMaterializationMode:
    def test_default_is_per_ticker_per_day(self):
        class S(Signal):
            signal_id = "test.foo"
            version = "v1"
            owner = "x"
            sla_hours = 1

            def compute(self, ticker, as_of):
                return self.observation(ticker, as_of, value=1.0)

        assert S.materialization_mode == "per_ticker_per_day"

    def test_per_partition_events_accepted(self):
        class S(Signal):
            signal_id = "test.raw"
            version = "v1"
            owner = "x"
            sla_hours = 1
            materialization_mode = "per_partition_events"

            def materialize_partition(self, partition_date):
                return []

        assert S.materialization_mode == "per_partition_events"

    def test_invalid_mode_rejected(self):
        with pytest.raises(TypeError, match="materialization_mode"):
            class S(Signal):
                signal_id = "test.bad"
                version = "v1"
                owner = "x"
                sla_hours = 1
                materialization_mode = "nonsense"

    def test_unimplemented_compute_raises(self):
        class S(Signal):
            signal_id = "test.raw"
            version = "v1"
            owner = "x"
            sla_hours = 1
            materialization_mode = "per_partition_events"

            def materialize_partition(self, partition_date):
                return []

        with pytest.raises(NotImplementedError, match="compute"):
            S().compute("AAPL", datetime.now(timezone.utc))

    def test_unimplemented_materialize_partition_raises(self):
        class S(Signal):
            signal_id = "test.derived"
            version = "v1"
            owner = "x"
            sla_hours = 1

            def compute(self, ticker, as_of):
                return self.observation(ticker, as_of, value=1.0)

        with pytest.raises(NotImplementedError, match="materialize_partition"):
            S().materialize_partition(datetime.now(timezone.utc))
