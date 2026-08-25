"""A weekend gap is not staleness, and the halt path must know that.

WHAT WENT WRONG

`FreshnessContract.business_hours_only` defaults to True and its comment says
"staleness ignores weekend + US market holiday hours". `business_age_hours()`
implements exactly that. But `assert_fresh` — the gate that HALTS a strategy —
compared RAW elapsed hours and never consulted either. The flag only ever
affected the admin display panel; its own docstring said "not for the runner
halt path".

The columns under contract are written by form4_pipeline, `30 17 * * 1-5`. On a
Monday morning they are ~60h old against contracts of 26h and 48h, so:

    quality_momentum: STALE_INPUT_HALT — trades.pit_cluster_size 60.1h (max 26.0h)
    reversal_dip:     STALE_INPUT_HALT — trades.cohen_routine    60.0h (max 48.0h)
    → Found 0 candidates

All three books scanned NOTHING, and had done so every Monday. One trading day
in five, lost silently.

THE PROPERTY THAT MUST SURVIVE

This gate is fail-closed on purpose — it stops a strategy trading on stale
inputs. Discounting only NON-TRADING days keeps that: a pipeline that misses a
WEEKDAY run still accrues hours and still trips its contract. These tests pin
both halves.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from framework.contracts import freshness as F


@pytest.fixture
def contract():
    return F.FreshnessContract(
        table="trades", column="pit_cluster_size", max_staleness_hours=26.0,
        required_for=("quality_momentum",), description="",
        populated_by="form4_pipeline",
    )


def _patch(monkeypatch, contract, last_written: datetime):
    """Point assert_fresh at a fixed contract and a fixed last-written time."""
    class Reg:
        def lookup(self, table, column):
            return contract
    monkeypatch.setattr(F.FreshnessRegistry, "get", staticmethod(lambda: Reg()))
    age = (datetime.now(timezone.utc) - last_written).total_seconds() / 3600
    monkeypatch.setattr(F, "get_freshness", lambda conn, table, column:
                        (last_written, age))


def _last_friday_1730_pt() -> datetime:
    """A real Friday 17:30 PACIFIC — form4_pipeline's actual fire time
    (`30 17 * * 1-5`, America/Los_Angeles) — as an aware UTC instant."""
    from zoneinfo import ZoneInfo
    PT = ZoneInfo("America/Los_Angeles")
    now_pt = datetime.now(PT)
    d = now_pt.date()
    while d.weekday() != 4:
        d -= timedelta(days=1)
    return datetime(d.year, d.month, d.day, 17, 30, tzinfo=PT).astimezone(timezone.utc)


def _pt_morning_after(friday: datetime, days: int) -> datetime:
    """06:25 Pacific, `days` after that Friday — when the runners preflight."""
    from zoneinfo import ZoneInfo
    PT = ZoneInfo("America/Los_Angeles")
    d = (friday.astimezone(PT) + timedelta(days=days)).date()
    return datetime(d.year, d.month, d.day, 6, 25, tzinfo=PT).astimezone(timezone.utc)


def test_a_friday_write_is_fresh_on_monday(monkeypatch, contract):
    """The exact failure. Friday 17:30 ET -> the following Monday morning is
    ~60 raw hours but only ~12 business hours."""
    friday = _last_friday_1730_pt()
    monday = _pt_morning_after(friday, 3)
    _patch(monkeypatch, contract, friday)
    raw = (monday - friday).total_seconds() / 3600
    assert raw > 26, f"premise: raw age {raw:.1f}h must exceed the 26h contract"
    business = F.business_age_hours(friday, monday)
    assert business <= 26, (
        f"a Friday write is {business:.1f} business hours old on Monday "
        "morning and must not halt a strategy"
    )


def test_a_missed_weekday_run_still_halts(contract):
    """The safety property. If the pipeline skips a WEEKDAY, business hours
    accumulate and the contract must still trip."""
    friday = _last_friday_1730_pt()
    # Monday came and went without a run; it is now Tuesday morning.
    tuesday = _pt_morning_after(friday, 4)
    business = F.business_age_hours(friday, tuesday)
    assert business > 26, (
        f"a skipped weekday run left only {business:.1f} business hours — the "
        "gate would not fire and a strategy would trade on stale inputs"
    )


def test_assert_fresh_uses_business_hours_when_the_flag_is_set(monkeypatch):
    """A generous threshold on purpose.

    The first version used the real 26h contract and was time-of-day
    dependent: a Friday 17:30 write is ~19h of business time old on Monday
    morning and ~26h by Monday evening, so the test passed before lunch and
    failed after it. What is being asserted is that assert_fresh consults
    business hours AT ALL, not where a particular boundary falls — so pick a
    threshold the raw age clears and the business age does not, with room on
    both sides.
    """
    generous = F.FreshnessContract(
        table="trades", column="pit_cluster_size", max_staleness_hours=40.0,
        required_for=("quality_momentum",), description="",
        populated_by="form4_pipeline",
    )
    friday = _last_friday_1730_pt()
    _patch(monkeypatch, generous, friday)

    raw = (datetime.now(timezone.utc) - friday).total_seconds() / 3600
    assert raw > 40, f"premise: raw age {raw:.1f}h must exceed the threshold"
    business = F.business_age_hours(friday)
    assert business < 40, f"premise: business age {business:.1f}h must not"

    # Must not raise — which can only be true if the weekend was discounted.
    F.assert_fresh(None, table="trades", column="pit_cluster_size",
                   strategy="quality_momentum")


def test_assert_fresh_uses_raw_hours_when_the_flag_is_cleared(
        monkeypatch, contract):
    """A populator that runs seven days a week gets no weekend discount."""
    seven_day = F.FreshnessContract(
        table="trades", column="pit_cluster_size", max_staleness_hours=26.0,
        required_for=("quality_momentum",), description="",
        populated_by="something_daily", business_hours_only=False,
    )
    friday = _last_friday_1730_pt()
    _patch(monkeypatch, seven_day, friday)
    with pytest.raises(F.StaleSignalError):
        F.assert_fresh(None, table="trades", column="pit_cluster_size",
                       strategy="quality_momentum")


def test_the_halt_path_actually_reads_the_flag():
    """Source-level: the flag was inert for months while reading as active."""
    src = (F.__file__ and open(F.__file__).read()) or ""
    block = src[src.index("def assert_fresh("):]
    block = block[:block.index("\ndef ")]
    assert "business_hours_only" in block, (
        "assert_fresh ignores contract.business_hours_only again — the flag "
        "goes back to being decorative on the only path where it matters"
    )
