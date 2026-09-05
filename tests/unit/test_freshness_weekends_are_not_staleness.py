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


def _patch(monkeypatch, contract, last_written: datetime, now: datetime):
    """Point assert_fresh at a fixed contract, last-written time AND clock.

    `now` is not optional. assert_fresh reads the wall clock twice — once
    through get_freshness for the raw age, once inside business_age_hours —
    and a test that lets either float is really asserting something about the
    day it happens to run on. Both are pinned here to the same instant.
    """
    class Reg:
        def lookup(self, table, column):
            return contract
    monkeypatch.setattr(F.FreshnessRegistry, "get", staticmethod(lambda: Reg()))
    age = (now - last_written).total_seconds() / 3600
    monkeypatch.setattr(F, "get_freshness", lambda conn, table, column:
                        (last_written, age))
    # assert_fresh calls business_age_hours(ts) with no `now`, so it would
    # otherwise fall back to the real clock. Bind it, keeping the real
    # weekend/holiday arithmetic — that is the thing under test.
    real = F.business_age_hours
    monkeypatch.setattr(F, "business_age_hours",
                        lambda ts, at=None: real(ts, at if at is not None else now))


def _last_friday_1730_pt() -> datetime:
    """The most recent Friday 17:30 PACIFIC strictly IN THE PAST.

    form4_pipeline's actual fire time (`30 17 * * 1-5`,
    America/Los_Angeles), as an aware UTC instant.

    "Strictly in the past" is the whole point. This walked back to the nearest
    weekday-4 date including TODAY, so running it on a Friday before 17:30
    returned a timestamp in the FUTURE: the age under test came out at −8.5
    hours and every premise built on it collapsed.
    """
    from zoneinfo import ZoneInfo
    PT = ZoneInfo("America/Los_Angeles")
    now_pt = datetime.now(PT)
    d = now_pt.date()
    while True:
        while d.weekday() != 4:
            d -= timedelta(days=1)
        fired = datetime(d.year, d.month, d.day, 17, 30, tzinfo=PT)
        if fired < now_pt:
            return fired.astimezone(timezone.utc)
        d -= timedelta(days=1)


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
    _patch(monkeypatch, contract, friday, monday)
    raw = (monday - friday).total_seconds() / 3600
    assert raw > 26, f"premise: raw age {raw:.1f}h must exceed the 26h contract"
    business = F.business_age_hours(friday, monday)
    assert business <= 26, (
        f"a Friday write is {business:.1f} business hours old on Monday "
        "morning and must not halt a strategy"
    )


def test_a_missed_weekday_run_still_halts(contract):
    """The safety property. If the pipeline skips a WEEKDAY, business hours
    accumulate and the contract must still trip.

    FIXED DATES, NOT "LAST FRIDAY + 4". Anchored to the live clock this test
    went red on 2026-09-04: the most recent Friday was 2026-09-04 itself, +4
    days is Tuesday 2026-09-08, and the Monday in between is LABOR DAY.
    business_age_hours discounted the holiday exactly as it should, leaving
    12.9 hours, and the test read a correct answer as a regression.

    That is the third calendar failure in this file — the two recorded in
    test_assert_fresh_uses_business_hours_when_the_flag_is_set were the same
    mistake in a different shape. A test about a missed WEEKDAY must own its
    calendar rather than borrow whatever week it runs in.

    2026-08-28 -> 2026-09-01 spans Monday 2026-08-31, an ordinary trading day.
    """
    from zoneinfo import ZoneInfo
    PT = ZoneInfo("America/Los_Angeles")
    friday = datetime(2026, 8, 28, 17, 30, tzinfo=PT).astimezone(timezone.utc)
    # Monday came and went without a run; it is now Tuesday morning.
    tuesday = datetime(2026, 9, 1, 6, 25, tzinfo=PT).astimezone(timezone.utc)
    business = F.business_age_hours(friday, tuesday)
    assert business > 26, (
        f"a skipped weekday run left only {business:.1f} business hours — the "
        "gate would not fire and a strategy would trade on stale inputs"
    )


def test_assert_fresh_uses_business_hours_when_the_flag_is_set(monkeypatch):
    """A generous threshold on purpose, against a PINNED clock.

    What is being asserted is that assert_fresh consults business hours AT
    ALL, not where a particular boundary falls — so the threshold is one the
    raw age clears and the business age does not, with room on both sides.

    Twice now this has been written against the wall clock and twice it has
    gone red on a calendar, not on a regression:

      1. The real 26h contract made it time-of-DAY dependent — a Friday 17:30
         write is ~19 business hours old on Monday morning and ~26h by Monday
         evening, so it passed before lunch and failed after it. Fixed by
         widening the threshold to 40h.
      2. Widening did not help, because business hours keep accruing all week.
         Anchored to "last Friday" against a live now, the premise
         `raw > 40 > business` only holds on a MONDAY or TUESDAY. Measured
         across a full week it was red five days in seven, and on a Friday
         before 17:30 `_last_friday_1730_pt` returned a FUTURE timestamp and
         the raw age came out at −8.5 hours.

    So the clock is pinned. `_patch` binds both places assert_fresh reads it,
    and the scenario is fixed at the one the module exists for: written Friday
    17:30, read at Monday 06:25 preflight. 60.9 raw hours, 12.9 business
    hours, every day of the week.
    """
    generous = F.FreshnessContract(
        table="trades", column="pit_cluster_size", max_staleness_hours=40.0,
        required_for=("quality_momentum",), description="",
        populated_by="form4_pipeline",
    )
    friday = _last_friday_1730_pt()
    monday = _pt_morning_after(friday, 3)
    _patch(monkeypatch, generous, friday, monday)

    raw = (monday - friday).total_seconds() / 3600
    assert raw > 40, f"premise: raw age {raw:.1f}h must exceed the threshold"
    business = F.business_age_hours(friday, monday)
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
    _patch(monkeypatch, seven_day, friday, _pt_morning_after(friday, 3))
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
