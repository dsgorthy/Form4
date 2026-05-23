"""Smoke tests for framework.decision.filters.evaluate_filters.

These pin the contract that sim and live must agree on. Any change here
is a breaking change to BOTH the simulator (which uses this directly)
and cw_runner (which is on the migration list to also use it).
"""
from framework.decision.filters import evaluate_filters


def make_trade(**overrides):
    base = {
        "trade_id": 1,
        "ticker": "FOO",
        "is_duplicate": False,
        "is_rare_reversal": 0,
        "consecutive_sells_before": 0,
        "dip_1mo": 0.0,
        "dip_3mo": 0.0,
        "above_sma50": 1,
        "above_sma200": 1,
        "is_largest_ever": 0,
        "is_10b5_1": 0,
        "is_recurring": 0,
        "is_tax_sale": 0,
        "cohen_routine": 0,
        "pit_grade": "B",
        "career_grade": "B",
    }
    base.update(overrides)
    return base


def test_passes_with_empty_filters():
    ok, fails = evaluate_filters({}, make_trade())
    assert ok and fails == []


def test_drops_duplicates_unconditionally():
    ok, fails = evaluate_filters({}, make_trade(is_duplicate=True))
    assert not ok and "is_duplicate=1" in fails


def test_min_consecutive_sells_threshold():
    # 0 < 2 → fail
    ok, _ = evaluate_filters({"min_consecutive_sells": 2}, make_trade(consecutive_sells_before=0))
    assert not ok
    # 3 >= 2 → pass
    ok, _ = evaluate_filters({"min_consecutive_sells": 2}, make_trade(consecutive_sells_before=3))
    assert ok


def test_grade_filter_accepts_list_or_string():
    cfg = {"pit_grade": "A"}
    assert evaluate_filters(cfg, make_trade(pit_grade="A"))[0]
    assert not evaluate_filters(cfg, make_trade(pit_grade="B"))[0]

    cfg = {"pit_grade": ["A", "A+"]}
    assert evaluate_filters(cfg, make_trade(pit_grade="A+"))[0]
    assert not evaluate_filters(cfg, make_trade(pit_grade="B"))[0]


def test_above_sma_requires_value_1():
    cfg = {"above_sma50": True}
    assert evaluate_filters(cfg, make_trade(above_sma50=1))[0]
    assert not evaluate_filters(cfg, make_trade(above_sma50=0))[0]


def test_exclude_routine_blocks_routine_trades():
    cfg = {"exclude_routine": True}
    assert evaluate_filters(cfg, make_trade(cohen_routine=0))[0]
    assert not evaluate_filters(cfg, make_trade(cohen_routine=1))[0]


def test_works_with_dataclass_like_objects():
    """Future callers will pass CandidateFact instances, not dicts."""
    class Fake:
        is_duplicate = False
        is_rare_reversal = 0
        consecutive_sells_before = 5
        dip_1mo = 0.0
        dip_3mo = 0.0
        above_sma50 = 1
        above_sma200 = 1
        is_largest_ever = 0
        is_10b5_1 = 0
        is_recurring = 0
        is_tax_sale = 0
        cohen_routine = 0
        pit_grade = "A"
        career_grade = "A"

    ok, _ = evaluate_filters({"pit_grade": "A", "min_consecutive_sells": 3}, Fake())
    assert ok
