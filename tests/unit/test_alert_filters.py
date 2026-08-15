"""User-defined alert filter evaluation.

The behaviour that matters most here is what happens when data is MISSING.
career_grade is populated on 6.6% of recent trades and pit_grade on 38.4% —
not because anything is broken, but because most Form 4 filers have too little
history to grade. A filter that treated NULL as passing would flood a user;
one that crashed on NULL would silence them. Both failure modes are worse than
the filter simply not matching.
"""
import pytest

from pipelines.alert_filters import (
    FIELDS,
    GRADE_ORDER,
    any_filter_matches,
    evaluate_condition,
    filter_matches,
)


def cond(field, op, value):
    return {"field": field, "op": op, "value": value}


class TestGradeComparison:
    """Replaces _GRADE_TO_TIER, which collapsed A+ and A into one tier."""

    @pytest.mark.parametrize("actual,minimum,expected", [
        ("A+", "A+", True),
        ("A",  "A+", False),      # the distinction the old tier map destroyed
        ("A",  "A",  True),
        ("A+", "A",  True),
        ("B",  "A",  False),
        ("B",  "B",  True),
        ("C",  "B",  False),
    ])
    def test_gte(self, actual, minimum, expected):
        trade = {"career_grade": actual}
        assert evaluate_condition(trade, "career_grade", "gte", minimum) is expected

    def test_only_a_plus_is_expressible(self):
        # The whole point: under the old tier mapping A+ and A were both tier 3
        # and this filter was impossible to write.
        f = [cond("career_grade", "gte", "A+")]
        assert filter_matches({"career_grade": "A+"}, f) is True
        assert filter_matches({"career_grade": "A"}, f) is False


class TestMissingData:
    def test_null_grade_never_satisfies_a_floor(self):
        # A missing grade means "no scored history", not "bad". It must not
        # pass a quality bar, or a filter for elite insiders matches the 93%
        # of trades that have no career grade at all.
        for g in (None, "", "unknown"):
            assert evaluate_condition({"career_grade": g}, "career_grade", "gte", "A") is False

    def test_null_number_does_not_match(self):
        assert evaluate_condition({"value": None}, "value", "gte", "50000") is False

    def test_unknown_field_is_permissive_not_fatal(self):
        # A filter referencing a retired column should degrade to "no opinion",
        # not silence the user. Other conditions still have to pass.
        assert evaluate_condition({}, "column_that_was_removed", "gte", "1") is True
        assert filter_matches(
            {"career_grade": "B"},
            [cond("column_that_was_removed", "gte", "1"), cond("career_grade", "gte", "A")],
        ) is False


class TestComposite:
    """The case Derek described: career grade AND a set of tickers."""

    def test_and_within_a_filter(self):
        f = [cond("career_grade", "gte", "A"), cond("ticker", "in", "NVDA,AAPL")]
        assert filter_matches({"career_grade": "A+", "ticker": "NVDA"}, f) is True
        assert filter_matches({"career_grade": "A+", "ticker": "TSLA"}, f) is False
        assert filter_matches({"career_grade": "C",  "ticker": "NVDA"}, f) is False

    def test_or_across_filters(self):
        filters = [
            {"conditions": [cond("career_grade", "gte", "A+")]},
            {"conditions": [cond("ticker", "in", "NVDA")]},
        ]
        assert any_filter_matches({"career_grade": "A+", "ticker": "TSLA"}, filters) is True
        assert any_filter_matches({"career_grade": "C",  "ticker": "NVDA"}, filters) is True
        assert any_filter_matches({"career_grade": "C",  "ticker": "TSLA"}, filters) is False

    def test_empty_filter_matches_nothing(self):
        # "I made a filter and left it blank" must not mean "send me all 1,000
        # filings a day".
        assert filter_matches({"career_grade": "A+"}, []) is False
        assert any_filter_matches({"career_grade": "A+"}, [{"conditions": []}]) is False


class TestFieldRegistry:
    def test_every_field_declares_coverage(self):
        # Coverage is shown next to each field in the UI so a user picking a
        # 6.6% field knows why their inbox is quiet.
        for name, spec in FIELDS.items():
            assert 0 < spec["coverage"] <= 100, name
            assert spec["label"], name
            assert spec["kind"] in ("grade", "number", "bool", "text"), name

    def test_grades_ordered_best_first(self):
        assert GRADE_ORDER[0] == "A+"
        assert GRADE_ORDER[-1] == "F"

    def test_both_grades_are_offered(self):
        # Deliberately not choosing for the user: career_grade is the whole
        # track record, pit_grade is recent form.
        assert "career_grade" in FIELDS
        assert "pit_grade" in FIELDS
