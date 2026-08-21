"""signal_class filter contract + the anti-drift guard that justifies it.

The DB-backed correctness test for form4_signal_class() and its trigger lives
in tests/integration/test_signal_class_db.py — it needs the form4 database and
is skipped on Mini. What can be checked without a database is the part that
actually broke last time: whether the classification exists in exactly one
place.

The bug this column replaces was not a wrong mapping. It was a *correct*
mapping, hand-written as `trans_code IN ('P','S')` on roughly twenty browsing
surfaces, and simply omitted from the scoring path — which is how option
exercises and grants came to supply 75% of the evidence behind a career grade
while every feed the user could see was clean. A second copy of that logic in
Python would reopen exactly that gap, so the guard below is the point of this
file, not an afterthought to it.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from api.filters import (
    ALL_CLASSES,
    MEANINGFUL_CLASSES,
    NON_MEANINGFUL_CLASSES,
    add_signal_class_filter,
)

REPO = Path(__file__).resolve().parents[2]
MIGRATION = REPO / "migrations" / "2026-08-17_trades_signal_class.sql"


def _emitted_classes(sql: str) -> set[str]:
    """Every class literal the CASE can return — the ELSE arm included.

    Matching only THEN silently skipped `ELSE 'other'`, which is exactly the
    kind of gap this pair of tests exists to catch.
    """
    body = sql.split("RETURNS TEXT", 1)[-1].split("$$", 2)[1]
    return set(re.findall(r"(?:THEN|ELSE)\s+'([a-z_]+)'", body))


class TestFilterContract:
    """add_signal_class_filter is the only way application code selects classes."""

    def test_meaningful_is_buy_and_sell_only(self):
        # Both directions, and nothing else. planned_sell in particular must not
        # be here: at +0.86% over 30d it inverts the discretionary-sell signal.
        assert MEANINGFUL_CLASSES == ("discretionary_buy", "discretionary_sell")

    def test_keyword_meaningful_expands(self):
        conditions, params = [], []
        add_signal_class_filter(conditions, params, "meaningful")
        assert conditions == ["t.signal_class IN (?,?)"]
        assert params == list(MEANINGFUL_CLASSES)

    @pytest.mark.parametrize("value", [None, "", "  ", "all", "ALL"])
    def test_no_filter_values_are_noops(self, value):
        conditions, params = [], []
        add_signal_class_filter(conditions, params, value)
        assert conditions == [] and params == []

    def test_explicit_list_passes_through(self):
        conditions, params = [], []
        add_signal_class_filter(conditions, params, "discretionary_buy, gift")
        assert conditions == ["t.signal_class IN (?,?)"]
        assert params == ["discretionary_buy", "gift"]

    def test_unknown_class_matches_nothing_rather_than_everything(self):
        # A typo must produce a visibly empty result, never a silently
        # unfiltered one. Dropping the filter would quietly widen a
        # "meaningful trades" view to include grants.
        conditions, params = [], []
        add_signal_class_filter(conditions, params, "discretionary_byu")
        assert conditions == ["1 = 0"] and params == []

    def test_partially_unknown_keeps_only_known(self):
        conditions, params = [], []
        add_signal_class_filter(conditions, params, "discretionary_buy,nonsense")
        assert params == ["discretionary_buy"]

    def test_alias_is_honored(self):
        conditions, params = [], []
        add_signal_class_filter(conditions, params, "meaningful", alias="tr")
        assert conditions == ["tr.signal_class IN (?,?)"]

    def test_class_sets_are_disjoint_and_complete(self):
        assert not set(MEANINGFUL_CLASSES) & set(NON_MEANINGFUL_CLASSES)
        assert set(ALL_CLASSES) == set(MEANINGFUL_CLASSES) | set(NON_MEANINGFUL_CLASSES)


class TestMigrationIsSourceOfTruth:
    """Every class the API can name must exist in the SQL that produces it."""

    def test_migration_exists(self):
        assert MIGRATION.exists(), f"missing {MIGRATION}"

    def test_every_api_class_is_emitted_by_the_sql(self):
        sql = MIGRATION.read_text()
        emitted = _emitted_classes(sql)
        missing = set(ALL_CLASSES) - emitted
        assert not missing, (
            f"api/filters.py names classes the SQL never produces: {sorted(missing)}. "
            "The column would silently match nothing for these."
        )

    def test_sql_emits_nothing_the_api_cannot_name(self):
        sql = MIGRATION.read_text()
        emitted = _emitted_classes(sql)
        unknown = emitted - set(ALL_CLASSES)
        assert not unknown, (
            f"SQL produces classes api/filters.py does not know about: {sorted(unknown)}. "
            "Rows would land in a class no endpoint can filter for."
        )

    def test_signature_types_match_the_columns(self):
        # is_10b5_1 and is_derivative are BIGINT on trades. An INTEGER
        # signature type-checks at definition and then fails at every call
        # site, because Postgres will not implicitly narrow bigint.
        sql = MIGRATION.read_text()
        assert "p_is_10b5_1     BIGINT" in sql
        assert "p_is_derivative BIGINT" in sql


class TestNoPythonReimplementation:
    """The classification lives in SQL. Nothing may recreate it in Python."""

    # Two modules legitimately name every class, and neither derives one.
    #
    #   api/filters.py         names the sets so callers can filter on them
    #   api/classification.py  maps an existing signal_class to its PUBLISHED
    #                          label ("planned_sell" -> "Scheduled")
    #
    # The distinction that matters: the SQL decides which class a filing IS,
    # from trans_code and the flags. These two only consume that answer. A
    # module that looked at trans_code and produced a class would be the
    # violation, and still is.
    ALLOWED = {"api/filters.py", "api/classification.py"}

    def test_no_module_maps_trans_codes_to_classes(self):
        # A module that mentions two classes which only co-occur in a full
        # mapping is almost certainly reproducing the CASE expression.
        fingerprint = ("option_exercise", "tax_withholding")
        offenders = []
        for path in REPO.rglob("*.py"):
            rel = path.relative_to(REPO).as_posix()
            if rel.startswith(("tests/", "node_modules/", ".venv/")) or rel in self.ALLOWED:
                continue
            try:
                text = path.read_text(errors="ignore")
            except OSError:
                continue
            if all(token in text for token in fingerprint):
                offenders.append(rel)
        assert not offenders, (
            "these modules appear to reimplement form4_signal_class in Python: "
            f"{offenders}. Read trades.signal_class instead — a second copy of "
            "this mapping is how the scoring path drifted from the feed."
        )
