"""form4_signal_class() and its trigger, against the real database.

REQUIRES the form4 PG database. Skipped if not reachable — typically that
means it's running on Mini (no local form4 DB).

The mapping table below is the specification. The SQL function is the
implementation, and application code reads the resulting column; nothing in
Python reproduces the mapping (tests/unit/test_signal_class.py enforces that).
So this file is the only place the intended behavior is written down twice, on
purpose, in the one form that can catch a change to the CASE expression.

See migrations/2026-08-17_trades_signal_class.sql for why each rule exists.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from api.filters import ALL_CLASSES, MEANINGFUL_CLASSES  # noqa: E402


@pytest.fixture(scope="module")
def conn():
    """form4 DB connection. Skip if unavailable."""
    try:
        from config.database import get_connection
        c = get_connection()
        c.execute("SELECT 1 FROM trades LIMIT 1").fetchone()
    except Exception as e:
        pytest.skip(f"form4 DB not reachable (run on Studio): {e}")
    yield c


# (trans_code, is_10b5_1, acquired_disp, is_derivative) -> expected class
CASES = [
    # Open-market purchases. NULL 10b5-1 reads as unplanned (the Form 4 box is
    # only affirmatively set when a plan exists); NULL acquired_disp predates
    # that field being parsed, and trans_code alone resolves direction.
    (("P", 0, "A", 0), "discretionary_buy"),
    (("P", None, None, 0), "discretionary_buy"),
    (("P", 0, None, 0), "discretionary_buy"),
    (("P", 1, "A", 0), "planned_buy"),

    # Open-market sales.
    (("S", 0, "D", 0), "discretionary_sell"),
    (("S", None, None, 0), "discretionary_sell"),
    (("S", 1, "D", 0), "planned_sell"),

    # Self-contradictory rows: the code says one direction, the
    # acquired/disposed flag says the other. Labelled, never coerced.
    (("P", 0, "D", 0), "inconsistent"),
    (("S", 0, "A", 0), "inconsistent"),
    (("S", 1, "A", 0), "inconsistent"),

    # Compensation mechanics.
    (("M", 0, "A", 0), "option_exercise"),
    (("X", 0, "A", 0), "option_exercise"),
    (("A", 0, "A", 0), "compensation"),
    (("F", 0, "D", 0), "tax_withholding"),
    (("G", 0, "A", 0), "gift"),

    # Derivatives settle on a different security than trade_returns prices, so
    # the flag wins over everything else including an otherwise-clean P.
    (("P", 0, "A", 1), "derivative"),
    (("S", 1, "D", 1), "derivative"),
    (("F", 0, "D", 1), "derivative"),

    # Unknown / missing codes fall through rather than guessing.
    (("Z", 0, None, 0), "other"),
    ((None, 0, None, 0), "other"),
]


class TestClassifier:
    @pytest.mark.parametrize("args,expected", CASES,
                             ids=[f"{a[0]}/{a[1]}/{a[2]}/{a[3]}->{e}" for a, e in CASES])
    def test_mapping(self, conn, args, expected):
        got = conn.execute(
            "SELECT form4_signal_class(?, ?, ?, ?)", args
        ).fetchone()[0]
        assert got == expected

    def test_function_is_immutable(self, conn):
        # IMMUTABLE is what lets the planner fold it and what makes it usable
        # in an index expression. 'i' = immutable, 's' = stable, 'v' = volatile.
        volatility = conn.execute(
            "SELECT provolatile FROM pg_proc WHERE proname = 'form4_signal_class'"
        ).fetchone()[0]
        assert volatility == "i"


class TestTrigger:
    def test_fires_on_insert(self, conn):
        """A new row is classified without the writer naming the column."""
        try:
            conn.execute("SAVEPOINT sc_insert")
            row = conn.execute(
                """INSERT INTO trades (insider_id, ticker, trade_type, trade_date,
                                       filing_date, trans_code, is_10b5_1,
                                       trans_acquired_disp, is_derivative)
                   VALUES (?, 'ZZTEST', 'buy', '2026-01-02', '2026-01-03',
                           'P', 0, 'A', 0)
                   RETURNING signal_class""",
                (1,),
            ).fetchone()
            assert row[0] == "discretionary_buy"
        finally:
            conn.execute("ROLLBACK TO SAVEPOINT sc_insert")

    def test_fires_on_update_of_source_column(self, conn):
        """Flipping the 10b5-1 flag re-routes the row to planned_*."""
        try:
            conn.execute("SAVEPOINT sc_update")
            tid = conn.execute(
                "SELECT trade_id FROM trades WHERE signal_class = 'discretionary_buy' LIMIT 1"
            ).fetchone()[0]
            conn.execute("UPDATE trades SET is_10b5_1 = 1 WHERE trade_id = ?", (tid,))
            got = conn.execute(
                "SELECT signal_class FROM trades WHERE trade_id = ?", (tid,)
            ).fetchone()[0]
            assert got == "planned_buy"
        finally:
            conn.execute("ROLLBACK TO SAVEPOINT sc_update")

    def test_trigger_is_scoped_to_the_four_source_columns(self, conn):
        """Routine batch writes (career_grade, returns) must not pay for it."""
        cols = conn.execute("""
            SELECT string_agg(a.attname, ',' ORDER BY a.attname)
              FROM pg_trigger tg
              JOIN unnest(tg.tgattr) AS attnum ON true
              JOIN pg_attribute a ON a.attrelid = tg.tgrelid AND a.attnum = attnum
             WHERE tg.tgname = 'trg_trades_signal_class'
        """).fetchone()[0]
        assert cols == "is_10b5_1,is_derivative,trans_acquired_disp,trans_code"


class TestPopulation:
    def test_no_unclassified_rows(self, conn):
        n = conn.execute(
            "SELECT count(*) FROM trades WHERE signal_class IS NULL"
        ).fetchone()[0]
        assert n == 0, f"{n} rows have no signal_class — the trigger has a gap"

    def test_every_value_is_a_known_class(self, conn):
        rows = conn.execute(
            "SELECT DISTINCT signal_class FROM trades"
        ).fetchall()
        found = {r[0] for r in rows}
        assert found <= set(ALL_CLASSES), f"unknown classes in trades: {found - set(ALL_CLASSES)}"

    def test_meaningful_classes_agree_with_trans_code(self, conn):
        """discretionary_* must be exactly the unplanned, non-derivative P/S."""
        mismatched = conn.execute("""
            SELECT count(*) FROM trades
             WHERE (signal_class IN ('discretionary_buy', 'discretionary_sell'))
               AND NOT (trans_code IN ('P', 'S')
                        AND COALESCE(is_10b5_1, 0) = 0
                        AND COALESCE(is_derivative, 0) = 0)
        """).fetchone()[0]
        assert mismatched == 0

    def test_meaningful_is_a_minority_of_the_table(self, conn):
        """Guards against a regression that widens the default back out.

        Discretionary P+S is ~42% of all filings. If this ever approaches
        100%, the classifier collapsed and every grant is back in the grade.
        """
        placeholders = ",".join("?" * len(MEANINGFUL_CLASSES))
        pct = conn.execute(
            f"""SELECT 100.0 * count(*) FILTER (WHERE signal_class IN ({placeholders}))
                       / NULLIF(count(*), 0)
                  FROM trades""",
            MEANINGFUL_CLASSES,
        ).fetchone()[0]
        assert 30 < float(pct) < 55, f"meaningful share is {pct:.1f}% — expected ~42%"
