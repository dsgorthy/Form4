"""No path may open, price, or model a position before the filing was public.

Two halves. The first tests the shared rule. The second is the one that
matters: it fails the build if a simulator goes back to deciding this for
itself, which is how three of them ended up filling at a close the filing had
not reached yet.
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from framework.decision.entry_timing import (
    filed_before_close,
    first_tradeable_index,
)

REPO = Path(__file__).resolve().parents[2]


class TestFiledBeforeClose:
    # filed_at is EASTERN as of 2026-08-19 — see
    # migrations/2026-08-19_filed_at_normalize_eastern.sql. These cases used to
    # be expressed in UTC, which is what let a real after-bell filing (CDNL,
    # accepted 17:37:34 ET) pass as tradeable at that afternoon's close.
    #
    # There is no DST arithmetic left to get wrong, which is the point: the
    # value is already in the timezone the market runs on.
    #
    # Note the bell test is applied to PICKUP, not acceptance, so anything from
    # 15:55 onward rounds to the 16:00 poll and misses.
    @pytest.mark.parametrize("eastern,expected", [
        ("2026-01-15 15:49:00", True),
        ("2026-01-15 16:00:00", False),
        ("2026-01-15 16:01:00", False),
        ("2026-07-15 15:49:00", True),
        ("2026-07-15 16:00:00", False),
        ("2026-07-15 16:30:00", False),
        ("2026-07-15 08:00:00", True),
        # Evening filings, the case that started all this.
        ("2026-07-15 22:00:00", False),
        ("2026-07-16 17:37:34", False),
    ])
    def test_bell_boundaries_in_eastern(self, eastern, expected):
        assert filed_before_close(eastern) is expected

    @pytest.mark.parametrize("bad", [
        None, "", "   ", "not-a-timestamp", "2026-07-15", "2026",
    ])
    def test_unusable_timestamps_are_after_close(self, bad):
        # Guessing "before" fabricates an edge; guessing "after" costs one
        # session of drift. Always guess after.
        assert filed_before_close(bad) is False

    def test_accepts_datetime_and_iso_t(self):
        assert filed_before_close("2026-07-15T09:59:00") is True
        assert filed_before_close(datetime(2026, 7, 15, 9, 59)) is True

    def test_datetime_is_read_as_eastern_not_utc(self):
        """19:59 is an evening filing. Read as UTC it would be 15:59 ET and
        squeak in before the bell — which is exactly the bug."""
        assert filed_before_close(datetime(2026, 7, 15, 19, 59)) is False
        assert filed_before_close(datetime(2026, 7, 15, 9, 30)) is True


class TestFirstTradeableIndex:
    def test_same_session_when_it_beat_the_bell(self):
        assert first_tradeable_index(100, "2026-07-15 09:59:00") == 100

    def test_next_session_when_it_did_not(self):
        assert first_tradeable_index(100, "2026-07-15 20:30:00") == 101

    def test_next_session_when_unknown(self):
        assert first_tradeable_index(100, None) == 101

    def test_never_earlier_than_the_filing(self):
        for ts in ("2026-07-15 19:59:00", "2026-07-15 20:30:00", None, "junk"):
            assert first_tradeable_index(42, ts) >= 42


class TestNoSimulatorDecidesThisAlone:
    """The regression guard. This is the point of the file.

    Any module that opens positions must either import the shared rule or
    demonstrably not price entries off a bare filing date.
    """

    # Modules that create positions from filings and therefore must obey it.
    POSITION_OPENERS = [
        "pipelines/insider_study/simulate_strategy_portfolio.py",
        "pipelines/insider_study/simulate_portfolio_intraday.py",
        "pipelines/insider_study/backfill_qm_v3.py",
        "pipelines/portfolio_simulator.py",
        "pipelines/insider_study/backfill_cw_portfolio.py",
    ]

    ENTRY_AT_FILING_DATE = re.compile(
        r"(get_close_for|_get_close|find_first_price_on_or_after)\s*\([^)]*filing_date",
        re.IGNORECASE,
    )

    # Exempt only by explicit, reasoned entry — never by a phrase appearing
    # somewhere in the file. The first version of this guard skipped any module
    # containing "next trading day", and backfill_qm_v3.py has that phrase in a
    # comment about EXIT dates while still pricing entries at the filing date.
    # A heuristic escape hatch is how the thing being guarded gets through.
    EXEMPT = {
        "pipelines/portfolio_simulator.py":
            "enters on the next trading day's open, strictly later than any "
            "filing on that date",
    }

    @pytest.mark.parametrize("rel", POSITION_OPENERS)
    def test_module_respects_publication_time(self, rel):
        path = REPO / rel
        if not path.exists():
            pytest.skip(f"{rel} not present")
        src = path.read_text(errors="ignore")

        if rel in self.EXEMPT:
            return
        aware = (
            "entry_timing" in src                    # imports the shared rule
            or "filed_before_close" in src
            or "first_tradeable_index" in src
            or "tradeable_same_day" in src           # SQL-side equivalent
            or "_filed_during_market_hours" in src   # pre-existing local impl
        )
        assert aware, (
            f"{rel} opens positions but shows no sign of checking when the "
            f"filing became public. Use framework.decision.entry_timing — a "
            f"filing dated today may not have existed at today's close, and "
            f"filling there was worth 26 points of CAGR in the strategy sweep."
        )

    @pytest.mark.parametrize("rel", POSITION_OPENERS)
    def test_no_bare_filing_date_entry_pricing(self, rel):
        path = REPO / rel
        if not path.exists():
            pytest.skip(f"{rel} not present")
        if rel in self.EXEMPT:
            return
        src = path.read_text(errors="ignore")
        hits = [m.group(0) for m in self.ENTRY_AT_FILING_DATE.finditer(src)]
        assert not hits, (
            f"{rel} prices an entry directly off filing_date: {hits[:2]}. "
            f"Gate it on framework.decision.entry_timing.filed_before_close."
        )


class TestPnlNeverReportsUnobtainableEntries:
    """Gain/loss must never be reported for a position that could not be opened.

    The DB side is a trigger on strategy_portfolio maintaining
    entry_before_publication (migrations/2026-08-17_entry_before_publication.sql).
    This is the application side: the portfolio API must actually filter on it.

    97 rows in the legacy 'backtest' archives are flagged TRUE. They happen to
    sit outside execution_source='simulated', so the API excluded them already
    — but by coincidence, not by design. If someone widens that filter the
    contaminated rows walk straight in, which is exactly what this test exists
    to stop.
    """

    PORTFOLIO_API = "api/routers/portfolio.py"

    # Reads that deliberately do NOT filter, each with a reason. The
    # single-trade detail endpoint returns the row and exposes the flag via
    # SELECT *, so the UI can mark it — 404-ing a real row would hide the
    # problem rather than surface it.
    EXEMPT_QUERIES = {
        "single-trade detail (SELECT * exposes the flag)": "WHERE id = ?",
    }

    def test_every_pnl_query_filters_the_flag(self):
        """Per-query, not a count. Counting lets one unguarded read hide."""
        import re
        src = (REPO / self.PORTFOLIO_API).read_text(errors="ignore")
        blocks = re.findall(r'conn\.execute\("""(.*?)"""', src, re.S)
        unguarded = []
        for q in blocks:
            if "FROM strategy_portfolio" not in q:
                continue
            if "entry_before_publication" in q:
                continue
            if any(marker in q for marker in self.EXEMPT_QUERIES.values()):
                continue
            unguarded.append(" ".join(q.split())[:110])
        assert not unguarded, (
            "these strategy_portfolio queries report P&L without excluding "
            "positions opened before their filing was public:\n  "
            + "\n  ".join(unguarded)
        )

    def test_migration_defines_the_trigger(self):
        sql = (REPO / "migrations/2026-08-17_entry_before_publication.sql").read_text()
        assert "CREATE TRIGGER trg_sp_entry_before_publication" in sql
        assert "BEFORE INSERT OR UPDATE OF trade_id, entry_date" in sql, (
            "The trigger must fire on the two columns the verdict depends on, "
            "so a pnl-only update inherits the verdict already on the row."
        )

    def test_missing_timestamp_is_not_permission_in_sql(self):
        # The SQL predicate and filed_before_close must agree that an unknown
        # filed_at means after-close. If they diverge, the trigger and the
        # Python path disagree about the same row.
        sql = (REPO / "migrations/2026-08-17_entry_before_publication.sql").read_text()
        assert "ELSE to_char(t.filing_date::date + 1, 'YYYY-MM-DD')" in sql
