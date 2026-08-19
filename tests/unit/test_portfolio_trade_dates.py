"""A position row carries three different days, and they must stay different.

    trade_date   when the insider actually dealt
    filing_date  when EDGAR received the Form 4
    entry_date   the first close we could have bought at

simulate_strategy_portfolio built its INSERT as

    c.company, c.entry_date, c.entry_date,

against a column list of `company, filing_date, trade_date`, so both date
columns got the day we opened. The candidate query had been selecting
`t.filing_date::text, t.trade_date::text` the whole time — fetched, then
dropped.

The visible result, on the page that links to the SEC filing it contradicts:
BFLY / Larry Robbins showed "Filing Date 2025-11-24, Trade Date 2025-11-24"
above a link to a filing reporting 2025-11-19 and filed 2025-11-21. All 381
simulated rows were wrong the same way; 630 rows were repaired by
migrations/2026-08-18_portfolio_filing_dates.sql.
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SIM = REPO / "pipelines/insider_study/simulate_strategy_portfolio.py"
DETAIL = REPO / "frontend/src/app/portfolio/trades/[id]/page.tsx"


def test_insert_does_not_stamp_entry_date_into_the_filing_columns():
    src = SIM.read_text()
    for wrong in ("c.company, c.entry_date, c.entry_date",
                  "o.company, o.entry_date, o.entry_date"):
        assert wrong not in src, (
            f"`{wrong}` writes the entry date into filing_date and trade_date"
        )


def test_insert_writes_the_filings_own_dates():
    src = SIM.read_text()
    assert "c.company, c.filing_date, c.trade_date" in src
    assert "o.company, o.filing_date, o.trade_date" in src


def test_positions_carry_the_filing_dates():
    """Both dataclasses need the fields, or the INSERT cannot reach them."""
    src = SIM.read_text()
    for cls in ("class OpenPosition", "class ClosedPosition"):
        body = src.split(cls, 1)[1].split("\n@dataclass", 1)[0]
        assert "filing_date: Optional[str]" in body, f"{cls} has no filing_date"
        assert "trade_date: Optional[str]" in body, f"{cls} has no trade_date"


def test_candidate_query_still_selects_them():
    assert "t.filing_date::text, t.trade_date::text" in SIM.read_text()


# ── the header that read "1.5 /10" beside "Grade A" ─────────────────────────

def test_conviction_and_grade_are_labelled():
    """signal_quality is the conviction score (floor 1.5, so a 1.5 means the
    grade carried the trade and conviction merely cleared). signal_grade is the
    insider's career grade. Rendered bare and adjacent they read as one broken
    measurement."""
    src = DETAIL.read_text()
    assert "Career grade" in src, "the letter grade is unlabelled"
    assert "Conviction" in src, "the /10 score is unlabelled"
    assert ">Signal Quality<" not in src, (
        "'Signal Quality' names two different numbers on this page; "
        "call the conviction score conviction"
    )


def test_dates_are_labelled_by_what_they_mean():
    src = DETAIL.read_text()
    assert 'label="Insider traded"' in src
    assert 'label="Filed with SEC"' in src


def test_detail_page_reads_the_stored_dates_not_the_entry_date():
    """Guards the other half: correct data rendered from the wrong field."""
    src = DETAIL.read_text()
    for row in re.findall(r'<Row label="(?:Insider traded|Filed with SEC)"[^/]*/>', src):
        assert "entry_date" not in row, f"filing row renders entry_date: {row}"
