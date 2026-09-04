"""One population, everywhere a count is shown to a reader.

THE DECISION, 2026-09-03

Form4 is a decisions product, not a Form 4 mirror. Every user-facing count is
therefore DISCRETIONARY trades, counted PER FILING.

WHAT IT REPLACED

The insider page stated four numbers about the same person, all correct, three
different populations:

    17 transactions   every discretionary ROW (execution tranches)
     8 purchases      discretionary FILINGS
     6 companies      every company filed on, grants included
     5 tickers        discretionary only

Company pages did the same: "reported by 38 insiders" in the summary
(trans_code IN ('P','S')) against an "INSIDER ROSTER (48)" listing everyone
who had ever filed, 800px below it.

THE COST WAS ACCEPTED, NOT OVERLOOKED. AAPL's headline goes from "2,381 SEC
Form 4 insider transactions by 38 insiders" to "261 open-market insider trades
by 31 insiders" — an 89% smaller number in the search snippet. A page whose
four counts disagree is worse than a page with a smaller true one.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
INSIDERS = (REPO / "api" / "routers" / "insiders.py").read_text(encoding="utf-8")
COMPANIES = (REPO / "api" / "routers" / "companies.py").read_text(encoding="utf-8")
SUMMARY = (REPO / "frontend" / "src" / "components" / "entity-summary.tsx").read_text(encoding="utf-8")
INSIDER_PAGE = (REPO / "frontend" / "src" / "app" / "insider" / "[id]" / "page.tsx").read_text(encoding="utf-8")


def test_insider_counts_are_filings_not_rows():
    """COUNT(*) over trades counts execution tranches. Eight decisions filled
    in seventeen lots read as seventeen buys."""
    i = INSIDERS.index("AS n_tickers")
    block = INSIDERS[max(0, i - 900):i]
    assert "COUNT(DISTINCT COALESCE(filing_key, accession))" in block, (
        "the profile's buy/sell counts are back to COUNT(*), which counts "
        "execution lots rather than decisions"
    )


def test_the_summary_sentence_uses_the_discretionary_company_count():
    """cos.length is every company filed on, including grant-only ones."""
    assert "nCompanies={tr?.n_tickers" in INSIDER_PAGE, (
        "the summary sentence is back to counting every company filed on, "
        "which read 6 under a page saying 5"
    )


def test_company_counts_are_discretionary_filings():
    i = COMPANIES.index("AS total_trades")
    block = COMPANIES[max(0, i - 300):i + 900]
    assert "COUNT(DISTINCT COALESCE(filing_key, accession))" in block, (
        "company total_trades counts rows again"
    )
    assert "trans_code IN ('P', 'S')" not in block, (
        "the company summary is back on trans_code, which admits 10b5-1 "
        "planned trades — a scheduled sale is not a decision"
    )


def test_the_roster_is_the_same_population_as_the_sentence():
    i = COMPANIES.index("FROM insider_companies ic")
    block = COMPANIES[i:i + 900]
    assert "EXISTS" in block and "signal_class" in block, (
        "the insider roster lists every filer again; it read 48 under a "
        "sentence saying 38"
    )


def test_the_class_names_are_never_typed_in_the_router():
    """MEANINGFUL_CLASSES is derived from KIND_META and re-exported once.
    Typing the members here is exactly the drift that definition prevents."""
    for name, src in (("companies", COMPANIES), ("insiders", INSIDERS)):
        # The insider router legitimately names them inside pit/grade helpers
        # that predate this; what must not appear is a NEW hand-typed IN-list
        # in the company summary path.
        if name == "companies":
            assert "'discretionary_buy', 'discretionary_sell'" not in src, (
                "companies.py types the meaningful class names again; use "
                "MEANINGFUL_CLASSES via _meaningful()"
            )
            assert "_MEANINGFUL_IN" in src and "MEANINGFUL_CLASSES" in src


@pytest.mark.parametrize("phrase", [
    "SEC Form 4 insider transactions",
    "insider transaction",
])
def test_the_copy_does_not_call_them_form_4_transactions(phrase: str):
    """The phrase hid which population it counted, and it counted the widest
    one. What is being counted is open-market trades."""
    body = re.sub(r"\{/\*[\s\S]*?\*/\}", "", SUMMARY)
    assert phrase not in body, (
        f"the summary copy says {phrase!r} again. Name the population: these "
        "are open-market trades, one row per filing."
    )
