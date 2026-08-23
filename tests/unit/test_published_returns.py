"""The published return figures have one definition, written down, and pinned.

WHY

The headline CAGR moved four times in two days — 52.3 -> 43.5 -> 48.8 -> 55.4 —
and not one move was a market event. Every one was a definitional gap or a data
defect:

  filed_at read in the server timezone            (2026-08-18)
  filed_at Eastern for 2026 rows, after-bell
    fills moved from the next close to the next open (2026-08-19)
  is_largest_ever wrong on 23.7% of flags         (2026-08-19)
  sleeve vs blended, and the period bug           (2026-08-20)

docs/published_returns_methodology.md is the definition. These tests pin the
parts of it that live in code, so the next move has to be deliberate.
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOC = REPO / "docs/published_returns_methodology.md"
PORTFOLIO = REPO / "api/routers/portfolio.py"


def test_the_methodology_document_exists():
    """If this file goes, the numbers have no definition again."""
    assert DOC.exists(), "docs/published_returns_methodology.md is missing"
    body = DOC.read_text()
    for required in ("blended", "SPY", "excess", "first trade → today"):
        assert required in body, f"methodology no longer specifies: {required}"


def test_cagr_period_runs_to_today_not_to_the_last_trade():
    """Measuring first-trade -> last-trade deletes the stretch where a book
    stopped trading, which is exactly when it is doing badly. It published
    15.4% for Insider Dip Buys where the full window gives 13.7%."""
    src = PORTFOLIO.read_text()
    block = src.split("final = curve[-1]", 1)[1].split("cagr =", 1)[0]
    assert "utcnow()" in block or "now()" in block, (
        "CAGR period no longer runs to the present"
    )
    assert 'summary["last_trade"]' not in block, (
        "CAGR period is measured to the last trade again — a strategy that "
        "goes quiet must carry the cost of going quiet"
    )


def test_cagr_uses_a_real_year_length():
    src = PORTFOLIO.read_text()
    block = src.split("final = curve[-1]", 1)[1].split("cagr =", 1)[0]
    assert "365.25" in block, "year length reverted to 365"


def test_methodology_records_the_selection_bias():
    """The strategy was chosen as best-of-eleven on the same data it is
    measured over. That caveat is the single most important thing on the page
    and it must not be quietly dropped."""
    body = DOC.read_text()
    assert "Eleven variants" in body or "eleven variants" in body
    assert "upper bound" in body


def test_methodology_records_the_conviction_fragility():
    """The gate's sensitivity must stay documented — but the BAND is a
    measurement, and a measurement can go stale.

    This used to assert the literal 46.2 / 55.7 endpoints. They were measured
    2026-08-20 and invalidated on 2026-08-22, when the tranche correction moved
    two of the gate's own inputs (career_grade and is_largest_ever). Pinning
    the numbers would have forced a stale band to stay quoted in the document
    that defines what we publish.

    So: the fragility must be recorded, and any band present must be marked
    stale until re-measured.
    """
    body = DOC.read_text()
    assert "min_conviction" in body, (
        "the conviction gate's sensitivity is no longer documented — it is the "
        "single most load-bearing parameter in these books"
    )
    if "46.2" in body or "55.7" in body:
        assert "STALE" in body or "stale" in body, (
            "the 2026-08-20 perturbation band is quoted without being marked "
            "stale. It predates the tranche correction, which changed two of "
            "the gate's inputs."
        )


def test_methodology_shows_annual_returns_not_just_a_cagr():
    """A single compounded figure hides that 2025 is 47% of A-List's P&L and
    that Insider Dip Buys is behind SPY in 2026."""
    body = DOC.read_text()
    for year in ("2023", "2024", "2025", "2026"):
        assert year in body
    assert "SPY" in body


def test_entry_rule_is_referenced_not_restated():
    """The methodology must point at the code, or the two drift."""
    body = DOC.read_text()
    assert "entry_timing" in body


def test_every_number_moved_has_a_regression_test():
    """Named in the doc, and they have to actually exist."""
    for name in ("test_entry_timing_eastern",
                 "test_cumulative_signal_windows",
                 "test_published_returns"):
        assert name in DOC.read_text(), f"{name} not cited in the methodology"
        assert (REPO / f"tests/unit/{name}.py").exists(), f"{name}.py is missing"
