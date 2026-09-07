"""A dollar aggregate must not include a price we do not believe.

WHAT THE SOURCE ACTUALLY SAYS

The phantom values on AMMA, CNTM and IHT were diagnosed for months as a parse
failure -- price_validator's correction is named `price_is_total_value`. With
Bronze in place it was tested against the source document on 2026-09-06:

    IHT 0001493152-25-015819
      trades:  price 22,625  qty 12,500  value $282,812,500
      XML:     <transactionPricePerShare><value>22625</value>
               <transactionShares><value>12500</value>

62 of 64 suspect rows matched the XML exactly. The parser is faithful; the
FILER entered 22,625 as a per-share price for a $1.50 stock. There is nothing
to re-parse and nothing to correct.

So the filed price is kept, and a separate column says we do not believe it.
The serving layer excludes it from dollar aggregates while the filing page
still shows what SEC holds -- the filing is real and a reader looking it up
must see it.

    IHT served  $7,207,876,940  ->  $2,325,931
    CNTM        $21,975,820,798 ->    $54,233,010
    AMMA         $5,212,300,000 ->       $100,000

THE GUARD MUST TRAVEL WITH value_suspect. There is no shared helper for the
published predicate -- it is duplicated across five routers -- so the only way
these stay together is a test that says so.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROUTERS = sorted((REPO / "api" / "routers").glob("*.py"))
MIGRATION = (REPO / "migrations" / "2026-09-06_price_quality.sql").read_text(encoding="utf-8")


def _guards(src: str):
    """Every line that filters on value_suspect (not merely selects it)."""
    return [l for l in src.split("\n")
            if "value_suspect" in l and "COALESCE" in l and "NOT" in l]


@pytest.mark.parametrize("path", ROUTERS, ids=lambda p: p.name)
def test_every_value_suspect_guard_has_a_price_quality_guard(path: Path):
    """Paired, because a row we do not believe is untrustworthy for the same
    reason in the same places. Without this they drift: `value_suspect` is
    reserved for what is impossible on its own terms, and nothing else would
    make a later reader add the second condition."""
    src = path.read_text(encoding="utf-8")
    guards = _guards(src)
    n_pq = src.count("price_quality IS DISTINCT FROM 'implausible'")

    # A ROUTER WITH NO GUARD IS NOT A PASS. Skipping when none was found is
    # how the company page's headline total escaped: that query sums `value`
    # and had neither guard, so this test skipped the whole file and IHT went
    # on serving $7,207,876,940 for a $1.50 stock.
    sums_value = re.search(r"SUM\(\s*(t\.)?value\s*\)", src)
    if sums_value and not guards:
        raise AssertionError(
            f"{path.name} sums `value` but applies no value_suspect guard. "
            "Every dollar aggregate must exclude rows we do not believe."
        )
    if not guards:
        pytest.skip("no published-population guard in this router")
    assert n_pq >= len(guards), (
        f"{path.name} has {len(guards)} value_suspect guard(s) but {n_pq} "
        "price_quality guard(s). A dollar aggregate would include a filed "
        "price of $22,625 for a $1.50 stock."
    )


def test_the_flag_never_replaces_the_filed_price():
    """The whole point. price_validator.apply_correction OVERWRITES price and
    value; that is a derived layer rewriting a source fact, and it destroyed 29
    real GOOG/AMZN/CMG trades on 2026-09-04 when an arithmetic identity fired
    on coincidence."""
    assert "ADD COLUMN IF NOT EXISTS price_quality" in MIGRATION
    assert "UPDATE" not in MIGRATION.split("ALTER TABLE trades")[1].upper()[:400], (
        "the migration modifies rows rather than only adding the assessment "
        "column"
    )


def test_the_threshold_and_its_evidence_are_recorded():
    """100x is not arbitrary: below it 12,343 rows sit on 39 tickers -- 316
    filings each, the signature of a split, which moves every pre-split filing
    by the same factor. Above it concentration collapses to ~2 rows per ticker
    and every affected ticker trades under $10."""
    assert "100x" in MIGRATION and "39" in MIGRATION, (
        "the migration no longer records where the threshold came from; the "
        "next person to touch it has no way to know it was measured"
    )


def test_implausible_rows_are_still_reachable_on_their_own_page():
    """Suppressing a real SEC filing entirely would be worse than showing it
    labelled -- the reader's recourse is the filing, and it exists."""
    comment = MIGRATION[MIGRATION.index("COMMENT ON COLUMN"):]
    assert "still shown on the filing page" in comment, (
        "the column comment no longer records that implausible filings remain "
        "visible on their own page"
    )
