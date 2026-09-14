"""Silver's parser reproduces the filing. It does not improve it.

The live ingest's parser (strategies/insider_catalog/backfill_live.py
parse_form4_xml) drops any transaction with a code outside P/S/F/M/A/G/V/X,
any P/S line with a non-positive price or quantity, any zero-quantity line,
any line whose date fails validation, and coerces every number to float. Good
choices for a screener feed; wrong for a layer whose one job is to hold what
SEC holds. price_validator then OVERWROTE price and value on trades and on
2026-09-04 destroyed 29 real GOOG/AMZN/CMG rows.

Fixtures are real Bronze documents pulled read-only from Studio on
2026-09-11. The IHT one is the filer error that shaped the layer model: the
filer entered 22,625 as a per-share price for a $1.50 stock, and Silver must
say 22,625.
"""
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
FIX = ROOT / "tests" / "unit" / "fixtures" / "bronze"

from pipelines.silver import build, parse  # noqa: E402


def _load(acc: str) -> str:
    return (FIX / f"{acc}.txt").read_text()


def test_the_iht_filer_error_is_reproduced_not_corrected():
    header, filing = parse.parse_submission(_load("0001493152-25-015819"))
    assert header.acceptance_datetime == datetime(2025, 9, 26, 17, 29, 1)   # naive, Eastern, as filed
    assert header.submission_type == "4"
    assert filing.ticker == "IHT"
    [ln] = filing.lines
    assert ln.trans_code == "S"
    assert ln.shares == Decimal("12500")
    assert ln.price_per_share == Decimal("22625"), "Silver must reproduce the filed number"
    assert ln.trans_date == date(2025, 9, 25)
    assert ln.deemed_execution_date == date(2025, 9, 26)
    assert ln.trans_acquired_disp == "D"
    assert ln.shares_owned_after == Decimal("6226796")
    assert ln.direct_indirect == "I"
    assert ln.nature_of_ownership == "By Spouse"
    assert ln.equity_swap is False
    assert filing.owners[0].cik == "0001055365"   # padded, as filed


def test_value_is_the_product_of_what_was_filed():
    """value is GENERATED in the table; the parse never invents a total."""
    _, filing = parse.parse_submission(_load("0001493152-25-015819"))
    ln = filing.lines[0]
    assert ln.shares * ln.price_per_share == Decimal("282812500")


def test_line_numbers_run_in_document_order_across_both_tables():
    _, filing = parse.parse_submission(_load("0001179110-07-016964"))
    assert [(l.line_no, l.is_derivative) for l in filing.lines] == [(1, False), (2, True)]
    d = filing.lines[1]
    assert d.underlying_title is not None or d.exercise_price is not None or d.expiration_date is not None


def test_a_plain_filing_parses_to_one_line():
    header, filing = parse.parse_submission(_load("0001240812-07-000005"))
    assert len(filing.lines) == 1
    assert filing.lines[0].is_derivative is False
    assert header.acceptance_datetime is not None


_SYNTH = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-01-05</periodOfReport>
  <issuer><issuerCik>0000001234</issuerCik><issuerTradingSymbol>ZZZ</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0000009999</rptOwnerCik><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector><isOfficer>0</isOfficer></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-01-05</value></transactionDate>
      <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>J</transactionCode><equitySwapInvolved>0</equitySwapInvolved></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>0</value></transactionShares>
        <transactionPricePerShare><footnoteId id="F1"/></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>100</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <footnotes><footnote id="F1">Price not applicable.</footnote></footnotes>
</ownershipDocument>"""


def test_nothing_is_dropped_and_absence_is_null_not_zero():
    """A code-J line with zero shares and a footnoted price: the live parser
    drops it three different ways. Silver keeps it, with price NULL (the
    filer did not say a price) and the footnote id recorded."""
    filing = parse.parse_ownership(_SYNTH)
    [ln] = filing.lines
    assert ln.trans_code == "J"
    assert ln.shares == Decimal("0")
    assert ln.price_per_share is None
    assert ln.footnote_ids == ["F1"]
    # Ten digits, as filed -- trades.rptowner_cik is padded the same way, and
    # v1's int-stripping is why the first parity run joined nothing.
    assert filing.owners[0].cik == "0000009999" and filing.owners[0].is_director is True
    assert filing.issuer_cik == "0000001234"


def test_rows_for_emits_one_row_per_owner_per_line_and_flags_amendments():
    content = _load("0001493152-25-015819").replace("CONFORMED SUBMISSION TYPE:\t4", "CONFORMED SUBMISSION TYPE:\t4/A")
    content = content.replace("<documentType>4</documentType>", "<documentType>4/A</documentType>")
    status, rows, err = build.rows_for("0001493152-25-015819", "sha", content)
    assert status == "ok" and err is None
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == "0001493152-25-015819" and row[2] == 1
    assert row[9] is True, "is_amendment"
    assert row[24] == Decimal("22625")


def test_a_submission_without_a_document_is_a_receipt_not_a_crash():
    status, rows, err = build.rows_for("x", "sha", "<SEC-DOCUMENT>x.txt : 20250101\n<SEC-HEADER>\n</SEC-HEADER>\nno xml here")
    assert status == "no_document" and rows == []


def test_garbage_xml_is_a_parse_error_receipt():
    content = "<SEC-HEADER>\n</SEC-HEADER>\n<ownershipDocument><unclosed></ownershipDocument>"
    status, rows, err = build.rows_for("x", "sha", content)
    assert status == "parse_error" and "ParseError" in (err or "")


def test_silver_never_imports_the_live_corrector():
    """price_validator.apply_correction is the overwrite the layer exists to
    stop. It must not be reachable from the Silver package."""
    import re
    for f in (ROOT / "pipelines" / "silver").glob("*.py"):
        src = f.read_text()
        # Mentions in docstrings are fine -- they explain WHY. Imports and
        # calls are not.
        assert not re.search(r"^\s*(from|import)\s+\S*(price_validator|backfill_live)", src, re.M), \
            f"{f.name} imports the live corrector or parser"
        assert "apply_correction(" not in src, f"{f.name} calls apply_correction"
