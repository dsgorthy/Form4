"""Unwrap a Bronze submission and parse its ownershipDocument AS FILED.

WHY NOT REUSE strategies/insider_catalog/backfill_live.parse_form4_xml

It is the live ingest's parser and it is not faithful, on purpose: it drops
any transaction whose code is outside P/S/F/M/A/G/V/X, any P/S line with a
non-positive price or quantity, any line with a quantity of zero, any line
whose date fails validate_trade_date, and it coerces every number to float.
Those are reasonable choices for a table that feeds a screener. They are the
wrong choices for Silver, whose one job is to reproduce what SEC holds -- a
line the filer entered with price 0 and a footnote is a fact about the filing,
and dropping it is how a "$0 exercise" becomes invisible. So this parses
every line, keeps every value as the exact decimal string SEC served, and
records absence as NULL rather than 0.

THE WRAPPER

`content` is the full SGML submission:

    <SEC-DOCUMENT>0001493152-25-015819.txt : 20250926
    <SEC-HEADER>...
    <ACCEPTANCE-DATETIME>20250926172901
    CONFORMED SUBMISSION TYPE:	4
    CONFORMED PERIOD OF REPORT:	20250925
    ...
    <DOCUMENT><TYPE>4 ... <TEXT><XML>
    <?xml version="1.0"?>
    <ownershipDocument>...</ownershipDocument>
    </XML></TEXT></DOCUMENT>

ACCEPTANCE-DATETIME is Eastern and is stored naive and Eastern -- see the
filed_at rule in memory: converting it is what produced 37 look-ahead entries.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

#: Bump when the parse changes shape. Rows carry it; a re-parse is
#: "receipts with an older version", not a truncate.
PARSER_VERSION = "2"   # 2: CIKs kept zero-padded as filed, not int-stripped

_HEADER_KV = re.compile(r"^([A-Z][A-Z0-9 \-/]+?):\s*(.*)$", re.M)
_ACCEPTANCE = re.compile(r"<ACCEPTANCE-DATETIME>(\d{14})")
_OWNERSHIP = re.compile(r"(<ownershipDocument\b.*?</ownershipDocument>)", re.S)


@dataclass
class Header:
    acceptance_datetime: Optional[datetime]   # naive, Eastern, as filed
    submission_type: Optional[str]
    period_of_report: Optional[date]


@dataclass
class Owner:
    cik: str
    name: Optional[str]
    is_director: Optional[bool]
    is_officer: Optional[bool]
    is_10pct: Optional[bool]
    is_other: Optional[bool]
    officer_title: Optional[str]


@dataclass
class Line:
    line_no: int
    is_derivative: bool
    security_title: Optional[str]
    trans_date: Optional[date]
    deemed_execution_date: Optional[date]
    trans_form_type: Optional[str]
    trans_code: Optional[str]
    equity_swap: Optional[bool]
    trans_acquired_disp: Optional[str]
    shares: Optional[Decimal]
    price_per_share: Optional[Decimal]
    shares_owned_after: Optional[Decimal]
    direct_indirect: Optional[str]
    nature_of_ownership: Optional[str]
    exercise_price: Optional[Decimal] = None
    exercise_date: Optional[date] = None
    expiration_date: Optional[date] = None
    underlying_title: Optional[str] = None
    underlying_shares: Optional[Decimal] = None
    footnote_ids: list[str] = field(default_factory=list)


@dataclass
class Filing:
    document_type: Optional[str]
    period_of_report: Optional[date]
    issuer_cik: Optional[str]
    ticker: Optional[str]
    owners: list[Owner]
    lines: list[Line]


class NoDocument(Exception):
    """The submission carries no <ownershipDocument> -- not a parse error,
    a fact about the filing (some are paper/PDF-only or withdrawn)."""


def unwrap(content: str) -> tuple[Header, str]:
    """(header, ownershipDocument xml). Raises NoDocument."""
    m = _ACCEPTANCE.search(content)
    acc = datetime.strptime(m.group(1), "%Y%m%d%H%M%S") if m else None
    kv = {}
    # Only the SEC-HEADER block, so a "TYPE:" inside an embedded document
    # cannot masquerade as a header field.
    hdr_end = content.find("</SEC-HEADER>")
    for k, v in _HEADER_KV.findall(content[: hdr_end if hdr_end > 0 else 4000]):
        kv.setdefault(k.strip(), v.strip())
    por = kv.get("CONFORMED PERIOD OF REPORT")
    header = Header(
        acceptance_datetime=acc,
        submission_type=kv.get("CONFORMED SUBMISSION TYPE"),
        period_of_report=_date(por) if por else None,
    )
    od = _OWNERSHIP.search(content)
    if not od:
        raise NoDocument("no <ownershipDocument> in submission")
    return header, od.group(1)


# ── XML helpers ─────────────────────────────────────────────────────────────

def _strip_ns(root: ET.Element) -> None:
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]


def _text(el: Optional[ET.Element], path: str) -> Optional[str]:
    """The element's text, stripped, or None if absent or empty. Empty is
    absence: a filer who left <value></value> did not say 0."""
    if el is None:
        return None
    t = el.findtext(path)
    if t is None:
        return None
    t = t.strip()
    return t or None


def _dec(s: Optional[str]) -> Optional[Decimal]:
    if s is None:
        return None
    try:
        return Decimal(s.replace(",", ""))
    except InvalidOperation:
        # Keep the row; the field is unparseable, which is itself a fact.
        return None


def _date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:10] if fmt == "%Y-%m-%d" else s, fmt).date()
        except ValueError:
            continue
    return None


def _bool(s: Optional[str]) -> Optional[bool]:
    if s is None:
        return None
    return s.strip().lower() in ("1", "true")


def _footnotes(el: ET.Element) -> list[str]:
    return sorted({f.get("id") for f in el.iter("footnoteId") if f.get("id")})


# ── the parse ───────────────────────────────────────────────────────────────

def parse_ownership(xml_text: str) -> Filing:
    """Every line, in document order, as filed. Raises ET.ParseError."""
    root = ET.fromstring(xml_text)
    _strip_ns(root)

    issuer = root.find("issuer")
    owners: list[Owner] = []
    for ro in root.findall("reportingOwner"):
        rid = ro.find("reportingOwnerId")
        rel = ro.find("reportingOwnerRelationship")
        cik = _text(rid, "rptOwnerCik")
        if not cik:
            continue          # a reportingOwner with no CIK cannot be keyed; SEC does not emit these
        # AS FILED: SEC pads CIKs to ten digits and so does trades.rptowner_cik.
        # v1 stripped them to int, which was both a departure from the source
        # and the reason the first parity run joined zero rows.
        owners.append(Owner(
            cik=cik,
            name=_text(rid, "rptOwnerName"),
            is_director=_bool(_text(rel, "isDirector")),
            is_officer=_bool(_text(rel, "isOfficer")),
            is_10pct=_bool(_text(rel, "isTenPercentOwner")),
            is_other=_bool(_text(rel, "isOther")),
            officer_title=_text(rel, "officerTitle"),
        ))

    lines: list[Line] = []
    n = 0
    nd = root.find("nonDerivativeTable")
    for txn in (nd.findall("nonDerivativeTransaction") if nd is not None else []):
        n += 1
        lines.append(_line(txn, n, derivative=False))
    dt = root.find("derivativeTable")
    for txn in (dt.findall("derivativeTransaction") if dt is not None else []):
        n += 1
        lines.append(_line(txn, n, derivative=True))

    return Filing(
        document_type=_text(root, "documentType"),
        period_of_report=_date(_text(root, "periodOfReport")),
        issuer_cik=_text(issuer, "issuerCik"),   # as filed, ten digits
        ticker=_text(issuer, "issuerTradingSymbol"),
        owners=owners,
        lines=lines,
    )


def _line(txn: ET.Element, n: int, derivative: bool) -> Line:
    amt = txn.find("transactionAmounts")
    post = txn.find("postTransactionAmounts")
    own = txn.find("ownershipNature")
    coding = txn.find("transactionCoding")
    ln = Line(
        line_no=n,
        is_derivative=derivative,
        security_title=_text(txn, "securityTitle/value"),
        trans_date=_date(_text(txn, "transactionDate/value")),
        deemed_execution_date=_date(_text(txn, "deemedExecutionDate/value")),
        trans_form_type=_text(coding, "transactionFormType"),
        trans_code=_text(coding, "transactionCode"),
        equity_swap=_bool(_text(coding, "equitySwapInvolved")),
        trans_acquired_disp=_text(amt, "transactionAcquiredDisposedCode/value"),
        shares=_dec(_text(amt, "transactionShares/value")),
        price_per_share=_dec(_text(amt, "transactionPricePerShare/value")),
        shares_owned_after=_dec(_text(post, "sharesOwnedFollowingTransaction/value")),
        direct_indirect=_text(own, "directOrIndirectOwnership/value"),
        nature_of_ownership=_text(own, "natureOfOwnership/value"),
        footnote_ids=_footnotes(txn),
    )
    if derivative:
        und = txn.find("underlyingSecurity")
        ln.exercise_price = _dec(_text(txn, "conversionOrExercisePrice/value"))
        ln.exercise_date = _date(_text(txn, "exerciseDate/value"))
        ln.expiration_date = _date(_text(txn, "expirationDate/value"))
        ln.underlying_title = _text(und, "underlyingSecurityTitle/value")
        ln.underlying_shares = _dec(_text(und, "underlyingSecurityShares/value"))
    return ln


def parse_submission(content: str) -> tuple[Header, Filing]:
    header, xml_text = unwrap(content)
    return header, parse_ownership(xml_text)
