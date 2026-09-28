"""The insider page title must carry the company and the intent term.

Measured on the Search Console export for the 28 days to 2026-09-20, scraper
queries removed: real demand was 5,483 impressions and 25 clicks, and 85% of
those impressions (4,672 across 801 queries) were PERSON NAMES. The subset with
buying intent is NAME + COMPANY, and it was already ranking at positions nobody
clicks:

    "gianluca romano seagate"         pos 38
    "ban seng teh seagate"            pos 33
    "duncan mckechnie vertex"         pos 27
    "joy liu vertex"                  pos 23
    "tom hough parthenon"             pos 16
    "brian ferdinand liquid holdings" pos 36

The title was `"{name} — Insider Profile"`, matching neither half. The company
page title already did this right — "GME Insider Trading — GameStop Corp." —
which is part of why company pages out-earn insider pages per submitted URL.

These are source-level checks plus a port of the pure title function, because
the repo has no JS test runner and the rules worth pinning are the truncation
order and the fact that the NAME is never cut.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TITLES = REPO / "frontend" / "src" / "lib" / "page-titles.ts"
PAGE = REPO / "frontend" / "src" / "app" / "insider" / "[id]" / "page.tsx"
SD = REPO / "frontend" / "src" / "lib" / "structured-data.ts"

TITLE_BUDGET = 52

SUFFIXES = [
    ", Incorporated", " Incorporated", ", Corporation", " Corporation",
    ", Company", " Company", ", Holdings", ", Group", ", L.P.", " L.P.",
    ", LLC", " LLC", ", Ltd.", " Ltd.", ", Ltd", " Ltd", ", PLC", " PLC",
    ", Corp.", " Corp.", ", Corp", " Corp", ", Inc.", " Inc.", ", Inc", " Inc",
    ", S.A.", " N.V.", " plc",
]


def _code_only(src: str) -> str:
    """Drop comments. A test that matches a comment is asserting about prose."""
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return "\n".join(
        ln for ln in src.splitlines() if not ln.lstrip().startswith(("//", "*"))
    )


SENTINELS = {"NONE", "NA", "N", "NULL", "UNKNOWN", "OTC"}


def clean_ticker(t):
    if not t:
        return None
    first = t.split(",")[0].strip().upper()
    if not re.fullmatch(r"[A-Z]{1,6}(\.[A-Z])?", first):
        return None
    if first in SENTINELS:
        return None
    return first


def short_company(c):
    if not c:
        return None
    s = c.strip()
    for suf in SUFFIXES:
        if s.lower().endswith(suf.lower()):
            s = s[: -len(suf)].strip().rstrip(",")
            break
    return s or None


def title(name, company=None, ticker=None):
    co = short_company(company)
    ticker = clean_ticker(ticker)
    tail = " Insider Trading"
    if co:
        t = f"{name} — {co}{tail}"
        if len(t) <= TITLE_BUDGET:
            return t
    if ticker:
        t = f"{name} — {ticker}{tail}"
        if len(t) <= TITLE_BUDGET:
            return t
    return f"{name} —{tail}"


def test_the_budget_and_suffix_list_match_the_implementation():
    """The port above is only meaningful if it tracks the real thing."""
    src = TITLES.read_text()
    m = re.search(r"const TITLE_BUDGET = (\d+)", src)
    assert m and int(m.group(1)) == TITLE_BUDGET, (
        "TITLE_BUDGET changed in page-titles.ts; update this test's port"
    )
    sent = re.search(r"SENTINEL_TICKERS = new Set\(\[(.*?)\]\)", src, re.S)
    assert sent, "SENTINEL_TICKERS not found"
    assert set(re.findall(r'"([^"]+)"', sent.group(1))) == SENTINELS, (
        "the sentinel ticker list drifted; update this test's port"
    )
    listed = re.search(r"const SUFFIXES = \[(.*?)\];", src, re.S)
    assert listed, "SUFFIXES not found"
    in_ts = set(re.findall(r'"([^"]+)"', listed.group(1)))
    assert in_ts == set(SUFFIXES), (
        f"suffix list drifted: only in ts {sorted(in_ts - set(SUFFIXES))}, "
        f"only in test {sorted(set(SUFFIXES) - in_ts)}"
    )


@pytest.mark.parametrize("name,company,ticker,expected", [
    # The query we are actually targeting.
    ("Gianluca Romano", "Seagate Technology Holdings plc", "STX",
     "Gianluca Romano — STX Insider Trading"),
    ("Joy Liu", "Vertex Pharmaceuticals, Inc.", "VRTX",
     "Joy Liu — Vertex Pharmaceuticals Insider Trading"),
    # Corporate suffix stripped: nobody searches "xponential fitness, inc."
    ("Anthony Geisler", "Xponential Fitness, Inc.", "XPOF",
     "Anthony Geisler — Xponential Fitness Insider Trading"),
    # No employer known at all.
    ("Jane Roe", None, None, "Jane Roe — Insider Trading"),
    # Blank company string; falls back to the ticker rather than rendering
    # "Jane Roe —  Insider Trading" with a hole in it.
    ("Jane Roe", "   ", "ABC", "Jane Roe — ABC Insider Trading"),
])
def test_titles(name, company, ticker, expected):
    assert title(name, company, ticker) == expected


@pytest.mark.parametrize("raw,expected", [
    # 5,344 rows literally say NONE. It must never reach a <title>.
    ("NONE", None), ("[NONE]", None), ("NA", None), ("*", None),
    ('"WM"', None), ("[USG]", None), ("NWIN(OB", None), ("[N", None),
    ("", None), (None, None),
    # Comma-joined share classes: take the first symbol.
    ("GEF,GEF.B", "GEF"), ("LEN,LEN.B", "LEN"), ("VIACA,VIAC", "VIACA"),
    # Ordinary symbols, including a class suffix on its own.
    ("STX", "STX"), ("brk.b", "BRK.B"), (" vrtx ", "VRTX"),
])
def test_dirty_tickers_never_reach_a_title(raw, expected):
    assert clean_ticker(raw) == expected


def test_a_sentinel_ticker_loses_the_employer_rather_than_printing_it():
    """A long company plus ticker='NONE' must fall through to no employer."""
    out = title("Jane Roe", "A" * 80, "NONE")
    assert out == "Jane Roe — Insider Trading", out


def test_the_name_is_never_truncated():
    """A title cut mid-name matches nothing. The company is what gives way."""
    long_name = "Bartholomew Fitzwilliam-Montgomery III"
    out = title(long_name, "Some Extremely Long Holding Company", "XYZ")
    assert out.startswith(long_name), out
    assert "Insider Trading" in out, "the intent term was dropped instead"


def test_a_long_company_degrades_to_the_ticker_then_to_nothing():
    assert title("Ann Lee", "A" * 80, "TKR") == "Ann Lee — TKR Insider Trading"
    assert title("Ann Lee", "A" * 80, None) == "Ann Lee — Insider Trading"


def test_the_page_uses_the_helper_and_not_a_literal():
    src = PAGE.read_text()
    assert "insiderPageTitle(" in src and "insiderPageDescription(" in src
    code = _code_only(src)
    assert "— Insider Profile" not in code, (
        'the page still builds a "— Insider Profile" title somewhere; that '
        "string matches no query anyone types"
    )
    # Every title field must come from the helper. openGraph and twitter were
    # set separately from the main one, so a fix to `title` alone left the
    # social cards saying "Insider Profile".
    titles = re.findall(r"title:\s*([^,\n]+)", code)
    literal = [t for t in titles if "Insider Profile" in t]
    assert len(literal) <= 1, (
        f"{len(literal)} title fields are still literals: {literal}. Only the "
        "no-data 403 fallback may be one, because it has no name to use."
    )


def test_the_title_names_the_same_employer_as_the_body():
    """Both must pick the primary company by trade COUNT. A title naming a
    different company than the <h1> is worse than a generic one."""
    src = PAGE.read_text()
    picks = re.findall(r"sort\(\s*\(a, b\) =>\s*\(?b\.trade_count", src)
    assert len(picks) >= 2, (
        "expected the metadata and the body to derive the primary company the "
        f"same way; found {len(picks)} trade_count sorts"
    )


def test_the_person_entity_links_to_edgar():
    """sameAs is what resolves a common name to one filer. A CIK is an
    authoritative identifier for exactly one person.

    Scoped to insiderJsonLd and to CODE, not the whole file: `sameAs` also
    appears in this module's comments and on other entity types, and a first
    version of this test passed against a commented-out mention.
    """
    src = SD.read_text()
    start = src.index("export function insiderJsonLd")
    end = src.index("export function", start + 10)
    fn = _code_only(src[start:end])
    assert "sameAs" in fn, "the Person node no longer links out"
    assert "browse-edgar" in fn, "sameAs does not point at the SEC filing index"
    assert "identifier" in fn, "the CIK is not published as an identifier"
    assert 'padStart(10, "0")' in fn or "padStart(10," in fn, (
        "the CIK is not zero-padded; EDGAR's canonical form is 10 digits and an "
        "unpadded one does not resolve"
    )


def test_the_stale_best_window_marker_is_gone():
    """`best_window` is still computed on the RETIRED lot-based basis. It was
    marking 7d as 'best' next to a 3% accuracy on a public page."""
    src = PAGE.read_text()
    assert "* Best window" not in src or "was removed" in src, (
        "the best-window asterisk is rendering again; it is computed on the "
        "lot basis that docs/insider_track_record.md retired"
    )
    assert 'tr.best_window === w' not in src, (
        "the header still highlights best_window"
    )
