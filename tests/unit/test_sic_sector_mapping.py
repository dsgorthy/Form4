"""SIC->sector must land in the SAME eleven names yfinance already uses.

WHY THIS MATTERS

industry_buy_pct_90d compares a ticker against its SECTOR PEERS. All 9,392
existing ticker_metadata rows carry yfinance's eleven sector names. Backfilling
the missing 3,836 from EDGAR SIC codes is only safe if the result speaks that
same vocabulary -- emit "Manufacturing" (an SIC division) next to "Technology"
(a yfinance sector) and half of every peer group silently falls out, which
would corrupt the signal rather than fill it.

So the mapping is pinned here: every value it can emit must be one of the
eleven, and the codes where a 2-digit fallback would be WRONG are checked
individually. 8731 is the important one -- "Commercial Physical & Biological
Research" is where a large share of small-cap biotech files, and its major
group 87 is otherwise Industrials.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "pipelines" / "insider_study" / "backfill_sector_from_edgar.py"


def _mod():
    spec = importlib.util.spec_from_file_location("bsfe", SRC)
    m = importlib.util.module_from_spec(spec)
    argv = sys.argv
    sys.argv = ["x"]
    try:
        spec.loader.exec_module(m)
    except SystemExit:
        pass
    finally:
        sys.argv = argv
    return m


# The eleven yfinance sectors present in ticker_metadata. Anything outside this
# set puts a ticker in a peer group that does not exist.
YFINANCE_SECTORS = {
    "Financial Services", "Healthcare", "Technology", "Industrials",
    "Consumer Cyclical", "Real Estate", "Consumer Defensive",
    "Communication Services", "Energy", "Basic Materials", "Utilities",
}


def test_every_emittable_value_is_a_yfinance_sector():
    m = _mod()
    emitted = set(m.SIC4.values()) | set(m.SIC2.values())
    assert emitted <= YFINANCE_SECTORS, (
        f"{emitted - YFINANCE_SECTORS} are not yfinance sector names. A ticker "
        "labelled with one would be invisible to its own peer group in "
        "industry_buy_pct_90d."
    )


def test_the_modules_own_vocabulary_matches():
    m = _mod()
    assert m.VALID == YFINANCE_SECTORS


@pytest.mark.parametrize("sic,expected", [
    # These would go to the WRONG sector on a 2-digit fallback.
    ("8731", "Healthcare"),          # biotech research; group 87 is Industrials
    ("2836", "Healthcare"),          # biological products; group 28 is Materials
    ("2834", "Healthcare"),          # pharma preparations
    ("3841", "Healthcare"),          # surgical instruments; group 38 varies
    ("6798", "Real Estate"),         # REIT; group 67 is Financial Services
    ("7372", "Technology"),          # prepackaged software
    ("3571", "Technology"),          # electronic computers; group 35 Industrials
    ("3711", "Consumer Cyclical"),   # motor vehicles; group 37 Industrials
    ("3721", "Industrials"),         # aircraft -- same group, other way
    ("1311", "Energy"),              # crude petroleum
    ("4911", "Utilities"),
    ("5912", "Consumer Defensive"),  # drug stores; group 59 Consumer Cyclical
])
def test_codes_that_need_a_four_digit_rule(sic, expected):
    assert _mod().sic_to_sector(sic) == expected


@pytest.mark.parametrize("sic,expected", [
    ("6022", "Financial Services"),   # state commercial banks
    ("2011", "Consumer Defensive"),   # meat packing
    ("1040", "Basic Materials"),      # gold mining
    ("4813", "Communication Services"),
    ("8011", "Healthcare"),           # offices of doctors
])
def test_two_digit_fallback(sic, expected):
    assert _mod().sic_to_sector(sic) == expected


def test_unknown_and_empty_are_none_not_a_guess():
    """A wrong sector is worse than no sector -- it creates a false peer."""
    m = _mod()
    for bad in (None, "", "9999", "abcd"):
        assert m.sic_to_sector(bad) is None, f"{bad!r} should not map"


def test_funds_are_excluded_by_the_query_not_mapped():
    """Mutual funds and ETFs must stay NULL. A sector for a fund is a category
    error, and inventing one puts it in a peer group it does not belong to."""
    src = SRC.read_text(encoding="utf-8")
    assert "MUTUALFUND" in src and "ETF" in src, (
        "the candidate query no longer excludes funds; 884 mutual funds and "
        "127 ETFs would be given operating-company sectors"
    )
