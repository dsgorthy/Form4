"""A real trade against split-adjusted history must never be "corrected".

WHAT WENT WRONG, 2026-09-04

price_validator's `price_is_total_value` rule is an arithmetic identity: if
price/qty lands inside the stock's price band, the price field must have held
the trade's total. That is usually right, and it fires on coincidences.

Our price history is SPLIT-ADJUSTED. Filings are not. So a real GOOG sale at
$2,076.19 x 30 shares in March 2021 is measured against a band of ~$69 — the
post-2022-20:1-split price — and 2076.19/30 = $69.21 lands right in it. The
rule then rewrote a correct $62,285 trade into a $2,076 trade at $69 a share.

Run unattended across all history it corrupted 29 real trades:

    GOOG  17 rows   filed $1,725-$2,963    20:1 split, 2022
    AMZN   4 rows   filed $2,301-$3,370    20:1 split, 2022
    CMG    8 rows   filed $1,021-$2,341    50:1 split, 2024

All were restored from price_as_filed. note_uncorrectable already documented
this hazard from the other direction — the Lucid PIF filing missing its band
because the history is adjusted for a later REVERSE split — so the trap was
known; what was missing was a guard on the side where a correction exists and
is wrong.

THE DISCRIMINATOR IS THE RATIO, and it is not subtle:

    split artifacts          20x - 50x the band
    genuine total-value      2,551x - 61,584x the band

which is what makes a cheap test sufficient. No split reference data needed.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SRC = (REPO / "strategies" / "insider_catalog" / "price_validator.py").read_text(encoding="utf-8")


def _load():
    """Exec just the pure helpers. The module imports config.database at import
    time, which needs a live Postgres this suite does not have."""
    ns = {}
    for name in ("SPLIT_RATIOS", "SPLIT_RATIO_TOLERANCE"):
        m = re.search(rf"^{name} = (.+)$", SRC, re.M)
        assert m, f"{name} is gone from price_validator"
        exec(f"{name} = {m.group(1).split('#')[0].strip()}", ns)
    block = SRC[SRC.index("def looks_like_a_split"):SRC.index("def attempt_correction")]
    exec(block, ns)
    return ns["looks_like_a_split"]


# ── the rows that were actually corrupted ──────────────────────────────────

@pytest.mark.parametrize("name,price,lo,hi", [
    ("GOOG 2021-03-02, 20:1 split in 2022", 2076.19, 60.0, 78.0),
    ("GOOG 2021-01-05",                     1725.00, 50.0, 66.0),
    ("AMZN 2022-03-31, 20:1 split in 2022", 3370.19, 150.0, 178.0),
    ("CMG 2023-12-15, 50:1 split in 2024",  2341.83, 42.0, 52.0),
])
def test_split_artifacts_are_refused(name, price, lo, hi):
    assert _load()(price, lo, hi) is not None, (
        f"{name} is no longer recognised as a split artifact. This row is a "
        "REAL trade; correcting it rewrites the filer's own figures."
    )


# ── the rows that genuinely needed correcting ──────────────────────────────

@pytest.mark.parametrize("name,price,lo,hi", [
    ("IHT  $52,316 for a $1.50 stock",   52316.70,  1.50,  1.84),
    ("CNTM $61,892 for a $0.89 stock",   61892.00,  0.82,  1.19),
    ("STNG $191,325 for a $76 stock",   191325.00, 70.00, 80.00),
    ("AMMA $50,000 for a $1.09 stock",   50000.00,  1.00,  1.20),
])
def test_genuine_total_value_errors_still_correct(name, price, lo, hi):
    assert _load()(price, lo, hi) is None, (
        f"{name} is being refused as a split. These are real parse failures "
        "and refusing them leaves phantom billions in the published data."
    )


def test_the_guard_runs_before_any_correction_is_offered():
    """A guard that runs after the identity has already matched is no guard."""
    body = SRC[SRC.index("def attempt_correction"):SRC.index("def find_suspects")]
    guard = body.index("looks_like_a_split")
    identity = body.index("price_is_total_value")
    assert guard < identity, (
        "the split check no longer runs before price_is_total_value is "
        "offered, so a split artifact can still be corrected"
    )


def test_auto_apply_is_limited_to_the_arithmetic_identity():
    """power_of_10_shift cannot tell a decimal typo from a foreign currency,
    and must not be written unattended from the ingest path."""
    m = re.search(r"AUTO_APPLY_METHODS = \(([^)]*)\)", SRC)
    assert m, "AUTO_APPLY_METHODS is gone"
    assert "power_of_10_shift" not in m.group(1), (
        "power_of_10_shift is in the unattended auto-apply set"
    )
