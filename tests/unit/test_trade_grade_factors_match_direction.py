"""A factor's words must describe the filing it is attached to.

`compute_trade_grade` runs on buys and sells alike, but its factor text was
written as if every row were a purchase. The Cluster factor said "N insiders
buying together" on a SELL — 46,856 of them in the twelve months to
2026-08-26, 42.3% of every sell published — and the filing page renders that
string verbatim as a reason for the score. EOSE on 2026-07-28 carried the
"Opportunistic" (bearish) tag and "+8 Cluster: 3 insiders buying together" on
the same page.

This does not assert anything about the SCORE, only that no factor shown to a
reader claims a direction the row does not have.
"""
from __future__ import annotations

import pytest

from api.trade_grade import compute_trade_grade

# Words that assert a direction. A factor description containing one of these
# must only appear on a row going that way.
BUY_WORDS = ("buying", "bought", "purchase", "buys")
SELL_WORDS = ("selling", "sold", "sale", "sells")

CLUSTERED_SELL = {
    "trade_type": "sell",
    "title": "Chief Commercial Officer",
    "cluster_size_pit": 3,
    "cohen_routine": 0,
    "is_10b5_1": 1,
    "dip_1mo": -0.47,
    "dip_3mo": -0.47,
    "value": 371_001.0,
}
CLUSTERED_BUY = dict(CLUSTERED_SELL, trade_type="buy", is_10b5_1=0)


def _descriptions(item: dict) -> list[str]:
    return [f["description"].lower() for f in compute_trade_grade(item)["factors"]]


def test_a_sell_is_never_described_as_buying():
    for d in _descriptions(CLUSTERED_SELL):
        assert not any(w in d for w in BUY_WORDS), (
            f"a sell filing is shown the factor {d!r}. The reader is told "
            "insiders bought on a page that says SELL."
        )


def test_a_buy_is_never_described_as_selling():
    for d in _descriptions(CLUSTERED_BUY):
        assert not any(w in d for w in SELL_WORDS), (
            f"a buy filing is shown the factor {d!r}."
        )


def test_the_cluster_factor_still_fires_on_both_sides():
    """Guard against 'fixing' this by deleting the factor from sells."""
    for item, word in ((CLUSTERED_BUY, "buying"), (CLUSTERED_SELL, "selling")):
        cluster = [f for f in compute_trade_grade(item)["factors"] if f["name"] == "Cluster"]
        assert len(cluster) == 1, f"expected one Cluster factor, got {cluster}"
        assert word in cluster[0]["description"], cluster[0]["description"]
        assert str(item["cluster_size_pit"]) in cluster[0]["description"]


@pytest.mark.parametrize("n,pts", [(2, 4), (3, 8), (4, 12), (9, 12)])
def test_every_cluster_band_is_covered(n: int, pts: int):
    """All three bands carried the same hardcoded string; all three need the fix."""
    f = [x for x in compute_trade_grade(dict(CLUSTERED_SELL, cluster_size_pit=n))["factors"]
         if x["name"] == "Cluster"]
    assert f and f[0]["points"] == pts
    assert "selling together" in f[0]["description"]


# ── A factor that only means something on a purchase may not score a sale ───
#
# Gating these was worth 17,147 sell filings changing published band (15.5% of
# a year), all downward. Buys are untouched: every factor here was already
# either buy-gated or buy-semantic.

#: factor name -> the row field that triggers it, for a sell that would have
#: scored on it under the old scorer.
BUY_ONLY = {
    "Insider Grade": {"pit_grade": "A+"},
    "Deep Dip": {"dip_1mo": -0.47},
    "Dip": {"dip_1mo": -0.30},
    "Moderate Dip": {"dip_1mo": -0.20},
    "Rare Reversal": {"is_rare_reversal": 1},
}


@pytest.mark.parametrize("name,fields", sorted(BUY_ONLY.items()))
def test_buy_only_factors_never_score_a_sell(name: str, fields: dict):
    sell = compute_trade_grade({"trade_type": "sell", **fields})
    assert name not in {f["name"] for f in sell["factors"]}, (
        f"{name} scored a SELL. It is evidence about purchases: a career grade "
        "is computed over buy decisions only, and a drawdown is a reason to buy, "
        "not a reason a sale is notable."
    )


@pytest.mark.parametrize("name,fields", sorted(BUY_ONLY.items()))
def test_buy_only_factors_still_score_a_buy(name: str, fields: dict):
    """The gate must not quietly delete the factor from the buy side too."""
    buy = compute_trade_grade({"trade_type": "buy", **fields})
    assert name in {f["name"] for f in buy["factors"]}, (
        f"{name} stopped firing on buys — the is_buy gate went too far."
    )


DIRECTION_NEUTRAL = {
    "Role": {"title": "Chief Financial Officer"},
    "Cluster": {"cluster_size_pit": 3},
    "Opportunistic": {"cohen_routine": 0},
    "Routine": {"cohen_routine": 1},
    "Pre-Planned": {"is_10b5_1": 1},
    "Largest Trade": {"is_largest_ever": 1},
}


@pytest.mark.parametrize("name,fields", sorted(DIRECTION_NEUTRAL.items()))
def test_direction_neutral_factors_fire_on_both_sides(name: str, fields: dict):
    """These describe HOW a filing was made, not which way it points."""
    for side in ("buy", "sell"):
        names = {f["name"] for f in compute_trade_grade({"trade_type": side, **fields})["factors"]}
        assert name in names, f"{name} stopped firing on a {side}"


def test_the_eose_filing_no_longer_scores_on_a_purchase_thesis():
    """The filing that started this: EOSE 2026-07-28, /filing/78gntk.

    Chief Commercial Officer, 10b5-1 SALE of $371K into a 47% drawdown, three
    officers filing together, tagged Opportunistic (bearish). It published as
    Notable 65/100 on '3 insiders buying together' (+8) and 'Stock down 47%'
    (+10).
    """
    out = compute_trade_grade(CLUSTERED_SELL)
    names = {f["name"] for f in out["factors"]}
    assert "Deep Dip" not in names
    assert "Insider Grade" not in names
    assert "Cluster" in names, "co-selling is still a real fact about the filing"
    assert out["score"] == 55, out["factors"]      # was 65
    assert out["rating"] == "Modest", out["rating"]  # was "Notable"


def test_the_dead_is_routine_factor_stays_gone():
    """It deducted 5 points on a column nothing ever wrote.

    is_routine was ~47% populated with residue predating any current writer
    and NULL on everything the SEC reload brought in, so the same filing
    scored differently depending on when it was ingested. Dropped 2026-08-27
    along with the column.

    Its jobs have owners: signal_class / is_discretionary() for "was this a
    decision", and api/programmatic.py for "is this insider on a schedule"
    (is_programmatic) and "how often" (prog_median_interval_days).
    """
    for side in ("buy", "sell"):
        names = {f["name"] for f in
                 compute_trade_grade({"trade_type": side, "is_routine": 1})["factors"]}
        assert "Routine Pattern" not in names, (
            "the Routine Pattern factor is back. If you want a routine factor, "
            "build it on is_programmatic and bring a measured spread — the old "
            "one scored filings differently based on ingestion date."
        )
