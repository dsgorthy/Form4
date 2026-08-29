"""
Trade Grade — unified trade quality score (0-100, 1-5 stars).

Replaces signal_quality.py and conviction_score.py's compute_conviction()
with a single empirically-grounded score that combines insider PIT grade
with trade-level factors.

Every factor is PIT-safe: only uses data knowable at filing_date.

Validated on 50K buy trades (2020-2026):
  5★: +3.0% avg 30d abnormal, 51.8% WR
  4★: +0.03% avg 30d abnormal, 45.7% WR
  3★: -0.62% avg 30d abnormal, 44.5% WR
  1★: -0.82% avg 30d abnormal, 43.4% WR

Bands and their measured performance now live in api/ratings.py. The figures
in this docstring are the ORIGINAL trade-date-anchored validation and are
retained only as history — they are inflated, because trade_returns measures
from the transaction date rather than from the first close a subscriber could
have acted on. The filing-anchored numbers are in api/ratings.py.

EVERY FACTOR BELOW THAT ONLY MEANS SOMETHING ON A PURCHASE IS GATED ON is_buy.
This scorer runs on sells too, and until 2026-08-26 it did not distinguish: the
bands were measured on buys, but the factors were applied to both sides and
worded for one. A reader of EOSE 2026-07-28 — a 10b5-1 sale into a 47%
drawdown, tagged bearish — was shown "3 insiders buying together", "Stock down
47%" and an A-grade bonus as the reasons it scored 65/100. 62% of published
sells carried at least one such factor.

The rule when adding a factor: ask what the factor asserts, then ask whether
that sentence is true of a SALE. If it is not, gate it. Direction-neutral
factors (role, 10b5-1, routine pattern, opportunistic, cluster size, largest
ever) apply to both and must read correctly on both — see the Cluster wording.
tests/unit/test_trade_grade_factors_match_direction.py fails the build on drift.
"""

from __future__ import annotations

from typing import Any


def _categorize_role(title: str | None) -> str:
    """Categorize insider title into a role bucket."""
    if not title:
        return "other"
    t = title.upper()
    if "CEO" in t or "CHIEF EXECUTIVE" in t:
        return "ceo"
    if "CFO" in t or "CHIEF FINANCIAL" in t:
        return "cfo"
    if "VICE PRESIDENT" in t or "SVP" in t or "EVP" in t or (" VP" in t or t.startswith("VP")):
        return "vp"
    if "DIRECTOR" in t:
        return "director"
    if "COO" in t or "CTO" in t or "CHIEF TECH" in t or "CHIEF OPERATING" in t:
        return "csuite"
    if "10%" in t or "TENPERCENT" in t:
        return "10pct"
    if "PRESIDENT" in t and not any(x in t for x in ["CEO", "CFO", "COO", "CTO", "CHIEF"]):
        return "president"
    return "other"


ROLE_POINTS = {
    "vp": 10,
    "cfo": 8,
    "csuite": 4,
    "ceo": 2,
    "director": 1,
    "10pct": -8,
    "president": -5,
    "other": 0,
}

# Bands live in api.ratings, which is the single definition of every rating
# this product publishes. They used to be declared here as 73/63/55/45 with
# their own labels, and drifted from the methodology page (which said 2 stars
# was "Below Average" where this said "Weak", and claimed +4.78% for 5 stars
# where the docstring above says +3.0%). Deriving them removes the second copy.
#
# The recut to 80/70/60/50 also fixed a real defect: under the old thresholds
# the top band returned +1.51% against the second band's +1.28% but had the
# LOWER win rate, so our best rating told a reader nothing the second-best did
# not. See api/ratings.py for the measurement.
from api.ratings import (  # noqa: E402
    TRADE_RATING_META,
    trade_rating,
    trade_rating_segments,
)

STAR_LABELS = {
    meta["segments"]: name for name, meta in TRADE_RATING_META.items()
}
STAR_THRESHOLDS = sorted(
    ((meta["min_score"], meta["segments"]) for meta in TRADE_RATING_META.values()
     if meta["min_score"] > 0),
    reverse=True,
)


def score_to_stars(score: int) -> int:
    """1-5 segments. A rendering of the band, not a separate scale."""
    return trade_rating_segments(score)


def compute_trade_grade(item: dict) -> dict:
    """
    Compute trade grade from trade-level factors.

    Args:
        item: dict with trade fields (from DB row or API enrichment)

    Returns:
        dict with: score (0-100), stars (1-5), label, factors (list of dicts)
    """
    score = 50  # baseline
    factors: list[dict] = []

    trade_type = item.get("trade_type", "buy")
    is_buy = trade_type == "buy"

    # --- 1. Insider PIT grade — BUYS ONLY ---
    # A grade is a claim about PURCHASES. pit_scoring._get_returns filters its
    # population to MEANINGFUL_BUY_CLASSES (fixed 2026-08-25), so the letter
    # means "this person's buys worked out". Adding 12 points to a SALE because
    # the seller is a good buyer is a category error: it was giving A+ insiders
    # credit for getting out. 21,574 sells carried it in the last year.
    pit_grade = item.get("pit_grade")
    if is_buy:
        if pit_grade in ("A+", "A"):
            score += 12
            factors.append({"name": "Insider Grade", "points": 12, "description": f"PIT {pit_grade}-grade insider"})
        elif pit_grade == "B":
            score += 6
            factors.append({"name": "Insider Grade", "points": 6, "description": "PIT B-grade insider"})
        elif pit_grade == "C":
            score += 2
            factors.append({"name": "Insider Grade", "points": 2, "description": "PIT C-grade insider"})

    # --- 2. Role ---
    role = _categorize_role(item.get("title") or item.get("normalized_title"))
    pts = ROLE_POINTS.get(role, 0)
    if pts != 0:
        score += pts
        label = role.upper().replace("10PCT", "10% Owner").replace("_", " ")
        factors.append({"name": "Role", "points": pts, "description": f"{label}"})

    # --- 3. Cluster size ---
    # EXPLICIT, and no fallback chain. A cluster gets counted four ways in
    # this codebase and they answer different questions:
    #
    #   cluster_size_pit    point-in-time — counts only filers who filed BEFORE
    #                       this row. Correct for a backtest; the first filer of
    #                       any cluster scores 0 by construction. Stored as
    #                       trades.pit_cluster_size.
    #   n_filers            as-of publication — everyone on the ticker that day,
    #                       known by the time a page renders. Computed per query.
    #
    # This scorer wants the PIT count, because a grade must not depend on
    # filings that arrived after the one being graded. It used to read
    # `item.get("cluster_size") or item.get("n_filers")`, which silently took
    # whichever the caller happened to supply — and on 2026-08-21 the detail
    # endpoint supplied neither, because it selected `t.pit_cluster_size` under
    # that name and nothing read it. The cluster factor, worth up to 12 points,
    # had been scoring 0 on filing pages for its whole life.
    #
    # The wording is DERIVED FROM THE ROW, never fixed. It said "buying
    # together" unconditionally, and this factor fires on sells too — 46,856
    # of them in the last year alone, 42.3% of every sell we publish. EOSE on
    # 2026-07-28 is the shape of it: three officers filed 10b5-1 sales into a
    # 47% drawdown, the page tagged the filing Opportunistic (bearish), and
    # then listed "+8 Cluster: 3 insiders buying together" underneath as a
    # reason it scored 65. Nobody bought anything.
    cluster = item.get("cluster_size_pit") or 0
    together = "buying together" if is_buy else "selling together"
    if cluster >= 4:
        score += 12
        factors.append({"name": "Cluster", "points": 12, "description": f"{cluster} insiders {together}"})
    elif cluster >= 3:
        score += 8
        factors.append({"name": "Cluster", "points": 8, "description": f"{cluster} insiders {together}"})
    elif cluster >= 2:
        score += 4
        factors.append({"name": "Cluster", "points": 4, "description": f"{cluster} insiders {together}"})

    # --- 4. Dip depth — BUYS ONLY ---
    # The whole content of this factor is "they bought weakness". Pointed the
    # other way it says an insider sold into a collapse and scores it +10 for
    # the depth of the collapse — EOSE's officers got the maximum 10 for
    # selling into a 47% drawdown. There is no measured claim that a deeper
    # drawdown makes a SALE more notable, so it earns nothing here. The
    # drawdown itself is still on the page; it is just not a reason.
    if is_buy:
        dips = [d for d in [item.get("dip_1mo"), item.get("dip_3mo")] if d is not None]
        best_dip = min(dips) if dips else 0
        if best_dip <= -0.40:
            score += 10
            factors.append({"name": "Deep Dip", "points": 10, "description": f"Stock down {abs(best_dip)*100:.0f}%"})
        elif best_dip <= -0.25:
            score += 5
            factors.append({"name": "Dip", "points": 5, "description": f"Stock down {abs(best_dip)*100:.0f}%"})
        elif best_dip <= -0.15:
            score += 2
            factors.append({"name": "Moderate Dip", "points": 2, "description": f"Stock down {abs(best_dip)*100:.0f}%"})

    # --- 5. Opportunistic vs routine ---
    cohen = item.get("cohen_routine")
    if cohen == 0:
        score += 5
        factors.append({"name": "Opportunistic", "points": 5, "description": "Non-routine trade pattern"})
    elif cohen == 1:
        score -= 5
        factors.append({"name": "Routine", "points": -5, "description": "Routine trade pattern"})

    # --- 6. Pre-planned (10b5-1) ---
    if item.get("is_10b5_1") == 1:
        score -= 8
        factors.append({"name": "Pre-Planned", "points": -8, "description": "10b5-1 pre-planned trade"})

    # --- 7. Routine pattern --- REMOVED 2026-08-27
    #
    # This deducted 5 points on `is_routine`, a column NOTHING has ever
    # written. It was ~47% populated with residue predating any current
    # writer and NULL on everything the SEC reload brought in, so the same
    # filing scored differently depending on when it was ingested.
    #
    # Its two jobs have owners now: signal_class / is_discretionary() says
    # whether the filing was a decision, and api/programmatic.py says whether
    # the insider is on a schedule (is_programmatic) and how often
    # (prog_median_interval_days). Re-add a factor here only against one of
    # those, and only with a measured spread behind it.

    # --- 8. Rare reversal — BUYS ONLY ---
    # The flag literally means "persistent seller now BUYING", and its own
    # description said so while sitting on 1,676 sells. Whatever set the flag
    # on those rows, the sentence cannot be true of a sale.
    if is_buy and item.get("is_rare_reversal") == 1:
        score += 8
        factors.append({"name": "Rare Reversal", "points": 8, "description": "Persistent seller now buying"})

    # --- 9. Holdings % increase (buys only) ---
    if is_buy:
        after = item.get("shares_owned_after") or 0
        qty = item.get("qty") or 0
        if after > 0 and qty > 0:
            before = after - qty
            if before > 0:
                pct = qty / before
                if pct >= 1.0:
                    score += 6
                    factors.append({"name": "Holdings Doubled", "points": 6, "description": f"Holdings increased {pct*100:.0f}%"})
                elif pct >= 0.5:
                    score += 4
                    factors.append({"name": "Large Increase", "points": 4, "description": f"Holdings increased {pct*100:.0f}%"})
                elif pct >= 0.1:
                    score += 2
                    factors.append({"name": "Meaningful Increase", "points": 2, "description": f"Holdings increased {pct*100:.0f}%"})
                elif pct < 0.01:
                    score -= 4
                    factors.append({"name": "Token Purchase", "points": -4, "description": "Trivial holdings increase"})

    # --- 10. Trade value $2M+ (buys) ---
    if is_buy and (item.get("value") or 0) >= 2_000_000:
        score -= 8
        factors.append({"name": "Large Block", "points": -8, "description": "$2M+ trade (negative signal)"})

    # --- 11. 52-week proximity (buys) ---
    if is_buy:
        w52 = item.get("week52_proximity")
        if w52 is not None:
            if w52 >= 0.8:
                score += 3
                factors.append({"name": "Near 52w High", "points": 3, "description": "Buying near annual high"})
            elif w52 <= 0.2:
                score -= 2
                factors.append({"name": "Near 52w Low", "points": -2, "description": "Buying near annual low"})

    # --- 12. Largest ever ---
    if item.get("is_largest_ever") == 1:
        score += 3
        factors.append({"name": "Largest Trade", "points": 3, "description": "Biggest trade ever by this insider"})

    # Clamp
    score = max(0, min(100, score))
    stars = score_to_stars(score)

    return {
        "score": score,
        # `rating` is the canonical answer. `stars` and `label` are kept as
        # aliases so existing consumers keep working and pick up the recut
        # bands automatically rather than rendering a stale scale.
        "rating": trade_rating(score),
        "stars": stars,
        "label": trade_rating(score),
        "factors": factors,
    }


def enrich_items_with_trade_grade(conn: Any, items: list[dict]) -> None:
    """Batch-enrich items with trade_grade. Modifies items in place."""
    for item in items:
        ticker = item.get("ticker", "")
        if not ticker or ticker in ("NONE", ""):
            item["trade_grade"] = None
        else:
            item["trade_grade"] = compute_trade_grade(item)
