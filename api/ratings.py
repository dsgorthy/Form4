"""The two ratings this product publishes, and the tag vocabulary beside them.

Dependency-free on purpose — no FastAPI, no DB — so Studio's host Python and
the test suite can both import it, same as `api.public_fields`.

────────────────────────────────────────────────────────────────────────────
WHY THIS FILE EXISTS
────────────────────────────────────────────────────────────────────────────

A single filing used to arrive at the reader carrying five verdicts on three
scales, with nothing saying how they related:

    pit_grade    D          career_grade  C
    trade_grade  {score: 61, stars: 3, label: "Average"}

plus, on a portfolio row, a sixth: conviction 1.5/10 printed next to "Grade A".
The leaderboard added `legacy_score` 2.99, `score_tier` 3 and `percentile`, and
offered to sort by any of them.

So: one question gets one answer on one scale. There are exactly two questions.

    Is this person worth following?   -> INSIDER RATING, 1-to-1 with
                                         (insider, ticker, as_of_date)
    How notable is this filing?       -> TRADE RATING, 1-to-1 with trade

and one more thing a reader wants that is not a rating at all:

    What is true about this filing?   -> TAGS, 1-to-many

Everything else is internal. Conviction is a strategy's own entry threshold
(floor 1.5, so a 1.5 means "cleared the bar", not "scored 1.5 out of 10") and
never belongs beside a rating. The insider_track_records family is a
documented PIT violation and may not rank anything.

────────────────────────────────────────────────────────────────────────────
THE EVIDENCE
────────────────────────────────────────────────────────────────────────────

Measured 2026-08-18 over 76,909 open-market buys, 2018-01-01 to 2026-03-31,
30 trading days held, return minus SPY. Entry is FILING-ANCHORED: the first
close after the filing was public, by the same rule the live strategies use.
That matters — `trade_returns.abnormal_*` is anchored on the transaction date,
typically two days before anyone could act, and every figure derived from it
is inflated. Reproduce with scripts in the session scratchpad; method is the
`filed_before_close` rule in framework.decision.entry_timing.

────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from typing import Any, Optional

# ═══════════════════════════════════════════════════════════════════════════
# 1. INSIDER RATING — 1-to-1 with (insider, ticker, as_of_date)
# ═══════════════════════════════════════════════════════════════════════════
#
# SOURCE IS career_grade, NOT pit_grade.
#
# Both are PIT-clean. Only one is monotonic. Measured mean abnormal / win rate:
#
#     pit_grade      A+ 4.26%/53.3   A 1.47%/47.9   B 1.10%/47.3
#                    C -0.90%/45.1   D -0.11%/45.0      <- C below D on mean
#     career_grade   A+ 4.86%/53.7   A 2.00%/48.5   B 0.42%/47.0
#                    C -0.37%/45.8   D -0.35%/44.2
#
# career_grade orders correctly on win rate at every step. pit_grade does not,
# and pit_grade is already an input to the Trade Rating below, so publishing it
# as a second headline shows a reader an input beside its own output.
#
# "UNRATED" IS A REAL VALUE, AND IT IS ALREADY IN THE DATA.
#
# compute_career_grades.py writes
#
#     career_grade = pit_score_to_grade(v3.blended_score)
#                    if v3.sufficient_data else None
#
# so a NULL career_grade already means "we did not have enough history at this
# company to judge this person". No join and no new column are needed — the
# fact was always there, and the API rendered it as an empty cell.
#
# An empty cell is the wrong rendering, because these are not bad insiders:
#
#     C  (measured)   n=17,437   -0.38%   45.8% win
#     D  (measured)   n=33,023   -0.18%   44.8% win
#     NULL            n= 9,269   +1.41%   46.1% win
#
# An unrated insider's buys beat every measured grade below A. Showing that as
# blank — or worse, letting a reader assume the worst — inverts the meaning.
#
# (insider_ticker_scores.sufficient_data tracks the V2/pit scorer, not V3, so
# the two disagree on ~4.8k rows. career_grade IS NULL is the V3 answer and the
# one that matches the column we publish. The `sufficient_data` argument below
# is kept for callers that have it, but it is not required.)
#
# C AND D ARE MERGED IN PRESENTATION.
#
# -0.38% vs -0.18%, and D is the *better* of the two. Two bands that do not
# separate — and that cross — are false precision, which is the thing this file
# exists to remove. Both letters stay in the database; only the published scale
# collapses them. Undo by deleting the "D" entry from _GRADE_DISPLAY.

#: Published scale, best first. `Unrated` is a real value, not a missing one.
INSIDER_RATINGS = ("A+", "A", "B", "C", "Unrated")

UNRATED = "Unrated"

#: Stored career_grade -> published rating.
_GRADE_DISPLAY = {"A+": "A+", "A": "A", "B": "B", "C": "C", "D": "C"}

INSIDER_RATING_META: dict[str, dict[str, Any]] = {
    "A+": {
        "label": "A+",
        "blurb": "Top 3% of insiders. Their buys beat the market by the widest margin we measure.",
        "share_pct": 3.2, "mean_abnormal_30d": 4.88, "win_rate": 53.7,
    },
    "A": {
        "label": "A",
        "blurb": "A consistent record of buying ahead of gains.",
        "share_pct": 4.0, "mean_abnormal_30d": 2.00, "win_rate": 48.5,
    },
    "B": {
        "label": "B",
        "blurb": "Slightly better than the market on average.",
        "share_pct": 15.1, "mean_abnormal_30d": 0.41, "win_rate": 47.0,
    },
    "C": {
        "label": "C",
        "blurb": "Measured, and no better than the market.",
        "share_pct": 65.6, "mean_abnormal_30d": -0.25, "win_rate": 45.1,
    },
    UNRATED: {
        "label": "Unrated",
        "blurb":
            "Only stock they chose to buy counts — not grants, option exercises "
            "or vesting. An insider can file often and still be unrated. Not a "
            "bad sign: unrated buys beat every graded tier below A.",
        "share_pct": 12.1, "mean_abnormal_30d": 1.41, "win_rate": 46.1,
    },
}


def insider_rating(
    career_grade: Optional[str],
    sufficient_data: Optional[Any] = None,
    *,
    pit_grade: Optional[str] = None,
) -> str:
    """Published insider rating for a filing.

    A NULL/absent career_grade is Unrated — that is how the writer already
    encodes "not enough history", so no extra column is required.

    `sufficient_data` is accepted for callers that happen to have the
    insider_ticker_scores flag to hand. It tracks the V2/pit scorer rather than
    V3, so it is a secondary override, never the primary test.

    `pit_grade` is accepted only as a last-resort fallback for rows predating
    the career scorer. It is never preferred over career_grade.
    """
    if sufficient_data is not None and not sufficient_data:
        return UNRATED
    grade = (career_grade or pit_grade or "").strip().upper()
    return _GRADE_DISPLAY.get(grade, UNRATED)


# ═══════════════════════════════════════════════════════════════════════════
# 2. TRADE RATING — 1-to-1 with a trade
# ═══════════════════════════════════════════════════════════════════════════
#
# Source is trade_grade.compute_trade_grade()'s 0-100 score, which already
# folds the insider grade in as one of twelve factors. The score is sound; the
# bands cut over it were not.
#
# SHIPPED BANDS 73/63/55/45 DID NOT SEPARATE AT THE TOP:
#
#     Exceptional (73+)   n= 5,793   +1.51%   48.6% win
#     Strong    (63-72)   n=20,690   +1.28%   49.3% win   <- higher win rate
#
# The best rating we could give a trade carried no more information than the
# second best, and lost to it on win rate. Recut off the decile curve:
#
#     Exceptional (80+)   n= 1,113   +2.74%   50.1% win
#     Strong    (70-79)   n= 8,700   +1.88%   49.8% win
#     Notable   (60-69)   n=26,505   +0.73%   47.8% win
#     Modest    (50-59)   n=27,025   +0.12%   44.4% win
#     Weak       (<50)    n=13,566   -1.35%   42.7% win
#
# Monotonic on both. This band was called "Average" and then "Routine", and is
# now "Modest". The first rename was right in substance — a bucket with +0.12%
# and a 44.4% win rate is not average performance, it is the absence of a
# signal, and a reader deciding whether to act should be told that.
#
# But "Routine" was the wrong WORD, because this product already uses routine
# to mean something specific and unrelated: pre-scheduled, 10b5-1, tax
# withholding — `is_routine`, `cohen_routine`, and the "SELL · Routine" chip.
# The two could disagree on one filing and did. A first-ever discretionary
# purchase in MED on 2026-08-21 scored 58 and so displayed "Routine" while
# `is_routine` was NULL, which reads as "pre-scheduled, ignore this" about a
# trade that was nothing of the sort. Renamed 2026-08-21.
#
# The rule this is an instance of: a rating band names a POSITION ON A SCALE.
# If the word also names a KIND OF FILING anywhere in the product, pick a
# different word.
#
# ONE RENDERING, NOT THREE. The API used to return score AND stars AND label
# for the same number. The band name is the answer; `score` stays available for
# sorting and for Pro's detail view. Stars are a presentation of the band, not
# a separate scale, so they are derived here and never stored.

TRADE_RATING_BANDS: tuple[tuple[int, str], ...] = (
    (80, "Exceptional"),
    (70, "Strong"),
    (60, "Notable"),
    (50, "Modest"),
    (0,  "Weak"),
)

TRADE_RATINGS = tuple(name for _, name in TRADE_RATING_BANDS)

TRADE_RATING_META: dict[str, dict[str, Any]] = {
    "Exceptional": {"min_score": 80, "segments": 5, "share_pct": 1.4,
                    "mean_abnormal_30d": 2.74, "win_rate": 50.1,
                    "blurb": "Several strong factors at once. The rarest rating we give."},
    "Strong":      {"min_score": 70, "segments": 4, "share_pct": 11.3,
                    "mean_abnormal_30d": 1.88, "win_rate": 49.8,
                    "blurb": "Clearly above the average filing."},
    "Notable":     {"min_score": 60, "segments": 3, "share_pct": 34.5,
                    "mean_abnormal_30d": 0.73, "win_rate": 47.8,
                    "blurb": "Something here stands out, but not much."},
    "Modest":      {"min_score": 50, "segments": 2, "share_pct": 35.1,
                    "mean_abnormal_30d": 0.12, "win_rate": 44.4,
                    "blurb": "Nothing distinguishes this filing."},
    "Weak":        {"min_score": 0,  "segments": 1, "share_pct": 17.6,
                    "mean_abnormal_30d": -1.35, "win_rate": 42.7,
                    "blurb": "Negative factors outweigh the positive ones."},
}


def trade_rating(score: Optional[float]) -> Optional[str]:
    """Band name for a 0-100 trade score. None in, None out."""
    if score is None:
        return None
    for minimum, name in TRADE_RATING_BANDS:
        if score >= minimum:
            return name
    return TRADE_RATING_BANDS[-1][1]


def trade_rating_segments(score: Optional[float]) -> int:
    """1-5 filled segments for the meter. A rendering of the band, not a scale."""
    name = trade_rating(score)
    return TRADE_RATING_META[name]["segments"] if name else 0


# ═══════════════════════════════════════════════════════════════════════════
# 3. TAGS — 1-to-many with a trade
# ═══════════════════════════════════════════════════════════════════════════
#
# Median 2 per trade, up to 11. Tags state facts; they never rate. Three kinds,
# because the 31 types in trade_signals were four different things in one bag:
#
#   pattern   what the insider did      buying_the_dip, first_time_buyer
#   scale     how big, relative to them size_anomaly, large_holdings_increase
#   strategy  one of our books fired    quality_momentum_buy
#   verdict   our opinion               top_trade, high_signal  <- RETIRED
#
# The verdict tags are retired from display, not deleted. `top_trade` is on
# 495,478 trades and `high_signal` on 12,724; both are an opinion about
# quality, which is what the Trade Rating is for. Showing "Top Trade" beside a
# rating of Modest is the contradiction this whole exercise is removing.
#
# DIRECTION IS NOT IN THE NAME. Six types carry both a bullish and a bearish
# signal_class — `opportunistic_trade` is 640,269 bearish and 123,643 bullish,
# because an opportunistic sell and an opportunistic buy are the same pattern
# in opposite directions. Read direction from signal_class on the row, never
# by pattern-matching the type string.

TAG_KIND_PATTERN = "pattern"
TAG_KIND_SCALE = "scale"
TAG_KIND_STRATEGY = "strategy"
TAG_KIND_VERDICT = "verdict"

TAG_KINDS: dict[str, str] = {
    # pattern — what the insider did
    "buying_the_dip": TAG_KIND_PATTERN,
    "deep_dip_buy": TAG_KIND_PATTERN,
    "selling_the_rip": TAG_KIND_PATTERN,
    "contrarian": TAG_KIND_PATTERN,
    "momentum_buy": TAG_KIND_PATTERN,
    "trend_reversal": TAG_KIND_PATTERN,
    "first_time_buyer": TAG_KIND_PATTERN,
    "opportunistic_trade": TAG_KIND_PATTERN,
    "exercise_and_sell": TAG_KIND_PATTERN,
    "post_vest_dump": TAG_KIND_PATTERN,
    "tax_sale_noise": TAG_KIND_PATTERN,
    "recurring_buyer_noise": TAG_KIND_PATTERN,
    "ten_pct_owner_buy": TAG_KIND_PATTERN,
    # scale — how big
    "size_anomaly": TAG_KIND_SCALE,
    "large_holdings_increase": TAG_KIND_SCALE,
    "small_holdings_increase": TAG_KIND_SCALE,
    "largest_purchase_ever": TAG_KIND_SCALE,
    # strategy — one of our books fired
    "quality_momentum_buy": TAG_KIND_STRATEGY,
    "reversal_buy": TAG_KIND_STRATEGY,
    "deep_reversal_dip_buy": TAG_KIND_STRATEGY,
    "reversal_quality_buy": TAG_KIND_STRATEGY,
    "tenb51_surprise_buy": TAG_KIND_STRATEGY,
    # verdict — retired, see above
    "top_trade": TAG_KIND_VERDICT,
    "high_signal": TAG_KIND_VERDICT,
    "insider_returns": TAG_KIND_VERDICT,
}

#: Kinds a reader sees. Verdict is absent by design.
PUBLISHED_TAG_KINDS = (TAG_KIND_PATTERN, TAG_KIND_SCALE, TAG_KIND_STRATEGY)


def tag_kind(signal_type: Optional[str]) -> str:
    """Kind for a signal_type. Unknown types are patterns — a new descriptive
    tag is the common case, and defaulting to `verdict` would silently hide it."""
    return TAG_KINDS.get((signal_type or "").strip(), TAG_KIND_PATTERN)


def is_published_tag(signal_type: Optional[str]) -> bool:
    return tag_kind(signal_type) in PUBLISHED_TAG_KINDS


def visible_tags(signals: Optional[list[dict]]) -> list[dict]:
    """Drop verdict tags from an API `signals` array, preserving order."""
    if not signals:
        return []
    return [s for s in signals if is_published_tag(s.get("signal_type"))]


# ═══════════════════════════════════════════════════════════════════════════
# 4. NOT RATINGS — never rank or display these
# ═══════════════════════════════════════════════════════════════════════════
#
# Kept as a named list so the test suite can assert they stay out of ranking
# paths, and so the next person reads why rather than rediscovering it.

#: Global all-time aggregates, recomputed over the entire history every
#: refresh. Safe only for "this insider's lifetime stats" display; a documented
#: PIT violation for any ranking, scoring or backtest decision.
PIT_VIOLATING_FIELDS = (
    "score",              # insider_track_records.score, 0-3
    "legacy_score",
    "score_tier",         # 0-3
    "percentile",
    "buy_win_rate_7d",
    "buy_avg_abnormal_7d",
    "sell_win_rate_7d",
)

#: Leaderboard sort keys that read the above. Removed 2026-08-18.
RETIRED_SORT_KEYS = ("win_rate", "alpha", "percentile", "buy_count")

#: A strategy's own entry threshold, not a quality measure. min_conviction is
#: 1.5 on both quality books, so a stored 1.5 means "cleared the bar".
INTERNAL_ONLY_FIELDS = ("conviction", "signal_quality")

#: Surfaces confirmed reading the canonical rating as of 2026-08-18: the feed,
#: filing pages, filing detail panel, trades tables, portfolio, signals table,
#: leaderboard, entity search, explore, clusters, the insider roster and the
#: sell-cessation card. The badge normalises whatever it is handed, so a new
#: surface is wrong only if it renders a grade without going through it.


# ═══════════════════════════════════════════════════════════════════════════
# 5. ONE PLACE THAT STAMPS THE PAYLOAD
# ═══════════════════════════════════════════════════════════════════════════


def attach_ratings(items: Optional[list[dict]]) -> None:
    """Stamp the canonical ratings onto API rows, in place.

    Call once per response, after trade_grade enrichment. Every consumer then
    reads `insider_rating` and `trade_grade.rating` instead of deciding for
    itself what a D means — which is how one filing came to show D, C, 61,
    3 stars and "Average" at the same time.

    career_grade is NOT backfilled from pit_grade here, deliberately. A row
    with no career grade is Unrated, and Unrated buys outperform every measured
    grade below A; substituting the pit grade would relabel them C or D and
    reintroduce exactly the error this module exists to remove.
    """
    for item in items or []:
        item["insider_rating"] = insider_rating(item.get("career_grade"))

        grade = item.get("trade_grade")
        if isinstance(grade, dict):
            item["trade_rating"] = grade.get("rating")
        else:
            item["trade_rating"] = None

        if item.get("signals"):
            item["signals"] = visible_tags(item["signals"])
        if item.get("signal_types"):
            kept = [
                t for t in str(item["signal_types"]).split(",")
                if t and is_published_tag(t.strip())
            ]
            item["signal_types"] = ",".join(kept)
