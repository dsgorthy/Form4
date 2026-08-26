/**
 * The two ratings this product publishes. Mirror of `api/ratings.py`.
 *
 * The Python module is the source of truth — it carries the reasoning, the
 * measured numbers behind every band, and the reason each retired thing was
 * retired. Read it before changing anything here.
 * `tests/unit/test_ratings_parity.py` fails the build if the two drift.
 *
 * Two questions, two ratings, one tag vocabulary:
 *
 *   Is this person worth following?  -> insiderRating()   1-to-1 with insider
 *   How notable is this filing?      -> tradeRating()     1-to-1 with trade
 *   What is true about this filing?  -> tags              1-to-many
 *
 * Nothing else gets rendered as a rating. Conviction is a strategy's entry
 * threshold, not a verdict on a trade; the insider_track_records score family
 * is a PIT violation and may not rank anything.
 */

import { GRADE_COLORS } from "@/lib/grade-colors";

// ── Insider rating ─────────────────────────────────────────────────────────

export const INSIDER_RATINGS = ["A+", "A", "B", "C", "Unrated"] as const;
export type InsiderRating = (typeof INSIDER_RATINGS)[number];

export const UNRATED: InsiderRating = "Unrated";

/**
 * Stored career_grade to published rating.
 *
 * D maps to C. Measured, D is -0.18% and C is -0.38%, so D is the *better* of
 * the two — the bands do not separate, and they cross. Publishing both is
 * false precision. Both letters stay in the database.
 */
const GRADE_DISPLAY: Record<string, InsiderRating> = {
  "A+": "A+", A: "A", B: "B", C: "C", D: "C",
};

export const INSIDER_RATING_BLURB: Record<InsiderRating, string> = {
  "A+": "Top 3% of insiders. Their buys beat the market by the widest margin we measure.",
  A: "A consistent record of buying ahead of gains.",
  B: "Slightly better than the market on average.",
  C: "Measured, and no better than the market.",
  Unrated:
    "Only stock they chose to buy counts — not grants, option exercises or vesting. An insider can file often and still be unrated. Not a bad sign: unrated buys beat every graded tier below A.",
};

/**
 * Published insider rating.
 *
 * A null/absent careerGrade is Unrated. That is not a convention invented
 * here — compute_career_grades.py writes NULL when the V3 scorer reports
 * insufficient history, so the fact was always in the column and the UI was
 * rendering it as an empty cell. It should not be empty: those buys average
 * +1.41% at 30d against -0.25% for a measured C, so an unrated insider
 * outperforms every graded tier below A.
 *
 * `sufficientData` is the insider_ticker_scores flag, for callers that have
 * it. It tracks the V2/pit scorer rather than V3, so it is a secondary
 * override and never the primary test.
 */
export function insiderRating(
  careerGrade: string | null | undefined,
  sufficientData?: number | boolean | null,
  pitGrade?: string | null,
): InsiderRating {
  if (sufficientData !== undefined && sufficientData !== null && !sufficientData) {
    return UNRATED;
  }
  const g = (careerGrade || pitGrade || "").trim().toUpperCase();
  return GRADE_DISPLAY[g] ?? UNRATED;
}

/** Unrated is deliberately neutral, never the red we use for a failing grade. */
export function insiderRatingColor(rating: InsiderRating): string {
  switch (rating) {
    case "A+": return GRADE_COLORS.strong;
    case "A":  return GRADE_COLORS.above;
    case "B":  return GRADE_COLORS.average;
    case "C":  return GRADE_COLORS.below;
    default:   return GRADE_COLORS.unknown;
  }
}

// ── Trade rating ───────────────────────────────────────────────────────────

export const TRADE_RATINGS = ["Exceptional", "Strong", "Notable", "Modest", "Weak"] as const;
export type TradeRating = (typeof TRADE_RATINGS)[number];

/** Recut 2026-08-18. The old 73/63/55/45 bands put +1.51% above +1.28% while
 *  the lower band had the better win rate — the top rating carried no extra
 *  information. These separate on both. */
const TRADE_BANDS: ReadonlyArray<readonly [number, TradeRating]> = [
  [80, "Exceptional"],
  [70, "Strong"],
  [60, "Notable"],
  [50, "Modest"],
  [0, "Weak"],
];

export const TRADE_RATING_SEGMENTS: Record<TradeRating, number> = {
  Exceptional: 5, Strong: 4, Notable: 3, Modest: 2, Weak: 1,
};

export const TRADE_RATING_BLURB: Record<TradeRating, string> = {
  Exceptional: "Several strong factors at once. The rarest rating we give.",
  Strong: "Clearly above the average filing.",
  Notable: "Something here stands out, but not much.",
  Modest: "Nothing distinguishes this filing.",
  Weak: "Negative factors outweigh the positive ones.",
};

export function tradeRating(score: number | null | undefined): TradeRating | null {
  if (score === null || score === undefined) return null;
  for (const [min, name] of TRADE_BANDS) {
    if (score >= min) return name;
  }
  return "Weak";
}

export function tradeRatingSegments(score: number | null | undefined): number {
  const r = tradeRating(score);
  return r ? TRADE_RATING_SEGMENTS[r] : 0;
}

export function tradeRatingColor(rating: TradeRating): string {
  switch (rating) {
    case "Exceptional": return GRADE_COLORS.strong;
    case "Strong":      return GRADE_COLORS.above;
    case "Notable":     return GRADE_COLORS.average;
    case "Modest":      return GRADE_COLORS.below;
    default:            return GRADE_COLORS.weak;
  }
}

// ── Tags ───────────────────────────────────────────────────────────────────

export type TagKind = "pattern" | "scale" | "strategy" | "verdict";

/** Verdict tags (`top_trade`, `high_signal`, `insider_returns`) are our own
 *  opinion about quality, which is what the Trade Rating is for. Rendering
 *  "Top Trade" beside a rating of Modest is the contradiction being removed.
 *  They stay in the database; they are not shown. */
const TAG_KINDS: Record<string, TagKind> = {
  buying_the_dip: "pattern",
  deep_dip_buy: "pattern",
  selling_the_rip: "pattern",
  contrarian: "pattern",
  momentum_buy: "pattern",
  trend_reversal: "pattern",
  first_time_buyer: "pattern",
  opportunistic_trade: "pattern",
  exercise_and_sell: "pattern",
  post_vest_dump: "pattern",
  tax_sale_noise: "pattern",
  recurring_buyer_noise: "pattern",
  ten_pct_owner_buy: "pattern",
  size_anomaly: "scale",
  large_holdings_increase: "scale",
  small_holdings_increase: "scale",
  largest_purchase_ever: "scale",
  quality_momentum_buy: "strategy",
  reversal_buy: "strategy",
  deep_reversal_dip_buy: "strategy",
  reversal_quality_buy: "strategy",
  tenb51_surprise_buy: "strategy",
  top_trade: "verdict",
  high_signal: "verdict",
  insider_returns: "verdict",
};

export const PUBLISHED_TAG_KINDS: readonly TagKind[] = ["pattern", "scale", "strategy"];

/** Unknown types are patterns: a new descriptive tag is the common case, and
 *  defaulting to verdict would silently hide it. */
export function tagKind(signalType: string | null | undefined): TagKind {
  return TAG_KINDS[(signalType || "").trim()] ?? "pattern";
}

export function isPublishedTag(signalType: string | null | undefined): boolean {
  return PUBLISHED_TAG_KINDS.includes(tagKind(signalType));
}

/** Direction lives on signal_class, never in the tag name — six types carry
 *  both classes, because an opportunistic sell and an opportunistic buy are
 *  the same pattern pointed opposite ways. */
export function visibleTags<T extends { signal_type?: string | null }>(
  signals: T[] | null | undefined,
): T[] {
  if (!signals) return [];
  return signals.filter((s) => isPublishedTag(s.signal_type));
}
