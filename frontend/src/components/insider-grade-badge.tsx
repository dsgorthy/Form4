import { Badge } from "@/components/ui/badge";
import {
  INSIDER_RATING_BLURB,
  insiderRating,
  insiderRatingColor,
  UNRATED,
  type InsiderRating,
} from "@/lib/ratings";

/**
 * The one place an insider rating is drawn.
 *
 * There are 22 call sites and, before 2026-08-18, they disagreed with each
 * other: nine passed `pit_grade`, three passed `career_grade ?? pit_grade`,
 * three drew two badges side by side, and a missing grade rendered as "New".
 * The same insider could therefore read A on one page and D on the next,
 * because those are two different scores of two different things.
 *
 * Rather than edit 22 sites and hope the 23rd gets it right, normalisation
 * happens here. Whatever a caller passes is resolved through
 * `insiderRating()`, which is the single definition:
 *
 *   - `career_grade` is the published scale (monotonic; pit_grade inverts)
 *   - D collapses into C (the two bands do not separate, and they cross)
 *   - absent means Unrated, NOT "New" and never blank
 *
 * "Unrated" replacing "New" is the substantive change. A null career grade
 * means we had too little history to judge, and those buys average +1.41%
 * at 30 days against -0.25% for a measured C. "New" invited the reader to
 * discount them; they outperform every graded tier below A.
 *
 * Pass `rating` when the payload already carries `insider_rating` from the
 * API. Pass `grade` (a raw career grade) otherwise.
 */
interface Props {
  /** Pre-resolved `insider_rating` from the API. Preferred. */
  rating?: InsiderRating | string | null;
  /** Raw stored grade; normalised here. */
  grade?: string | null;
  bestTicker?: string | null;
  tickerCount?: number;
  compact?: boolean;
  showLabel?: boolean;
  label?: string;
  tooltip?: string;
}

export function InsiderGradeBadge({
  rating, grade, bestTicker, tickerCount, compact, showLabel, label, tooltip,
}: Props) {
  const resolved: InsiderRating = rating
    ? insiderRating(String(rating))
    : insiderRating(grade);

  const labelText = label || (showLabel ? "Insider" : null);
  const tip = tooltip || `${resolved} — ${INSIDER_RATING_BLURB[resolved]}`;
  const isUnrated = resolved === UNRATED;

  const badge = (
    <Badge
      style={
        isUnrated
          ? { backgroundColor: "transparent", color: insiderRatingColor(resolved),
              boxShadow: `inset 0 0 0 1px ${insiderRatingColor(resolved)}66` }
          : { backgroundColor: insiderRatingColor(resolved), color: "#fff" }
      }
      className={`font-mono px-1.5 py-0.5 text-center inline-flex justify-center ${
        isUnrated ? "text-[10px] min-w-0" : "text-xs min-w-[28px]"
      }`}
      title={tip}
    >
      {resolved}
    </Badge>
  );

  return (
    <span className="inline-flex items-center gap-1">
      {labelText && (
        <span className="text-[9px] text-[#81819A] uppercase tracking-wide font-medium">
          {labelText}
        </span>
      )}
      {badge}
      {!compact && bestTicker && (
        <span className="hidden md:inline text-[9px] text-[#81819A] font-mono">
          {bestTicker}
          {tickerCount != null && tickerCount > 1 && (
            <span className="text-[#3A3A4A]">{` +${tickerCount - 1}`}</span>
          )}
        </span>
      )}
    </span>
  );
}
