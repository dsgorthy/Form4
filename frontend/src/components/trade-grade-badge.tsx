import {
  TRADE_RATING_BLURB,
  tradeRating,
  tradeRatingColor,
  tradeRatingSegments,
  type TradeRating,
} from "@/lib/ratings";
import { GRADE_COLORS } from "@/lib/grade-colors";

/**
 * The Trade Rating, drawn as a word.
 *
 * This used to render five stars. Stars looked tidy in a table and told the
 * reader nothing: three filled stars beside a letter grade is two symbol
 * systems for one filing, and the reader has to already know the scale to
 * decode either. Worse, the two were routinely read as one measurement —
 * "1.5/10 next to Grade A" was the same mistake in another costume.
 *
 * The band name says it outright. Score stays in the tooltip for anyone who
 * wants the number.
 */
interface TradeGrade {
  score: number;
  stars?: number;
  label?: string;
  rating?: string | null;
  factors?: { name: string; points: number; description: string }[];
}

function resolve(grade: TradeGrade): TradeRating | null {
  // Prefer the API's own answer; fall back to recomputing from the score so a
  // cached response from before `rating` existed still renders correctly.
  return (grade.rating as TradeRating) || tradeRating(grade.score);
}

export function TradeGradeBadge({
  grade,
  showLabel,
}: {
  grade?: TradeGrade | null;
  showLabel?: boolean;
}) {
  if (!grade) return null;
  const rating = resolve(grade);
  if (!rating) return null;
  const color = tradeRatingColor(rating);

  return (
    <span className="inline-flex items-center gap-1">
      {showLabel && (
        <span className="text-[9px] text-[#55556A] uppercase tracking-wide font-medium">
          Trade
        </span>
      )}
      <span
        className="rounded px-1.5 py-0.5 text-[10px] font-medium whitespace-nowrap"
        style={{ color, boxShadow: `inset 0 0 0 1px ${color}59` }}
        title={`${rating} — ${TRADE_RATING_BLURB[rating]} (${grade.score}/100)`}
      >
        {rating}
      </span>
    </span>
  );
}

/** Five segments, filled to the band. A meter is a rendering of the rating,
 *  not a second scale — the number of segments is derived from the same band
 *  the word comes from. */
export function TradeRatingMeter({ score }: { score: number }) {
  const rating = tradeRating(score);
  if (!rating) return null;
  const filled = tradeRatingSegments(score);
  const color = tradeRatingColor(rating);
  return (
    <span className="inline-flex items-center gap-[3px]" aria-label={`${rating}, ${filled} of 5`}>
      {[1, 2, 3, 4, 5].map((i) => (
        <span
          key={i}
          className="h-3 w-1.5 rounded-[1px]"
          style={{ background: i <= filled ? color : "#2A2A3A" }}
        />
      ))}
    </span>
  );
}

export function TradeGradeDetail({ grade }: { grade?: TradeGrade | null }) {
  if (!grade) return null;
  const rating = resolve(grade);
  if (!rating) return null;
  const color = tradeRatingColor(rating);

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4">
      <div className="flex flex-wrap items-center gap-3 mb-1">
        <TradeRatingMeter score={grade.score} />
        <span className="text-sm font-semibold" style={{ color }}>
          {rating}
        </span>
        <span className="text-xs text-[#55556A]">{grade.score}/100</span>
      </div>
      <p className="text-xs text-[#8888A0] mb-3">{TRADE_RATING_BLURB[rating]}</p>

      {/* Pro only — the API strips `factors` for everyone else, because the
          named signals and their point values are the model itself. */}
      {grade.factors && grade.factors.length > 0 && (
        <div className="space-y-1 pt-3 border-t border-[#2A2A3A]">
          {grade.factors.map((f, i) => (
            <div key={i} className="flex items-center justify-between text-xs">
              <span className="text-[#8888A0]">{f.description}</span>
              <span
                className="font-mono"
                style={{
                  color:
                    f.points > 0
                      ? GRADE_COLORS.strong
                      : f.points < 0
                      ? GRADE_COLORS.weak
                      : GRADE_COLORS.unknown,
                }}
              >
                {f.points > 0 ? "+" : ""}{f.points}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
