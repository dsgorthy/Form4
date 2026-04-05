import { Badge } from "@/components/ui/badge";
import { tierColor, tierLabel, pitGradeColor } from "@/lib/format";

export function TierBadge({
  tier,
  pitGrade,
  bestTicker,
  tickerCount,
  compact,
}: {
  tier?: number | null;
  pitGrade?: string | null;
  bestTicker?: string | null;
  tickerCount?: number;
  compact?: boolean;
}) {
  // Prefer PIT grade when available
  if (pitGrade) {
    return (
      <span className="inline-flex items-center gap-0.5">
        <Badge
          style={{ backgroundColor: pitGradeColor(pitGrade), color: "#fff" }}
          className="text-xs font-mono px-1.5 py-0.5 min-w-[28px] text-center inline-flex justify-center"
        >
          {pitGrade}
        </Badge>
        {!compact && bestTicker && (
          <span className="hidden md:inline text-[9px] text-[#55556A] font-mono">
            {bestTicker}
            {tickerCount != null && tickerCount > 1 && (
              <span className="text-[#3A3A4A]">{` +${tickerCount - 1}`}</span>
            )}
          </span>
        )}
      </span>
    );
  }
  if (tier == null) return <span className="text-[10px] text-[#55556A] font-mono">New</span>;
  return (
    <Badge
      style={{ backgroundColor: tierColor(tier), color: "#fff" }}
      className="text-xs font-mono px-1.5 py-0.5 min-w-[28px] text-center inline-flex justify-center"
    >
      {tierLabel(tier)}
    </Badge>
  );
}
