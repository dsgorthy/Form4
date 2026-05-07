import { Badge } from "@/components/ui/badge";
import { insiderGradeColor } from "@/lib/grade-colors";

interface Props {
  grade?: string | null;
  bestTicker?: string | null;
  tickerCount?: number;
  compact?: boolean;
  showLabel?: boolean;
}

export function InsiderGradeBadge({ grade, bestTicker, tickerCount, compact, showLabel }: Props) {
  if (!grade) {
    return <span className="text-[10px] text-[#55556A] font-mono">New</span>;
  }

  const badge = (
    <Badge
      style={{ backgroundColor: insiderGradeColor(grade), color: "#fff" }}
      className="text-xs font-mono px-1.5 py-0.5 min-w-[28px] text-center inline-flex justify-center"
      title={`Insider Grade: ${grade}`}
    >
      {grade}
    </Badge>
  );

  return (
    <span className="inline-flex items-center gap-1">
      {showLabel && (
        <span className="text-[9px] text-[#55556A] uppercase tracking-wide font-medium">
          Insider
        </span>
      )}
      {badge}
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
