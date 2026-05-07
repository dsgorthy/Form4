import { tradeGradeStarsColor, GRADE_COLORS } from "@/lib/grade-colors";

interface TradeGrade {
  score: number;
  stars: number;
  label: string;
  factors?: { name: string; points: number; description: string }[];
}

export function TradeGradeBadge({
  grade,
  showLabel,
}: {
  grade?: TradeGrade | null;
  showLabel?: boolean;
}) {
  if (!grade) return null;
  const color = tradeGradeStarsColor(grade.stars);
  const filled = "★".repeat(grade.stars);
  const empty = "☆".repeat(5 - grade.stars);
  return (
    <span className="inline-flex items-center gap-1">
      {showLabel && (
        <span className="text-[9px] text-[#55556A] uppercase tracking-wide font-medium">
          Trade
        </span>
      )}
      <span
        className="text-[11px] font-mono tracking-tight"
        style={{ color }}
        title={`Trade Grade: ${grade.score}/100 (${grade.label})`}
      >
        {filled}
        <span className="opacity-30">{empty}</span>
      </span>
    </span>
  );
}

export function TradeGradeDetail({ grade }: { grade?: TradeGrade | null }) {
  if (!grade) return null;
  const color = tradeGradeStarsColor(grade.stars);
  const filled = "★".repeat(grade.stars);
  const empty = "☆".repeat(5 - grade.stars);
  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4">
      <div className="flex items-center gap-3 mb-3">
        <span className="text-lg font-mono" style={{ color }}>
          {filled}<span className="opacity-30">{empty}</span>
        </span>
        <div>
          <span className="text-sm font-medium text-[#E8E8ED]">{grade.label}</span>
          <span className="text-xs text-[#55556A] ml-2">Score: {grade.score}/100</span>
        </div>
      </div>
      {grade.factors && grade.factors.length > 0 && (
        <div className="space-y-1">
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
