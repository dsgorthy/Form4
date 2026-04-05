interface TradeGrade {
  score: number;
  stars: number;
  label: string;
  factors?: { name: string; points: number; description: string }[];
}

const starColors: Record<number, string> = {
  5: "#F59E0B",
  4: "#22C55E",
  3: "#94A3B8",
  2: "#8888A0",
  1: "#55556A",
};

export function TradeGradeBadge({ grade }: { grade?: TradeGrade | null }) {
  if (!grade) return null;
  const color = starColors[grade.stars] || "#55556A";
  const filled = "\u2605".repeat(grade.stars);
  const empty = "\u2606".repeat(5 - grade.stars);
  return (
    <span
      className="text-[11px] font-mono tracking-tight"
      style={{ color }}
      title={`Trade Grade: ${grade.score}/100 (${grade.label})`}
    >
      {filled}
      <span className="opacity-30">{empty}</span>
    </span>
  );
}

export function TradeGradeDetail({ grade }: { grade?: TradeGrade | null }) {
  if (!grade) return null;
  const color = starColors[grade.stars] || "#55556A";
  const filled = "\u2605".repeat(grade.stars);
  const empty = "\u2606".repeat(5 - grade.stars);
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
              <span className={`font-mono ${f.points > 0 ? "text-[#22C55E]" : f.points < 0 ? "text-[#EF4444]" : "text-[#55556A]"}`}>
                {f.points > 0 ? "+" : ""}{f.points}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
