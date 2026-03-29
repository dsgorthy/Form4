interface SignalQuality {
  score: number;
  grade: string;
  factors: string[];
}

const GRADE_COLORS: Record<string, string> = {
  A: "bg-[#22C55E]/20 text-[#22C55E] border-[#22C55E]/40",
  B: "bg-[#3B82F6]/20 text-[#3B82F6] border-[#3B82F6]/40",
  C: "bg-[#8888A0]/15 text-[#8888A0] border-[#8888A0]/30",
  D: "bg-[#F59E0B]/15 text-[#F59E0B] border-[#F59E0B]/30",
  F: "bg-[#EF4444]/15 text-[#EF4444] border-[#EF4444]/30",
};

const GRADE_LABELS: Record<string, string> = {
  A: "Very High Signal",
  B: "High Signal",
  C: "Average",
  D: "Low Signal",
  F: "Noise",
};

export function SignalQualityBadge({ quality }: { quality?: SignalQuality }) {
  if (!quality) return null;

  const colors = GRADE_COLORS[quality.grade] || GRADE_COLORS.C;
  const label = GRADE_LABELS[quality.grade] || "Unknown";

  return (
    <span
      className={`inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-xs font-semibold ${colors}`}
      title={`Signal Quality: ${quality.score}/10 — ${label}\n${quality.factors.join("\n")}`}
    >
      {quality.grade}
      <span className="font-normal opacity-75">{quality.score.toFixed(0)}</span>
    </span>
  );
}

export function SignalQualityDetail({ quality }: { quality?: SignalQuality }) {
  if (!quality) return null;

  const colors = GRADE_COLORS[quality.grade] || GRADE_COLORS.C;
  const label = GRADE_LABELS[quality.grade] || "Unknown";

  return (
    <div className={`rounded-lg border p-4 ${colors.replace(/text-\[.*?\]/, "").trim()}`}>
      <div className="flex items-center gap-3 mb-2">
        <span className={`text-2xl font-bold ${colors.split(" ").find(c => c.startsWith("text-"))}`}>
          {quality.grade}
        </span>
        <div>
          <div className={`text-sm font-semibold ${colors.split(" ").find(c => c.startsWith("text-"))}`}>
            {label}
          </div>
          <div className="text-xs text-[#55556A]">
            Score: {quality.score}/10
          </div>
        </div>
      </div>
      {quality.factors.length > 0 && (
        <ul className="space-y-0.5 mt-2">
          {quality.factors.map((f, i) => (
            <li key={i} className="text-xs text-[#8888A0] flex items-start gap-1.5">
              <span className="text-[#55556A] mt-px shrink-0">&bull;</span>
              {f}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
