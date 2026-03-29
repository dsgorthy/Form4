"use client";

/**
 * Option 4: Horizontal comparison bar.
 * Shows this insider's avg return vs the global average insider.
 */

interface InsiderComparisonBarProps {
  avgReturnPct: number;
  globalAvgPct: number;
  dominantType: string;
  totalTrades: number;
}

export function InsiderComparisonBar({
  avgReturnPct,
  globalAvgPct,
  dominantType,
  totalTrades,
}: InsiderComparisonBarProps) {
  if (totalTrades === 0) return null;

  const isSell = dominantType === "sell";
  // For sells, lower stock move = better signal
  const insiderIsGood = isSell ? avgReturnPct < globalAvgPct : avgReturnPct > globalAvgPct;
  const label = isSell ? "Avg Stock Move (7d)" : "Avg Return (7d)";

  // Scale: center at 0, range based on values
  const maxAbs = Math.max(Math.abs(avgReturnPct), Math.abs(globalAvgPct), 2) * 1.5;
  const toPercent = (v: number) => ((v + maxAbs) / (2 * maxAbs)) * 100;

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
        {label} vs All Insiders
      </div>

      <div className="relative h-8 bg-[#12121A] rounded overflow-hidden">
        {/* Zero line */}
        <div
          className="absolute top-0 bottom-0 w-px bg-[#55556A]"
          style={{ left: `${toPercent(0)}%` }}
        />

        {/* Global average marker */}
        <div
          className="absolute top-0 bottom-0 w-0.5 bg-[#55556A]"
          style={{ left: `${toPercent(globalAvgPct)}%` }}
          title={`All insiders avg: ${globalAvgPct > 0 ? "+" : ""}${globalAvgPct}%`}
        />
        <div
          className="absolute -top-5 text-[9px] text-[#55556A] whitespace-nowrap"
          style={{ left: `${toPercent(globalAvgPct)}%`, transform: "translateX(-50%)" }}
        >
          Avg: {globalAvgPct > 0 ? "+" : ""}{globalAvgPct}%
        </div>

        {/* This insider's bar */}
        {(() => {
          const zeroPos = toPercent(0);
          const valPos = toPercent(avgReturnPct);
          const left = Math.min(zeroPos, valPos);
          const width = Math.abs(valPos - zeroPos);
          return (
            <div
              className={`absolute top-1 bottom-1 rounded-sm ${insiderIsGood ? "bg-[#22C55E]" : "bg-[#EF4444]"}`}
              style={{ left: `${left}%`, width: `${width}%`, opacity: 0.85 }}
            />
          );
        })()}
      </div>

      <div className="flex items-center justify-between mt-2 text-xs">
        <span className={`font-mono ${insiderIsGood ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
          This insider: {avgReturnPct > 0 ? "+" : ""}{avgReturnPct}%
        </span>
        <span className="text-[#55556A]">
          {totalTrades} trades
        </span>
      </div>
    </div>
  );
}
