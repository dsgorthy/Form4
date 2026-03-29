"use client";

/**
 * Option 2: Simple stat sentence.
 * "Stock declined within 7d on 14 of 21 sells (67%)"
 */
interface TradeOutcomeSummaryProps {
  totalTrades: number;
  winCount: number;
  dominantType: string;
}

export function TradeOutcomeSummary({ totalTrades, winCount, dominantType }: TradeOutcomeSummaryProps) {
  if (totalTrades === 0) return null;

  const pct = Math.round((winCount / totalTrades) * 100);
  const isSell = dominantType === "sell";
  const good = pct >= 50;

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-2">
        Track Record Summary
      </div>
      <p className="text-sm text-[#E8E8ED]">
        {isSell ? "Stock declined" : "Stock rose"} within 7d on{" "}
        <span className={`font-mono font-bold ${good ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
          {winCount} of {totalTrades}
        </span>{" "}
        {isSell ? "sells" : "buys"}{" "}
        <span className={`font-mono ${good ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
          ({pct}%)
        </span>
      </p>
    </div>
  );
}
