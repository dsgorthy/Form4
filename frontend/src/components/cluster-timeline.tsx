"use client";

import { formatCurrency } from "@/lib/format";
import { useState } from "react";

interface TimelineInsider {
  insider_id: string;
  name: string;
  cik: string | null;
  score_tier: number | null;
  trade_value: number;
  last_trade_date: string;
  is_csuite: number;
}

interface ClusterTimelineProps {
  insiders: TimelineInsider[];
  firstTrade: string;
  lastTrade: string;
}

const TIER_COLORS: Record<number, string> = {
  3: "#F59E0B",
  2: "#94A3B8",
  1: "#CD7F32",
  0: "#55556A",
};

export function ClusterTimeline({ insiders, firstTrade, lastTrade }: ClusterTimelineProps) {
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);

  const start = new Date(firstTrade).getTime();
  const end = new Date(lastTrade).getTime();
  const range = Math.max(end - start, 1);
  const isSameDay = firstTrade === lastTrade;

  // Sort by date
  const sorted = [...insiders].sort(
    (a, b) => new Date(a.last_trade_date).getTime() - new Date(b.last_trade_date).getTime()
  );

  const maxValue = Math.max(...sorted.map((i) => i.trade_value), 1);

  if (sorted.length < 2) return null;

  const HEIGHT = 32;
  const PADDING_X = 16;
  const WIDTH_USABLE = 100; // percentage

  return (
    <div className="mt-3 px-2">
      {/* Date axis labels */}
      <div className="flex justify-between text-[10px] text-[#55556A] font-mono mb-1 px-1">
        <span>{firstTrade}</span>
        {!isSameDay && <span>{lastTrade}</span>}
      </div>

      {/* Timeline bar */}
      <div className="relative h-8 rounded-full bg-[#1A1A26] border border-[#2A2A3A]/50">
        {sorted.map((ins, i) => {
          const pos = isSameDay
            ? ((i + 1) / (sorted.length + 1)) * WIDTH_USABLE
            : ((new Date(ins.last_trade_date).getTime() - start) / range) * (WIDTH_USABLE - 8) + 4;

          const size = Math.max(
            10,
            Math.min(24, 10 + (ins.trade_value / maxValue) * 14)
          );

          const color = TIER_COLORS[ins.score_tier ?? 0] || TIER_COLORS[0];
          const isHovered = hoveredIdx === i;

          return (
            <div
              key={ins.insider_id}
              className="absolute top-1/2 -translate-y-1/2 rounded-full border-2 transition-all cursor-pointer"
              style={{
                left: `${pos}%`,
                width: size,
                height: size,
                backgroundColor: `${color}33`,
                borderColor: color,
                transform: `translate(-50%, -50%) ${isHovered ? "scale(1.3)" : ""}`,
                zIndex: isHovered ? 10 : 1,
              }}
              onMouseEnter={() => setHoveredIdx(i)}
              onMouseLeave={() => setHoveredIdx(null)}
            />
          );
        })}

        {/* Tooltip */}
        {hoveredIdx !== null && (
          <div
            className="absolute -top-14 z-20 rounded-md border border-[#2A2A3A] bg-[#12121A] px-3 py-1.5 text-xs shadow-xl pointer-events-none whitespace-nowrap"
            style={{
              left: `${
                isSameDay
                  ? ((hoveredIdx + 1) / (sorted.length + 1)) * WIDTH_USABLE
                  : ((new Date(sorted[hoveredIdx].last_trade_date).getTime() - start) / range) *
                      (WIDTH_USABLE - 8) +
                    4
              }%`,
              transform: "translateX(-50%)",
            }}
          >
            <div className="font-medium text-[#E8E8ED]">{sorted[hoveredIdx].name}</div>
            <div className="text-[#8888A0]">
              {formatCurrency(sorted[hoveredIdx].trade_value)} &middot;{" "}
              {sorted[hoveredIdx].last_trade_date}
              {sorted[hoveredIdx].is_csuite ? " (C-Suite)" : ""}
            </div>
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="flex items-center gap-3 mt-2 text-[10px] text-[#55556A]">
        <span>Dot size = trade value</span>
        <span className="w-px h-3 bg-[#2A2A3A]" />
        {isSameDay ? (
          <span>All trades on same day</span>
        ) : (
          <span>
            {Math.round(range / (24 * 60 * 60 * 1000))}d spread
          </span>
        )}
      </div>
    </div>
  );
}
