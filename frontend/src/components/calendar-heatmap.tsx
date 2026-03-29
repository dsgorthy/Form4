"use client";

import type { HeatmapDay } from "@/lib/types";
import { formatCurrency } from "@/lib/format";
import { useState } from "react";

interface CalendarHeatmapProps {
  data: HeatmapDay[];
  days?: number;
}

function getColor(count: number, maxCount: number): string {
  if (count === 0) return "#1A1A26";
  const intensity = Math.min(count / Math.max(maxCount * 0.6, 1), 1);
  if (intensity < 0.25) return "#0F3D1E";
  if (intensity < 0.5) return "#166534";
  if (intensity < 0.75) return "#22C55E";
  return "#4ADE80";
}

const DAY_LABELS = ["", "Mon", "", "Wed", "", "Fri", ""];
const LABEL_WIDTH = 28;

export function CalendarHeatmap({ data, days = 90 }: CalendarHeatmapProps) {
  const [tooltip, setTooltip] = useState<{
    x: number;
    y: number;
    day: HeatmapDay;
  } | null>(null);

  // Build date lookup
  const byDate = new Map<string, HeatmapDay>();
  for (const d of data) {
    byDate.set(d.date, d);
  }

  // Generate grid: columns = weeks, rows = days of week
  const endDate = data.length > 0 ? new Date(data[data.length - 1].date) : new Date();
  const startDate = new Date(endDate);
  startDate.setDate(startDate.getDate() - days);

  // Align to start of week (Sunday)
  const gridStart = new Date(startDate);
  gridStart.setDate(gridStart.getDate() - gridStart.getDay());

  const cells: { date: string; col: number; row: number; day: HeatmapDay | null }[] = [];
  const current = new Date(gridStart);
  const maxCount = Math.max(...data.map((d) => d.count), 1);

  while (current <= endDate) {
    const dateStr = current.toISOString().slice(0, 10);
    const dayOfWeek = current.getDay();
    const weekNum = Math.floor(
      (current.getTime() - gridStart.getTime()) / (7 * 24 * 60 * 60 * 1000)
    );

    const dayData = byDate.get(dateStr) ?? null;
    cells.push({
      date: dateStr,
      col: weekNum,
      row: dayOfWeek,
      day: dayData,
    });
    current.setDate(current.getDate() + 1);
  }

  const numWeeks = Math.max(...cells.map((c) => c.col)) + 1;

  // Use a ratio-based cell size so the SVG scales with viewBox
  const cellSize = 10;
  const cellGap = 2;
  const cellTotal = cellSize + cellGap;
  const svgWidth = numWeeks * cellTotal + LABEL_WIDTH;
  const svgHeight = 7 * cellTotal + 4;

  // Month labels
  const monthLabels: { label: string; x: number }[] = [];
  let lastMonth = -1;
  for (const cell of cells) {
    if (cell.row !== 0) continue;
    const d = new Date(cell.date);
    const month = d.getMonth();
    if (month !== lastMonth) {
      lastMonth = month;
      monthLabels.push({
        label: d.toLocaleString("en-US", { month: "short" }),
        x: cell.col * cellTotal + LABEL_WIDTH,
      });
    }
  }

  return (
    <div className="relative w-full">
      <svg
        viewBox={`0 0 ${svgWidth} ${svgHeight + 14}`}
        className="w-full h-auto overflow-visible"
        preserveAspectRatio="xMinYMin meet"
        onMouseLeave={() => setTooltip(null)}
      >
        {/* Month labels */}
        {monthLabels.map((m, i) => (
          <text
            key={i}
            x={m.x}
            y={9}
            className="fill-[#55556A]"
            fontSize={7}
            fontFamily="monospace"
          >
            {m.label}
          </text>
        ))}

        {/* Day labels */}
        {DAY_LABELS.map((label, i) =>
          label ? (
            <text
              key={i}
              x={0}
              y={i * cellTotal + cellSize - 1 + 14}
              className="fill-[#55556A]"
              fontSize={7}
              fontFamily="monospace"
            >
              {label}
            </text>
          ) : null
        )}

        {/* Cells */}
        {cells.map((cell) => {
          const count = cell.day?.count ?? 0;
          return (
            <rect
              key={cell.date}
              x={cell.col * cellTotal + LABEL_WIDTH}
              y={cell.row * cellTotal + 14}
              width={cellSize}
              height={cellSize}
              rx={1.5}
              fill={getColor(count, maxCount)}
              className="cursor-pointer"
              onMouseEnter={(e) => {
                if (cell.day) {
                  const rect = (e.target as SVGRectElement).getBoundingClientRect();
                  setTooltip({ x: rect.x, y: rect.y, day: cell.day });
                } else {
                  setTooltip(null);
                }
              }}
            />
          );
        })}
      </svg>

      {/* Tooltip */}
      {tooltip && (
        <div
          className="fixed z-50 rounded-md border border-[#2A2A3A] bg-[#12121A] px-3 py-2 text-xs shadow-xl pointer-events-none"
          style={{
            left: tooltip.x,
            top: tooltip.y - 60,
          }}
        >
          <div className="font-mono text-[#E8E8ED]">{tooltip.day.date}</div>
          <div className="text-[#8888A0]">
            {tooltip.day.count} filings &middot; {formatCurrency(tooltip.day.total_value)}
          </div>
          {tooltip.day.top_ticker && (
            <div className="text-[#55556A]">Top: {tooltip.day.top_ticker}</div>
          )}
        </div>
      )}
    </div>
  );
}
