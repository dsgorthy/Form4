"use client";

import dynamic from "next/dynamic";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { registerForm4Theme, baseGrid, timeSeriesDataZoom, COLORS } from "@/lib/echarts-theme";

registerForm4Theme();

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

interface TimelinePoint {
  date: string;
  ticker: string;
  trade_type: string;
  value: number;
  return_pct: number;
}

interface TradeOutcomeTimelineProps {
  timeline: TimelinePoint[];
  dominantType: string;
}

export function TradeOutcomeTimeline({ timeline, dominantType }: TradeOutcomeTimelineProps) {
  if (!timeline || timeline.length === 0) return null;

  const isSell = dominantType === "sell";
  const hasBoth = timeline.some((t) => t.trade_type === "buy") && timeline.some((t) => t.trade_type === "sell");
  const title = hasBoth ? "Signal Accuracy Over Time" : isSell ? "Stock Move After Each Sell" : "Return After Each Buy";

  const buys: any[] = [];
  const sells: any[] = [];

  for (const t of timeline) {
    const isSellTrade = t.trade_type === "sell";
    const normalized = isSellTrade ? -t.return_pct : t.return_pct;
    const isGood = normalized > 0;
    const point = {
      value: [t.date, normalized],
      ticker: t.ticker,
      rawReturn: t.return_pct,
      isSellTrade,
      isGood,
      itemStyle: { color: isGood ? "#22C55E" : "#EF4444", opacity: 0.8 },
    };
    if (isSellTrade) sells.push(point);
    else buys.push(point);
  }

  const option = {
    grid: { ...baseGrid(), bottom: 50, right: 12 },
    xAxis: {
      type: "time" as const,
      axisLabel: { fontSize: 10, color: COLORS.muted },
    },
    yAxis: {
      type: "value" as const,
      axisLabel: {
        fontSize: 11,
        color: COLORS.muted,
        formatter: (v: number) => `${v > 0 ? "+" : ""}${v}%`,
      },
      splitLine: { lineStyle: { color: COLORS.border, type: "dashed" as const } },
    },
    series: [
      {
        name: "Buys",
        type: "scatter" as const,
        data: buys,
        symbol: "circle",
        symbolSize: 6,
      },
      {
        name: "Sells",
        type: "scatter" as const,
        data: sells,
        symbol: "diamond",
        symbolSize: 7,
      },
      {
        type: "line" as const,
        markLine: {
          silent: true,
          symbol: "none",
          lineStyle: { color: COLORS.muted, type: "dashed" as const },
          label: { show: hasBoth, formatter: "breakeven", position: "end" as const, color: COLORS.muted, fontSize: 9 },
          data: [{ yAxis: 0 }],
        },
        data: [],
      },
    ],
    dataZoom: timeSeriesDataZoom(),
    tooltip: {
      trigger: "item" as const,
      formatter: (params: any) => {
        const d = params.data;
        if (!d) return "";
        const label = d.isSellTrade ? "SELL" : "BUY";
        const labelColor = d.isSellTrade ? "#EF4444" : "#22C55E";
        const resultColor = d.isGood ? "#22C55E" : "#EF4444";
        const result = d.isGood ? "Correct" : "Wrong";
        return `<div style="color:#E8E8ED;font-weight:500">${d.ticker} &middot; ${d.value[0]} <span style="color:${labelColor}">${label}</span></div><div style="color:${resultColor}">${result}: stock ${d.rawReturn > 0 ? "+" : ""}${d.rawReturn}%</div>`;
      },
    },
    legend: hasBoth
      ? { data: ["Buys", "Sells"], bottom: 28, textStyle: { color: COLORS.text, fontSize: 11 } }
      : undefined,
  };

  return (
    <Card className="bg-[#12121A] border-[#2A2A3A]">
      <CardHeader>
        <CardTitle className="text-sm font-medium text-[#8888A0]">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <ReactECharts option={option} theme="form4" className="h-[180px] sm:h-[240px]" opts={{ renderer: "svg" }} />
        {hasBoth && (
          <div className="flex items-center gap-4 mt-2 text-[10px] text-[#55556A]">
            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-[#8888A0] inline-block" /> Buy</span>
            <span className="flex items-center gap-1"><span className="w-2 h-2 rotate-45 bg-[#8888A0] inline-block" /> Sell</span>
            <span className="flex items-center gap-1"><span className="w-2 h-2 bg-[#22C55E] inline-block rounded-sm" /> Correct call</span>
            <span className="flex items-center gap-1"><span className="w-2 h-2 bg-[#EF4444] inline-block rounded-sm" /> Wrong call</span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
