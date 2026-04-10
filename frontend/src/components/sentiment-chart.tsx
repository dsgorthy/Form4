"use client";

import dynamic from "next/dynamic";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { registerForm4Theme, baseGrid, COLORS } from "@/lib/echarts-theme";
import { formatCurrency } from "@/lib/format";
import type { SentimentPoint } from "@/lib/types";

registerForm4Theme();

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

interface SentimentChartProps {
  data: SentimentPoint[];
}

export function SentimentChart({ data }: SentimentChartProps) {
  if (data.length === 0) {
    return (
      <Card className="bg-[#12121A] border-[#2A2A3A]">
        <CardHeader>
          <CardTitle className="text-sm font-medium text-[#8888A0]">Market Insider Sentiment</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-[300px] items-center justify-center text-[#55556A]">No sentiment data available</div>
        </CardContent>
      </Card>
    );
  }

  const option = {
    grid: { ...baseGrid(), bottom: 40, right: 12 },
    xAxis: {
      type: "category" as const,
      data: data.map((p) => p.date),
      axisLabel: { fontSize: 10, color: COLORS.muted },
    },
    yAxis: {
      type: "value" as const,
      axisLabel: { formatter: (v: number) => formatCurrency(v), fontSize: 11, color: COLORS.muted },
      splitLine: { lineStyle: { color: COLORS.border, type: "dashed" as const } },
    },
    series: [
      {
        name: "Buy Volume",
        type: "line" as const,
        data: data.map((p) => p.buy_value),
        smooth: true,
        lineStyle: { color: "#22C55E", width: 2 },
        itemStyle: { color: "#22C55E" },
        showSymbol: false,
        areaStyle: {
          color: {
            type: "linear" as const,
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(34,197,94,0.3)" },
              { offset: 1, color: "rgba(34,197,94,0)" },
            ],
          },
        },
      },
      {
        name: "Sell Volume",
        type: "line" as const,
        data: data.map((p) => p.sell_value),
        smooth: true,
        lineStyle: { color: "#EF4444", width: 2 },
        itemStyle: { color: "#EF4444" },
        showSymbol: false,
        areaStyle: {
          color: {
            type: "linear" as const,
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(239,68,68,0.3)" },
              { offset: 1, color: "rgba(239,68,68,0)" },
            ],
          },
        },
      },
    ],
    legend: {
      data: ["Buy Volume", "Sell Volume"],
      bottom: 28,
      textStyle: { color: COLORS.text, fontSize: 11 },
    },
    tooltip: {
      trigger: "axis" as const,
      formatter: (params: any) => {
        if (!Array.isArray(params) || !params.length) return "";
        const date = params[0].axisValueLabel;
        const rows = params.map((p: any) => {
          const color = p.seriesName === "Buy Volume" ? "#22C55E" : "#EF4444";
          return `<div style="display:flex;justify-content:space-between;gap:12px"><span style="color:${color}">${p.seriesName}</span><span style="font-family:monospace;color:#E8E8ED">${formatCurrency(p.value)}</span></div>`;
        }).join("");
        return `<div style="color:#8888A0;margin-bottom:4px">${date}</div>${rows}`;
      },
    },
  };

  return (
    <Card className="bg-[#12121A] border-[#2A2A3A]">
      <CardHeader>
        <CardTitle className="text-sm font-medium text-[#8888A0]">Market Insider Sentiment</CardTitle>
      </CardHeader>
      <CardContent>
        <ReactECharts option={option} theme="form4" className="h-[220px] sm:h-[300px]" opts={{ renderer: "svg" }} />
      </CardContent>
    </Card>
  );
}
