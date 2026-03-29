"use client";

import dynamic from "next/dynamic";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { registerForm4Theme, baseGrid, COLORS } from "@/lib/echarts-theme";
import type { FilingDelayData } from "@/lib/types";

registerForm4Theme();

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

const BAR_COLORS = [
  "#22C55E", "#4ADE80", "#86EFAC", "#FDE047",
  "#FBBF24", "#F59E0B", "#D97706",
];

interface FilingDelayChartProps {
  data: FilingDelayData;
}

export function FilingDelayChart({ data }: FilingDelayChartProps) {
  const { bins, stats } = data;

  if (bins.length === 0) {
    return (
      <Card className="bg-[#12121A] border-[#2A2A3A]">
        <CardHeader>
          <CardTitle className="text-sm font-medium text-[#8888A0]">Filing Delay Distribution</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-[200px] items-center justify-center text-[#55556A]">No filing delay data available</div>
        </CardContent>
      </Card>
    );
  }

  const labels = bins.map((b) => `${b.label} day${b.label === "0" || b.label === "1" ? "" : "s"}`);
  const values = bins.map((b, i) => ({
    value: b.count,
    itemStyle: { color: BAR_COLORS[i % BAR_COLORS.length] },
  }));

  const option = {
    grid: { ...baseGrid(), left: 70, right: 20, top: 4, bottom: 4 },
    xAxis: { type: "value" as const, splitLine: { lineStyle: { color: COLORS.border, type: "dashed" as const } } },
    yAxis: {
      type: "category" as const,
      data: labels,
      axisLabel: { color: COLORS.bright, fontSize: 11 },
    },
    series: [
      {
        type: "bar" as const,
        data: values,
        barWidth: 16,
        itemStyle: { borderRadius: [0, 4, 4, 0] },
      },
    ],
    tooltip: {
      trigger: "axis" as const,
      axisPointer: { type: "shadow" as const },
      formatter: (params: any) => {
        const p = Array.isArray(params) ? params[0] : params;
        const idx = p.dataIndex;
        const pct = bins[idx]?.pct ?? "";
        return `<div style="color:#8888A0">${p.name}</div><div style="font-family:monospace;color:#E8E8ED">${p.value.toLocaleString()} filings (${pct}%)</div>`;
      },
    },
  };

  return (
    <Card className="bg-[#12121A] border-[#2A2A3A]">
      <CardHeader>
        <CardTitle className="text-sm font-medium text-[#8888A0]">Filing Delay Distribution</CardTitle>
      </CardHeader>
      <CardContent>
        <ReactECharts option={option} theme="form4" style={{ height: 200 }} opts={{ renderer: "svg" }} />
        <div className="mt-3 flex items-center justify-center gap-4 text-xs text-[#55556A]">
          <span>Avg: <span className="text-[#E8E8ED]">{stats.avg_delay} days</span></span>
          <span className="text-[#2A2A3A]">&middot;</span>
          <span>Median: <span className="text-[#E8E8ED]">{stats.median_delay} days</span></span>
          <span className="text-[#2A2A3A]">&middot;</span>
          <span><span className="text-[#E8E8ED]">{stats.pct_within_2d}%</span> filed within 2 days</span>
        </div>
      </CardContent>
    </Card>
  );
}
