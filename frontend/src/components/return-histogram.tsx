"use client";

import dynamic from "next/dynamic";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { registerForm4Theme, baseGrid, COLORS } from "@/lib/echarts-theme";

registerForm4Theme();

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

interface Bin {
  label: string;
  count: number;
  avg_return: number;
}

interface ReturnDistributionData {
  bins: Bin[];
  total_trades: number;
  win_count: number;
  loss_count: number;
}

interface ReturnHistogramProps {
  data: ReturnDistributionData;
  tradeType?: "buy" | "sell";
}

function isPositiveBin(label: string): boolean {
  if (label.startsWith(">")) return true;
  if (label.startsWith("<")) return false;
  const match = label.match(/^(-?\d+)%/);
  if (match) return parseInt(match[1], 10) >= 0;
  return false;
}

export function ReturnHistogram({ data, tradeType = "buy" }: ReturnHistogramProps) {
  const isSell = tradeType === "sell";
  const title = isSell ? "Stock Move After Sell" : "Return Distribution";

  if (!data.bins.length) {
    return (
      <Card className="bg-[#12121A] border-[#2A2A3A]">
        <CardHeader>
          <CardTitle className="text-sm font-medium text-[#8888A0]">{title}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-[200px] items-center justify-center text-[#55556A]">No return data available</div>
        </CardContent>
      </Card>
    );
  }

  const winRate = data.total_trades > 0 ? ((data.win_count / data.total_trades) * 100).toFixed(1) : "0.0";

  const option = {
    grid: { ...baseGrid(), bottom: 40, left: 40, right: 10 },
    xAxis: {
      type: "category" as const,
      data: data.bins.map((b) => b.label),
      axisLabel: { fontSize: 10, color: COLORS.muted, rotate: 35 },
    },
    yAxis: {
      type: "value" as const,
      splitLine: { lineStyle: { color: COLORS.border, type: "dashed" as const } },
      axisLabel: { fontSize: 11, color: COLORS.muted },
    },
    series: [
      {
        type: "bar" as const,
        data: data.bins.map((bin) => {
          const isPositive = isPositiveBin(bin.label);
          const isGood = isSell ? !isPositive : isPositive;
          return {
            value: bin.count,
            itemStyle: { color: isGood ? "#22C55E" : "#EF4444", opacity: 0.85 },
          };
        }),
        itemStyle: { borderRadius: [3, 3, 0, 0] },
      },
    ],
    tooltip: {
      trigger: "axis" as const,
      axisPointer: { type: "shadow" as const },
      formatter: (params: any) => {
        const p = Array.isArray(params) ? params[0] : params;
        const idx = p.dataIndex;
        const bin = data.bins[idx];
        const avg = bin?.avg_return;
        return `<div style="color:#8888A0">Bin: ${p.name}</div><div style="font-family:monospace;color:#E8E8ED">${p.value} trade${p.value !== 1 ? "s" : ""}${avg != null ? ` (avg ${avg > 0 ? "+" : ""}${avg}%)` : ""}</div>`;
      },
    },
  };

  return (
    <Card className="bg-[#12121A] border-[#2A2A3A]">
      <CardHeader>
        <CardTitle className="text-sm font-medium text-[#8888A0]">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <ReactECharts option={option} theme="form4" style={{ height: 200 }} opts={{ renderer: "svg" }} />
        <div className="mt-3 flex items-center gap-4 text-xs text-[#8888A0]">
          <span>{data.total_trades} trade{data.total_trades !== 1 ? "s" : ""}</span>
          <span className="text-[#22C55E]">{data.win_count} {isSell ? "correct" : "win"}{data.win_count !== 1 ? "s" : ""} ({winRate}%)</span>
          <span className="text-[#EF4444]">{data.loss_count} loss{data.loss_count !== 1 ? "es" : ""}</span>
        </div>
      </CardContent>
    </Card>
  );
}
