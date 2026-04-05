"use client";

import { useEffect, useState } from "react";
import dynamic from "next/dynamic";
import { useAuth } from "@clerk/nextjs";
import { registerForm4Theme, baseGrid, COLORS } from "@/lib/echarts-theme";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

interface ScorePoint {
  date: string;
  blended_score: number | null;
  global_score: number | null;
  ticker_score: number | null;
  trade_count: number;
}

interface ScoreHistoryData {
  by_ticker: Record<string, ScorePoint[]>;
  global_series: { date: string; score: number | null; ticker: string }[];
  grade_thresholds: { grade: string; score: number }[];
  total_snapshots: number;
}

type ViewMode = "all" | string; // "all" or a specific ticker

function gradeForScore(s: number): string {
  if (s >= 2.5) return "A+";
  if (s >= 2.0) return "A";
  if (s >= 1.2) return "B";
  if (s >= 0.6) return "C";
  return "D";
}

export function InsiderScoreChart({ insiderId }: { insiderId: string }) {
  const { getToken } = useAuth();
  const [data, setData] = useState<ScoreHistoryData | null>(null);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState<ViewMode>("all");

  useEffect(() => {
    registerForm4Theme();
    (async () => {
      try {
        const token = await getToken();
        const res = await fetch(`${apiBase}/insiders/${insiderId}/score-history`, {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        if (res.ok) {
          setData(await res.json());
        }
      } catch {}
      setLoading(false);
    })();
  }, [insiderId, getToken]);

  if (loading) {
    return <div className="h-[240px] bg-[#1A1A26] rounded-lg border border-[#2A2A3A] animate-pulse" />;
  }

  if (!data || data.total_snapshots < 2) return null;

  // Don't show chart if all scores are near-zero (insider only sells, no buy history)
  const allScores = data.global_series.filter((p) => p.score != null).map((p) => p.score!);
  const avgScore = allScores.length > 0 ? allScores.reduce((a, b) => a + b, 0) / allScores.length : 0;
  const hasVariance = allScores.some((s) => Math.abs(s - avgScore) > 0.3);
  if (allScores.length > 10 && avgScore < 0.2 && !hasVariance) return null;

  // Build ticker list sorted by snapshot count
  const tickers = Object.entries(data.by_ticker)
    .map(([tk, pts]) => ({ ticker: tk, count: pts.length }))
    .sort((a, b) => b.count - a.count);

  // Get series based on view
  let series: { date: string; score: number; ticker: string }[];
  if (view === "all") {
    series = data.global_series.filter((p) => p.score != null) as any;
  } else {
    const pts = data.by_ticker[view] || [];
    series = pts
      .filter((p) => p.blended_score != null)
      .map((p) => ({ date: p.date, score: p.blended_score!, ticker: view }));
  }

  if (series.length < 2) {
    return (
      <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
        <div className="flex items-center justify-between mb-3">
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
              PIT Score Over Time
            </div>
          </div>
          {tickers.length > 1 && (
            <select
              value={view}
              onChange={(e) => setView(e.target.value)}
              className="bg-[#1A1A26] border border-[#2A2A3A] text-[#E8E8ED] text-xs font-mono rounded px-2 py-1 focus:outline-none focus:border-[#3B82F6]"
            >
              <option value="all">All Tickers</option>
              {tickers.map((t) => (
                <option key={t.ticker} value={t.ticker}>{t.ticker} ({t.count})</option>
              ))}
            </select>
          )}
        </div>
        <div className="text-xs text-[#55556A] py-8 text-center">Not enough data for {view === "all" ? "this insider" : view}</div>
      </div>
    );
  }

  const gradeColors: Record<string, string> = {
    "A+": "#D97706", A: "#F59E0B", B: "#94A3B8", C: "#CD7F32",
  };

  const option = {
    grid: { ...baseGrid(), top: 30, bottom: 40, right: 40 },
    tooltip: {
      trigger: "item" as const,
      formatter: (p: any) => {
        const score = p.value?.[1]?.toFixed(2) ?? "\u2014";
        const grade = gradeForScore(p.value?.[1] ?? 0);
        return `<b>${p.value?.[2] || ""}</b><br/>${p.value?.[0]}<br/>Score: ${score} (${grade})`;
      },
    },
    xAxis: { type: "time" as const },
    yAxis: {
      type: "value" as const,
      min: 0, max: 3,
      axisLabel: {
        formatter: (v: number) => {
          if (v >= 2.5) return "A+";
          if (v >= 2) return "A";
          if (v >= 1.2) return "B";
          if (v >= 0.6) return "C";
          return "D";
        },
      },
    },
    series: [
      // Smoothed line — PRIMARY visual (10-point moving average)
      {
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 3, color: COLORS.blue },
        areaStyle: { color: COLORS.blue, opacity: 0.08 },
        z: 10,
        tooltip: {
          formatter: (p: any) => {
            const score = p.value?.[1]?.toFixed(2) ?? "\u2014";
            const grade = gradeForScore(p.value?.[1] ?? 0);
            return `${p.value?.[0]}<br/>PIT Score: ${score} <b>(${grade})</b>`;
          },
        },
        data: (() => {
          const sorted = [...series].sort((a, b) => a.date.localeCompare(b.date));
          const result: [string, number][] = [];
          const W = Math.min(10, Math.max(3, Math.floor(sorted.length / 8)));
          for (let i = 0; i < sorted.length; i++) {
            const win = sorted.slice(Math.max(0, i - W + 1), i + 1);
            const avg = win.reduce((s, p) => s + (p.score ?? 0), 0) / win.length;
            result.push([sorted[i].date, avg]);
          }
          return result;
        })(),
        markLine: {
          silent: true,
          symbol: "none",
          lineStyle: { type: "dashed", opacity: 0.3 },
          label: { position: "end", fontSize: 10 },
          data: [
            { yAxis: 2.5, label: { formatter: "A+", color: "#D97706" }, lineStyle: { color: "#D97706" } },
            { yAxis: 2.0, label: { formatter: "A", color: COLORS.amber }, lineStyle: { color: COLORS.amber } },
            { yAxis: 1.2, label: { formatter: "B", color: COLORS.blue }, lineStyle: { color: COLORS.blue } },
            { yAxis: 0.6, label: { formatter: "C", color: COLORS.text }, lineStyle: { color: COLORS.text } },
          ],
        },
      },
      // Individual trade dots — subtle, secondary
      {
        type: "scatter",
        symbolSize: 3,
        z: 5,
        data: series.map((p) => [p.date, p.score, p.ticker]),
        itemStyle: {
          opacity: 0.3,
          color: (p: any) => {
            const s = p.value?.[1] ?? 0;
            if (s >= 2.5) return "#D97706";
            if (s >= 2) return COLORS.amber;
            if (s >= 1.2) return COLORS.blue;
            if (s >= 0.6) return COLORS.text;
            return COLORS.muted;
          },
        },
      },
    ],
  };

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
      <div className="flex items-center justify-between mb-3">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
            PIT Score Over Time
          </div>
          <div className="text-xs text-[#55556A] mt-0.5">
            Point-in-time grade evolution with each new trade
          </div>
        </div>
        {tickers.length > 1 && (
          <select
            value={view}
            onChange={(e) => setView(e.target.value)}
            className="bg-[#1A1A26] border border-[#2A2A3A] text-[#E8E8ED] text-xs font-mono rounded px-2 py-1 focus:outline-none focus:border-[#3B82F6]"
          >
            <option value="all">All Tickers</option>
            {tickers.map((t) => (
              <option key={t.ticker} value={t.ticker}>{t.ticker} ({t.count})</option>
            ))}
          </select>
        )}
      </div>
      <ReactECharts option={option} theme="form4" className="h-[220px]" opts={{ renderer: "svg" }} />
    </div>
  );
}
