export const dynamic = "force-dynamic";

export const metadata = {
  title: "Market Overview — Form4",
  description: "Filing activity, sentiment, heatmaps, and intelligence signals for insider trading research.",
};

import { Suspense } from "react";
import Link from "next/link";
import { auth } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { StatCard } from "@/components/ui/stat-card";
import { SentimentChart } from "@/components/sentiment-chart";
import { CalendarHeatmap } from "@/components/calendar-heatmap";
import { FilingDelayChart } from "@/components/filing-delay-chart";
import { ConvergenceAlerts } from "@/components/dashboard/convergence-alerts";
import { ActivitySpikes } from "@/components/dashboard/activity-spikes";
import { SellCessation } from "@/components/dashboard/sell-cessation";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency } from "@/lib/format";
import { SyncStatus } from "@/components/sync-status";
import { ProGate } from "@/components/pro-gate";
import type { DashboardStats, SentimentPoint, HeatmapDay, FilingDelayData } from "@/lib/types";

async function getMarketData() {
  try {
    const [stats, sentiment, heatmap, filingDelays] = await Promise.all([
      fetchAPIAuth<DashboardStats>("/dashboard/stats"),
      fetchAPIAuth<SentimentPoint[]>("/dashboard/sentiment", { days: "30" }),
      fetchAPIAuth<HeatmapDay[]>("/dashboard/heatmap", { days: "365" }),
      fetchAPIAuth<FilingDelayData>("/dashboard/filing-delays"),
    ]);
    return { stats, sentiment, heatmap, filingDelays, error: null };
  } catch {
    return { stats: null, sentiment: [], heatmap: [], filingDelays: null, error: "Failed to load market data." };
  }
}

function SectionSkeleton({ title }: { title: string }) {
  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4 animate-pulse">
      <h3 className="text-sm font-medium text-[#8888A0] mb-3">{title}</h3>
      <div className="space-y-2">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="h-10 rounded-md bg-[#1A1A26]" />
        ))}
      </div>
    </div>
  );
}

export default async function MarketOverviewPage() {
  const { userId } = await auth();
  if (!userId) redirect("/");

  const { stats, sentiment, heatmap, filingDelays, error } = await getMarketData();

  return (
    <div className="space-y-6">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Market Overview</h1>
        <p className="text-sm text-[#55556A] mt-1">
          Insider filing activity, sentiment, and intelligence signals.
        </p>
      </div>

      {error && (
        <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 px-4 py-3 text-sm text-[#F59E0B]">
          {error}
        </div>
      )}

      <div className="flex justify-end">
        <SyncStatus />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard title="Signals Today" value={stats?.signals_today ?? "--"} subtitle="New filings detected" trend="neutral" />
        <StatCard
          title="Active Clusters"
          value={stats?.active_clusters ?? "--"}
          subtitle="Multi-insider convergence"
          trend={stats && stats.active_clusters > 0 ? "up" : "neutral"}
        />
        <StatCard
          title="Buy / Sell Ratio"
          value={stats ? stats.buy_sell_ratio.toFixed(2) : "--"}
          subtitle={
            stats ? (stats.buy_sell_ratio >= 1.5 ? "Bullish bias" : stats.buy_sell_ratio <= 0.7 ? "Bearish bias" : "Neutral") : undefined
          }
          trend={stats ? (stats.buy_sell_ratio >= 1.5 ? "up" : stats.buy_sell_ratio <= 0.7 ? "down" : "neutral") : "neutral"}
        />
        <StatCard
          title="Top Mover"
          value={stats?.top_mover ? `${stats.top_mover.ticker} ${formatCurrency(stats.top_mover.value)}` : "--"}
          subtitle="Highest value filing today"
          trend="neutral"
        />
      </div>

      {heatmap.length > 0 && (
        <Card className="bg-[#12121A] border-[#2A2A3A]">
          <CardHeader>
            <CardTitle className="text-sm font-medium text-[#8888A0]">Filing Activity (1 year)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <CalendarHeatmap data={heatmap} days={365} />
            </div>
          </CardContent>
        </Card>
      )}

      <ProGate label="Intelligence Signals">
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
          <Suspense fallback={<SectionSkeleton title="Convergence Alerts" />}>
            <ConvergenceAlerts />
          </Suspense>
          <Suspense fallback={<SectionSkeleton title="Activity Spikes" />}>
            <ActivitySpikes />
          </Suspense>
          <Suspense fallback={<SectionSkeleton title="Sell Cessation" />}>
            <SellCessation />
          </Suspense>
        </div>
      </ProGate>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <SentimentChart data={sentiment} />
        {filingDelays && <FilingDelayChart data={filingDelays} />}
      </div>
    </div>
  );
}
