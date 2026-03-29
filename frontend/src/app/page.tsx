export const dynamic = "force-dynamic";

import { Suspense } from "react";
import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { StatCard } from "@/components/ui/stat-card";
import { SignalsTable } from "@/components/signals-table";
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
import type { DashboardStats, Filing, SentimentPoint, HeatmapDay, FilingDelayData } from "@/lib/types";

const QUICK_FILTERS = [
  { label: "C-Suite Buys $100K+", href: "/feed?trade_type=buy&min_value=100000&min_tier=2" },
  { label: "Tier 3 Signals", href: "/feed?min_tier=3" },
  { label: "Large Sells $1M+", href: "/feed?trade_type=sell&min_value=1000000" },
  { label: "Active Clusters", href: "/clusters?days=7" },
  { label: "High-Value Screener", href: "/screener?min_value=500000&min_tier=2" },
] as const;

async function getDashboardData() {
  try {
    const [stats, filingsResp, sentiment, heatmap, filingDelays] = await Promise.all([
      fetchAPIAuth<DashboardStats>("/dashboard/stats"),
      fetchAPIAuth<{ items: Filing[]; total: number }>("/filings", { limit: "10" }),
      fetchAPIAuth<SentimentPoint[]>("/dashboard/sentiment", { days: "30" }),
      fetchAPIAuth<HeatmapDay[]>("/dashboard/heatmap", { days: "365" }),
      fetchAPIAuth<FilingDelayData>("/dashboard/filing-delays"),
    ]);
    return { stats, filings: filingsResp.items, sentiment, heatmap, filingDelays, error: null };
  } catch {
    return {
      stats: null,
      filings: [],
      sentiment: [],
      heatmap: [],
      filingDelays: null,
      error: "Unable to connect to API. Make sure the backend is running on localhost:8000.",
    };
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

export default async function DashboardPage() {
  const { stats, filings, sentiment, heatmap, filingDelays, error } = await getDashboardData();

  return (
    <div className="space-y-6">
      {/* Error banner */}
      {error && (
        <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 px-4 py-3 text-sm text-[#F59E0B]">
          {error}
        </div>
      )}

      {/* Sync Status */}
      <div className="flex justify-end">
        <SyncStatus />
      </div>

      {/* Stat Cards */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          title="Signals Today"
          value={stats?.signals_today ?? "--"}
          subtitle="New filings detected"
          trend="neutral"
        />
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
            stats
              ? stats.buy_sell_ratio >= 1.5
                ? "Bullish bias"
                : stats.buy_sell_ratio <= 0.7
                  ? "Bearish bias"
                  : "Neutral"
              : undefined
          }
          trend={
            stats
              ? stats.buy_sell_ratio >= 1.5
                ? "up"
                : stats.buy_sell_ratio <= 0.7
                  ? "down"
                  : "neutral"
              : "neutral"
          }
        />
        <StatCard
          title="Top Mover"
          value={
            stats?.top_mover
              ? `${stats.top_mover.ticker} ${formatCurrency(stats.top_mover.value)}`
              : "--"
          }
          subtitle="Highest value filing today"
          trend="neutral"
        />
      </div>

      {/* Quick Filters */}
      <div className="flex flex-wrap items-center gap-2">
        {QUICK_FILTERS.map((filter) => (
          <Link
            key={filter.href}
            href={filter.href}
            className="rounded-md border border-[#2A2A3A] bg-[#12121A] px-3 py-1.5 text-xs font-medium text-[#8888A0] transition-colors hover:text-[#E8E8ED] hover:border-[#3B82F6]/50 hover:bg-[#3B82F6]/10"
          >
            {filter.label}
          </Link>
        ))}
      </div>

      {/* Activity Heatmap */}
      {heatmap.length > 0 && (
        <Card className="bg-[#12121A] border-[#2A2A3A]">
          <CardHeader>
            <CardTitle className="text-sm font-medium text-[#8888A0]">
              Filing Activity (1 year)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <CalendarHeatmap data={heatmap} days={365} />
            </div>
          </CardContent>
        </Card>
      )}

      {/* Dashboard sections: Convergence, Activity Spikes, Sell Cessation */}
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

      {/* Main content: Table + Chart */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-5">
        {/* Recent Signals Table */}
        <Card className="bg-[#12121A] border-[#2A2A3A] lg:col-span-3">
          <CardHeader>
            <CardTitle className="text-sm font-medium text-[#8888A0]">
              Recent High-Confidence Signals
            </CardTitle>
          </CardHeader>
          <CardContent>
            <SignalsTable filings={filings} />
          </CardContent>
        </Card>

        {/* Sentiment Chart + Filing Delays */}
        <div className="lg:col-span-2 space-y-6">
          <SentimentChart data={sentiment} />
          {filingDelays && <FilingDelayChart data={filingDelays} />}
        </div>
      </div>
    </div>
  );
}
