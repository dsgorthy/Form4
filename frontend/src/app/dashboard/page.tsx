export const dynamic = "force-dynamic";

export const metadata = {
  title: "Form4 — Live Insider Trading Strategies",
  description:
    "Three live insider-trading strategies on real paper accounts. Every trade is public, research-backed, and fully transparent.",
};

import { auth } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import Link from "next/link";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency } from "@/lib/format";
import { SignalsTable } from "@/components/signals-table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { Filing } from "@/lib/types";

interface StrategySnapshot {
  name: string;
  label: string;
  starting_capital: number;
  current_equity?: number;
  total_pnl?: number;
  total_pnl_pct?: number;
  day_change?: number;
  day_change_pct?: number;
  position_count?: number;
  deviation_status?: string;
  backtest?: { cagr: number; sharpe: number; win_rate: number; max_dd: number };
  error?: string;
}

interface PaperDashboard {
  as_of: string;
  strategies: StrategySnapshot[];
}

const STRATEGY_META: Record<string, { brief: string; experimental?: boolean }> = {
  quality_momentum: { brief: "A+/A insiders buying in uptrends" },
  reversal_dip: { brief: "Persistent sellers reversing into deep dips" },
  tenb51_surprise: { brief: "10b5-1 plan sellers breaking pattern to buy", experimental: true },
};

async function getDashboardData() {
  const [paperResult, filingsResult] = await Promise.allSettled([
    fetchAPIAuth<PaperDashboard>("/paper-trading/dashboard"),
    fetchAPIAuth<{ items: Filing[]; total: number }>("/filings", { limit: "8", min_grade: "B" }),
  ]);

  const paper = paperResult.status === "fulfilled" ? paperResult.value : null;
  const filings = filingsResult.status === "fulfilled" ? filingsResult.value.items : [];

  const errors: string[] = [];
  if (paperResult.status === "rejected") errors.push(`Strategies: ${paperResult.reason}`);
  if (filingsResult.status === "rejected") errors.push(`Filings: ${filingsResult.reason}`);

  return { paper, filings, error: errors.length > 0 ? errors.join(". ") : null };
}

function StrategyCard({ s }: { s: StrategySnapshot }) {
  const meta = STRATEGY_META[s.name] || { brief: "" };
  const pnl = s.total_pnl ?? 0;
  const pnlPct = s.total_pnl_pct ?? 0;
  const dayChg = s.day_change ?? 0;
  const dayPct = s.day_change_pct ?? 0;
  const eq = s.current_equity ?? s.starting_capital;

  const statusColor =
    s.deviation_status === "on_track" ? "text-[#22C55E]" :
    s.deviation_status === "below" ? "text-[#F59E0B]" :
    s.deviation_status === "well_below" ? "text-[#EF4444]" : "text-[#8888A0]";

  return (
    <Link href={`/portfolio?strategy=${s.name}`} className="block group">
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5 transition-colors group-hover:border-[#3B82F6]/40">
        <div className="flex items-center justify-between mb-3">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-sm font-semibold text-[#E8E8ED]">{s.label}</span>
              {meta.experimental && (
                <span className="rounded-full bg-[#F59E0B]/15 px-2 py-0.5 text-[9px] font-semibold text-[#F59E0B]">Experimental</span>
              )}
            </div>
            <div className="text-xs text-[#55556A] mt-0.5">{meta.brief}</div>
          </div>
          <div className={`text-xs font-medium ${statusColor}`}>
            {s.position_count ?? 0} open
          </div>
        </div>

        <div className="grid grid-cols-3 gap-3">
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">Equity</div>
            <div className="text-lg font-mono font-bold text-[#E8E8ED]">{formatCurrency(eq)}</div>
          </div>
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">Total P&L</div>
            <div className={`text-lg font-mono font-bold ${pnl >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
              {pnl >= 0 ? "+" : ""}{formatCurrency(pnl)}
            </div>
            <div className="text-xs text-[#55556A] font-mono">{pnlPct >= 0 ? "+" : ""}{pnlPct.toFixed(1)}%</div>
          </div>
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">Today</div>
            <div className={`text-lg font-mono font-bold ${dayChg >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
              {dayChg >= 0 ? "+" : ""}{formatCurrency(dayChg)}
            </div>
            <div className="text-xs text-[#55556A] font-mono">{dayPct >= 0 ? "+" : ""}{dayPct.toFixed(2)}%</div>
          </div>
        </div>

        {s.backtest && (
          <div className="flex gap-4 mt-3 pt-3 border-t border-[#2A2A3A]/50 text-[10px] text-[#55556A] font-mono">
            <span>Sharpe {s.backtest.sharpe}</span>
            <span>WR {s.backtest.win_rate}%</span>
            <span>CAGR {s.backtest.cagr}%</span>
            <span>MaxDD {s.backtest.max_dd}%</span>
          </div>
        )}
      </div>
    </Link>
  );
}

export default async function DashboardPage() {
  const { userId } = await auth();
  if (!userId) redirect("/");

  const { paper, filings, error } = await getDashboardData();

  return (
    <div className="space-y-6">
      {error && (
        <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 px-4 py-3 text-sm text-[#F59E0B]">
          {error}
        </div>
      )}

      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#E8E8ED]">Live Strategies</h1>
          <p className="text-sm text-[#55556A] mt-1">
            Three insider-trading strategies on real paper accounts. Every trade is public.
          </p>
        </div>
        {paper && (
          <div className="text-xs text-[#55556A] font-mono">
            Updated {new Date(paper.as_of).toLocaleTimeString()}
          </div>
        )}
      </div>

      {/* Strategy cards */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {paper?.strategies.map((s) => (
          <StrategyCard key={s.name} s={s} />
        )) ?? (
          <div className="lg:col-span-3 text-sm text-[#55556A]">Loading strategies...</div>
        )}
      </div>

      {/* Today's notable trades */}
      <Card className="bg-[#12121A] border-[#2A2A3A]">
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm font-medium text-[#8888A0]">Recent Notable Insider Trades</CardTitle>
            <Link href="/feed" className="text-xs text-[#3B82F6] hover:underline">View all</Link>
          </div>
        </CardHeader>
        <CardContent>
          <SignalsTable filings={filings} />
        </CardContent>
      </Card>

      {/* Quick links */}
      <div className="flex flex-wrap items-center gap-2">
        <Link href="/research/market-overview" className="rounded-md border border-[#2A2A3A] bg-[#12121A] px-3 py-1.5 text-xs font-medium text-[#8888A0] transition-colors hover:text-[#E8E8ED] hover:border-[#3B82F6]/50 hover:bg-[#3B82F6]/10">
          Market Overview
        </Link>
        <Link href="/clusters?days=7" className="rounded-md border border-[#2A2A3A] bg-[#12121A] px-3 py-1.5 text-xs font-medium text-[#8888A0] transition-colors hover:text-[#E8E8ED] hover:border-[#3B82F6]/50 hover:bg-[#3B82F6]/10">
          Active Clusters
        </Link>
        <Link href="/scoring" className="rounded-md border border-[#2A2A3A] bg-[#12121A] px-3 py-1.5 text-xs font-medium text-[#8888A0] transition-colors hover:text-[#E8E8ED] hover:border-[#3B82F6]/50 hover:bg-[#3B82F6]/10">
          Scoring Methodology
        </Link>
        <Link href="/feed?trade_type=buy&min_grade=A" className="rounded-md border border-[#2A2A3A] bg-[#12121A] px-3 py-1.5 text-xs font-medium text-[#8888A0] transition-colors hover:text-[#E8E8ED] hover:border-[#3B82F6]/50 hover:bg-[#3B82F6]/10">
          A+ Grade Buys Today
        </Link>
      </div>
    </div>
  );
}
