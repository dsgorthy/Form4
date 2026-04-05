export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { fetchAPIAuth } from "@/lib/auth";
import { ProGate } from "@/components/pro-gate";
import { formatCurrency } from "@/lib/format";
import { InsiderTradeChart } from "@/components/insider-trade-chart";
import { WatchButton } from "@/components/watch-button";
import { TradesTable } from "@/components/trades-table";
import { CongressTable } from "@/components/congress-table";
import { InsiderRoster } from "@/components/insider-roster";
import type { Filing, PaginatedResponse } from "@/lib/types";

interface CompanyOverview {
  company: string;
  ticker: string;
  total_trades: number;
  total_value: number;
  first_trade: string;
  last_trade: string;
  insiders: {
    insider_id: string;
    name: string;
    cik: string;
    title: string;
    normalized_title: string | null;
    is_entity?: number;
    controlled_by?: { insider_id: string; name: string };
    trade_count: number;
    total_value: number;
    first_trade: string;
    last_trade: string;
    score: number | null;
    score_tier: number | null;
    percentile: number | null;
  }[];
}

export async function generateMetadata({ params }: { params: Promise<{ ticker: string }> }): Promise<Metadata> {
  const { ticker } = await params;
  try {
    const overview = await fetchAPI<CompanyOverview>(`/companies/${ticker}`);
    const insiderCount = overview.insiders.length;
    const title = `${overview.ticker} Insider Trading — ${overview.company}`;
    const description = `Track insider trades at ${overview.company} (${overview.ticker}). ${overview.total_trades} trades by ${insiderCount} insiders. Real-time SEC Form 4 analysis on Form4.app.`;
    return {
      title,
      description,
      openGraph: { title, description },
    };
  } catch {
    return { title: `${ticker} — Insider Trades` };
  }
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
      {children}
    </div>
  );
}

const TRADES_LIMIT = 25;
const CONGRESS_LIMIT = 10;

export default async function CompanyPage({ params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;

  let overview: CompanyOverview;
  let trades: PaginatedResponse<Filing>;

  try {
    [overview, trades] = await Promise.all([
      fetchAPIAuth<CompanyOverview>(`/companies/${ticker}`),
      fetchAPIAuth<PaginatedResponse<Filing>>(`/companies/${ticker}/trades`, { limit: String(TRADES_LIMIT) }),
    ]);
  } catch {
    notFound();
  }

  // Fetch congress trades (non-blocking — page still renders if this fails)
  let congressData: { ticker: string; trades: unknown[]; total: number; limit: number; offset: number } | null = null;
  try {
    congressData = await fetchAPIAuth<{
      ticker: string;
      trades: unknown[];
      total: number;
      limit: number;
      offset: number;
    }>(`/congress/by-ticker/${ticker}`, { limit: String(CONGRESS_LIMIT) });
  } catch {}

  return (
    <div>
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span>Company</span>
        <span>/</span>
        <span className="text-[#E8E8ED]">{overview.ticker}</span>
      </nav>

      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl md:text-3xl font-bold font-mono text-[#E8E8ED]">{overview.ticker}</h1>
          <WatchButton ticker={overview.ticker} />
        </div>
        <p className="text-[#8888A0] mt-1">{overview.company}</p>
        <div className="flex flex-wrap items-center gap-2 md:gap-4 mt-2 text-xs text-[#55556A]">
          <span>{overview.total_trades} total trades</span>
          <span>{formatCurrency(overview.total_value)} total value</span>
          <span>
            {overview.first_trade} to {overview.last_trade}
          </span>
        </div>
      </div>

      {/* Convergence Banner */}
      {congressData && congressData.total > 0 && (
        <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 px-4 py-3 text-sm mb-8">
          <span className="font-medium text-[#F59E0B]">Convergence Signal:</span>
          <span className="text-[#E8E8ED] ml-2">
            {congressData.total} politician trade{congressData.total !== 1 ? "s" : ""} detected
            for {ticker} alongside insider activity
          </span>
        </div>
      )}

      {/* Trade Scatter Chart */}
      <div className="mb-8">
        <InsiderTradeChart ticker={overview.ticker} />
      </div>

      {/* Insider Roster */}
      <div className="mb-8">
        <SectionLabel>Insider Roster ({overview.insiders.length})</SectionLabel>
        <ProGate label="Insider Track Records">
          <InsiderRoster insiders={overview.insiders} />
        </ProGate>
      </div>

      {/* Political Activity (Congress) — paginated */}
      {congressData && (
        <CongressTable
          ticker={ticker}
          initialData={congressData as never}
        />
      )}

      {/* All Trades — paginated */}
      <TradesTable ticker={ticker} initialData={trades} />
    </div>
  );
}
