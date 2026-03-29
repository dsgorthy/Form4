export const dynamic = "force-dynamic";

import Link from "next/link";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency } from "@/lib/format";
import { InsiderTradeChart } from "@/components/insider-trade-chart";
import { TickerInput } from "@/components/ticker-input";
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

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
      {children}
    </div>
  );
}

interface Props {
  searchParams: Promise<{
    ticker?: string;
  }>;
}

const TRADES_LIMIT = 25;
const CONGRESS_LIMIT = 10;

export default async function ScreenerPage({ searchParams }: Props) {
  const sp = await searchParams;
  const ticker = sp.ticker?.toUpperCase() || "AAPL";

  let overview: CompanyOverview | null = null;
  let trades: PaginatedResponse<Filing> | null = null;
  let congressData: { ticker: string; trades: unknown[]; total: number; limit: number; offset: number } | null = null;
  let error: string | null = null;

  if (ticker) {
    try {
      [overview, trades] = await Promise.all([
        fetchAPIAuth<CompanyOverview>(`/companies/${ticker}`),
        fetchAPIAuth<PaginatedResponse<Filing>>(`/companies/${ticker}/trades`, {
          limit: String(TRADES_LIMIT),
        }),
      ]);
    } catch {
      error = `No data found for "${ticker}"`;
    }

    if (overview) {
      try {
        congressData = await fetchAPIAuth<{
          ticker: string;
          trades: unknown[];
          total: number;
          limit: number;
          offset: number;
        }>(`/congress/by-ticker/${ticker}`, { limit: String(CONGRESS_LIMIT) });
      } catch {}
    }
  }

  return (
    <div>
      {/* Header — when no company loaded, show full intro + centered search */}
      {!overview && (
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-[#E8E8ED] mb-1">Screener</h1>
          <p className="text-sm text-[#55556A] mb-4">
            Look up any ticker to see insider activity, trade history, and political overlap
          </p>
          <TickerInput />
        </div>
      )}

      {/* Error state */}
      {error && (
        <div className="flex flex-col items-center justify-center h-48 rounded-lg border border-[#2A2A3A] bg-[#12121A]">
          <div className="text-lg text-[#8888A0] mb-2">{error}</div>
          <p className="text-xs text-[#55556A]">Check the ticker symbol and try again</p>
        </div>
      )}

      {/* Company content */}
      {overview && trades && (
        <div>
          {/* Company header + inline search */}
          <div className="flex flex-col gap-4 mb-8 md:flex-row md:items-start md:justify-between md:gap-6">
            <div>
              <div className="flex items-center gap-3">
                <h2 className="text-2xl md:text-3xl font-bold font-mono text-[#E8E8ED]">
                  {overview.ticker}
                </h2>
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
            <div className="shrink-0">
              <TickerInput />
            </div>
          </div>

          {/* Convergence Banner */}
          {congressData && congressData.total > 0 && (
            <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 px-4 py-3 text-sm mb-8">
              <span className="font-medium text-[#F59E0B]">
                Convergence Signal:
              </span>
              <span className="text-[#E8E8ED] ml-2">
                {congressData.total} politician trade
                {congressData.total !== 1 ? "s" : ""} detected for{" "}
                {ticker} alongside insider activity
              </span>
            </div>
          )}

          {/* Trade Scatter Chart */}
          <div className="mb-8">
            <InsiderTradeChart ticker={overview.ticker} />
          </div>

          {/* Insider Roster */}
          <div className="mb-8">
            <SectionLabel>
              Insider Roster ({overview.insiders.length})
            </SectionLabel>
            <InsiderRoster insiders={overview.insiders} gated />
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
      )}
    </div>
  );
}
