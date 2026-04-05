export const dynamic = "force-dynamic";

export const metadata = {
  title: "Insider Cluster Detection",
  description:
    "Find stocks where multiple insiders are buying or selling together. Real-time cluster detection across all SEC Form 4 filings.",
};

import Link from "next/link";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency, formatPercent } from "@/lib/format";
import { TierBadge } from "@/components/ui/tier-badge";
import { Badge } from "@/components/ui/badge";
import { TickerDisplay } from "@/components/ui/ticker-display";
import { ClusterTimeline } from "@/components/cluster-timeline";
import { ProGate } from "@/components/pro-gate";

interface ClusterInsider {
  insider_id: string;
  name: string;
  cik: string;
  score: number | null;
  score_tier: number | null;
  trade_value: number;
  title: string;
  is_csuite: number;
  last_trade_date: string;
}

interface Cluster {
  ticker: string;
  trade_type: string;
  company: string;
  insider_count: number;
  total_value: number;
  first_trade: string;
  last_trade: string;
  latest_filing: string;
  trade_count: number;
  csuite_count: number;
  avg_score: number | null;
  insiders: ClusterInsider[];
}

interface ClusterResponse {
  total: number;
  limit: number;
  offset: number;
  items: Cluster[];
}

interface Props {
  searchParams: Promise<{
    days?: string;
    trade_type?: string;
    min_value?: string;
    page?: string;
  }>;
}

const PAGE_SIZE = 30;

export default async function ClustersPage({ searchParams }: Props) {
  const sp = await searchParams;
  const days = sp.days || "14";
  const tradeType = sp.trade_type || "";
  const minValue = sp.min_value || "";
  const page = parseInt(sp.page || "1", 10);
  const offset = (page - 1) * PAGE_SIZE;

  const params: Record<string, string> = {
    days,
    limit: String(PAGE_SIZE),
    offset: String(offset),
  };
  if (tradeType) params.trade_type = tradeType;
  if (minValue) params.min_value = minValue;

  const data = await fetchAPIAuth<ClusterResponse & { gated?: boolean }>("/clusters", params);
  const isGated = (data as any).gated === true;

  const totalPages = Math.ceil(data.total / PAGE_SIZE);

  function buildUrl(overrides: Record<string, string>) {
    const merged = { days, trade_type: tradeType, min_value: minValue, ...overrides };
    const sp = new URLSearchParams();
    Object.entries(merged).forEach(([k, v]) => {
      if (v) sp.set(k, v);
    });
    return `/clusters?${sp.toString()}`;
  }

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#E8E8ED]">Clusters</h1>
          <p className="text-sm text-[#55556A] mt-1">
            {data.total} active clusters ({days}d window) &middot; Multiple insiders trading the same stock
          </p>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-2 md:gap-3">
          {/* Window */}
          {["7", "14", "30"].map((d) => (
            <Link
              key={d}
              href={buildUrl({ days: d, page: "1" })}
              className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
                days === d
                  ? "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]"
                  : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
              }`}
            >
              {d}d
            </Link>
          ))}

          <span className="w-px h-5 bg-[#2A2A3A]" />

          {/* Trade type */}
          {["buy", "sell"].map((type) => (
            <Link
              key={type}
              href={buildUrl({
                trade_type: tradeType === type ? "" : type,
                page: "1",
              })}
              className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
                tradeType === type
                  ? type === "buy"
                    ? "border-[#22C55E]/50 bg-[#22C55E]/10 text-[#22C55E]"
                    : "border-[#EF4444]/50 bg-[#EF4444]/10 text-[#EF4444]"
                  : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
              }`}
            >
              {type === "buy" ? "Buys" : "Sells"}
            </Link>
          ))}

          <span className="w-px h-5 bg-[#2A2A3A]" />

          {/* Min value */}
          {["100000", "1000000"].map((val) => {
            const label = parseInt(val) >= 1000000 ? "$1M+" : "$100K+";
            return (
              <Link
                key={val}
                href={buildUrl({
                  min_value: minValue === val ? "" : val,
                  page: "1",
                })}
                className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
                  minValue === val
                    ? "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]"
                    : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
                }`}
              >
                {label}
              </Link>
            );
          })}

          {(tradeType || minValue || days !== "14") && (
            <Link
              href="/clusters"
              className="text-xs text-[#55556A] hover:text-[#8888A0]"
            >
              Reset
            </Link>
          )}
        </div>
      </div>

      {/* Cluster cards */}
      <div className="space-y-4">
        {data.items.map((cluster) => (
          <div
            key={`${cluster.ticker}-${cluster.trade_type}`}
            className="rounded-lg border border-[#2A2A3A] bg-[#12121A] overflow-hidden"
          >
            {/* Cluster header */}
            <div className="p-4 md:p-5 border-b border-[#2A2A3A]/50 space-y-3 md:space-y-0">
              <div className="flex items-center gap-3 flex-wrap">
                <TickerDisplay ticker={cluster.ticker} company={cluster.company} className="text-xl font-bold" />
                <Badge
                  variant="outline"
                  className={`text-xs font-mono ${
                    cluster.trade_type === "buy"
                      ? "border-[#22C55E]/30 text-[#22C55E] bg-[#22C55E]/10"
                      : "border-[#EF4444]/30 text-[#EF4444] bg-[#EF4444]/10"
                  }`}
                >
                  {cluster.trade_type.toUpperCase()} CLUSTER
                </Badge>
                <span className="text-sm text-[#8888A0] truncate">{cluster.company}</span>
              </div>

              <div className="flex items-center gap-4 md:gap-6 text-sm flex-wrap md:justify-end">
                <div className="text-left md:text-right">
                  <div className="text-[10px] uppercase tracking-wider text-[#55556A]">Insiders</div>
                  <div className="font-mono font-bold text-[#E8E8ED]">{cluster.insider_count}</div>
                </div>
                <div className="text-left md:text-right">
                  <div className="text-[10px] uppercase tracking-wider text-[#55556A]">Total Value</div>
                  <div className="font-mono font-bold text-[#E8E8ED]">{formatCurrency(cluster.total_value)}</div>
                </div>
                <div className="text-left md:text-right">
                  <div className="text-[10px] uppercase tracking-wider text-[#55556A]">C-Suite</div>
                  <div className="font-mono font-bold text-[#E8E8ED]">{cluster.csuite_count}</div>
                </div>
                <div className="text-left md:text-right">
                  <div className="text-[10px] uppercase tracking-wider text-[#55556A]">Window</div>
                  <div className="text-xs text-[#8888A0]">
                    {cluster.first_trade === cluster.last_trade
                      ? cluster.first_trade
                      : `${cluster.first_trade} to ${cluster.last_trade}`}
                  </div>
                </div>
              </div>
            </div>

            {/* Insiders grid */}
            <div className="p-4">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
                {cluster.insiders.map((ins) => {
                  const InsWrapper = isGated ? "div" : Link;
                  const insProps = isGated ? {} : { href: `/insider/${ins.cik || ins.insider_id}` };
                  return (
                    <InsWrapper
                      key={ins.insider_id}
                      {...(insProps as any)}
                      className={`flex items-center gap-3 rounded-md border border-[#2A2A3A]/50 bg-[#1A1A26]/30 px-3 py-2.5 transition-colors ${isGated ? "select-none" : "hover:bg-[#2A2A3A]/30"}`}
                    >
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className={`text-sm truncate ${isGated ? "text-blue-400/40 blur-[3px]" : "text-blue-400"}`}>{ins.name}</span>
                          {(ins as any).pit_grade != null || ins.score_tier != null ? <TierBadge pitGrade={(ins as any).pit_grade} tier={ins.score_tier} /> : null}
                        </div>
                        <div className={`text-xs truncate mt-0.5 ${isGated ? "text-[#55556A]/40 blur-[3px]" : "text-[#55556A]"}`}>
                          {ins.title}
                          {ins.is_csuite ? " (C-Suite)" : ""}
                        </div>
                      </div>
                      <div className="text-right shrink-0">
                        <div className={`font-mono text-xs ${isGated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>
                          {formatCurrency(ins.trade_value)}
                        </div>
                        <div className={`text-[10px] ${isGated ? "text-[#55556A]/40 blur-[3px]" : "text-[#55556A]"}`}>
                          {ins.last_trade_date}
                        </div>
                      </div>
                    </InsWrapper>
                  );
                })}
              </div>

              {/* Timeline */}
              {cluster.insiders.length >= 2 && (
                <ClusterTimeline
                  insiders={cluster.insiders}
                  firstTrade={cluster.first_trade}
                  lastTrade={cluster.last_trade}
                />
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Empty state */}
      {data.items.length === 0 && (
        <div className="flex flex-col items-center justify-center h-40 rounded-lg border border-[#2A2A3A] text-[#55556A]">
          <div className="text-lg mb-1">No clusters detected</div>
          <div className="text-xs">Try widening the time window or removing filters</div>
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-6">
          <p className="text-sm text-[#55556A]">
            Showing {offset + 1}–{Math.min(offset + PAGE_SIZE, data.total)} of {data.total}
          </p>
          <div className="flex items-center gap-2">
            {page > 1 && (
              <Link
                href={buildUrl({ page: String(page - 1) })}
                className="rounded-md border border-[#2A2A3A] px-3 py-1.5 text-sm text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
              >
                Previous
              </Link>
            )}
            <span className="text-sm text-[#8888A0] px-2">
              Page {page} of {totalPages}
            </span>
            {page < totalPages && (
              <Link
                href={buildUrl({ page: String(page + 1) })}
                className="rounded-md border border-[#2A2A3A] px-3 py-1.5 text-sm text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
              >
                Next
              </Link>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
