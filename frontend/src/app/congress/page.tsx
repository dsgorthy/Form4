export const dynamic = "force-dynamic";

export const metadata = {
  title: "Congressional Trading Tracker",
  description:
    "Track stock trades by members of Congress. Compare congressional trading activity with corporate insider signals.",
};

import Link from "next/link";
import { fetchAPI } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { CongressAnalytics } from "@/components/congress-analytics";
import type { HeatmapDay } from "@/lib/types";

interface CongressTrade {
  congress_trade_id: number;
  politician_name: string;
  party: string;
  state: string;
  chamber: string;
  ticker: string;
  company: string;
  trade_type: string;
  trade_date: string;
  filing_date: string;
  value_low: number | null;
  value_high: number | null;
  value_estimate: number | null;
  owner: string;
  asset_type: string;
}

interface PaginatedResponse<T> {
  items: T[];
  total: number;
}

interface Props {
  searchParams: Promise<{
    chamber?: string;
    trade_type?: string;
    min_value?: string;
    ticker?: string;
    page?: string;
  }>;
}

const PAGE_SIZE = 50;

function formatValueRange(low: number | null, high: number | null): string {
  if (!low && !high) return "\u2014";
  const fmt = (v: number) =>
    v >= 1_000_000
      ? `$${(v / 1_000_000).toFixed(1)}M`
      : v >= 1_000
      ? `$${(v / 1_000).toFixed(0)}K`
      : `$${v}`;
  if (low && high) return `${fmt(low)}-${fmt(high)}`;
  return fmt(low || high!);
}

const PARTY_STYLES: Record<string, string> = {
  D: "border-[#3B82F6]/30 text-[#3B82F6] bg-[#3B82F6]/10",
  R: "border-[#EF4444]/30 text-[#EF4444] bg-[#EF4444]/10",
  I: "border-[#8888A0]/30 text-[#8888A0] bg-[#8888A0]/10",
};

export default async function CongressPage({ searchParams }: Props) {
  const sp = await searchParams;
  const chamber = sp.chamber || "";
  const tradeType = sp.trade_type || "";
  const minValue = sp.min_value || "";
  const ticker = sp.ticker || "";
  const page = parseInt(sp.page || "1", 10);
  const offset = (page - 1) * PAGE_SIZE;

  const params: Record<string, string> = {
    limit: String(PAGE_SIZE),
    offset: String(offset),
  };
  if (chamber) params.chamber = chamber;
  if (tradeType) params.trade_type = tradeType;
  if (minValue) params.min_value = minValue;
  if (ticker) params.ticker = ticker;

  const [data, analytics] = await Promise.all([
    fetchAPI<PaginatedResponse<CongressTrade>>("/congress/trades", params),
    fetchAPI<{
      summary: {
        total_trades: number;
        total_value: number;
        buys: number;
        sells: number;
        buy_value: number;
        sell_value: number;
        active_politicians: number;
        unique_tickers: number;
        avg_filing_delay: number | null;
      };
      heatmap: HeatmapDay[];
      top_tickers: {
        ticker: string;
        trade_count: number;
        total_value: number;
        buys: number;
        sells: number;
        politicians: number;
      }[];
      top_politicians: {
        name: string;
        party: string;
        chamber: string;
        trade_count: number;
        total_value: number;
        buys: number;
        sells: number;
      }[];
    }>("/congress/analytics", { days: "365" }),
  ]);
  const totalPages = Math.ceil(data.total / PAGE_SIZE);

  const hasFilters = !!(chamber || tradeType || minValue || ticker);

  function buildUrl(overrides: Record<string, string>) {
    const merged = {
      chamber,
      trade_type: tradeType,
      min_value: minValue,
      ticker,
      ...overrides,
    };
    const qs = new URLSearchParams();
    Object.entries(merged).forEach(([k, v]) => {
      if (v) qs.set(k, v);
    });
    return `/congress?${qs.toString()}`;
  }

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#E8E8ED]">
            Congress Trades
          </h1>
          <p className="text-sm text-[#55556A] mt-1">
            {data.total.toLocaleString()} trades
            <span className="ml-2 text-[#55556A]/60">· Data delayed ~1 day from STOCK Act filings</span>
          </p>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-2 md:gap-3">
          {/* Chamber */}
          {(["house", "senate"] as const).map((ch) => (
            <Link
              key={ch}
              href={buildUrl({
                chamber: chamber === ch ? "" : ch,
                page: "1",
              })}
              className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
                chamber === ch
                  ? "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]"
                  : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
              }`}
            >
              {ch === "house" ? "House" : "Senate"}
            </Link>
          ))}

          {/* Trade type */}
          {(["buy", "sell"] as const).map((type) => (
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

          {/* Min value */}
          {(["100000", "1000000"] as const).map((val) => (
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
              {val === "100000" ? "$100K+" : "$1M+"}
            </Link>
          ))}

          {/* Ticker search */}
          <form action="/congress" method="get">
            {chamber && <input type="hidden" name="chamber" value={chamber} />}
            {tradeType && (
              <input type="hidden" name="trade_type" value={tradeType} />
            )}
            {minValue && (
              <input type="hidden" name="min_value" value={minValue} />
            )}
            <input
              type="text"
              name="ticker"
              defaultValue={ticker}
              placeholder="Ticker..."
              className="w-20 rounded-md border border-[#2A2A3A] bg-[#12121A] px-2 py-1.5 text-xs font-mono text-[#E8E8ED] placeholder-[#55556A] focus:border-[#3B82F6] focus:outline-none"
            />
          </form>

          {/* Reset */}
          {hasFilters && (
            <Link
              href="/congress"
              className="text-xs text-[#55556A] hover:text-[#8888A0]"
            >
              Clear
            </Link>
          )}
        </div>
      </div>

      {/* Analytics */}
      <CongressAnalytics
        summary={analytics.summary}
        heatmap={analytics.heatmap}
        topTickers={analytics.top_tickers}
        topPoliticians={analytics.top_politicians}
      />

      {/* Ticker filter badge */}
      {ticker && (
        <div className="mb-4 flex items-center gap-2">
          <span className="text-xs text-[#55556A]">Filtered to:</span>
          <span className="rounded-md bg-[#1A1A26] border border-[#2A2A3A] px-2 py-1 text-xs font-mono text-[#E8E8ED]">
            {ticker}
          </span>
          <Link
            href={buildUrl({ ticker: "", page: "1" })}
            className="text-xs text-[#55556A] hover:text-[#8888A0]"
          >
            Remove
          </Link>
        </div>
      )}

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
              <th className="px-3 md:px-4 py-3 text-left text-[#55556A] font-medium">
                Politician
              </th>
              <th className="px-3 md:px-4 py-3 text-left text-[#55556A] font-medium">
                Ticker
              </th>
              <th className="px-3 md:px-4 py-3 text-center text-[#55556A] font-medium">
                Type
              </th>
              <th className="hidden md:table-cell px-4 py-3 text-left text-[#55556A] font-medium">
                Trade Date
              </th>
              <th className="px-3 md:px-4 py-3 text-right text-[#55556A] font-medium">
                Value
              </th>
              <th className="hidden md:table-cell px-4 py-3 text-left text-[#55556A] font-medium">
                Owner
              </th>
              <th className="hidden md:table-cell px-4 py-3 text-left text-[#55556A] font-medium">
                Filed
              </th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((trade) => (
              <tr
                key={trade.congress_trade_id}
                className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30 transition-colors"
              >
                {/* Politician */}
                <td className="px-3 md:px-4 py-3">
                  <div className="flex items-center gap-2">
                    <span className="text-[#E8E8ED] font-medium">
                      {trade.politician_name}
                    </span>
                    {trade.party ? (
                      <Badge
                        variant="outline"
                        className={`text-[10px] font-mono ${
                          PARTY_STYLES[trade.party] || PARTY_STYLES.I
                        }`}
                      >
                        {trade.party}
                      </Badge>
                    ) : (
                      <span className="text-[10px] text-[#55556A]">{"\u2014"}</span>
                    )}
                    <span className="hidden md:inline text-[10px] text-[#55556A]">
                      {trade.state}
                    </span>
                  </div>
                </td>

                {/* Ticker */}
                <td className="px-3 md:px-4 py-3">
                  <Link
                    href={`/company/${trade.ticker}`}
                    className="font-mono font-bold text-blue-400 hover:text-blue-300"
                  >
                    {trade.ticker}
                  </Link>
                </td>

                {/* Type */}
                <td className="px-3 md:px-4 py-3 text-center">
                  <Badge
                    variant="outline"
                    className={`text-[10px] font-mono ${
                      trade.trade_type === "buy"
                        ? "border-[#22C55E]/30 text-[#22C55E]"
                        : "border-[#EF4444]/30 text-[#EF4444]"
                    }`}
                  >
                    {trade.trade_type.toUpperCase()}
                  </Badge>
                </td>

                {/* Date */}
                <td className="hidden md:table-cell px-4 py-3 text-xs text-[#E8E8ED]">
                  {trade.trade_date}
                </td>

                {/* Value */}
                <td className="px-3 md:px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  {formatValueRange(trade.value_low, trade.value_high)}
                </td>

                {/* Owner */}
                <td className="hidden md:table-cell px-4 py-3 text-xs text-[#8888A0]">
                  {trade.owner || "\u2014"}
                </td>

                {/* Filed */}
                <td className="hidden md:table-cell px-4 py-3 text-xs text-[#E8E8ED]">
                  {trade.filing_date}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Empty state */}
      {data.items.length === 0 && (
        <div className="flex h-40 items-center justify-center text-[#55556A] rounded-lg border border-[#2A2A3A] mt-4">
          No trades match your filters
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-6">
          <p className="text-sm text-[#55556A]">
            Showing {offset + 1}–{Math.min(offset + PAGE_SIZE, data.total)} of{" "}
            {data.total.toLocaleString()}
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
