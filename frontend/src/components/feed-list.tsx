"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { formatCurrency, formatPercent, isReturnUnavailable, unavailableReason } from "@/lib/format";
import { RelativeTime } from "@/components/relative-time";
import { TierBadge } from "@/components/ui/tier-badge";
import { Badge } from "@/components/ui/badge";
import { TickerDisplay } from "@/components/ui/ticker-display";
import { SignalBadges } from "@/components/signal-badge";
import { SignalQualityBadge } from "@/components/signal-quality-badge";
import { ContextFacts } from "@/components/context-facts";
import { Pagination } from "@/components/pagination";
import type { Filing, PaginatedResponse } from "@/lib/types";

const apiBase =
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
const PAGE_SIZE = 50;

import { FeedFilters, EMPTY_FILTERS } from "@/components/feed-filters";
import type { FeedFilterState } from "@/components/feed-filters";

interface FeedListProps {
  initialTicker?: string;
}

function FeedCardSkeleton() {
  return (
    <div className="flex items-center gap-4 rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4 animate-pulse">
      <div className="w-24 shrink-0">
        <div className="h-5 w-14 bg-[#1A1A26] rounded" />
        <div className="h-4 w-10 bg-[#1A1A26] rounded mt-1" />
      </div>
      <div className="flex-1 min-w-0 space-y-2">
        <div className="h-4 w-40 bg-[#1A1A26] rounded" />
        <div className="h-3 w-56 bg-[#1A1A26] rounded" />
      </div>
      <div className="w-20 shrink-0 space-y-1">
        <div className="h-4 w-16 bg-[#1A1A26] rounded ml-auto" />
        <div className="h-3 w-12 bg-[#1A1A26] rounded ml-auto" />
      </div>
      <div className="hidden md:flex gap-0">
        {[1, 2, 3].map((i) => (
          <div key={i} className="w-16 space-y-1">
            <div className="h-4 w-12 bg-[#1A1A26] rounded ml-auto" />
            <div className="h-3 w-6 bg-[#1A1A26] rounded ml-auto" />
          </div>
        ))}
      </div>
      <div className="hidden md:block w-28 space-y-1">
        <div className="h-3 w-20 bg-[#1A1A26] rounded ml-auto" />
        <div className="h-3 w-16 bg-[#1A1A26] rounded ml-auto" />
      </div>
    </div>
  );
}

export function FeedList({ initialTicker = "" }: FeedListProps) {
  const { getToken } = useAuth();
  const [data, setData] = useState<PaginatedResponse<Filing> | null>(null);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState<FeedFilterState>({
    ...EMPTY_FILTERS,
    ticker: initialTicker,
  });

  const fetchPage = useCallback(
    async (newOffset: number, currentFilters: FeedFilterState) => {
      setLoading(true);
      try {
        const token = await getToken();
        const params = new URLSearchParams({
          limit: String(PAGE_SIZE),
          offset: String(newOffset),
        });
        // Map filter state to API params
        if (currentFilters.trade_type) params.set("trade_type", currentFilters.trade_type);
        if (currentFilters.min_value) params.set("min_value", currentFilters.min_value);
        if (currentFilters.min_tier) params.set("min_tier", currentFilters.min_tier);
        if (currentFilters.min_grade) params.set("min_grade", currentFilters.min_grade);
        if (currentFilters.hide_routine) {
          params.set("hide_routine", "true");
          params.set("hide_planned", "true");
        }
        if (currentFilters.ticker) params.set("ticker", currentFilters.ticker);

        const res = await fetch(`${apiBase}/filings?${params}`, {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        if (res.ok) {
          const json: PaginatedResponse<Filing> = await res.json();
          setData(json);
          setOffset(newOffset);
        }
      } catch {
        // silent
      } finally {
        setLoading(false);
      }
    },
    [getToken],
  );

  // Fetch on mount
  useEffect(() => {
    fetchPage(0, filters);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Handle filter changes
  const handleFilterChange = useCallback(
    (newFilters: FeedFilterState) => {
      setFilters(newFilters);
      setOffset(0);
      fetchPage(0, newFilters);
    },
    [fetchPage],
  );

  // Skeleton loading state
  if (!data) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 8 }).map((_, i) => (
          <FeedCardSkeleton key={i} />
        ))}
      </div>
    );
  }

  const items = data.items;

  return (
    <div>
      {/* Unified filter system */}
      <div className="mb-4">
        <FeedFilters
          filters={filters}
          onChange={handleFilterChange}
          resultCount={data?.total}
        />
      </div>

      {/* Sell signal explainer */}
      {filters.trade_type === "sell" && (
        <div className="mb-4 rounded-lg border border-[#2A2A3A]/50 bg-[#1A1A26]/30 p-3">
          <div className="text-xs text-[#8888A0]">
            <span className="text-[#22C55E] font-mono">Green</span> = stock declined after sell (good signal) · <span className="text-[#EF4444] font-mono">Red</span> = stock rose after sell
          </div>
        </div>
      )}

      <div className={`space-y-2 ${loading ? "opacity-60" : ""} transition-opacity`}>
        {items.map((f) => {
          const isRoutineSell =
            f.trade_type === "sell" &&
            (f.is_routine === 1 || f.is_10b5_1 === 1);
          const isGood7d =
            f.return_7d != null &&
            (f.trade_type === "buy" ? f.return_7d >= 0 : f.return_7d <= 0);
          const isGood30d =
            f.return_30d != null &&
            (f.trade_type === "buy"
              ? f.return_30d >= 0
              : f.return_30d <= 0);
          const isGood90d =
            f.return_90d != null &&
            (f.trade_type === "buy"
              ? f.return_90d >= 0
              : f.return_90d <= 0);
          const gated = f.gated;

          const Wrapper = gated ? "div" : Link;
          const wrapperProps = gated ? {} : { href: `/filing/${f.trade_id}` };

          return (
            <Wrapper
              key={f.trade_id}
              {...(wrapperProps as any)}
              className={`flex items-center gap-4 rounded-lg border p-4 transition-colors group ${
                gated
                  ? "border-[#2A2A3A] bg-[#12121A] select-none"
                  : isRoutineSell
                    ? "border-[#2A2A3A]/50 bg-[#12121A]/70"
                    : "border-[#2A2A3A] bg-[#12121A] hover:bg-[#1A1A26]/60"
              }`}
            >
              {/* Ticker + Type */}
              <div className="w-24 shrink-0">
                <div
                  className={`font-mono font-bold transition-colors ${
                    gated
                      ? "text-[#E8E8ED]/40 blur-[3px]"
                      : "text-[#E8E8ED] group-hover:text-blue-400"
                  }`}
                >
                  <TickerDisplay
                    ticker={f.ticker}
                    company={f.company}
                    href={null}
                  />
                </div>
                <div className="flex items-center gap-1 mt-1">
                  <Badge
                    variant="outline"
                    className={`text-[10px] font-mono ${
                      f.trade_type === "buy"
                        ? "border-[#22C55E]/30 text-[#22C55E]"
                        : isRoutineSell
                          ? "border-[#55556A]/30 text-[#55556A]"
                          : "border-[#EF4444]/30 text-[#EF4444]"
                    }`}
                  >
                    {isRoutineSell
                      ? f.is_10b5_1 === 1
                        ? "SELL \u00b7 10b5-1"
                        : "SELL \u00b7 Routine"
                      : f.trade_type.toUpperCase()}
                  </Badge>
                </div>
              </div>

              {/* Insider info */}
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span
                    className={`text-sm truncate ${
                      gated
                        ? "text-[#E8E8ED]/40 blur-[3px]"
                        : "text-[#E8E8ED]"
                    }`}
                  >
                    {f.insider_name}
                  </span>
                  {(f.score_tier ?? f.tier) != null && (
                    <TierBadge tier={f.score_tier ?? f.tier} />
                  )}
                  {(f as any).signal_quality && (
                    <SignalQualityBadge quality={(f as any).signal_quality} />
                  )}
                </div>
                <div
                  className={`text-xs truncate mt-0.5 ${
                    gated
                      ? "text-[#55556A]/40 blur-[3px]"
                      : "text-[#55556A]"
                  }`}
                >
                  {(f.normalized_title || f.title)?.replace(/;/g, ", ")} at{" "}
                  {f.company}
                </div>
                {!gated && (
                  <div className="flex items-center gap-1 mt-1 flex-wrap">
                    {(f as any).is_rare_reversal === 1 && (
                      <span className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#F59E0B]/10 text-[#F59E0B] border border-[#F59E0B]/20">
                        Rare Reversal
                      </span>
                    )}
                    {(f as any).week52_proximity != null && (f as any).week52_proximity >= 0.8 && f.trade_type === "buy" && (
                      <span className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#22C55E]/10 text-[#22C55E] border border-[#22C55E]/20">
                        Near 52w High
                      </span>
                    )}
                    {f.signals && f.signals.length > 0 && <SignalBadges signals={f.signals} />}
                  </div>
                )}
                {!gated && f.context && f.context.length > 0 && (
                  <div className="mt-1">
                    <ContextFacts context={f.context} limit={2} inline />
                  </div>
                )}
              </div>

              {/* Value */}
              <div className="text-right shrink-0 w-20">
                <div
                  className={`font-mono text-sm ${
                    gated
                      ? "text-[#E8E8ED]/40 blur-[3px]"
                      : "text-[#E8E8ED]"
                  }`}
                >
                  {formatCurrency(f.value)}
                </div>
                <div
                  className={`text-[10px] ${
                    gated
                      ? "text-[#55556A]/40 blur-[3px]"
                      : "text-[#55556A]"
                  }`}
                >
                  {f.qty?.toLocaleString()} shares
                </div>
              </div>

              {/* 7d/30d/90d Returns */}
              <div className="hidden md:flex gap-0">
                {[
                  { val: f.return_7d, good: isGood7d, label: "7d", days: 7 },
                  { val: f.return_30d, good: isGood30d, label: "30d", days: 30 },
                  { val: f.return_90d, good: isGood90d, label: "90d", days: 90 },
                ].map(({ val, good, label, days }) => {
                  const unavailable = isReturnUnavailable(f.trade_date, val, days);
                  return (
                    <div key={label} className="text-right shrink-0 w-16" title={unavailable ? unavailableReason(f.price_data_end) : undefined}>
                      <div
                        className={`font-mono text-sm ${
                          val != null
                            ? good
                              ? "text-[#22C55E]"
                              : "text-[#EF4444]"
                            : "text-[#55556A]"
                        }`}
                      >
                        {unavailable ? "N/A" : formatPercent(val)}
                      </div>
                      <div className="text-[10px] text-[#55556A]">{label}</div>
                    </div>
                  );
                })}
              </div>

              {/* Date */}
              <div className="hidden md:block text-right shrink-0 w-28">
                <div
                  className={`text-xs ${
                    gated
                      ? "text-[#E8E8ED]/40 blur-[3px]"
                      : "text-[#E8E8ED]"
                  }`}
                >
                  filed <RelativeTime date={f.filed_at || f.filing_date} />
                </div>
                <div
                  className={`text-[10px] ${
                    gated
                      ? "text-[#55556A]/40 blur-[3px]"
                      : "text-[#55556A]"
                  }`}
                >
                  traded {f.trade_date}
                </div>
              </div>
            </Wrapper>
          );
        })}
      </div>

      {/* Empty state */}
      {!loading && items.length === 0 && (
        <div className="flex h-40 items-center justify-center text-[#55556A] rounded-lg border border-[#2A2A3A]">
          No filings match your filters
        </div>
      )}

      <Pagination
        total={data.total}
        limit={PAGE_SIZE}
        offset={offset}
        onPageChange={(o) => fetchPage(o, filters)}
      />
    </div>
  );
}
