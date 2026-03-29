"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { formatCurrency, formatPercent, isReturnUnavailable, unavailableReason } from "@/lib/format";
import { Badge } from "@/components/ui/badge";
import { SignalBadges } from "@/components/signal-badge";
import { ContextFacts } from "@/components/context-facts";
import { TickerDisplay } from "@/components/ui/ticker-display";
import { Pagination } from "@/components/pagination";
import { TransCodeSelector } from "@/components/trans-code-selector";
import type { Filing, PaginatedResponse } from "@/lib/types";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
const PAGE_SIZE = 25;

interface InsiderTradesTableProps {
  identifier: string;
  initialData: PaginatedResponse<Filing>;
}

export function InsiderTradesTable({ identifier, initialData }: InsiderTradesTableProps) {
  const { getToken } = useAuth();
  const [data, setData] = useState(initialData);
  const [offset, setOffset] = useState(initialData.offset);
  const [loading, setLoading] = useState(false);
  const [transCodes, setTransCodes] = useState<Set<string>>(new Set(["P", "S"]));

  const fetchPage = useCallback(
    async (newOffset: number, codes?: Set<string>) => {
      setLoading(true);
      const activeCodes = codes || transCodes;
      try {
        const token = await getToken();
        const params = new URLSearchParams({
          limit: String(PAGE_SIZE),
          offset: String(newOffset),
          trans_codes: Array.from(activeCodes).join(","),
        });
        const res = await fetch(
          `${apiBase}/insiders/${identifier}/trades?${params}`,
          { headers: token ? { Authorization: `Bearer ${token}` } : {} },
        );
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
    [getToken, identifier, transCodes],
  );

  const handleTransCodeChange = useCallback(
    (codes: Set<string>) => {
      setTransCodes(codes);
      fetchPage(0, codes);
    },
    [fetchPage],
  );

  useEffect(() => {
    setData(initialData);
    setOffset(initialData.offset);
  }, [initialData]);

  return (
    <div>
      <div className="flex items-center justify-between mb-3">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
          Trade History
        </div>
        <span className="text-xs text-[#55556A]">{data.total} total</span>
      </div>
      <div className="mb-4">
        <TransCodeSelector selected={transCodes} onChange={handleTransCodeChange} />
      </div>

      {/* Mobile: Card layout */}
      <div className={`md:hidden space-y-2 ${loading ? "opacity-60" : ""} transition-opacity`}>
        {data.items.map((t) => {
          const isRoutineSell = t.trade_type === "sell" && (t.is_routine === 1 || t.is_10b5_1 === 1);
          return (
          <Link
            key={t.trade_id}
            href={`/filing/${t.trade_id}`}
            className={`block rounded-lg border p-3 transition-colors ${isRoutineSell ? "border-[#2A2A3A]/50 bg-[#12121A]/70" : "border-[#2A2A3A] bg-[#12121A] hover:bg-[#1A1A26]/60"}`}
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2 min-w-0">
                {t.ticker === "NONE" ? (
                  <TickerDisplay ticker={t.ticker} company={t.company} />
                ) : (
                  <span className="font-mono font-bold text-[#E8E8ED]">{t.ticker}</span>
                )}
                <Badge
                  variant="outline"
                  className={`text-xs font-mono shrink-0 ${
                    t.trade_type === "buy"
                      ? "border-[#22C55E]/30 text-[#22C55E]"
                      : isRoutineSell
                        ? "border-[#55556A]/30 text-[#55556A]"
                        : "border-[#EF4444]/30 text-[#EF4444]"
                  }`}
                >
                  {isRoutineSell
                    ? (t.is_10b5_1 === 1 ? "SELL · 10b5-1" : "SELL · Routine")
                    : t.trade_type.toUpperCase()}
                </Badge>
              </div>
              <span className="font-mono text-sm text-[#E8E8ED] shrink-0">
                {formatCurrency(t.value)}
              </span>
            </div>
            <div className="flex items-center justify-between mt-1.5">
              <span className="text-xs text-[#8888A0]">{t.trade_date}</span>
              {t.return_7d != null ? (
                <span className={`text-xs font-mono ${
                  (t.trade_type === "buy" ? t.return_7d >= 0 : t.return_7d <= 0)
                    ? "text-[#22C55E]"
                    : "text-[#EF4444]"
                }`}>
                  7d: {formatPercent(t.return_7d)}
                </span>
              ) : isReturnUnavailable(t.trade_date, t.return_7d, 7) ? (
                <span className="text-[10px] text-[#55556A]" title={unavailableReason((t as any).price_data_end)}>N/A</span>
              ) : null}
            </div>
            {t.signals && t.signals.length > 0 && (
              <div className="mt-1.5"><SignalBadges signals={t.signals} /></div>
            )}
            {t.context && t.context.length > 0 && (
              <div className="mt-1"><ContextFacts context={t.context} limit={2} /></div>
            )}
          </Link>
          );
        })}
      </div>

      {/* Desktop: Table layout */}
      <div className={`hidden md:block overflow-x-auto rounded-lg border border-[#2A2A3A] ${loading ? "opacity-60" : ""} transition-opacity`}>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Ticker</th>
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Type</th>
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Traded</th>
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Filed</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Price</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Value</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">7d Stock</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">30d Stock</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">90d Stock</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((t) => {
              const isRoutineSell = t.trade_type === "sell" && (t.is_routine === 1 || t.is_10b5_1 === 1);
              return (
              <tr
                key={t.trade_id}
                className={`border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30 transition-colors ${isRoutineSell ? "text-[#55556A]" : ""}`}
              >
                <td className="px-4 py-3">
                  {t.ticker === "NONE" ? (
                    <TickerDisplay ticker={t.ticker} company={t.company} />
                  ) : (
                    <Link href={`/filing/${t.trade_id}`} className="font-mono font-semibold text-[#E8E8ED] hover:text-blue-400 transition-colors">
                      {t.ticker}
                    </Link>
                  )}
                </td>
                <td className="px-4 py-3">
                  <Badge
                    variant="outline"
                    className={`text-xs font-mono ${
                      t.trade_type === "buy"
                        ? "border-[#22C55E]/30 text-[#22C55E]"
                        : isRoutineSell
                          ? "border-[#55556A]/30 text-[#55556A]"
                          : "border-[#EF4444]/30 text-[#EF4444]"
                    }`}
                  >
                    {isRoutineSell
                      ? (t.is_10b5_1 === 1 ? "SELL · 10b5-1" : "SELL · Routine")
                      : t.trade_type.toUpperCase()}
                  </Badge>
                </td>
                <td className="px-4 py-3 text-[#E8E8ED]">{t.trade_date}</td>
                <td className="px-4 py-3 text-[#55556A]">{t.filing_date}</td>
                <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  ${t.price?.toFixed(2) ?? "\u2014"}
                </td>
                <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  {formatCurrency(t.value)}
                </td>
                {[
                  { ret: t.return_7d, days: 7 },
                  { ret: t.return_30d, days: 30 },
                  { ret: t.return_90d, days: 90 },
                ].map(({ ret, days }, i) => {
                  const unavailable = isReturnUnavailable(t.trade_date, ret, days);
                  const isGoodSignal =
                    ret != null &&
                    (t.trade_type === "buy" ? ret >= 0 : ret <= 0);
                  return (
                    <td
                      key={i}
                      className={`px-4 py-3 text-right font-mono ${
                        ret != null
                          ? isGoodSignal
                            ? "text-[#22C55E]"
                            : "text-[#EF4444]"
                          : "text-[#55556A]"
                      }`}
                      title={unavailable ? unavailableReason((t as any).price_data_end) : undefined}
                    >
                      {unavailable ? "N/A" : formatPercent(ret)}
                    </td>
                  );
                })}
              </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <Pagination
        total={data.total}
        limit={PAGE_SIZE}
        offset={offset}
        onPageChange={fetchPage}
      />
    </div>
  );
}
