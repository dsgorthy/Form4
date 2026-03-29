"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { formatCurrency, formatPercent } from "@/lib/format";
import { TierBadge } from "@/components/ui/tier-badge";
import { Badge } from "@/components/ui/badge";
import { ProGate } from "@/components/pro-gate";
import { Pagination } from "@/components/pagination";
import { SignalQualityBadge } from "@/components/signal-quality-badge";
import type { Filing, PaginatedResponse } from "@/lib/types";

function TradeTags({ item }: { item: Filing }) {
  const tags: React.ReactNode[] = [];
  const sq = (item as any).signal_quality;
  if (sq?.grade) {
    tags.push(<SignalQualityBadge key="sq" quality={sq} />);
  }
  if ((item as any).is_10b5_1 === 1) {
    tags.push(
      <span key="10b5" className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#55556A]/20 text-[#8888A0] border border-[#55556A]/30">
        10b5-1
      </span>
    );
  }
  if ((item as any).is_routine === 1) {
    tags.push(
      <span key="routine" className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#55556A]/20 text-[#8888A0] border border-[#55556A]/30">
        Routine
      </span>
    );
  }
  if ((item as any).is_rare_reversal === 1) {
    tags.push(
      <span key="rev" className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#F59E0B]/10 text-[#F59E0B] border border-[#F59E0B]/20">
        Rare Reversal
      </span>
    );
  }
  if ((item as any).cohen_routine === 0 && !((item as any).is_10b5_1 === 1) && !((item as any).is_routine === 1)) {
    tags.push(
      <span key="opp" className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#3B82F6]/10 text-[#3B82F6] border border-[#3B82F6]/20">
        Opportunistic
      </span>
    );
  }
  const w52 = (item as any).week52_proximity;
  if (w52 != null && w52 >= 0.8 && item.trade_type === "buy") {
    tags.push(
      <span key="52h" className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#22C55E]/10 text-[#22C55E] border border-[#22C55E]/20">
        Near 52w High
      </span>
    );
  } else if (w52 != null && w52 <= 0.2 && item.trade_type === "sell") {
    tags.push(
      <span key="52l" className="rounded px-1.5 py-0.5 text-[9px] font-medium bg-[#EF4444]/10 text-[#EF4444] border border-[#EF4444]/20">
        Near 52w Low
      </span>
    );
  }
  if (tags.length === 0) return null;
  return <div className="flex items-center gap-1 mt-0.5 flex-wrap">{tags}</div>;
}

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
const PAGE_SIZE = 25;

interface TradesTableProps {
  ticker: string;
  initialData: PaginatedResponse<Filing>;
}

export function TradesTable({ ticker, initialData }: TradesTableProps) {
  const { getToken } = useAuth();
  const [data, setData] = useState(initialData);
  const [offset, setOffset] = useState(initialData.offset);
  const [loading, setLoading] = useState(false);

  const fetchPage = useCallback(
    async (newOffset: number) => {
      setLoading(true);
      try {
        const token = await getToken();
        const params = new URLSearchParams({
          limit: String(PAGE_SIZE),
          offset: String(newOffset),
        });
        const res = await fetch(
          `${apiBase}/companies/${ticker}/trades?${params}`,
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
    [getToken, ticker],
  );

  // Reset when ticker changes
  useEffect(() => {
    setData(initialData);
    setOffset(initialData.offset);
  }, [initialData]);

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
          All Trades
        </div>
        <span className="text-xs text-[#55556A]">{data.total} total</span>
      </div>

      {/* Mobile: Card layout */}
      <div className={`md:hidden space-y-2 ${loading ? "opacity-60" : ""} transition-opacity`}>
        {data.items.map((t) => {
          const isGoodSignal7d =
            t.return_7d != null &&
            (t.trade_type === "buy" ? t.return_7d >= 0 : t.return_7d <= 0);

          const Wrapper = t.gated ? "div" : Link;
          const wrapperProps = t.gated ? {} : { href: `/filing/${t.trade_id}` };

          return (
            <Wrapper
              key={t.trade_id}
              {...(wrapperProps as any)}
              className={`block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-3 transition-colors ${t.gated ? "select-none" : "hover:bg-[#1A1A26]/60"}`}
            >
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-2 min-w-0">
                  <span className={`text-sm truncate ${t.gated ? "text-blue-400/40 blur-[3px]" : "text-blue-400"}`}>
                    {t.insider_name}
                  </span>
                  {(t.score_tier ?? t.tier) != null && (
                    <TierBadge tier={t.score_tier ?? t.tier} />
                  )}
                </div>
                <Badge
                  variant="outline"
                  className={`text-xs font-mono shrink-0 ${
                    t.trade_type === "buy"
                      ? "border-[#22C55E]/30 text-[#22C55E]"
                      : "border-[#EF4444]/30 text-[#EF4444]"
                  }`}
                >
                  {t.trade_type.toUpperCase()}
                </Badge>
              </div>
              <div className="flex items-center justify-between mt-1.5">
                <span className={`text-xs ${t.gated ? "text-[#8888A0]/40 blur-[3px]" : "text-[#8888A0]"}`}>
                  {t.trade_date}
                </span>
                <span className={`font-mono text-sm ${t.gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>
                  {formatCurrency(t.value)}
                </span>
              </div>
              {!t.gated && <TradeTags item={t} />}
              {t.return_7d != null && !t.gated && (
                <div className="flex justify-end mt-1">
                  <span className={`text-xs font-mono ${isGoodSignal7d ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                    7d: {formatPercent(t.return_7d)}
                  </span>
                </div>
              )}
            </Wrapper>
          );
        })}
      </div>

      {/* Desktop: Table layout */}
      <div className={`hidden md:block overflow-x-auto rounded-lg border border-[#2A2A3A] ${loading ? "opacity-60" : ""} transition-opacity`}>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
              <th className="px-3 py-3 text-left text-[#55556A] font-medium">Insider</th>
              <th className="px-3 py-3 text-left text-[#55556A] font-medium">Type</th>
              <th className="hidden lg:table-cell px-3 py-3 text-left text-[#55556A] font-medium">Signal</th>
              <th className="hidden lg:table-cell px-3 py-3 text-center text-[#55556A] font-medium">Tier</th>
              <th className="px-3 py-3 text-left text-[#55556A] font-medium">Date</th>
              <th className="hidden xl:table-cell px-3 py-3 text-left text-[#55556A] font-medium">Filed</th>
              <th className="hidden lg:table-cell px-3 py-3 text-right text-[#55556A] font-medium">Price</th>
              <th className="px-3 py-3 text-right text-[#55556A] font-medium">Value</th>
              <th className="px-3 py-3 text-right text-[#55556A] font-medium">7d</th>
              <th className="hidden lg:table-cell px-3 py-3 text-right text-[#55556A] font-medium">30d</th>
              <th className="hidden xl:table-cell px-3 py-3 text-right text-[#55556A] font-medium">90d</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((t) => (
              <tr
                key={t.trade_id}
                className={`border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30 transition-colors ${t.gated ? "select-none" : ""}`}
              >
                <td className="px-3 py-3">
                  {t.gated ? (
                    <span className="text-blue-400/40 text-xs blur-[3px]">{t.insider_name}</span>
                  ) : (
                    <Link
                      href={`/filing/${t.trade_id}`}
                      className="text-blue-400 hover:text-blue-300 text-xs"
                    >
                      {t.insider_name}
                    </Link>
                  )}
                </td>
                <td className="px-3 py-3">
                  <Badge
                    variant="outline"
                    className={`text-xs font-mono ${
                      t.trade_type === "buy"
                        ? "border-[#22C55E]/30 text-[#22C55E]"
                        : "border-[#EF4444]/30 text-[#EF4444]"
                    }`}
                  >
                    {t.trade_type.toUpperCase()}
                  </Badge>
                </td>
                <td className="hidden lg:table-cell px-3 py-3">
                  {!t.gated && <TradeTags item={t} />}
                </td>
                <td className="hidden lg:table-cell px-3 py-3 text-center">
                  {(t.score_tier ?? t.tier) != null ? (
                    <TierBadge tier={t.score_tier ?? t.tier} />
                  ) : (
                    <span className="text-[#55556A]">{"\u2014"}</span>
                  )}
                </td>
                <td className={`px-3 py-3 text-[#E8E8ED] ${t.gated ? "blur-[3px]" : ""}`}>{t.trade_date}</td>
                <td className={`hidden xl:table-cell px-3 py-3 text-[#55556A] ${t.gated ? "blur-[3px]" : ""}`}>{t.filing_date}</td>
                <td className={`hidden lg:table-cell px-3 py-3 text-right font-mono text-[#E8E8ED] ${t.gated ? "blur-[3px]" : ""}`}>
                  ${t.price?.toFixed(2) ?? "\u2014"}
                </td>
                <td className={`px-3 py-3 text-right font-mono text-[#E8E8ED] ${t.gated ? "blur-[3px]" : ""}`}>
                  {formatCurrency(t.value)}
                </td>
                {[t.return_7d, t.return_30d, (t as any).return_90d].map((ret, i) => {
                  const hiddenClass = i === 1 ? "hidden lg:table-cell " : i === 2 ? "hidden xl:table-cell " : "";
                  const isGoodSignal =
                    ret != null &&
                    (t.trade_type === "buy" ? ret >= 0 : ret <= 0);
                  return (
                    <td key={i} className={`${hiddenClass}px-3 py-3 text-right font-mono`}>
                      <ProGate compact>
                        <span
                          className={
                            ret != null
                              ? isGoodSignal
                                ? "text-[#22C55E]"
                                : "text-[#EF4444]"
                              : "text-[#55556A]"
                          }
                        >
                          {formatPercent(ret)}
                        </span>
                      </ProGate>
                    </td>
                  );
                })}
              </tr>
            ))}
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
