"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { formatCurrency } from "@/lib/format";
import { TierBadge } from "@/components/ui/tier-badge";
import { Badge } from "@/components/ui/badge";
import { Pagination } from "@/components/pagination";
import type { Filing, PaginatedResponse } from "@/lib/types";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
const PAGE_SIZE = 25;

interface PrivateTradesTableProps {
  slug: string;
  initialData: PaginatedResponse<Filing>;
}

export function PrivateTradesTable({ slug, initialData }: PrivateTradesTableProps) {
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
          `${apiBase}/private-companies/${slug}/trades?${params}`,
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
    [getToken, slug],
  );

  // Reset when slug changes
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
            </Wrapper>
          );
        })}
      </div>

      {/* Desktop: Table layout */}
      <div className={`hidden md:block overflow-x-auto rounded-lg border border-[#2A2A3A] ${loading ? "opacity-60" : ""} transition-opacity`}>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Insider</th>
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Type</th>
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Traded</th>
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Filed</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Value</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((t) => (
              <tr
                key={t.trade_id}
                className={`border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30 transition-colors ${t.gated ? "select-none" : ""}`}
              >
                <td className="px-4 py-3">
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
                <td className="px-4 py-3">
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
                <td className={`px-4 py-3 text-[#E8E8ED] ${t.gated ? "blur-[3px]" : ""}`}>{t.trade_date}</td>
                <td className={`px-4 py-3 text-[#55556A] ${t.gated ? "blur-[3px]" : ""}`}>{t.filing_date}</td>
                <td className={`px-4 py-3 text-right font-mono text-[#E8E8ED] ${t.gated ? "blur-[3px]" : ""}`}>
                  {formatCurrency(t.value)}
                </td>
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
