"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { formatPercent } from "@/lib/format";
import { formatTitle } from "@/lib/title-format";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";
import Sparkline from "@/components/sparkline";
import { Pagination } from "@/components/pagination";
import type { LeaderboardEntry, PaginatedResponse } from "@/lib/types";
import { insiderPath } from "@/lib/insider-url";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
const PAGE_SIZE = 50;

interface LeaderboardTableProps {
  filters: Record<string, string>;
  sort: string;
  order: string;
}

function SkeletonRow({ i }: { i: number }) {
  return (
    <tr className="border-b border-[#2A2A3A]/50 animate-pulse">
      <td className="px-3 md:px-4 py-3"><div className="h-4 w-6 bg-[#1A1A26] rounded" /></td>
      <td className="px-3 md:px-4 py-3"><div className="h-4 w-32 bg-[#1A1A26] rounded" /></td>
      <td className="hidden md:table-cell px-4 py-3"><div className="h-4 w-20 bg-[#1A1A26] rounded" /></td>
      <td className="px-3 md:px-4 py-3 text-center"><div className="h-5 w-8 bg-[#1A1A26] rounded mx-auto" /></td>
      <td className="hidden md:table-cell px-4 py-3"><div className="h-4 w-12 bg-[#1A1A26] rounded ml-auto" /></td>
      <td className="px-3 md:px-4 py-3"><div className="h-4 w-8 bg-[#1A1A26] rounded ml-auto" /></td>
      <td className="hidden md:table-cell px-4 py-3"><div className="h-4 w-10 bg-[#1A1A26] rounded ml-auto" /></td>
      <td className="hidden md:table-cell px-4 py-3"><div className="h-4 w-12 bg-[#1A1A26] rounded ml-auto" /></td>
      <td className="hidden md:table-cell px-4 py-3"><div className="h-4 w-16 bg-[#1A1A26] rounded mx-auto" /></td>
      <td className="hidden md:table-cell px-4 py-3"><div className="h-4 w-10 bg-[#1A1A26] rounded ml-auto" /></td>
      <td className="hidden lg:table-cell px-4 py-3"><div className="h-4 w-16 bg-[#1A1A26] rounded ml-auto" /></td>
    </tr>
  );
}

export function LeaderboardTable({ filters, sort, order }: LeaderboardTableProps) {
  const { getToken } = useAuth();
  const [data, setData] = useState<
    | (PaginatedResponse<LeaderboardEntry> & {
        gated?: boolean;
        preview?: boolean;
        preview_limit?: number | null;
      })
    | null
  >(null);
  const [sparklines, setSparklines] = useState<Record<string, number[]>>({});
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);

  const fetchPage = useCallback(
    async (newOffset: number) => {
      setLoading(true);
      try {
        const token = await getToken();
        const headers: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {};
        const params = new URLSearchParams({
          sort_by: sort,
          order,
          limit: String(PAGE_SIZE),
          offset: String(newOffset),
          ...filters,
        });
        const res = await fetch(`${apiBase}/leaderboard?${params}`, { headers });
        if (res.ok) {
          const json = await res.json();
          setData(json);
          setOffset(newOffset);

          // Fetch sparklines
          if (json.items?.length > 0) {
            const ids = json.items.map((e: any) => e.insider_id).join(",");
            try {
              const spRes = await fetch(`${apiBase}/leaderboard/sparklines?insider_ids=${ids}`, { headers });
              if (spRes.ok) setSparklines(await spRes.json());
            } catch {}
          }
        }
      } catch {
      } finally {
        setLoading(false);
      }
    },
    [getToken, sort, order, filters],
  );

  useEffect(() => {
    setOffset(0);
    fetchPage(0);
  }, [fetchPage]);

  if (!data) {
    return (
      <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
        <table className="w-full text-sm">
          <tbody>
            {Array.from({ length: 10 }).map((_, i) => <SkeletonRow key={i} i={i} />)}
          </tbody>
        </table>
      </div>
    );
  }

  const isGated = data.gated === true;

  return (
    <div>
      <div className={`overflow-x-auto rounded-lg border border-[#2A2A3A] ${loading ? "opacity-60" : ""} transition-opacity`}>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
              <th className="px-3 md:px-4 py-3 text-left text-[#55556A] font-medium w-10">#</th>
              <th className="px-3 md:px-4 py-3 text-left text-[#55556A] font-medium">Insider</th>
              <th className="hidden md:table-cell px-4 py-3 text-left text-[#55556A] font-medium">Title</th>
              <th className="px-3 md:px-4 py-3 text-center text-[#55556A] font-medium w-16">Rating</th>
              <th className="px-3 md:px-4 py-3 text-right text-[#55556A] font-medium">Buys</th>
              <th className="hidden md:table-cell px-4 py-3 text-center text-[#55556A] font-medium">Trend</th>
              <th className="hidden md:table-cell px-4 py-3 text-right text-[#55556A] font-medium">Ticker</th>
              <th className="hidden lg:table-cell px-4 py-3 text-right text-[#55556A] font-medium">Last Active</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((entry, i) => (
              <tr
                key={entry.insider_id}
                className={`border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30 transition-colors ${isGated ? "select-none" : ""}`}
              >
                <td className="px-3 md:px-4 py-3 text-[#55556A] font-mono text-xs">
                  {offset + i + 1}
                </td>
                <td className="px-3 md:px-4 py-3">
                  {isGated ? (
                    <span className="font-medium text-blue-400/40 blur-[3px]">{entry.name}</span>
                  ) : (
                    <Link
                      href={insiderPath(entry.name, entry.cik || entry.insider_id)}
                      className="font-medium text-blue-400 hover:text-blue-300"
                    >
                      {entry.name}
                    </Link>
                  )}
                </td>
                <td className={`hidden md:table-cell px-4 py-3 text-[#8888A0] text-xs truncate max-w-[200px] ${isGated ? "blur-[3px]" : ""}`}>
                  {formatTitle(entry.primary_title) || "\u2014"}
                </td>
                <td className="px-3 md:px-4 py-3 text-center">
                  <InsiderGradeBadge
                    rating={(entry as any).insider_rating}
                    grade={(entry as any).best_career_grade}
                    compact
                  />
                </td>
                <td className="px-3 md:px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  {entry.buy_count}
                </td>
                <td className={`hidden md:table-cell px-4 py-3 text-center ${isGated ? "blur-[3px]" : ""}`}>
                  <Sparkline returns={sparklines[entry.insider_id] || []} />
                </td>
                <td className="hidden md:table-cell px-4 py-3 text-right">
                  {entry.primary_ticker ? (
                    isGated ? (
                      <span className="font-mono text-xs text-[#8888A0]/40 blur-[3px]">{entry.primary_ticker}</span>
                    ) : (
                      <Link href={`/company/${entry.primary_ticker}`} className="font-mono text-xs text-[#8888A0] hover:text-[#E8E8ED]">
                        {entry.primary_ticker}
                      </Link>
                    )
                  ) : (
                    <span className="text-[#55556A]">{"\u2014"}</span>
                  )}
                </td>
                <td className="hidden lg:table-cell px-4 py-3 text-right text-xs text-[#55556A]">
                  {(() => {
                    const lastBuy = (entry as any).buy_last_date;
                    const lastSell = (entry as any).sell_last_date;
                    const last = [lastBuy, lastSell].filter(Boolean).sort().reverse()[0];
                    if (!last) return "\u2014";
                    const days = Math.floor((Date.now() - new Date(last).getTime()) / 86400000);
                    if (days <= 30) return <span className="text-[#22C55E]">{last}</span>;
                    if (days <= 180) return last;
                    return <span className="text-[#55556A]/60">{last}</span>;
                  })()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.preview ? (
        <div className="mt-4 rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5 text-center">
          <p className="text-sm text-[#E8E8ED]">
            Showing the top {data.items.length} of{" "}
            {data.total.toLocaleString()} ranked insiders.
          </p>
          <p className="mt-1 text-sm text-[#8888A0]">
            Pro unlocks the full leaderboard and its filters.
          </p>
          <Link
            href="/pricing"
            className="mt-4 inline-block rounded-md bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-[#2563EB]"
          >
            See plans
          </Link>
        </div>
      ) : (
        <Pagination total={data.total} limit={PAGE_SIZE} offset={offset} onPageChange={fetchPage} />
      )}
    </div>
  );
}
