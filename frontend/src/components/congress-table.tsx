"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import { Badge } from "@/components/ui/badge";
import { Pagination } from "@/components/pagination";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
const PAGE_SIZE = 10;

interface CongressTrade {
  congress_trade_id: number;
  politician_name: string;
  chamber: string;
  party: string;
  state: string;
  trade_type: string;
  trade_date: string;
  value_low: number | null;
  value_high: number | null;
  value_estimate: number | null;
  filing_date: string | null;
  owner: string;
}

interface CongressResponse {
  ticker: string;
  trades: CongressTrade[];
  total: number;
  limit: number;
  offset: number;
}

const partyColor: Record<string, string> = {
  D: "border-blue-500/30 text-blue-400 bg-blue-500/10",
  R: "border-red-500/30 text-red-400 bg-red-500/10",
  I: "border-gray-500/30 text-gray-400 bg-gray-500/10",
};

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

interface CongressTableProps {
  ticker: string;
  initialData: CongressResponse;
}

export function CongressTable({ ticker, initialData }: CongressTableProps) {
  const { getToken } = useAuth();
  const [data, setData] = useState(initialData);
  const [offset, setOffset] = useState(initialData.offset ?? 0);
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
          `${apiBase}/congress/by-ticker/${ticker}?${params}`,
          { headers: token ? { Authorization: `Bearer ${token}` } : {} },
        );
        if (res.ok) {
          const json: CongressResponse = await res.json();
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

  useEffect(() => {
    setData(initialData);
    setOffset(initialData.offset ?? 0);
  }, [initialData]);

  if (data.total === 0) return null;

  return (
    <div className="mb-8">
      <div className="flex items-baseline gap-2 mb-3">
        <span className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
          Political Activity (Congress) ({data.total})
        </span>
        <span className="text-[10px] text-[#55556A]/50">· ~1 day delay</span>
      </div>
      <div className={`overflow-x-auto rounded-lg border border-[#2A2A3A] ${loading ? "opacity-60" : ""} transition-opacity`}>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
              <th className="px-3 md:px-4 py-3 text-left text-[#55556A] font-medium">Politician</th>
              <th className="hidden md:table-cell px-4 py-3 text-left text-[#55556A] font-medium">Party</th>
              <th className="px-3 md:px-4 py-3 text-left text-[#55556A] font-medium">Type</th>
              <th className="px-3 md:px-4 py-3 text-left text-[#55556A] font-medium">Date</th>
              <th className="px-3 md:px-4 py-3 text-right text-[#55556A] font-medium">Value Range</th>
              <th className="hidden md:table-cell px-4 py-3 text-left text-[#55556A] font-medium">Owner</th>
            </tr>
          </thead>
          <tbody>
            {data.trades.map((ct) => (
              <tr
                key={ct.congress_trade_id}
                className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30 transition-colors"
              >
                <td className="px-3 md:px-4 py-3 font-medium text-[#E8E8ED]">
                  {ct.politician_name}
                  <span className="ml-2 text-xs text-[#55556A]">
                    {ct.chamber} &middot; {ct.state}
                  </span>
                </td>
                <td className="hidden md:table-cell px-4 py-3">
                  <Badge
                    variant="outline"
                    className={`text-xs font-mono ${partyColor[ct.party] || partyColor.I}`}
                  >
                    {ct.party}
                  </Badge>
                </td>
                <td className="px-3 md:px-4 py-3">
                  <Badge
                    variant="outline"
                    className={`text-xs font-mono ${
                      ct.trade_type.toLowerCase().includes("buy") ||
                      ct.trade_type.toLowerCase().includes("purchase")
                        ? "border-[#22C55E]/30 text-[#22C55E]"
                        : ct.trade_type.toLowerCase().includes("sell") ||
                            ct.trade_type.toLowerCase().includes("sale")
                          ? "border-[#EF4444]/30 text-[#EF4444]"
                          : "border-[#55556A]/30 text-[#8888A0]"
                    }`}
                  >
                    {ct.trade_type.toUpperCase()}
                  </Badge>
                </td>
                <td className="px-3 md:px-4 py-3 text-[#E8E8ED]">{ct.trade_date}</td>
                <td className="px-3 md:px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  {formatValueRange(ct.value_low, ct.value_high)}
                </td>
                <td className="hidden md:table-cell px-4 py-3 text-[#8888A0]">{ct.owner || "\u2014"}</td>
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
