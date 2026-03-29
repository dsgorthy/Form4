"use client";

import { useState } from "react";
import Link from "next/link";
import { formatCurrency } from "@/lib/format";
import { TierBadge } from "@/components/ui/tier-badge";
import { ProGate } from "@/components/pro-gate";
import { Pagination } from "@/components/pagination";

const PAGE_SIZE = 10;

interface Insider {
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
}

interface InsiderRosterProps {
  insiders: Insider[];
  gated?: boolean;
}

export function InsiderRoster({ insiders, gated = false }: InsiderRosterProps) {
  const [offset, setOffset] = useState(0);
  const page = insiders.slice(offset, offset + PAGE_SIZE);

  return (
    <div>
      {/* Mobile: Card layout */}
      <div className="md:hidden space-y-2">
        {page.map((ins) => (
          <Link
            key={ins.insider_id}
            href={`/insider/${ins.cik || ins.insider_id}`}
            className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-3 hover:bg-[#1A1A26]/60 transition-colors"
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2 min-w-0">
                <span className="font-medium text-blue-400 truncate">{ins.name}</span>
                {ins.score_tier != null && <TierBadge tier={ins.score_tier} />}
              </div>
              <span className="font-mono text-sm text-[#E8E8ED] shrink-0">
                {formatCurrency(ins.total_value)}
              </span>
            </div>
            <div className="flex items-center justify-between mt-1.5">
              <div className="text-xs text-[#55556A] truncate">
                {ins.normalized_title
                  ? ins.normalized_title.split(";").join(" / ")
                  : ins.title || "\u2014"}
              </div>
              <span className="text-xs text-[#55556A] shrink-0 ml-2">
                {ins.trade_count} trades
              </span>
            </div>
            {ins.is_entity === 1 && ins.controlled_by && (
              <div className="text-[10px] text-[#55556A] mt-1">
                via {ins.controlled_by.name}
              </div>
            )}
          </Link>
        ))}
      </div>

      {/* Desktop: Table layout */}
      <div className="hidden md:block overflow-x-auto rounded-lg border border-[#2A2A3A]">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Name</th>
              <th className="px-4 py-3 text-left text-[#55556A] font-medium">Title</th>
              <th className="px-4 py-3 text-center text-[#55556A] font-medium">Tier</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Score</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Trades</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Value</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Last Trade</th>
            </tr>
          </thead>
          <tbody>
            {page.map((ins) => (
              <tr
                key={ins.insider_id}
                className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30 transition-colors"
              >
                <td className="px-4 py-3">
                  <Link
                    href={`/insider/${ins.cik || ins.insider_id}`}
                    className="font-medium text-blue-400 hover:text-blue-300"
                  >
                    {ins.name}
                  </Link>
                  {ins.is_entity === 1 && ins.controlled_by && (
                    <div className="text-[10px] text-[#55556A] mt-0.5">
                      via{" "}
                      <Link
                        href={`/insider/${ins.controlled_by.insider_id}`}
                        className="text-[#8888A0] hover:text-blue-300"
                      >
                        {ins.controlled_by.name}
                      </Link>
                    </div>
                  )}
                </td>
                <td className="px-4 py-3 text-xs max-w-[240px]">
                  {ins.normalized_title ? (
                    <div className="flex flex-wrap gap-1">
                      {ins.normalized_title.split(";").map((tag) => (
                        <span
                          key={tag}
                          className="inline-block rounded px-1.5 py-0.5 text-[10px] font-medium border border-[#2A2A3A] bg-[#1A1A26] text-[#8888A0]"
                        >
                          {tag}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <span className="text-[#55556A]">{ins.title || "\u2014"}</span>
                  )}
                </td>
                <td className="px-4 py-3 text-center">
                  {ins.score_tier != null ? (
                    <TierBadge tier={ins.score_tier} />
                  ) : (
                    <span className="text-[#55556A]">{"\u2014"}</span>
                  )}
                </td>
                <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  {gated ? (
                    <ProGate compact>
                      <span>{ins.score?.toFixed(2) ?? "\u2014"}</span>
                    </ProGate>
                  ) : (
                    <span>{ins.score?.toFixed(2) ?? "\u2014"}</span>
                  )}
                </td>
                <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  {ins.trade_count}
                </td>
                <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                  {formatCurrency(ins.total_value)}
                </td>
                <td className="px-4 py-3 text-right text-[#8888A0]">{ins.last_trade}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pagination
        total={insiders.length}
        limit={PAGE_SIZE}
        offset={offset}
        onPageChange={setOffset}
      />
    </div>
  );
}
