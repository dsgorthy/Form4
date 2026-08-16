"use client";

import { useState } from "react";
import { useUser } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";
import Link from "next/link";
import { formatCurrency } from "@/lib/format";
import { formatTitle } from "@/lib/title-format";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";
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
  pit_grade?: string | null;
}

interface InsiderRosterProps {
  insiders: Insider[];
  gated?: boolean;
}

export function InsiderRoster({ insiders, gated = false }: InsiderRosterProps) {
  const [offset, setOffset] = useState(0);
  const page = insiders.slice(offset, offset + PAGE_SIZE);
  const { user } = useUser();

  // `gated` says this call site WANTS gating; whether it applies depends on the
  // viewer. ProGate resolves that itself for the blurred cells, but the score
  // column is dropped outright rather than blurred, so it needs the check here
  // — otherwise a Pro subscriber loses a column they are paying for.
  const hideScore = gated && !isPro(user);

  /**
   * The top insider's grade stays visible as a proof row.
   *
   * Blurring every grade is indistinguishable from having no grades, and asks
   * a first-time visitor to pay for something they have never seen work. One
   * unblurred example shows the analysis exists, is specific to this company,
   * and is not a placeholder — then the wall over the remaining rows is an
   * offer rather than an assertion. Only on the first page: a proof row that
   * follows the reader through pagination is just an ungated column.
   */
  function Grade({ ins, i }: { ins: Insider; i: number }) {
    if (!ins.pit_grade) return null;
    const badge = <InsiderGradeBadge grade={ins.pit_grade} />;
    if (!gated || (offset === 0 && i === 0)) return badge;
    return <ProGate compact>{badge}</ProGate>;
  }

  return (
    <div>
      {/* Mobile: Card layout */}
      <div className="md:hidden space-y-2">
        {page.map((ins, i) => (
          <Link
            key={ins.insider_id}
            href={`/insider/${ins.cik || ins.insider_id}`}
            className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-3 hover:bg-[#1A1A26]/60 transition-colors"
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2 min-w-0">
                <span className="font-medium text-blue-400 truncate">{ins.name}</span>
                <Grade ins={ins} i={i} />
              </div>
              <span className="font-mono text-sm text-[#E8E8ED] shrink-0">
                {formatCurrency(ins.total_value)}
              </span>
            </div>
            <div className="flex items-center justify-between mt-1.5">
              <div className="text-xs text-[#55556A] truncate">
                {formatTitle(ins.normalized_title || ins.title) || "\u2014"}
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
              {!hideScore && (
                <th className="px-4 py-3 text-right text-[#55556A] font-medium">Score</th>
              )}
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Trades</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Value</th>
              <th className="px-4 py-3 text-right text-[#55556A] font-medium">Last Trade</th>
            </tr>
          </thead>
          <tbody>
            {page.map((ins, i) => (
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
                  {(() => {
                    const ft = formatTitle(ins.normalized_title || ins.title);
                    return ft ? (
                      <div className="flex flex-wrap gap-1">
                        {ft.split(", ").map((tag) => (
                          <span
                            key={tag}
                            className="inline-block rounded px-1.5 py-0.5 text-[10px] font-medium border border-[#2A2A3A] bg-[#1A1A26] text-[#8888A0]"
                          >
                            {tag}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className="text-[#55556A]">{"\u2014"}</span>
                    );
                  })()}
                </td>
                <td className="px-4 py-3 text-center">
                  {ins.pit_grade ? (
                    <Grade ins={ins} i={i} />
                  ) : (
                    <span className="text-[#55556A]">{"\u2014"}</span>
                  )}
                </td>
                {/* Dropped rather than blurred when gated. The API nulls score
                    for non-Pro, so a blur here covers an em-dash — a wall with
                    nothing behind it, which teaches the visitor the wall is
                    empty. The grade column carries the gated signal instead,
                    and it has real values behind it. */}
                {!hideScore && (
                  <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                    <span>{ins.score?.toFixed(2) ?? "\u2014"}</span>
                  </td>
                )}
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
