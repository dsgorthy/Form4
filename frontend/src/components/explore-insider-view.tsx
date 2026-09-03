import Link from "next/link";
import { formatCurrency } from "@/lib/format";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";
import { InsiderTradesTable } from "@/components/insider-trades-table";
import { insiderPath } from "@/lib/insider-url";
import type { Filing, PaginatedResponse } from "@/lib/types";
import { SectionLabel } from "@/components/ui/section-label";

/**
 * The insider half of /explore.
 *
 * Mirrors the company view's job: everything a signed-in user wants about
 * ONE entity, with the public /insider/[slug] page acting as the indexable
 * teaser that funnels here.
 *
 * Analytics (track record, grades, score history) are Pro-gated at the API,
 * so this renders whatever came back and simply omits sections the caller
 * isn't entitled to rather than blocking the page.
 */

export interface InsiderProfileLite {
  insider_id: string;
  name: string;
  cik: string | null;
  slug?: string | null;
  is_entity?: number;
  track_record?: {
    buy_count: number;
    sell_count: number;
    n_tickers: number;
    win_rate_7d?: number | null;
  } | null;
  best_pit_grade?: string | null;
  best_career_grade?: string | null;
  gated?: boolean;
}

export interface InsiderCompanyRow {
  ticker: string;
  company: string | null;
  trade_count: number;
  total_value: number;
  last_trade: string;
}


export function ExploreInsiderView({
  profile,
  companies,
  trades,
  teaser,
}: {
  profile: InsiderProfileLite;
  companies: InsiderCompanyRow[];
  trades: PaginatedResponse<Filing> | null;
  /** Rendered in place of the tables for a signed-out visitor. The caller
   *  resolves auth server-side, so the payload is never fetched for them. */
  teaser?: React.ReactNode;
}) {
  const tr = profile.track_record;

  return (
    <div>
      <div className="mb-8">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-bold text-[#E8E8ED] md:text-3xl">{profile.name}</h1>
          {profile.best_career_grade && (
            <InsiderGradeBadge grade={profile.best_career_grade} label="Insider" />
          )}
          {/* The "Form" badge is gone. It rendered pit_grade as a second
              published scale beside the Insider Rating — two letters for one
              person, from two scores of two different things, which is the
              contradiction api/ratings.py exists to remove. pit_grade is also
              not monotonic, so "Form" could read worse than the truth. */}
        </div>
        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-[#81819A] md:gap-4">
          {tr && <span>{tr.buy_count + tr.sell_count} trades</span>}
          {tr && <span>{tr.n_tickers} companies</span>}
          {companies.length > 0 && <span>Most recent: {companies[0].last_trade}</span>}
          <Link
            href={insiderPath(profile.name, profile.cik || profile.insider_id, profile.slug)}
            className="text-[#3B82F6] transition-colors hover:text-[#60A5FA]"
          >
            Public profile &rarr;
          </Link>
        </div>
      </div>

      {profile.gated && (
        <div className="mb-8 rounded-lg border border-[#3B82F6]/30 bg-[#3B82F6]/10 px-4 py-3 text-sm">
          <span className="text-[#E8E8ED]">
            Track record, grades and score history are a Pro feature.
          </span>
          <Link href="/pricing" className="ml-2 font-medium text-[#3B82F6] hover:text-[#60A5FA]">
            See plans &rarr;
          </Link>
        </div>
      )}

      {teaser}

      {!teaser && companies.length > 0 && (
        <div className="mb-8">
          <SectionLabel>Companies Traded ({companies.length})</SectionLabel>
          <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[#2A2A3A] text-xs uppercase text-[#81819A]">
                  <th className="px-4 py-2 text-left font-medium">Ticker</th>
                  <th className="px-4 py-2 text-left font-medium">Company</th>
                  <th className="px-4 py-2 text-right font-medium">Trades</th>
                  <th className="px-4 py-2 text-right font-medium">Value</th>
                  <th className="px-4 py-2 text-right font-medium">Last</th>
                </tr>
              </thead>
              <tbody>
                {companies.map((c) => (
                  <tr key={c.ticker} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                    <td className="px-4 py-2">
                      <Link
                        href={`/explore?ticker=${c.ticker}`}
                        className="font-mono text-[#3B82F6] hover:text-[#60A5FA]"
                      >
                        {c.ticker}
                      </Link>
                    </td>
                    <td className="px-4 py-2 text-[#8888A0]">{c.company || "—"}</td>
                    <td className="px-4 py-2 text-right font-mono text-[#E8E8ED]">{c.trade_count}</td>
                    <td className="px-4 py-2 text-right font-mono text-[#E8E8ED]">
                      {formatCurrency(c.total_value)}
                    </td>
                    <td className="px-4 py-2 text-right font-mono text-[#81819A]">{c.last_trade}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!teaser && trades && (
        <div>
          <SectionLabel>All Trades</SectionLabel>
          <InsiderTradesTable
            identifier={profile.cik || profile.insider_id}
            initialData={trades}
          />
        </div>
      )}
    </div>
  );
}
