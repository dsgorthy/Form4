export const dynamic = "force-dynamic";

import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency } from "@/lib/format";
import { InsiderRoster } from "@/components/insider-roster";
import { PrivateTradesTable } from "@/components/private-trades-table";
import type { Filing, PaginatedResponse } from "@/lib/types";

interface PrivateCompanyOverview {
  company: string;
  ticker: string;
  total_trades: number;
  total_value: number;
  first_trade: string;
  last_trade: string;
  insiders: {
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
  }[];
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
      {children}
    </div>
  );
}

const TRADES_LIMIT = 25;

export default async function PrivateCompanyPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;

  let overview: PrivateCompanyOverview;
  let trades: PaginatedResponse<Filing>;

  try {
    [overview, trades] = await Promise.all([
      fetchAPIAuth<PrivateCompanyOverview>(`/private-companies/${slug}`),
      fetchAPIAuth<PaginatedResponse<Filing>>(`/private-companies/${slug}/trades`, { limit: String(TRADES_LIMIT) }),
    ]);
  } catch {
    notFound();
  }

  return (
    <div>
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span>Private Companies</span>
        <span>/</span>
        <span className="text-[#E8E8ED] truncate">{overview.company}</span>
      </nav>

      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl md:text-3xl font-bold text-[#E8E8ED]">{overview.company}</h1>
          <span className="rounded px-2 py-1 text-xs font-medium border border-[#55556A]/30 bg-[#55556A]/10 text-[#8888A0] shrink-0">
            Private / Unlisted
          </span>
        </div>
        <div className="flex flex-wrap items-center gap-2 md:gap-4 mt-3 text-xs text-[#55556A]">
          <span>{overview.total_trades} total trades</span>
          <span>{formatCurrency(overview.total_value)} total value</span>
          <span>
            {overview.first_trade} to {overview.last_trade}
          </span>
        </div>
      </div>

      {/* Info Banner */}
      <div className="rounded-lg border border-[#3B82F6]/20 bg-[#3B82F6]/5 px-4 py-3 text-sm mb-8">
        <span className="text-[#8888A0]">
          This entity is not publicly traded. SEC Form 4 filings are required for insiders of both public and private issuers.
        </span>
      </div>

      {/* Insider Roster */}
      <div className="mb-8">
        <SectionLabel>Insider Roster ({overview.insiders.length})</SectionLabel>
        <InsiderRoster insiders={overview.insiders} />
      </div>

      {/* All Trades — paginated */}
      <PrivateTradesTable slug={slug} initialData={trades} />
    </div>
  );
}
