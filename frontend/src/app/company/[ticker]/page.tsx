export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { fetchAPIAuth } from "@/lib/auth";
import { ProGate } from "@/components/pro-gate";
import { FollowCta } from "@/components/follow-cta";
import { formatCurrency } from "@/lib/format";
import { WatchButton } from "@/components/watch-button";
import { CompanySummary } from "@/components/entity-summary";
import { companyJsonLd, jsonLdScript } from "@/lib/structured-data";
import { InsiderRoster } from "@/components/insider-roster";
import type { Filing, PaginatedResponse } from "@/lib/types";

interface CompanyOverview {
  company: string;
  ticker: string;
  total_trades: number;
  total_value: number;
  first_trade: string;
  last_trade: string;
  // Aggregates for the summary sentence. Optional so a frontend deployed ahead
  // of the API degrades to a shorter sentence rather than rendering NaN.
  distinct_insiders?: number;
  buy_value_6mo?: number;
  sell_value_6mo?: number;
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

export async function generateMetadata({ params }: { params: Promise<{ ticker: string }> }): Promise<Metadata> {
  const { ticker } = await params;
  try {
    const overview = await fetchAPI<CompanyOverview>(`/companies/${ticker}`);
    const insiderCount = overview.insiders.length;
    const title = `${overview.ticker} Insider Trading — ${overview.company}`;
    const description = `Track insider trades at ${overview.company} (${overview.ticker}). ${overview.total_trades} trades by ${insiderCount} insiders. Real-time SEC Form 4 analysis on Form4.app.`;
    return {
      title,
      description,
      // /explore?ticker= canonicalises to this page, so this page must declare
      // itself canonical — otherwise the consolidation points at a URL that
      // expresses no preference of its own.
      alternates: { canonical: `https://form4.app/company/${ticker}` },
      openGraph: { title, description },
    };
  } catch {
    return { title: `${ticker} — Insider Trades` };
  }
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    // <h2>, not a styled <div>. These label the real content sections of an
    // indexed page — "Insider Roster", "Recent Insider Trades" — and as divs
    // they carried no structure at all: every SEO surface rendered exactly one
    // heading, the H1, with nothing beneath it. Tailwind's preflight zeroes
    // heading margins so this is visually identical.
    <h2 className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
      {children}
    </h2>
  );
}

const TRADES_LIMIT = 25;
const CONGRESS_LIMIT = 10;

// How many trades the SEO surface renders. Enough to be substantive
// content for a crawler, few enough that it stays a teaser for the tool.
const SEO_TRADE_ROWS = 15;

export default async function CompanyPage({ params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;

  let overview: CompanyOverview;
  let trades: PaginatedResponse<Filing>;

  try {
    [overview, trades] = await Promise.all([
      fetchAPIAuth<CompanyOverview>(`/companies/${ticker}`),
      fetchAPIAuth<PaginatedResponse<Filing>>(`/companies/${ticker}/trades`, { limit: String(TRADES_LIMIT) }),
    ]);
  } catch {
    notFound();
  }

  // Fetch congress trades (non-blocking — page still renders if this fails)
  let congressData: { ticker: string; trades: unknown[]; total: number; limit: number; offset: number } | null = null;
  try {
    congressData = await fetchAPIAuth<{
      ticker: string;
      trades: unknown[];
      total: number;
      limit: number;
      offset: number;
    }>(`/congress/by-ticker/${ticker}`, { limit: String(CONGRESS_LIMIT) });
  } catch {}

  // "Most active filer" by trade COUNT, not dollar value — a single large sale
  // makes someone the biggest, not the most active, and the sentence claims
  // activity.
  const topFiler = [...overview.insiders].sort(
    (a, b) => (b.trade_count ?? 0) - (a.trade_count ?? 0),
  )[0];

  return (
    <div>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={jsonLdScript(
          companyJsonLd({
            ticker: overview.ticker,
            company: overview.company,
            totalTrades: overview.total_trades,
            distinctInsiders: overview.distinct_insiders ?? overview.insiders.length,
            firstTrade: overview.first_trade,
            lastTrade: overview.last_trade,
          }),
        )}
      />
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span>Company</span>
        <span>/</span>
        <span className="text-[#E8E8ED]">{overview.ticker}</span>
      </nav>

      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl md:text-3xl font-bold font-mono text-[#E8E8ED]">
            {overview.ticker}{" "}
            {/* Explicit space: JSX trims whitespace at line boundaries, so
                without it the heading extracts as "NVDANVIDIA CORP" — one
                garbage token where two real search terms should be.
                The company name carries the search term — people search
                "NVIDIA insider trading", not "NVDA". Visually secondary so the
                ticker still reads as the headline. */}
            <span className="ml-3 font-sans text-base font-normal text-[#8888A0] md:text-lg">
              {overview.company} insider trading
            </span>
          </h1>
          <WatchButton ticker={overview.ticker} />
        </div>
        {/* Replaces a bare repeat of the company name, which the H1 already
            carries. Google quotes a sentence; it cannot quote a stat row. */}
        <CompanySummary
          ticker={overview.ticker}
          company={overview.company}
          totalTrades={overview.total_trades}
          distinctInsiders={overview.distinct_insiders ?? overview.insiders.length}
          buyValue6mo={overview.buy_value_6mo ?? 0}
          sellValue6mo={overview.sell_value_6mo ?? 0}
          firstTrade={overview.first_trade}
          topInsiderName={topFiler?.name}
          topInsiderTitle={topFiler?.normalized_title || topFiler?.title}
        />
        <div className="flex flex-wrap items-center gap-2 md:gap-4 mt-2 text-xs text-[#55556A]">
          <span>{overview.total_trades} total trades</span>
          <span>{formatCurrency(overview.total_value)} total value</span>
          <span>
            {overview.first_trade} to {overview.last_trade}
          </span>
        </div>
      </div>

      {/* Convergence Banner */}
      {congressData && congressData.total > 0 && (
        <div className="rounded-lg border border-[#F59E0B]/30 bg-[#F59E0B]/10 px-4 py-3 text-sm mb-8">
          <span className="font-medium text-[#F59E0B]">Convergence Signal:</span>
          <span className="text-[#E8E8ED] ml-2">
            {congressData.total} politician trade{congressData.total !== 1 ? "s" : ""} detected
            for {ticker} alongside insider activity
          </span>
        </div>
      )}

      {/* Primary route into the product. /company/[ticker] is an SEO surface:
          indexable, public, deliberately a SUBSET. Explore is the actual
          tool, so every visitor who wants to DO something goes there. */}
      <Link
        href={`/explore?ticker=${overview.ticker}`}
        className="mb-8 flex items-center justify-between gap-4 rounded-lg border border-[#3B82F6]/40 bg-[#3B82F6]/10 px-5 py-4 transition-colors hover:border-[#3B82F6] hover:bg-[#3B82F6]/15"
      >
        <div>
          <div className="text-sm font-semibold text-[#E8E8ED]">
            Open {overview.ticker} in Explore
          </div>
          <div className="mt-0.5 text-xs text-[#8888A0]">
            Full trade history, insider track records, price chart and
            congressional activity
          </div>
        </div>
        <span className="whitespace-nowrap text-sm font-medium text-[#3B82F6]">
          Open &rarr;
        </span>
      </Link>

      {/* Insider Roster — names + grades are the most valuable indexable
          content on the page, so it stays. */}
      <div className="mb-8">
        <SectionLabel>Insider Roster ({overview.insiders.length})</SectionLabel>
        {/* Names, titles, trade counts and dollar values are public: they are
            the indexable substance of this page and what every competitor
            publishes freely. Only the scoring is gated, and the top insider's
            grade stays visible as proof. Blurring the whole roster hid the
            content Google came for. */}
        <InsiderRoster insiders={overview.insiders} gated />
        <FollowCta
          entity={ticker.toUpperCase()}
          detail={`Grades for ${Math.max(overview.insiders.length - 1, 0)} more insiders at ${ticker.toUpperCase()}`}
        />
      </div>

      {/* Recent trades — a static, crawlable subset. The paginated
          TradesTable, the price chart and the congress table are tool
          features and live in Explore; rendering them here duplicated
          the tool on a page whose job is to rank in search. */}
      <div className="mb-8">
        <SectionLabel>Recent Insider Trades</SectionLabel>
        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#2A2A3A] text-xs uppercase text-[#55556A]">
                <th className="px-4 py-2 text-left font-medium">Insider</th>
                <th className="px-4 py-2 text-left font-medium">Title</th>
                <th className="px-4 py-2 text-left font-medium">Type</th>
                <th className="px-4 py-2 text-right font-medium">Value</th>
                <th className="px-4 py-2 text-right font-medium">Date</th>
              </tr>
            </thead>
            <tbody>
              {(trades.items || []).slice(0, SEO_TRADE_ROWS).map((t) => (
                <tr key={t.trade_id} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                  <td className="px-4 py-2 text-[#E8E8ED]">{t.insider_name}</td>
                  <td className="px-4 py-2 text-[#8888A0]">
                    {t.normalized_title || t.title || "—"}
                  </td>
                  <td className="px-4 py-2">
                    <span className={t.trade_type === "buy" ? "text-[#22C55E]" : "text-[#EF4444]"}>
                      {t.trade_type === "buy" ? "Buy" : "Sell"}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-right font-mono text-[#E8E8ED]">
                    {formatCurrency(t.value)}
                  </td>
                  <td className="px-4 py-2 text-right font-mono text-[#55556A]">{t.trade_date}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {trades.total > SEO_TRADE_ROWS && (
          <Link
            href={`/explore?ticker=${overview.ticker}`}
            className="mt-3 inline-block text-sm text-[#3B82F6] transition-colors hover:text-[#60A5FA]"
          >
            View all {trades.total.toLocaleString()} trades in Explore &rarr;
          </Link>
        )}
      </div>
    </div>
  );
}
