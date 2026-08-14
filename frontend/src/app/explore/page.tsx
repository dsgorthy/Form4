export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { auth } from "@clerk/nextjs/server";
import Link from "next/link";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency } from "@/lib/format";
import { InsiderTradeChart } from "@/components/insider-trade-chart";
import { EntitySearch } from "@/components/entity-search";
import {
  ExploreResultsView,
  type ResultsTicker,
  type ResultsInsider,
} from "@/components/explore-results-view";
import {
  ExploreInsiderView,
  type InsiderProfileLite,
  type InsiderCompanyRow,
} from "@/components/explore-insider-view";
import { WatchButton } from "@/components/watch-button";
import { TradesTable } from "@/components/trades-table";
import { CongressTable } from "@/components/congress-table";
import { InsiderRoster } from "@/components/insider-roster";
import { SignInTeaser } from "@/components/signin-teaser";
import type { Filing, PaginatedResponse } from "@/lib/types";

interface CompanyOverview {
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

interface Props {
  searchParams: Promise<{
    ticker?: string;
    insider?: string;
    q?: string;
  }>;
}

// One route, three displays, resolved in priority order:
//   ?q=       an ambiguous term -> faceted results, pick from a list
//   ?insider= one person
//   ?ticker=  one company (the default)
//
// ?q= wins because it is the only one a user reaches WITHOUT having already
// chosen an entity; the other two are the outcome of that choice. ?insider=
// beats ?ticker= so a stale ticker default cannot hijack an explicit link.

/**
 * Explore holds the same content as the public /company and /insider pages —
 * it is the signed-in view of the same entity. Now that ALL search routes
 * here, both surfaces are reachable for every entity, and without a canonical
 * they compete: Google picks a winner between them and it may not be the one
 * built to rank.
 *
 * So each tool view points at its public twin, and ?q= is noindex outright —
 * a search results page is thin content by construction, and indexing one
 * generates a URL per query anyone ever typed.
 *
 * follow stays true on ?q= so the links out to real entities are still
 * crawled; it is the results page itself that should not be in the index.
 */
export async function generateMetadata({ searchParams }: Props): Promise<Metadata> {
  const sp = await searchParams;
  const q = sp.q?.trim();
  const insider = sp.insider?.trim();
  const ticker = sp.ticker?.trim();

  if (q) {
    return {
      title: `Search: ${q}`,
      robots: { index: false, follow: true },
    };
  }

  if (insider) {
    // Resolve to the stored slug rather than echoing the param: the API also
    // accepts a bare sqid and a retired slug, and a canonical pointing at
    // either of those defeats the point of having one.
    try {
      const res = await fetch(
        `${process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1"}/insiders/${insider}`,
        { next: { revalidate: 300 } },
      );
      if (res.ok) {
        const profile = await res.json();
        const slug = profile.slug || insider;
        return {
          title: `${profile.name} — Explore`,
          alternates: { canonical: `https://form4.app/insider/${slug}` },
        };
      }
    } catch {}
    return { title: "Explore" };
  }

  if (ticker) {
    const t = ticker.toUpperCase();
    return {
      title: `${t} — Explore`,
      alternates: { canonical: `https://form4.app/company/${t}` },
    };
  }

  return { title: "Explore — Companies and Insiders" };
}

const TRADES_LIMIT = 25;
const CONGRESS_LIMIT = 10;
const SEARCH_LIMIT = 50;

export default async function ExplorePage({ searchParams }: Props) {
  const sp = await searchParams;
  const queryParam = sp.q?.trim();
  const insiderParam = sp.insider?.trim();

  if (queryParam) {
    return <QueryMode term={queryParam} />;
  }

  if (insiderParam) {
    return <InsiderMode identifier={insiderParam} />;
  }

  const ticker = sp.ticker?.toUpperCase() || "AAPL";

  // Read auth BEFORE fetching. A signed-out visitor gets the teaser, so the
  // trades, roster and congress data are never fetched and never serialized
  // into the response — no flash of content, nothing to read in view-source,
  // and a faster reply to the visitor most likely to bounce.
  const { userId } = await auth();
  const signedIn = !!userId;

  let overview: CompanyOverview | null = null;
  let trades: PaginatedResponse<Filing> | null = null;
  let congressData: { ticker: string; trades: unknown[]; total: number; limit: number; offset: number } | null = null;
  let error: string | null = null;

  if (ticker) {
    try {
      // The overview IS the teaser (ticker, name, totals, date range), so it
      // is fetched either way.
      overview = await fetchAPIAuth<CompanyOverview>(`/companies/${ticker}`);
      if (signedIn) {
        trades = await fetchAPIAuth<PaginatedResponse<Filing>>(
          `/companies/${ticker}/trades`,
          { limit: String(TRADES_LIMIT) },
        );
      }
    } catch {
      error = `No data found for "${ticker}"`;
    }

    if (overview && signedIn) {
      try {
        congressData = await fetchAPIAuth<{
          ticker: string;
          trades: unknown[];
          total: number;
          limit: number;
          offset: number;
        }>(`/congress/by-ticker/${ticker}`, { limit: String(CONGRESS_LIMIT) });
      } catch {}
    }
  }

  return (
    <div>
      {/* Empty state is the ONLY place this page renders a search input. The
          nav bar carries it everywhere else, so the hero shows once, on
          arrival, and is gone as soon as there is something to look at. The
          page used to render a search bar three times, in a different position
          per mode, which read as the bar jumping around mid-search. */}
      {!overview && (
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-[#E8E8ED] mb-1">Explore</h1>
          <p className="text-sm text-[#55556A] mb-4">
            Look up any company or insider — trade history, track records, and political overlap
          </p>
          <EntitySearch variant="hero" />
        </div>
      )}

      {/* Error state */}
      {error && (
        <div className="flex flex-col items-center justify-center h-48 rounded-lg border border-[#2A2A3A] bg-[#12121A]">
          <div className="text-lg text-[#8888A0] mb-2">{error}</div>
          <p className="text-xs text-[#55556A]">Check the ticker symbol and try again</p>
        </div>
      )}

      {/* Company content */}
      {overview && (
        <div>
          {/* Company header */}
          <div className="mb-8">
            <div className="flex items-center gap-3">
              <h2 className="text-2xl md:text-3xl font-bold font-mono text-[#E8E8ED]">
                {overview.ticker}
              </h2>
              <WatchButton ticker={overview.ticker} />
            </div>
            <p className="text-[#8888A0] mt-1">{overview.company}</p>
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
              <span className="font-medium text-[#F59E0B]">
                Convergence Signal:
              </span>
              <span className="text-[#E8E8ED] ml-2">
                {congressData.total} politician trade
                {congressData.total !== 1 ? "s" : ""} detected for{" "}
                {ticker} alongside insider activity
              </span>
            </div>
          )}

          {/* Everything above stays visible to a signed-out visitor: ticker,
              totals, date range, convergence banner. That is the teaser, and
              it has to be genuinely informative or the wall reads as bait.

              Everything below is what an account buys. Keyed on signed-in
              rather than Pro — the job here is converting a cold visitor into
              a signup, and a signed-in free user should get the tool. */}
          {!signedIn ? (
            <SignInTeaser
              headline={`See every insider trade at ${overview.ticker}`}
              detail={`${overview.total_trades.toLocaleString()} trades from ${overview.insiders.length} insiders, with track records, grades and political overlap.`}
            />
          ) : (
          <>
            {/* Trade Scatter Chart */}
            <div className="mb-8">
              <InsiderTradeChart ticker={overview.ticker} />
            </div>

            {/* Insider Roster */}
            <div className="mb-8">
              <SectionLabel>
                Insider Roster ({overview.insiders.length})
              </SectionLabel>
              <InsiderRoster insiders={overview.insiders} gated />
            </div>

            {/* Political Activity (Congress) — paginated */}
            {congressData && (
              <CongressTable
                ticker={ticker}
                initialData={congressData as never}
              />
            )}

            {/* All Trades — paginated */}
            {trades && <TradesTable ticker={ticker} initialData={trades} />}
          </>
          )}
        </div>
      )}
    </div>
  );
}


async function InsiderMode({ identifier }: { identifier: string }) {
  const { userId } = await auth();
  const signedIn = !!userId;

  let profile: InsiderProfileLite | null = null;
  let companies: InsiderCompanyRow[] = [];
  let trades: PaginatedResponse<Filing> | null = null;
  let error: string | null = null;

  try {
    profile = await fetchAPIAuth<InsiderProfileLite>(`/insiders/${identifier}`);
  } catch {
    error = `No insider found for "${identifier}"`;
  }

  // Companies and trades are best-effort: a failure here should degrade the
  // page, not blank it, the way the all-or-nothing Promise.all on the public
  // profile page used to.
  if (profile && signedIn) {
    try {
      const res = await fetchAPIAuth<{ companies: InsiderCompanyRow[] }>(
        `/insiders/${identifier}/companies`,
      );
      companies = res.companies || [];
    } catch {}
    try {
      trades = await fetchAPIAuth<PaginatedResponse<Filing>>(
        `/insiders/${identifier}/trades`,
        { limit: String(TRADES_LIMIT) },
      );
    } catch {}
  }

  return (
    <div>
      {error && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-6 text-sm text-[#8888A0]">
          {error}
        </div>
      )}
      {profile && (
        <ExploreInsiderView
          profile={profile}
          companies={companies}
          trades={trades}
          teaser={
            signedIn ? undefined : (
              <SignInTeaser
                headline={`See ${profile.name}'s full trading record`}
                detail="Every filing, the companies they trade, and how those trades actually performed."
              />
            )
          }
        />
      )}
    </div>
  );
}


/**
 * Faceted results for an ambiguous term. Reached from the search dropdown's
 * group counts and from Enter-with-no-selection, which used to guess that the
 * raw text was a ticker.
 */
async function QueryMode({ term }: { term: string }) {
  let data: {
    tickers: ResultsTicker[];
    insiders: ResultsInsider[];
    ticker_total?: number;
    insider_total?: number;
  } | null = null;

  try {
    data = await fetchAPIAuth(`/search`, {
      q: term,
      limit: String(SEARCH_LIMIT),
    });
  } catch {
    // An empty result set and a failed lookup should not look different to
    // the user here — both mean "nothing to show for this term".
  }

  return (
    <div>
      <div className="mb-8">
        <EntitySearch variant="hero" initial={term} />
      </div>
      <ExploreResultsView
        term={term}
        tickers={data?.tickers ?? []}
        insiders={data?.insiders ?? []}
        tickerTotal={data?.ticker_total ?? data?.tickers.length ?? 0}
        insiderTotal={data?.insider_total ?? data?.insiders.length ?? 0}
      />
    </div>
  );
}
