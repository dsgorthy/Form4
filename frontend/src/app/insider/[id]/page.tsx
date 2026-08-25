export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency, formatPercent } from "@/lib/format";
import { formatTitle, titleSummary, titleTags } from "@/lib/title-format";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";
import { TradeOutcomeTimeline } from "@/components/trade-outcome-timeline";
import { UpgradePrompt } from "@/components/upgrade-prompt";
import { InsiderTradesTable } from "@/components/insider-trades-table";
import { InsiderScoreChart } from "@/components/insider-score-chart";
import { TickerDisplay, companyToSlug } from "@/components/ui/ticker-display";
import type { InsiderProfile, InsiderCompany, Filing, PaginatedResponse } from "@/lib/types";
import { insiderPath, idFromSlug } from "@/lib/insider-url";
import { InsiderSummary } from "@/components/entity-summary";
import { FollowCta } from "@/components/follow-cta";
import { GATED_CLASS, insiderJsonLd, jsonLdScript } from "@/lib/structured-data";

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  try {
    const res = await fetch(
      `${process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1"}/insiders/${id}`,
      { next: { revalidate: 60 } },
    );
    if (res.status === 403) {
      return { title: "Insider Profile" };
    }
    if (!res.ok) {
      return { title: "Insider Not Found", robots: { index: false, follow: true } };
    }
    const profile: InsiderProfile = await res.json();
    const tr = profile.track_record;
    // career_grade, not pit_grade: the page's own badge renders the career
    // grade, so describing the page with the other scale made the meta
    // description disagree with the page it describes.
    const grade = (profile as any).best_career_grade;
    const parts: string[] = [];
    if (grade) parts.push(`Grade ${grade}`);
    if (tr) parts.push(`${tr.buy_count + tr.sell_count} trades across ${tr.n_tickers} companies`);
    const description = parts.length > 0
      ? `${profile.name} insider trading profile. ${parts.join(". ")}. SEC Form 4 analysis on Form4.app.`
      : `${profile.name} insider trading profile on Form4.app.`;
    return {
      // Canonical points at the slugged form so the bare-ID URL (still valid,
      // and what older links use) does not split ranking signal with it.
      alternates: { canonical: `https://form4.app${insiderPath(profile.name, idFromSlug(id), (profile as any).slug)}` },
      title: `${profile.name} — Insider Profile`,
      description,
      openGraph: { title: `${profile.name} — Insider Profile`, description,
                   siteName: "Form4", type: "profile" },
      // Same reason as the company page: the root layout declares a site-wide
      // twitter block, and page metadata that sets only openGraph inherits it
      // — so every insider profile unfurled as the generic Form4 card.
      twitter: {
        card: "summary_large_image",
        title: `${profile.name} — Insider Profile`,
        description,
      },
    };
  } catch {
    return { title: "Insider Not Found", robots: { index: false, follow: true } };
  }
}

function StatBox({
  label,
  value,
  sub,
  color,
}: {
  label: string;
  value: string;
  sub?: string;
  color?: string;
}) {
  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#81819A] mb-1">
        {label}
      </div>
      <div className={`text-xl font-mono font-bold ${color || "text-[#E8E8ED]"}`}>{value}</div>
      {sub && <div className="text-xs text-[#81819A] mt-0.5">{sub}</div>}
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    // <h2>, not a styled <div>. These label the real content sections of an
    // indexed page — "Insider Roster", "Recent Insider Trades" — and as divs
    // they carried no structure at all: every SEO surface rendered exactly one
    // heading, the H1, with nothing beneath it. Tailwind's preflight zeroes
    // heading margins so this is visually identical.
    <h2 className="text-[10px] font-semibold uppercase tracking-widest text-[#81819A] mb-3">
      {children}
    </h2>
  );
}

export default async function InsiderPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let profile: InsiderProfile;
  let companies: { companies: InsiderCompany[] };
  let trades: PaginatedResponse<Filing>;
  let returnDist: {
    bins: { label: string; count: number; avg_return: number }[];
    total_trades: number;
    win_count: number;
    loss_count: number;
    dominant_type?: string;
    timeline?: { date: string; ticker: string; trade_type: string; value: number; return_pct: number }[];
    avg_return_pct?: number;
    global_avg_pct?: number;
  } | null = null;

  try {
    [profile, companies, trades] = await Promise.all([
      fetchAPIAuth<InsiderProfile>(`/insiders/${id}`),
      fetchAPIAuth<{ companies: InsiderCompany[] }>(`/insiders/${id}/companies`),
      fetchAPIAuth<PaginatedResponse<Filing>>(`/insiders/${id}/trades`, { limit: "25" }),
    ]);
    try {
      returnDist = await fetchAPIAuth(`/insiders/${id}/return-distribution`, { window: "7d" });
    } catch {}
  } catch (e: any) {
    if (e.message?.includes("403")) {
      return (
        <div>
          <nav className="flex items-center gap-2 text-sm text-[#81819A] mb-6">
            <Link href="/" className="hover:text-[#8888A0] transition-colors">Dashboard</Link>
            <span>/</span>
            <Link href="/leaderboard" className="hover:text-[#8888A0] transition-colors">Leaderboard</Link>
            <span>/</span>
            <span className="text-[#E8E8ED]">Insider Profile</span>
          </nav>
          <UpgradePrompt feature="Insider Profiles">
            <div className="space-y-6">
              <div className="h-8 w-48 bg-[#1A1A26] rounded" />
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {[1, 2, 3, 4].map((i) => (
                  <div key={i} className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4 h-20" />
                ))}
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5 h-40" />
                <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5 h-40" />
              </div>
            </div>
          </UpgradePrompt>
        </div>
      );
    }
    notFound();
  }

  // Retired slugs (insider_slug_aliases) are redirected to the canonical URL
  // in middleware.ts, NOT here. A page-level permanentRedirect() does not work
  // on this route: generateMetadata resolves first and flushes the document
  // head, and once the response is streaming the status code is already
  // settled, so the redirect never reaches the client.
  const tr = profile.track_record;
  // Server-provided: the API sets this for any non-Pro viewer. Used instead
  // of a client-side Clerk check so the server-rendered HTML — the only
  // version a crawler sees — is already correct.
  const isGated = (profile as any).gated === true;

  // Hoisted above the JSON-LD: a Person node with jobTitle and worksFor links
  // this page to the employer as an entity, rather than leaving a bare name.
  // Same derivation the summary sentence uses — by trade COUNT, since a single
  // large sale makes someone the biggest filer, not the most active.
  const primaryCompany = companies.companies.length
    ? [...companies.companies].sort((a, b) => b.trade_count - a.trade_count)[0]
    : null;
  const primaryTitle =
    formatTitle((primaryCompany as any)?.normalized_title || primaryCompany?.title) ||
    formatTitle(tr?.primary_title);

  return (
    <div>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={jsonLdScript(
          insiderJsonLd({
            name: profile.name,
            slug: (profile as any).slug || idFromSlug(id),
            title: primaryTitle,
            company: primaryCompany?.company,
            ticker: primaryCompany?.ticker,
            totalTrades: (tr?.buy_count ?? 0) + (tr?.sell_count ?? 0),
          }),
        )}
      />
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#81819A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <Link href="/leaderboard" className="hover:text-[#8888A0] transition-colors">
          Leaderboard
        </Link>
        <span>/</span>
        <span className="text-[#E8E8ED]">{profile.name}</span>
      </nav>

      {/* Header */}
      <div className="flex items-center gap-4 mb-2 flex-wrap">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">{profile.name}</h1>
        {/* One rating. This drew a "Career" badge and a "Form" badge side by
            side — two scores of the same person on two scales, and Recent Form
            is not a scale we publish any more. See api/ratings.py. */}
        <InsiderGradeBadge
          grade={(profile as any).best_career_grade}
          bestTicker={(profile as any).best_career_ticker}
          tickerCount={(profile as any).n_scored_tickers}
          showLabel
        />
      </div>
      {(() => {
        const cos = companies.companies;
        const primary = primaryCompany;
        const title = primaryTitle;
        const otherCount = cos.length > 1 ? cos.length - 1 : 0;
        const skipTitle = !title;
        const totalTrades = (tr?.buy_count ?? 0) + (tr?.sell_count ?? 0);
        const lastTrade = cos.length
          ? [...cos].sort((a, b) => (b.last_trade > a.last_trade ? 1 : -1))[0].last_trade
          : null;
        // Earliest disclosure on record. Buys and sells carry separate first
        // dates, and an insider may have only one of the two.
        const firstTrade =
          [tr?.buy_first_date, tr?.sell_first_date].filter(Boolean).sort()[0] ?? null;
        return (
          <>
            {/* Sits directly under the H1 — the position competitors use and
                the text Google lifts as the snippet. */}
            <InsiderSummary
              name={profile.name}
              title={titleSummary(primaryTitle)}
              companyName={primary?.company}
              ticker={primary?.ticker}
              nCompanies={cos.length}
              totalTrades={totalTrades}
              lastTrade={lastTrade}
              firstTrade={firstTrade}
            />
            {!skipTitle && (
              <p className="text-sm text-[#8888A0] mb-1">
                {title}
                {primary && primary.ticker !== "NONE" && (
                  <>
                    {" at "}
                    <Link
                      href={`/company/${primary.ticker}`}
                      className="text-blue-400 hover:text-blue-300"
                    >
                      {primary.ticker}
                    </Link>
                  </>
                )}
                {otherCount > 0 && (
                  <span className="text-[#81819A]">
                    {" "}(+{otherCount} {otherCount === 1 ? "company" : "companies"})
                  </span>
                )}
              </p>
            )}
            <p className="text-xs text-[#81819A] mb-8">
              {profile.cik && `CIK: ${profile.cik}`}
              {profile.cik && tr && " · "}
              {tr && `${tr.n_tickers} ${tr.n_tickers === 1 ? "company" : "companies"} traded`}
            </p>
          </>
        );
      })()}

      {/* Entity Relationships */}
      {profile.entity_group && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 px-4 py-3 mb-6">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#81819A] mb-2">
            Entity Group
          </div>
          <div className="text-sm text-[#8888A0]">
            {profile.is_entity ? (
              <span>
                Controlled by{" "}
                <Link
                  href={insiderPath(
                    profile.entity_group.members.find((m: any) => m.is_primary)?.name,
                    profile.entity_group.primary_insider_id,
                  )}
                  className="text-blue-400 hover:text-blue-300"
                >
                  {profile.entity_group.members.find((m: any) => m.is_primary)?.name || "Unknown"}
                </Link>
              </span>
            ) : (
              <span>Related entities:</span>
            )}
            <div className="flex flex-wrap gap-2 mt-2">
              {profile.entity_group.members
                .filter((m: any) => m.insider_id !== id)
                .map((m: any) => (
                  <Link
                    key={m.insider_id}
                    href={insiderPath(m.name, m.insider_id)}
                    className="rounded-md border border-[#2A2A3A] px-2 py-1 text-xs text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#2A2A3A]/40 transition-colors"
                  >
                    {m.name}
                    {m.is_entity ? " (entity)" : ""}
                  </Link>
                ))}
            </div>
          </div>
        </div>
      )}

      {/* Position & Last 12 Months.

          Placed above the track-record material, and UNGATED, because these
          are the two questions a visitor actually arrives with: what does
          this person own, and what have they done lately. Both come straight
          off the Form 4 -- `shares_owned_after` marked at the last close, and
          a twelve-month rollup -- rather than out of anything we score, so
          there is nothing here that gating would be protecting.

          Counted in FILINGS, not execution lots. A purchase filled in five
          tranches is one decision, and showing it as five was what made
          every sell on this page read as a flurry. */}
      {(() => {
        const holdings = profile.holdings ?? [];
        const ttm = profile.ttm;
        const held = holdings.filter((h) => h.shares > 0);
        const ttmActive = !!ttm && (ttm.buys.filings > 0 || ttm.sells.filings > 0);
        if (!held.length && !ttmActive) return null;

        const totalValue = held.reduce((a, h) => a + (h.value ?? 0), 0);
        // Only sum tickers we could actually price. Treating an unpriced
        // holding as $0 would understate the total silently; saying so is
        // better than quietly being wrong.
        const unpriced = held.filter((h) => h.value == null).length;
        const asOf = held.map((h) => h.as_of).filter(Boolean).sort().pop();

        return (
          <div className="grid gap-4 md:grid-cols-2 mb-8">
            {held.length > 0 && (
              <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
                <SectionLabel>Current Position</SectionLabel>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-[10px] uppercase tracking-widest text-[#5A5A70]">
                      <th className="text-left font-semibold pb-2">Ticker</th>
                      <th className="text-right font-semibold pb-2">Shares</th>
                      <th className="text-right font-semibold pb-2">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {held.map((h) => (
                      <tr key={h.ticker} className="border-t border-[#2A2A3A]/60">
                        <td className="py-2">
                          <TickerDisplay ticker={h.ticker} />
                        </td>
                        <td className="py-2 text-right font-mono tabular-nums text-[#E8E8ED]">
                          {h.shares.toLocaleString()}
                        </td>
                        <td className="py-2 text-right font-mono tabular-nums text-[#E8E8ED]">
                          {h.value != null ? formatCurrency(h.value) : "\u2014"}
                        </td>
                      </tr>
                    ))}
                    {held.length > 1 && (
                      <tr className="border-t border-[#2A2A3A]">
                        <td className="py-2 text-[#81819A]">Total</td>
                        <td />
                        <td className="py-2 text-right font-mono tabular-nums font-bold text-[#E8E8ED]">
                          {formatCurrency(totalValue)}
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
                <p className="text-[11px] text-[#5A5A70] mt-3 leading-relaxed">
                  Shares reported on the most recent Form 4
                  {asOf ? `, priced at the ${asOf} close` : ""}.
                  {unpriced > 0 &&
                    ` ${unpriced} holding${unpriced > 1 ? "s" : ""} could not be priced and ${unpriced > 1 ? "are" : "is"} excluded from the total.`}
                </p>
              </div>
            )}

            {ttmActive && (
              <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
                <SectionLabel>Last 12 Months</SectionLabel>
                <div className="grid grid-cols-2 gap-4">
                  {([["Bought", ttm!.buys, "text-emerald-400"],
                     ["Sold", ttm!.sells, "text-red-400"]] as const).map(
                    ([label, side, color]) => (
                      <div key={label}>
                        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#81819A] mb-1">
                          {label}
                        </div>
                        <div className={`text-xl font-mono font-bold tabular-nums ${side.filings ? color : "text-[#5A5A70]"}`}>
                          {side.filings ? formatCurrency(side.value) : "\u2014"}
                        </div>
                        <div className="text-xs text-[#81819A] mt-0.5">
                          {side.filings === 0
                            ? "No filings"
                            : `${side.filings} filing${side.filings > 1 ? "s" : ""} \u00b7 ${side.shares.toLocaleString()} sh`}
                        </div>
                      </div>
                    ),
                  )}
                </div>
                <p className="text-[11px] text-[#5A5A70] mt-3 leading-relaxed">
                  Counted per filing, not per execution lot \u2014 a purchase
                  filled in several tranches is one decision.
                </p>
              </div>
            )}
          </div>
        );
      })()}

      {/* Score */}
      {tr && (() => {
        const fc = profile.filing_counts;
        const buyCount = fc?.buy ?? tr.buy_count;
        const sellCount = fc?.sell ?? tr.sell_count;
        return (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
            {/* "Track Record 3.00 / 100th percentile" was itr.score and
                itr.percentile — the insider_track_records family, recomputed
                over the entire history on every refresh and a documented PIT
                violation. It also sat directly beside the Insider Rating,
                giving the same person two scores on two scales, which is the
                thing the rating taxonomy exists to stop. Removed 2026-08-19;
                the rating in the header is the answer.

                Best Window stays, gated: it names a holding period, not a
                score, and a StatBox reading "\u2014" for an anonymous visitor
                says we have no data on this person, which is the opposite of
                the pitch. */}
            {!isGated && <StatBox label="Best Window" value={tr.best_window || "\u2014"} />}
            <StatBox label="Tickers Traded" value={String(tr.n_tickers)} />
            <StatBox
              label="Total Filings"
              value={String(buyCount + sellCount)}
              sub={`${buyCount} buys, ${sellCount} sells`}
            />
          </div>
        );
      })()}

      {/* Grade by Ticker */}
      {(profile as any).ticker_grades?.length > 0 && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4 mb-8">
          <h3 className="text-xs font-semibold uppercase tracking-widest text-[#81819A] mb-3">
            Grade by Ticker
          </h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2">
            {((profile as any).ticker_grades as { ticker: string; grade: string | null; score: number | null; career_grade: string | null; career_score: number | null; trade_count: number; career_trade_count: number }[]).map(
              (tg, i) => (
                <Link
                  key={tg.ticker}
                  href={`/company/${tg.ticker}`}
                  className={`flex items-center justify-between rounded-md border px-3 py-2 transition-colors ${
                    i === 0
                      ? "border-[#F59E0B]/30 bg-[#F59E0B]/5"
                      : "border-[#2A2A3A] hover:border-[#3A3A4A]"
                  }`}
                >
                  <div className="flex items-center gap-2 min-w-0">
                    {tg.career_grade && <InsiderGradeBadge grade={tg.career_grade} compact tooltip={`Career: ${tg.career_grade}`} />}
                    {tg.grade && tg.grade !== tg.career_grade && (
                      <span className="text-[9px] text-[#81819A] font-mono">
                        Form&nbsp;<span style={{ color: "#8888A0" }}>{tg.grade}</span>
                      </span>
                    )}
                    <span className="text-sm font-mono text-[#E8E8ED] truncate">{tg.ticker}</span>
                  </div>
                  {/* Sample size behind the grade. A scored row with
                      trade_count 0 is normal, not pending: the trade that
                      created the row is excluded by the return lag, so a
                      first-ever trade in this ticker leaves nothing behind it
                      and the grade comes off the career record instead. Say
                      which record it is. "awaiting returns" is reserved for
                      rows carrying no score at all. */}
                  <span className="text-[10px] text-[#81819A] font-mono shrink-0 ml-2">
                    {tg.trade_count > 0
                      ? `${tg.trade_count} ${tg.trade_count === 1 ? "trade" : "trades"}`
                      : tg.career_trade_count > 0
                        ? `${tg.career_trade_count} career ${tg.career_trade_count === 1 ? "trade" : "trades"}`
                        : "awaiting returns"}
                  </span>
                </Link>
              ),
            )}
          </div>
        </div>
      )}

      {/* Track Records + Transaction Volume.
          Suppressed entirely when gated — every metric in these tables is
          Pro-only, so for an anonymous visitor they render as a full grid of
          em-dashes. The follow CTA takes their place: it states what is behind
          the wall instead of drawing an empty one. */}
      {isGated && (
        // GATED_CLASS goes on a SERVER-rendered element, not on the CTA inside
        // it. FollowCta is a client component that renders nothing until Clerk
        // resolves, so a crawler reading the delivered HTML would find the
        // page's JSON-LD declaring a paywall over a selector matching nothing.
        // This wrapper is what makes that declaration resolve, and it names
        // what is withheld so the block is not simply a hole in the page.
        <div className={`${GATED_CLASS} mb-8`}>
          <SectionLabel>Track Record</SectionLabel>
          {/* Show the SHAPE of what is withheld, not a paragraph describing it.
              A reader deciding whether to pay needs to see that there are nine
              specific measurements behind the wall — three metrics across three
              windows — which a sentence cannot convey. The tiles are the real
              layout the Pro view renders, with the figures replaced. */}
          {/* A real table, because this is tabular data: three metrics across
              three windows. The first attempt laid it out as a CSS grid with
              three columns and then emitted four cells per row — a label plus
              the windows — so every row wrapped and the whole thing collapsed
              into a list. A <table> cannot make that mistake.

              The placeholder bars are aria-hidden with one caption explaining
              the whole block, rather than nine repetitions of "Pro subscribers
              only" that a screen reader would read out one at a time and any
              copy-paste would pick up. */}
          <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <caption className="sr-only">
                  Track record for {profile.name}. Available to Pro subscribers.
                </caption>
                <thead>
                  <tr className="border-b border-[#2A2A3A]">
                    <th scope="col" className="pb-2 text-left text-[10px] font-medium uppercase tracking-wider text-[#81819A]">
                      After
                    </th>
                    {["7 days", "30 days", "90 days"].map((w) => (
                      <th key={w} scope="col" className="pb-2 text-right text-[10px] font-medium uppercase tracking-wider text-[#81819A]">
                        {w}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {["Win rate", "Average move", "Alpha vs SPY"].map((metric) => (
                    <tr key={metric} className="border-b border-[#2A2A3A]/40 last:border-0">
                      <th scope="row" className="py-3 text-left font-normal text-[#8888A0]">
                        {metric}
                      </th>
                      {[0, 1, 2].map((i) => (
                        <td key={i} className="py-3 text-right">
                          <span
                            className="inline-block h-3.5 w-12 rounded bg-[#2A2A3A]"
                            aria-hidden="true"
                          />
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="mt-4 text-sm text-[#8888A0]">
              Every purchase below already shows what the stock did afterwards.
              Pro is what they add up to for {profile.name}, measured against
              SPY over the same windows.
            </p>
          </div>
          <FollowCta
            entity={profile.name}
            detail="Win rate, average move and alpha across 7/30/90-day windows"
          />
        </div>
      )}
      {tr && !isGated && (() => {
        const fc = profile.filing_counts;
        const fs = profile.filing_stats;
        const buyCount = fc?.buy ?? tr.buy_count;
        const sellCount = fc?.sell ?? tr.sell_count;
        const buyWinRate = fs?.buy_win_rate_7d ?? tr.buy_win_rate_7d;
        const buyAvgReturn = fs?.buy_avg_return_7d ?? tr.buy_avg_return_7d;
        const buyAvgAlpha = fs?.buy_avg_abnormal_7d ?? tr.buy_avg_abnormal_7d;
        const sellWinRate = fs?.sell_win_rate_7d ?? tr.sell_win_rate_7d;
        const sellAvgReturn = fs?.sell_avg_return_7d ?? tr.sell_avg_return_7d;
        return (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
          {/* Buy Track Record */}
          {buyCount > 0 && (
            <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
              <SectionLabel>Buy Track Record</SectionLabel>
              <div className="text-sm">
                <div className="flex justify-between mb-3">
                  <span className="text-[#8888A0]">Filings</span>
                  <span className="font-mono text-[#E8E8ED]">{buyCount}</span>
                </div>
                {/* Multi-window table */}
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-[#81819A]">
                      <th className="text-left font-medium pb-1.5"></th>
                      {["7d", "30d", "90d"].map(w => (
                        <th key={w} className={`text-right font-medium pb-1.5 ${tr.best_window === w ? "text-[#3B82F6]" : ""}`}>
                          {w}{tr.best_window === w ? " *" : ""}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="font-mono">
                    <tr>
                      <td className="text-[#8888A0] py-1">Accuracy</td>
                      {[buyWinRate, tr.buy_win_rate_30d, tr.buy_win_rate_90d].map((wr, i) => (
                        <td key={i} className={`text-right py-1 ${wr != null && wr >= 0.6 ? "text-[#22C55E]" : wr != null ? "text-[#E8E8ED]" : "text-[#81819A]"}`}>
                          {wr != null ? `${(wr * 100).toFixed(0)}%` : "\u2014"}
                        </td>
                      ))}
                    </tr>
                    <tr>
                      <td className="text-[#8888A0] py-1">Avg Move</td>
                      {[buyAvgReturn, tr.buy_avg_return_30d, tr.buy_avg_return_90d].map((r, i) => (
                        <td key={i} className={`text-right py-1 ${r != null ? (r >= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#81819A]"}`}>
                          {r != null ? formatPercent(r) : "\u2014"}
                        </td>
                      ))}
                    </tr>
                    <tr>
                      <td className="text-[#8888A0] py-1">Alpha</td>
                      {[buyAvgAlpha, tr.buy_avg_abnormal_30d, tr.buy_avg_abnormal_90d].map((r, i) => (
                        <td key={i} className={`text-right py-1 ${r != null ? (r >= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#81819A]"}`}>
                          {r != null ? formatPercent(r) : "\u2014"}
                        </td>
                      ))}
                    </tr>
                  </tbody>
                </table>
                {tr.best_window && (
                  <div className="text-[10px] text-[#81819A] mt-2">* Best window</div>
                )}
              </div>
            </div>
          )}

          {/* Sell Track Record */}
          {sellCount > 0 && (
            <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
              <SectionLabel>Sell Track Record</SectionLabel>
              <div className="text-sm">
                <div className="flex justify-between mb-3">
                  <span className="text-[#8888A0]">Filings</span>
                  <span className="font-mono text-[#E8E8ED]">{sellCount}</span>
                </div>
                {sellWinRate != null ? (
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-[#81819A]">
                        <th className="text-left font-medium pb-1.5"></th>
                        <th className="text-right font-medium pb-1.5">7d</th>
                        <th className="text-right font-medium pb-1.5">30d</th>
                        <th className="text-right font-medium pb-1.5">90d</th>
                      </tr>
                    </thead>
                    <tbody className="font-mono">
                      <tr>
                        <td className="text-[#8888A0] py-1">Accuracy</td>
                        {[sellWinRate, tr.sell_win_rate_30d, tr.sell_win_rate_90d].map((wr, i) => (
                          <td key={i} className={`text-right py-1 ${wr != null && wr >= 0.6 ? "text-[#22C55E]" : wr != null ? "text-[#E8E8ED]" : "text-[#81819A]"}`}>
                            {wr != null ? `${(wr * 100).toFixed(0)}%` : "\u2014"}
                          </td>
                        ))}
                      </tr>
                      <tr>
                        <td className="text-[#8888A0] py-1">Avg Move</td>
                        {[sellAvgReturn, tr.sell_avg_return_30d, tr.sell_avg_return_90d].map((r, i) => (
                          <td key={i} className={`text-right py-1 ${r != null ? (r <= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#81819A]"}`}>
                            {r != null ? formatPercent(r) : "\u2014"}
                          </td>
                        ))}
                      </tr>
                    </tbody>
                  </table>
                ) : (
                  <div className="text-xs text-[#81819A]">Not yet computed</div>
                )}
                {profile.sell_pattern && profile.sell_pattern.total_sells > 0 && (() => {
                  const sp = profile.sell_pattern!;
                  const routinePct = Math.round((sp.routine_sells / sp.total_sells) * 100);
                  const plannedPct = Math.round((sp.planned_sells / sp.total_sells) * 100);
                  if (routinePct < 20 && plannedPct < 20) return null;
                  return (
                    <div className="border-t border-[#2A2A3A]/50 mt-3 pt-2 space-y-1">
                      <div className="flex justify-between text-sm">
                        <span className="text-[#8888A0]">Routine</span>
                        <span className="font-mono text-[#E8E8ED]">{routinePct}%</span>
                      </div>
                      {plannedPct > 0 && (
                        <div className="flex justify-between text-sm">
                          <span className="text-[#8888A0]">10b5-1 Plan</span>
                          <span className="font-mono text-[#E8E8ED]">{plannedPct}%</span>
                        </div>
                      )}
                    </div>
                  );
                })()}
              </div>
            </div>
          )}

          {/* Transaction Volume by Type */}
          {profile.volume_by_type && profile.volume_by_type.length > 0 && (
            <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
              <SectionLabel>Volume by Type</SectionLabel>
              <div className="space-y-2">
                {profile.volume_by_type.map((v) => (
                  <div key={`${v.trans_code}-${v.trade_type}`} className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      <span className={`font-mono text-xs px-1.5 py-0.5 rounded border ${
                        v.trans_code === "P" ? "border-[#22C55E]/30 text-[#22C55E]" :
                        v.trans_code === "S" ? "border-[#EF4444]/30 text-[#EF4444]" :
                        "border-[#2A2A3A] text-[#81819A]"
                      }`}>{v.trans_code}</span>
                      <span className="text-[#8888A0] text-xs">{v.label}</span>
                    </div>
                    <div className="text-right">
                      <div className="font-mono text-xs text-[#E8E8ED]">{formatCurrency(v.total_value)}</div>
                      <div className="text-[10px] text-[#81819A]">{v.count} filings</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
        );
      })()}

      {/* PIT Score Over Time */}
      <div className="mb-8">
        <InsiderScoreChart insiderId={id} />
      </div>

      {/* Trade Performance Scatter */}
      {returnDist && returnDist.timeline && returnDist.timeline.length > 0 && (
        <div className="mb-8">
          <TradeOutcomeTimeline
            timeline={returnDist.timeline}
            dominantType={returnDist.dominant_type || "buy"}
          />
        </div>
      )}

      {/* Companies */}
      {companies.companies.length > 0 && (
        <div className="mb-8">
          <SectionLabel>Companies</SectionLabel>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {companies.companies.map((c) => {
              const isPrivate = c.ticker === "NONE";
              const cardHref = isPrivate
                ? `/company/private/${companyToSlug(c.company)}`
                : `/company/${c.ticker}`;
              return (
                <Link
                  key={isPrivate ? c.company : c.ticker}
                  href={cardHref}
                  className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4 hover:bg-[#2A2A3A]/40 transition-colors"
                >
                  <div className="flex items-center justify-between mb-1">
                    <TickerDisplay ticker={c.ticker} company={c.company} href={null} />
                    <span className="text-xs text-[#81819A]">{c.trade_count} trades</span>
                  </div>
                  {!isPrivate && (
                    <div className="text-xs text-[#8888A0] truncate">{c.company}</div>
                  )}
                  <div className="text-xs text-[#81819A] mt-1">
                    {(() => {
                      const t = formatTitle((c as any).normalized_title || c.title);
                      return t ? `${t} · ` : "";
                    })()}{formatCurrency(c.total_value)}
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      )}

      {/* Trade History */}
      <InsiderTradesTable
        identifier={id}
        initialData={trades}
        volumeByType={profile.volume_by_type}
      />
    </div>
  );
}
