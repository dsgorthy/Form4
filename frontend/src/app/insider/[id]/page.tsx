export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { RelatedInsiders, type RelatedInsider } from "@/components/related-insiders";
import { InsiderVerdict } from "@/components/insider-verdict";
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
import { PendingFollow } from "@/components/pending-follow";
import { GATED_CLASS, insiderJsonLd, jsonLdScript } from "@/lib/structured-data";
import { SectionLabel } from "@/components/ui/section-label";

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
    <div className="border-t border-[#24242F] pt-3">
      <div className="mb-1 font-mono text-[10px] font-medium uppercase tracking-[0.14em] text-[#63636F]">
        {label}
      </div>
      <div className={`text-xl font-mono font-bold ${color || "text-[#E8E8ED]"}`}>{value}</div>
      {sub && <div className="text-xs text-[#81819A] mt-0.5">{sub}</div>}
    </div>
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
  // Best-effort: the section is a navigation aid, so a failure here must
  // never cost the reader the profile they actually asked for.
  let related: { related: RelatedInsider[] } | null = null;

  try {
    [profile, companies, trades] = await Promise.all([
      fetchAPIAuth<InsiderProfile>(`/insiders/${id}`),
      fetchAPIAuth<{ companies: InsiderCompany[] }>(`/insiders/${id}/companies`),
      fetchAPIAuth<PaginatedResponse<Filing>>(`/insiders/${id}/trades`, { limit: "25" }),
    ]);
    try {
      returnDist = await fetchAPIAuth(`/insiders/${id}/return-distribution`, { window: "7d" });
    } catch {}
    try {
      related = await fetchAPIAuth(`/insiders/${id}/related`);
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

      {/* Header. NO GRADE BADGE HERE — the verdict block below renders the
          rating as a 62px glyph beside the meters it summarises. Having both
          put the same claim on the page twice, 200px apart, which is what the
          "one rating" note on InsiderGradeBadge exists to prevent. */}
      <h1 className="mb-2 font-serif text-[40px] font-medium leading-[1.05] tracking-[-0.02em] text-[#F2F2F6]">
        {profile.name}
      </h1>
      {(() => {
        const cos = companies.companies;
        const primary = primaryCompany;
        const title = primaryTitle;
        const otherCount = cos.length > 1 ? cos.length - 1 : 0;
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
              // n_tickers, not cos.length: the companies list includes
              // companies where the only filings were grants or exercises.
              // 6 there against 5 discretionary was the visible contradiction.
              nCompanies={tr?.n_tickers ?? cos.length}
              totalTrades={totalTrades}
              lastTrade={lastTrade}
              firstTrade={firstTrade}
            />
            {/* CIK only. The company count was here, in the title line above
                it, and inside the summary sentence above that — three times on
                one screen. */}
            {profile.cik && (
              <p className="mb-8 font-mono text-xs text-[#63636F]">CIK {profile.cik}</p>
            )}
          </>
        );
      })()}

      {/* Why an active filer can still be Unrated. Before 2026-08-25 the grade
          was built from every row with trade_type='buy' — 42% compensation
          grants and 39% option exercises. It now counts open-market purchases
          only, so plenty of people with a long filing history have nothing to
          grade. Saying so beats letting the reader assume we have no data. */}
      {!(profile as any).best_career_grade && (
        <p className="text-sm text-[#8888A0] mb-3 max-w-2xl">
          <span className="text-[#E8E8ED]">Unrated.</span>{" "}
          We grade insiders only on stock they chose to buy on the open market.
          Grants, option exercises and vesting say nothing about timing, so they
          don&apos;t count — which is why someone can file often and still have
          no grade. It isn&apos;t a bad sign: unrated buys beat every graded
          tier below A.
        </p>
      )}

      {/* The opening: a written verdict, then the same three numbers as signed
          meters. Summary before detail — the tables below still carry every
          window and both sides; this states the conclusion first, which is
          what a visitor arriving from search needs and what thirteen identical
          boxes never gave them.

          Every figure is from filing_stats, already allowlisted public and
          already floored at MIN_SCORED_FILINGS upstream of gating, so this
          shows a gated reader nothing new — it just stops making them
          assemble it. */}
      <InsiderVerdict
        name={profile.name}
        stats={profile.filing_stats}
        buyCount={profile.filing_counts?.buy ?? tr?.buy_count ?? 0}
        sellCount={profile.filing_counts?.sell ?? tr?.sell_count ?? 0}
        // trans_code 'P' ONLY. volume_by_type also carries Award/Grant and
        // option exercises, and a sentence that says "purchases totalling X"
        // must not be summing stock a board handed them.
        purchaseValue={
          (profile.volume_by_type || []).find((v: any) => v.trans_code === "P")
            ?.total_value ?? null
        }
        firstBuyYear={tr?.buy_first_date ? String(tr.buy_first_date).slice(0, 4) : null}
        grade={(profile as any).best_career_grade}
      />

      {/* Entity relationships. One short fact — who else this filer is — so
          it reads as one row. It was a block with a hanging left rule,
          floating between the meters and the tables with nothing tying it to
          either. */}
      {profile.entity_group && (
        <div className="mb-8 flex flex-wrap items-baseline gap-x-3 gap-y-2 border-t border-[#24242F] pt-4">
          <div className="font-mono text-[10px] font-medium uppercase tracking-[0.14em] text-[#63636F]">
            Also files as
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
            ) : null}
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
          <div className="mb-8 grid gap-6 md:grid-cols-2 md:gap-0 md:divide-x md:divide-[#1D1D26]">
            {held.length > 0 && (
              <div className="md:pr-6">
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
              <div className="md:pl-6">
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
                  Counted per filing, not per execution lot — a purchase
                  filled in several tranches is one decision.
                </p>
              </div>
            )}
          </div>
        );
      })()}

      {/* Score */}
      {/* NO STAT STRIP HERE. It held Best Window, Tickers Traded and Total
          Filings; the first reads a column stale since February 2026, the
          second showed 5 under a summary saying 6 (both correct -- one counts
          discretionary tickers, the other every company filed on -- which is
          worse than a plain error because the reader cannot tell), and the
          third restated the verdict sentence verbatim 400px below it. The
          Companies list names all six with values. */}

      {/* Grade by Ticker */}
      {(profile as any).ticker_grades?.length > 0 && (
        <div className="mb-8 border-t border-[#24242F] pt-4">
          <h3 className="mb-3 font-mono text-[10.5px] font-medium uppercase tracking-[0.15em] text-[#63636F]">
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
                    {/* NO "FORM" CHIP. It rendered tg.grade — the per-ticker
                        PIT grade — as a second user-facing rating whenever it
                        differed from the career grade, which for a null career
                        grade meant every time. api/ratings.py is explicit that
                        pit_grade must never be published as a rating, and
                        CLAUDE.md records this exact badge being removed once
                        already. It survived because it was Pro-only; opening
                        ticker_grades put it in front of everyone. */}
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
      {/* The conversion path, for anyone not signed in. It used to sit under
          a table of grey placeholder bars; those numbers are public now, so
          the CTA sells what Pro actually is — an alert when this person files
          again — instead of the analysis printed directly above it. */}
      {isGated && (
        <div className="mb-8">
          <PendingFollow />
          <FollowCta
            entity={profile.name}
            detail="Alerts when they file, saved screens, and the strategy books"
            follow={{ kind: "insider", id }}
          />
        </div>
      )}

      {/* Everyone. Both sides, all three windows, per-ticker grades. */}
      {tr && (() => {
        const fc = profile.filing_counts;
        const fs = profile.filing_stats;
        const buyCount = fc?.buy ?? tr.buy_count;
        const sellCount = fc?.sell ?? tr.sell_count;
        // Every figure in this block comes from filing_stats and nothing else.
        // The `tr.*` win-rate columns counted execution lots and stopped being
        // refreshed in February 2026; they were retired 2026-08-25. Reading
        // them here is what put two denominators in one table row.
        const buyRates = [fs?.buy_win_rate_7d, fs?.buy_win_rate_30d, fs?.buy_win_rate_90d];
        const buyMoves = [fs?.buy_avg_return_7d, fs?.buy_avg_return_30d, fs?.buy_avg_return_90d];
        const buyAlphas = [fs?.buy_avg_abnormal_7d, fs?.buy_avg_abnormal_30d, fs?.buy_avg_abnormal_90d];
        const buyScored = [fs?.buy_scored_filings_7d, fs?.buy_scored_filings_30d, fs?.buy_scored_filings_90d];
        const sellRates = [fs?.sell_win_rate_7d, fs?.sell_win_rate_30d, fs?.sell_win_rate_90d];
        const sellMoves = [fs?.sell_avg_return_7d, fs?.sell_avg_return_30d, fs?.sell_avg_return_90d];
        const sellScored = [fs?.sell_scored_filings_7d, fs?.sell_scored_filings_30d, fs?.sell_scored_filings_90d];
        // The API nulls every figure for a window that falls below the
        // publishing floor, so "nothing survived the floor" is exactly
        // "no window has a rate".
        const buyScorable = buyRates.some(r => r != null);
        const sellScorable = sellRates.some(r => r != null);
        const buyBasis = Math.max(0, ...buyScored.map(n => n ?? 0));
        const sellBasis = Math.max(0, ...sellScored.map(n => n ?? 0));
        const tooFew = (n: number, noun = "discretionary") =>
          n === 0
            ? `No scored ${noun} filings`
            : `Only ${n} ${noun} ${n === 1 ? "filing" : "filings"} \u2014 too few to score`;
        const BUY_BASIS_NOTE =
          "Discretionary filings only. 10b5-1 plan trades, tax withholding and option exercises are excluded, and each column is one row per filing rather than per execution lot.";
        // Sells carry a further restriction, and it is not a cosmetic one:
        // an ordinary discretionary sale does not predict returns at all.
        const SELL_BASIS_NOTE =
          "Decision sells only \u2014 a first sale after buying the stock, or a sale by a fund or 10% owner. Ordinary sales by officers and directors show no relationship to what the stock does next, so they are counted but not scored.";
        return (
        <div className="mb-8 grid grid-cols-1 gap-6 md:grid-cols-3 md:gap-0 md:divide-x md:divide-[#1D1D26]">
          {/* Buy Track Record */}
          {buyCount > 0 && (
            <div className="md:pr-6">
              <SectionLabel>Buy Track Record</SectionLabel>
              <div className="text-sm">
                <div className="flex justify-between mb-3">
                  <span className="text-[#8888A0]">Filings</span>
                  <span className="font-mono text-[#E8E8ED]">{buyCount}</span>
                </div>
                {buyScorable ? (
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
                      {buyRates.map((wr, i) => (
                        <td key={i} className={`text-right py-1 ${wr != null && wr >= 0.6 ? "text-[#22C55E]" : wr != null ? "text-[#E8E8ED]" : "text-[#81819A]"}`}>
                          {wr != null ? `${(wr * 100).toFixed(0)}%` : "\u2014"}
                        </td>
                      ))}
                    </tr>
                    <tr>
                      <td className="text-[#8888A0] py-1">Avg Move</td>
                      {buyMoves.map((r, i) => (
                        <td key={i} className={`text-right py-1 ${r != null ? (r >= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#81819A]"}`}>
                          {r != null ? formatPercent(r) : "\u2014"}
                        </td>
                      ))}
                    </tr>
                    <tr>
                      <td className="text-[#8888A0] py-1">Alpha</td>
                      {buyAlphas.map((r, i) => (
                        <td key={i} className={`text-right py-1 ${r != null ? (r >= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#81819A]"}`}>
                          {r != null ? formatPercent(r) : "\u2014"}
                        </td>
                      ))}
                    </tr>
                    <tr>
                      <td className="text-[#8888A0] py-1">Scored</td>
                      {buyScored.map((n, i) => (
                        <td key={i} className="text-right py-1 text-[#81819A]">{n ?? 0}</td>
                      ))}
                    </tr>
                  </tbody>
                </table>
                ) : (
                  <div className="text-xs text-[#81819A]">{tooFew(buyBasis)}</div>
                )}
                {tr.best_window && buyScorable && (
                  <div className="text-[10px] text-[#81819A] mt-2">* Best window</div>
                )}
                <div className="text-[10px] text-[#81819A] mt-2">{BUY_BASIS_NOTE}</div>
              </div>
            </div>
          )}

          {/* Sell Track Record */}
          {sellCount > 0 && (
            <div className="md:px-6">
              <SectionLabel>Sell Track Record</SectionLabel>
              <div className="text-sm">
                <div className="flex justify-between mb-3">
                  <span className="text-[#8888A0]">Filings</span>
                  <span className="font-mono text-[#E8E8ED]">{sellCount}</span>
                </div>
                {sellScorable ? (
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
                        {sellRates.map((wr, i) => (
                          <td key={i} className={`text-right py-1 ${wr != null && wr >= 0.6 ? "text-[#22C55E]" : wr != null ? "text-[#E8E8ED]" : "text-[#81819A]"}`}>
                            {wr != null ? `${(wr * 100).toFixed(0)}%` : "\u2014"}
                          </td>
                        ))}
                      </tr>
                      <tr>
                        <td className="text-[#8888A0] py-1">Avg Move</td>
                        {sellMoves.map((r, i) => (
                          <td key={i} className={`text-right py-1 ${r != null ? (r <= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#81819A]"}`}>
                            {r != null ? formatPercent(r) : "\u2014"}
                          </td>
                        ))}
                      </tr>
                      <tr>
                        <td className="text-[#8888A0] py-1">Scored</td>
                        {sellScored.map((n, i) => (
                          <td key={i} className="text-right py-1 text-[#81819A]">{n ?? 0}</td>
                        ))}
                      </tr>
                    </tbody>
                  </table>
                ) : (
                  <div className="text-xs text-[#81819A]">{tooFew(sellBasis, "decision-sell")}</div>
                )}
                <div className="text-[10px] text-[#81819A] mt-2">{SELL_BASIS_NOTE}</div>
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
            <div className="md:pl-6">
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
          <ul className="m-0 list-none p-0">
            {companies.companies.map((c) => {
              const isPrivate = c.ticker === "NONE";
              const cardHref = isPrivate
                ? `/company/private/${companyToSlug(c.company)}`
                : `/company/${c.ticker}`;
              const role = formatTitle((c as any).normalized_title || c.title);
              const years =
                c.first_trade && c.last_trade
                  ? String(c.first_trade).slice(0, 4) === String(c.last_trade).slice(0, 4)
                    ? String(c.first_trade).slice(0, 4)
                    : `${String(c.first_trade).slice(0, 4)}\u2013${String(c.last_trade).slice(0, 4)}`
                  : null;
              return (
                <li key={isPrivate ? c.company : c.ticker} className="border-b border-[#1D1D26] last:border-0">
                  <Link
                    href={cardHref}
                    className="flex items-baseline gap-4 py-3 transition-colors hover:bg-[#14141C]/60"
                  >
                    <span className="w-[4.5rem] shrink-0 font-mono font-semibold text-[#E8E8ED]">
                      {isPrivate ? "\u2014" : c.ticker}
                    </span>
                    <span className="min-w-0 flex-1 truncate text-[13.5px] text-[#8A8A9E]">
                      {c.company}
                      {role && <span className="text-[#63636F]"> · {role}</span>}
                    </span>
                    <span className="shrink-0 font-mono text-[13.5px] tabular-nums text-[#E8E8ED]">
                      {formatCurrency(c.total_value)}
                    </span>
                    <span className="w-[5.5rem] shrink-0 text-right font-mono text-[12px] text-[#63636F]">
                      {years || `${c.trade_count} filings`}
                    </span>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>
      )}

      {/* Trade History */}
      <InsiderTradesTable
        identifier={id}
        initialData={trades}
        volumeByType={profile.volume_by_type}
      />

      <RelatedInsiders items={related?.related ?? []} />
    </div>
  );
}
