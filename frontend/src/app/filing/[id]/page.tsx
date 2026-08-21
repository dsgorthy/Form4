export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency, formatPercent, isReturnUnavailable, unavailableReason } from "@/lib/format";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";
import { FilingSummary } from "@/components/entity-summary";
import { FollowCta } from "@/components/follow-cta";
import { filingJsonLd, jsonLdScript } from "@/lib/structured-data";
import { Badge } from "@/components/ui/badge";
import { TickerDisplay, companyToSlug } from "@/components/ui/ticker-display";
import { SignalBadges } from "@/components/signal-badge";
import { ContextFacts } from "@/components/context-facts";
import { FilingCorrectionNotice } from "@/components/filing-correction-notice";
import { WhatIfSimulator } from "@/components/what-if-simulator";
import { TradeGradeDetail } from "@/components/trade-grade-badge";
import type { Filing } from "@/lib/types";
import { insiderPath } from "@/lib/insider-url";

interface Lot {
  trade_id: string;
  price: number;
  qty: number;
  value: number;
  accession?: string;
}

interface FilingDetail extends Filing {
  percentile?: number;
  buy_count?: number;
  buy_win_rate_7d?: number;
  buy_avg_return_7d?: number;
  buy_avg_abnormal_7d?: number;
  sell_count?: number;
  sell_win_rate_7d?: number;
  primary_title?: string;
  primary_ticker?: string;
  entry_price?: number;
  spy_return_7d?: number;
  spy_return_30d?: number;
  spy_return_90d?: number;
  source?: string;
  lots?: Lot[];
  total_qty?: number;
  total_value?: number;
  narrative?: {
    tier?: "high_signal" | "high_signal_pending" | "routine" | "low_signal";
    summary?: string;
    price_context?: string | null;
    // Lists, not prose. The API normalises whatever is stored — a real list,
    // a JSON array, or a legacy Postgres array literal — into string[].
    catalysts?: string[] | string | null;
    risks?: string[] | string | null;
    generated_at?: string | null;
    model_name?: string | null;
  };
}

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  try {
    const filing = await fetchAPI<FilingDetail>(`/filings/${id}`);
    const title = `${filing.ticker} ${filing.trade_type.toUpperCase()} by ${filing.insider_name}`;
    const parts: string[] = [];
    if (filing.value) parts.push(`Value: ${formatCurrency(filing.value)}`);
    if (filing.price) parts.push(`at $${filing.price.toFixed(2)}/share`);
    if (filing.filing_date) parts.push(`filed ${filing.filing_date}`);
    const grade = (filing as any).trade_grade?.grade;
    if (grade) parts.push(`Grade: ${grade}`);
    const description = `${filing.insider_name} ${filing.trade_type} ${filing.ticker} (${filing.company}). ${parts.join(". ")}. SEC Form 4 analysis on Form4.app.`;
    return {
      title,
      description,
      openGraph: { title, description, siteName: "Form4", type: "article" },
      // Third page with this gap. Page metadata that sets only openGraph
      // inherits the root layout's site-wide twitter block, so every filing
      // unfurled as the generic Form4 card on any platform preferring
      // twitter:* over og:*.
      twitter: { card: "summary_large_image", title, description },
    };
  } catch {
    return { title: "SEC Form 4 Filing" };
  }
}

function secEdgarUrl(accession: string): string {
  const filerCik = String(parseInt(accession.substring(0, 10), 10));
  const accNoDashes = accession.replace(/-/g, "");
  return `https://www.sec.gov/Archives/edgar/data/${filerCik}/${accNoDashes}/${accession}-index.htm`;
}

function InfoRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between py-2 border-b border-[#2A2A3A]/50 last:border-b-0">
      <span className="text-[#8888A0] text-sm">{label}</span>
      <span className="text-[#E8E8ED] text-sm font-mono">{children}</span>
    </div>
  );
}

function ReturnRow({ label, value, tradeType }: { label: string; value?: number | null; tradeType?: "buy" | "sell" }) {
  if (value == null) return null;
  const isGoodSignal = tradeType === "sell" ? value <= 0 : value >= 0;
  return (
    <InfoRow label={label}>
      <span className={isGoodSignal ? "text-[#22C55E]" : "text-[#EF4444]"}>
        {formatPercent(value)}
      </span>
    </InfoRow>
  );
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

export default async function FilingPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let filing: FilingDetail;
  let related: Filing[];

  try {
    // Main filing fetch uses the authed fetcher so the API can identify the
    // user (Pro tier, admin) and return gated fields (e.g., trade_narrative).
    // Related-filings list stays anonymous — no user-specific gating there.
    [filing, related] = await Promise.all([
      fetchAPIAuth<FilingDetail>(`/filings/${id}`),
      fetchAPI<Filing[]>(`/filings/${id}/related`),
    ]);
  } catch {
    notFound();
  }

  const hasReturns =
    filing.return_7d != null || filing.return_30d != null || filing.return_90d != null;
  const allReturnsUnavailable = !hasReturns &&
    isReturnUnavailable(filing.trade_date, filing.return_7d, 7);

  return (
    <div>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={jsonLdScript(
          filingJsonLd({
            id,
            insiderName: filing.insider_name,
            company: filing.company,
            ticker: filing.ticker,
            tradeType: filing.trade_type,
            tradeDate: filing.trade_date,
            filingDate: filing.filing_date,
            value: filing.value,
          }),
        )}
      />
      {/* Breadcrumb */}
      <nav className="flex flex-wrap items-center gap-2 text-sm text-[#55556A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span>Filing</span>
        <span>/</span>
        {/* Ticker only. The trade type was here too, and the h1 immediately
            below already carries it as a coloured badge — "CPAY SELL" then
            "CPAY [SELL] CORPAY, INC." two lines apart reads as a rendering
            bug rather than a hierarchy. */}
        <span className="text-[#E8E8ED] flex items-center gap-1 min-w-0">
          <TickerDisplay ticker={filing.ticker} company={filing.company} href={null} />
        </span>
      </nav>

      {/* Header */}
      {/* h1, not a bare div. Filing pages are the largest section of the
          sitemap — 32,935 of 52,304 — and they were shipping four h2s with no
          h1 above them, which is a broken outline on two thirds of the
          indexed site. The heading states what the page is: who traded what. */}
      <h1 className="flex flex-wrap items-center gap-3 sm:gap-4 mb-8 text-2xl sm:text-3xl font-bold">
        <TickerDisplay ticker={filing.ticker} company={filing.company} href={null} className="text-2xl sm:text-3xl font-bold" />
        {" "}
        <span className="sr-only">
          {filing.insider_name} {filing.trade_type === "buy" ? "bought" : "sold"}{" "}
          {filing.company || filing.ticker} shares — SEC Form 4
        </span>
        <Badge
          variant="outline"
          className={`text-sm font-mono ${
            filing.trade_type === "buy"
              ? "border-[#22C55E]/30 text-[#22C55E] bg-[#22C55E]/10"
              : "border-[#EF4444]/30 text-[#EF4444] bg-[#EF4444]/10"
          }`}
        >
          {filing.trade_type.toUpperCase()}
        </Badge>
        {filing.ticker !== "NONE" && (
          <span className="text-[#8888A0] text-sm sm:text-base break-words">{filing.company}</span>
        )}
      </h1>

      {/* Directly under the H1: the sentence Google quotes. A filing page is
          thin by nature, so this prose is a meaningful share of its content. */}
      <FilingSummary
        insiderName={filing.insider_name}
        title={filing.title}
        company={filing.ticker !== "NONE" ? filing.company : null}
        ticker={filing.ticker}
        tradeType={filing.trade_type}
        qty={filing.qty}
        price={filing.price}
        value={filing.value}
        tradeDate={filing.trade_date}
        filingDate={filing.filing_date}
      />

      {/* SEC Link prominent */}
      {filing.accession && (
        <div className="mb-8 rounded-lg border border-blue-500/20 bg-blue-500/5 px-5 py-3 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          <div>
            <div className="text-sm text-[#8888A0]">SEC EDGAR Filing</div>
            <div className="text-xs font-mono text-[#55556A] mt-0.5 break-all">{filing.accession}</div>
          </div>
          <a
            href={secEdgarUrl(filing.accession)}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-1.5 font-medium shrink-0"
          >
            View on SEC EDGAR
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6M15 3h6v6M10 14L21 3" />
            </svg>
          </a>
        </div>
      )}

      {/* Amendment notice */}
      {/*
         DEAD UI REMOVED 2026-08-21. `is_amendment` has no writer anywhere in
         the codebase — the 6,033 rows that carry it predate 2026 and came
         from a writer that no longer exists. Coverage by year: 408 (2023),
         390 (2024), 316 (2025), 0 (2026). document_type and superseded_by
         are equally unpopulated, and trans_form_type only ever holds '4' or
         '5', never '4/A', so amendment status is not derivable from anything
         we currently ingest. Restoring the badge means capturing the form
         suffix from EDGAR at ingest first; until then this rendered a
         condition that could never be true.
      */}

      {/* Trade narrative — depth varies by tier:
         - high_signal: full 4-field LLM narrative, blue accent
         - high_signal_pending: short placeholder while LLM catches up
         - routine: 1-sentence templated reason (scheduled/tax/recurring), muted
         - low_signal: 2-sentence templated open-market summary, muted */}
      {filing.narrative?.summary && (() => {
        const tier = filing.narrative.tier || "low_signal";
        const isHighSignal = tier === "high_signal";
        const isPending = tier === "high_signal_pending";

        // Visual treatment scales with signal strength
        const borderClass = isHighSignal
          ? "border-[#3B82F6]/30 bg-[#3B82F6]/5"
          : isPending
          ? "border-[#3B82F6]/15 bg-[#3B82F6]/[0.02]"
          : "border-[#2A2A3A] bg-[#12121A]";
        const labelClass = isHighSignal
          ? "text-[#3B82F6]"
          : isPending
          ? "text-[#3B82F6]/70"
          : "text-[#55556A]";
        const tierLabel =
          tier === "high_signal"
            ? "Why this matters"
            : tier === "high_signal_pending"
            ? "Why this matters (generating…)"
            : tier === "routine"
            ? "Trade context"
            : "Trade context";

        return (
          <div className={`mb-6 rounded-lg border p-5 ${borderClass}`}>
            <div className="flex items-baseline justify-between mb-3">
              <div className={`text-[10px] font-semibold uppercase tracking-widest ${labelClass}`}>
                {tierLabel}
              </div>
              {isHighSignal && (
                <div className="text-[10px] text-[#55556A]">
                  {filing.narrative.model_name || "AI summary"}
                  {filing.narrative.generated_at
                    ? ` · ${filing.narrative.generated_at.slice(0, 10)}`
                    : ""}
                </div>
              )}
            </div>
            <div className="space-y-3 text-sm text-[#E8E8ED] leading-relaxed">
              <p className={isHighSignal ? "" : "text-[#8888A0]"}>{filing.narrative.summary}</p>

              {filing.narrative.price_context && (
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-[#55556A] mb-1">
                    Price context
                  </div>
                  <p>{filing.narrative.price_context}</p>
                </div>
              )}

              {([
                ["Catalysts to watch", filing.narrative.catalysts, "#22C55E"],
                ["Risks", filing.narrative.risks, "#F59E0B"],
              ] as const).map(([heading, value, tone]) => {
                // Tolerate a bare string as well as a list: the API normalises,
                // but a cached response from before that change may not have.
                const bullets = Array.isArray(value)
                  ? value.filter(Boolean)
                  : value
                    ? [String(value)]
                    : [];
                if (bullets.length === 0) return null;
                return (
                  <div key={heading}>
                    <div className="text-[10px] font-semibold uppercase tracking-wider text-[#55556A] mb-1">
                      {heading}
                    </div>
                    {bullets.length === 1 ? (
                      <p style={{ color: `${tone}E6` }}>{bullets[0]}</p>
                    ) : (
                      <ul className="space-y-1">
                        {bullets.map((b, i) => (
                          <li key={i} className="flex gap-2" style={{ color: `${tone}E6` }}>
                            <span className="select-none opacity-50">&bull;</span>
                            <span>{b}</span>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                );
              })}
            </div>
            {isHighSignal && (
              <div className="mt-3 pt-3 border-t border-[#3B82F6]/10 text-[10px] text-[#55556A] italic">
                AI-generated context from public data. Not investment advice. Verify before trading.
              </div>
            )}
          </div>
        );
      })()}

      {/* Signal Quality + Signal badges */}
      <div className="flex flex-col md:flex-row gap-4 mb-6">
        {(filing as any).trade_grade && (
          <div className="md:w-64 shrink-0">
            <TradeGradeDetail grade={(filing as any).trade_grade} />
          </div>
        )}
        {filing.signals && filing.signals.length > 0 && (
          <div className="flex-1">
            <SignalBadges signals={filing.signals} />
          </div>
        )}
      </div>

      {/* Trade Context */}
      {filing.context && filing.context.length > 0 && (
        <div className="mb-6 rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
          <SectionLabel>Trade Context</SectionLabel>
          <ContextFacts context={filing.context} limit={5} />
        </div>
      )}

      {/* Main content grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-10">
        {/* Filing Info */}
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
          <SectionLabel>Filing Info</SectionLabel>
          <InfoRow label="Filed">{filing.filing_date}</InfoRow>
          <InfoRow label="Traded">{filing.trade_date}</InfoRow>
          {filing.accession && (
            <InfoRow label="Accession">
              <span className="text-xs">{filing.accession}</span>
            </InfoRow>
          )}
          {filing.source && (
            <InfoRow label="Source">
              {({
                edgar_live: "SEC EDGAR (Live)",
                edgar_bulk: "SEC EDGAR (Historical)",
                openinsider: "OpenInsider",
              } as Record<string, string>)[filing.source] || filing.source}
            </InfoRow>
          )}
        </div>

        {/* Insider */}
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
          <SectionLabel>Insider</SectionLabel>
          <InfoRow label="Name">
            <Link
              href={insiderPath(filing.insider_name, filing.cik || filing.insider_id, filing.insider_slug)}
              className="text-blue-400 hover:text-blue-300"
            >
              {filing.insider_name}
            </Link>
          </InfoRow>
          {/* `title` arrives cleaned by api/titles.clean_title. Do NOT prefer
              normalized_title here — it is a classification, not a label, and
              it buckets anything it does not recognise to "Other". CPAY's
              "GroupPresident IntlVehiclePmts" normalises to "Other" and reads
              as though we lost the data. */}
          <InfoRow label="Title">{filing.title}</InfoRow>
          <div className="flex items-center justify-between py-2 border-b border-[#2A2A3A]/50">
            <span className="text-[#8888A0] text-sm">PIT Grade</span>
            <InsiderGradeBadge rating={(filing as any).insider_rating} grade={(filing as any).career_grade} showLabel />
          </div>
          <InfoRow label="Score">{filing.score?.toFixed(2) ?? "\u2014"}</InfoRow>
          {filing.percentile != null && (
            <InfoRow label="Percentile">{(filing.percentile * 100).toFixed(0)}th</InfoRow>
          )}
        </div>

        {/* Trade Details */}
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
          <SectionLabel>Trade Details</SectionLabel>
          {filing.lots && filing.lots.length > 0 ? (
            <>
              <InfoRow label="Total Shares">{filing.total_qty?.toLocaleString() ?? "\u2014"}</InfoRow>
              <InfoRow label="Total Value">{formatCurrency(filing.total_value ?? filing.value)}</InfoRow>
              <InfoRow label="Lots">{filing.lots.length}</InfoRow>
              {filing.entry_price != null && (
                <InfoRow label="Entry Price">${filing.entry_price.toFixed(2)}</InfoRow>
              )}
            </>
          ) : (
            <>
              <InfoRow label="Price per Share">${filing.price?.toFixed(2) ?? "\u2014"}</InfoRow>
              <InfoRow label="Shares">{filing.qty?.toLocaleString() ?? "\u2014"}</InfoRow>
              <InfoRow label="Total Value">{formatCurrency(filing.value)}</InfoRow>
              {filing.entry_price != null && (
                <InfoRow label="Entry Price">${filing.entry_price.toFixed(2)}</InfoRow>
              )}
            </>
          )}
        </div>

        {/* Directly under the figures it applies to, so the two are read
            together rather than the correction being found afterwards. */}
        <FilingCorrectionNotice
          priceAsFiled={filing.price_as_filed}
          valueAsFiled={filing.value_as_filed}
          method={filing.correction_method}
          accessionUrl={filing.accession ? secEdgarUrl(filing.accession) : null}
          valueSuspect={filing.value_suspect}
        />

        {/* Outcomes */}
        {hasReturns && (
          <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
            <SectionLabel>Outcomes</SectionLabel>
            <ReturnRow label="7d Stock" value={filing.return_7d} tradeType={filing.trade_type} />
            <ReturnRow label="30d Stock" value={filing.return_30d} tradeType={filing.trade_type} />
            <ReturnRow label="90d Stock" value={filing.return_90d} tradeType={filing.trade_type} />
            <ReturnRow label="7d vs SPY" value={filing.abnormal_7d} tradeType={filing.trade_type} />
            <ReturnRow label="30d vs SPY" value={filing.abnormal_30d} tradeType={filing.trade_type} />
            <ReturnRow label="90d vs SPY" value={filing.abnormal_90d} tradeType={filing.trade_type} />
            {(filing.return_30d == null && isReturnUnavailable(filing.trade_date, filing.return_30d, 30)) && (
              <div className="text-xs text-[#55556A] mt-2 pt-2 border-t border-[#2A2A3A]/50">
                {unavailableReason((filing as any).price_data_end)}
              </div>
            )}
          </div>
        )}
        {allReturnsUnavailable && (
          <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5">
            <SectionLabel>Outcomes</SectionLabel>
            <div className="text-xs text-[#55556A]">
              {unavailableReason((filing as any).price_data_end)}
            </div>
          </div>
        )}
      </div>

      {/* What If Simulator */}
      <div className="mb-10">
        <WhatIfSimulator tradeId={id} />
      </div>

      {/* Lot Breakdown */}
      {filing.lots && filing.lots.length > 0 && (
        <div className="mb-10">
          <h2 className="text-lg font-semibold text-[#E8E8ED] mb-4">
            Lot Breakdown
            <span className="text-sm font-normal text-[#55556A] ml-2">
              {filing.lots.length} lots across {(() => {
                const prices = new Set(filing.lots!.map(l => l.price));
                return prices.size;
              })()} price{new Set(filing.lots!.map(l => l.price)).size > 1 ? "s" : ""}
            </span>
          </h2>
          <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                  <th className="px-4 py-3 text-left text-[#55556A] font-medium">#</th>
                  <th className="px-4 py-3 text-right text-[#55556A] font-medium">Price</th>
                  <th className="px-4 py-3 text-right text-[#55556A] font-medium">Shares</th>
                  <th className="px-4 py-3 text-right text-[#55556A] font-medium">Value</th>
                  <th className="px-4 py-3 text-right text-[#55556A] font-medium">% of Total</th>
                </tr>
              </thead>
              <tbody>
                {filing.lots.map((lot, i) => (
                  <tr key={lot.trade_id} className="border-b border-[#2A2A3A]/50">
                    <td className="px-4 py-2.5 text-[#55556A] font-mono text-xs">{i + 1}</td>
                    <td className="px-4 py-2.5 text-right font-mono text-[#E8E8ED]">
                      ${lot.price.toFixed(2)}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono text-[#E8E8ED]">
                      {lot.qty.toLocaleString()}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono text-[#E8E8ED]">
                      {formatCurrency(lot.value)}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono text-[#8888A0]">
                      {((lot.value / (filing.total_value ?? filing.value)) * 100).toFixed(1)}%
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="bg-[#1A1A26]/50">
                  <td className="px-4 py-2.5 text-[#8888A0] text-xs font-medium">Total</td>
                  <td className="px-4 py-2.5 text-right font-mono text-[#8888A0] text-xs">
                    {(() => {
                      const totalVal = filing.total_value ?? filing.value;
                      const totalQty = filing.total_qty ?? filing.qty;
                      return `$${(totalVal / totalQty).toFixed(2)} avg`;
                    })()}
                  </td>
                  <td className="px-4 py-2.5 text-right font-mono font-medium text-[#E8E8ED]">
                    {filing.total_qty?.toLocaleString()}
                  </td>
                  <td className="px-4 py-2.5 text-right font-mono font-medium text-[#E8E8ED]">
                    {formatCurrency(filing.total_value ?? filing.value)}
                  </td>
                  <td className="px-4 py-2.5 text-right font-mono text-[#8888A0]">100%</td>
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      )}

      {/* Action links */}
      <div className="flex gap-3 mb-10">
        <Link
          href={filing.ticker === "NONE" ? `/company/private/${companyToSlug(filing.company)}` : `/company/${filing.ticker}`}
          className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26] px-5 py-2.5 text-sm font-medium text-[#E8E8ED] hover:bg-[#2A2A3A]/60 transition-colors"
        >
          View Company
        </Link>
        <Link
          href={insiderPath(filing.insider_name, filing.cik || filing.insider_id, filing.insider_slug)}
          className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26] px-5 py-2.5 text-sm font-medium text-[#E8E8ED] hover:bg-[#2A2A3A]/60 transition-colors"
        >
          View Insider
        </Link>
      </div>

      {/* Related Trades */}
      {related.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold text-[#E8E8ED] mb-4">
            Related Trades by This Insider
          </h2>
          <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                  <th className="px-4 py-3 text-left text-[#55556A] font-medium">Ticker</th>
                  <th className="px-4 py-3 text-left text-[#55556A] font-medium">Type</th>
                  <th className="px-4 py-3 text-left text-[#55556A] font-medium">Filed</th>
                  <th className="px-4 py-3 text-right text-[#55556A] font-medium">Value</th>
                  <th className="px-4 py-3 text-right text-[#55556A] font-medium">7d Stock</th>
                  <th className="px-4 py-3 text-right text-[#55556A] font-medium sr-only">Filing</th>
                </tr>
              </thead>
              <tbody>
                {related.map((r) => (
                  <tr key={r.trade_id} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30">
                    {/* A ticker goes to the company. It read as a filing link
                        only because this table had nowhere else to click, so
                        the row now carries an explicit one in the last cell. */}
                    <td className="px-4 py-3">
                      <Link
                        href={`/company/${r.ticker}`}
                        className="font-mono font-semibold text-blue-400 hover:text-blue-300"
                      >
                        {r.ticker}
                      </Link>
                    </td>
                    <td className="px-4 py-3">
                      <Badge
                        variant="outline"
                        className={`text-xs font-mono ${
                          r.trade_type === "buy"
                            ? "border-[#22C55E]/30 text-[#22C55E]"
                            : "border-[#EF4444]/30 text-[#EF4444]"
                        }`}
                      >
                        {r.trade_type.toUpperCase()}
                      </Badge>
                    </td>
                    <td className="px-4 py-3 text-[#8888A0]">{r.filing_date}</td>
                    <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                      {formatCurrency(r.value)}
                    </td>
                    <td
                      className={`px-4 py-3 text-right font-mono ${
                        r.return_7d != null
                          ? (r.trade_type === "buy" ? r.return_7d >= 0 : r.return_7d <= 0)
                            ? "text-[#22C55E]"
                            : "text-[#EF4444]"
                          : "text-[#55556A]"
                      }`}
                    >
                      {formatPercent(r.return_7d)}
                    </td>
                    <td className="px-4 py-3 text-right whitespace-nowrap">
                      <Link
                        href={`/filing/${r.trade_id}`}
                        className="text-sm text-[#8888A0] hover:text-blue-300"
                      >
                        Filing →
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Retention, not a wall. This page withholds nothing from an anonymous
          visitor, so marksGate is false — the JSON-LD declares the filing fully
          free and must stay true. Filing pages are the long-tail search entry
          points, so this is where most first visits land and the only place
          many of them see an ask at all. */}
      <FollowCta
        entity={filing.ticker}
        detail={`Every new ${filing.ticker} Form 4, within minutes of it hitting EDGAR`}
        marksGate={false}
      />
    </div>
  );
}
