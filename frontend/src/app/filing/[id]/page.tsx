export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { formatCurrency, formatPercent, isReturnUnavailable, unavailableReason } from "@/lib/format";
import { TierBadge } from "@/components/ui/tier-badge";
import { Badge } from "@/components/ui/badge";
import { TickerDisplay, companyToSlug } from "@/components/ui/ticker-display";
import { SignalBadges } from "@/components/signal-badge";
import { ContextFacts } from "@/components/context-facts";
import { WhatIfSimulator } from "@/components/what-if-simulator";
import { TradeGradeDetail } from "@/components/trade-grade-badge";
import type { Filing } from "@/lib/types";

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
      openGraph: { title, description },
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
    <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
      {children}
    </div>
  );
}

export default async function FilingPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let filing: FilingDetail;
  let related: Filing[];

  try {
    [filing, related] = await Promise.all([
      fetchAPI<FilingDetail>(`/filings/${id}`),
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
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span>Filing</span>
        <span>/</span>
        <span className="text-[#E8E8ED] flex items-center gap-1">
          <TickerDisplay ticker={filing.ticker} company={filing.company} href={null} /> {filing.trade_type.toUpperCase()}
        </span>
      </nav>

      {/* Header */}
      <div className="flex items-center gap-4 mb-8">
        <TickerDisplay ticker={filing.ticker} company={filing.company} href={null} className="text-3xl font-bold" />
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
          <span className="text-[#8888A0]">{filing.company}</span>
        )}
      </div>

      {/* SEC Link prominent */}
      {filing.accession && (
        <div className="mb-8 rounded-lg border border-blue-500/20 bg-blue-500/5 px-5 py-3 flex items-center justify-between">
          <div>
            <div className="text-sm text-[#8888A0]">SEC EDGAR Filing</div>
            <div className="text-xs font-mono text-[#55556A] mt-0.5">{filing.accession}</div>
          </div>
          <a
            href={secEdgarUrl(filing.accession)}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-1.5 font-medium"
          >
            View on SEC EDGAR
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6M15 3h6v6M10 14L21 3" />
            </svg>
          </a>
        </div>
      )}

      {/* Amendment notice */}
      {(filing as any).is_amendment === 1 && (
        <div className="mb-6 rounded-lg border border-[#F59E0B]/20 bg-[#F59E0B]/5 px-5 py-3">
          <div className="flex items-center gap-2">
            <span className="text-[#F59E0B] text-sm font-medium">Amended Filing</span>
            <span className="text-[#8888A0] text-xs">
              (Form {(filing as any).document_type ?? "4/A"})
            </span>
          </div>
          <div className="text-xs text-[#55556A] mt-1">
            This filing is an amendment that corrects an earlier submission
            {(filing as any).date_of_orig_sub && (
              <> originally filed on {(filing as any).date_of_orig_sub}</>
            )}
            . The data shown reflects the corrected values.
          </div>
        </div>
      )}

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
              href={`/insider/${filing.cik || filing.insider_id}`}
              className="text-blue-400 hover:text-blue-300"
            >
              {filing.insider_name}
            </Link>
          </InfoRow>
          <InfoRow label="Title">{(filing.normalized_title || filing.title)?.replace(/;/g, ", ")}</InfoRow>
          <div className="flex items-center justify-between py-2 border-b border-[#2A2A3A]/50">
            <span className="text-[#8888A0] text-sm">PIT Grade</span>
            <TierBadge tier={filing.tier} pitGrade={filing.pit_grade} />
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
          href={`/insider/${filing.cik || filing.insider_id}`}
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
                </tr>
              </thead>
              <tbody>
                {related.map((r) => (
                  <tr key={r.trade_id} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]/30">
                    <td className="px-4 py-3">
                      <Link
                        href={`/filing/${r.trade_id}`}
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
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
