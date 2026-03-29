"use client";

import { useEffect, useCallback } from "react";
import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import { TierBadge } from "@/components/ui/tier-badge";
import { formatCurrency, formatPercent } from "@/lib/format";
import { TickerDisplay, companyToSlug } from "@/components/ui/ticker-display";
import type { Filing } from "@/lib/types";

interface FilingDetailPanelProps {
  filing: Filing | null;
  onClose: () => void;
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-2">
      {children}
    </div>
  );
}

function InfoRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between py-1.5">
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

function secEdgarUrl(accession: string): string {
  const filerCik = String(parseInt(accession.substring(0, 10), 10));
  const accNoDashes = accession.replace(/-/g, "");
  return `https://www.sec.gov/Archives/edgar/data/${filerCik}/${accNoDashes}/${accession}-index.htm`;
}

export function FilingDetailPanel({ filing, onClose }: FilingDetailPanelProps) {
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    },
    [onClose],
  );

  useEffect(() => {
    if (filing) {
      document.addEventListener("keydown", handleKeyDown);
      return () => document.removeEventListener("keydown", handleKeyDown);
    }
  }, [filing, handleKeyDown]);

  const isOpen = filing !== null;

  return (
    <>
      {/* Backdrop */}
      <div
        className={`fixed inset-0 z-40 bg-black/60 transition-opacity duration-300 ${
          isOpen ? "opacity-100 pointer-events-auto" : "opacity-0 pointer-events-none"
        }`}
        onClick={onClose}
      />

      {/* Panel */}
      <div
        className={`fixed top-0 right-0 z-50 h-full w-full sm:w-[480px] bg-[#12121A] border-l border-[#2A2A3A] shadow-2xl transition-transform duration-300 ease-in-out overflow-y-auto ${
          isOpen ? "translate-x-0" : "translate-x-full"
        }`}
      >
        {filing && (
          <div className="p-6">
            {/* Close button */}
            <button
              onClick={onClose}
              className="absolute top-4 right-4 text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
              aria-label="Close"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M18 6L6 18M6 6l12 12" />
              </svg>
            </button>

            {/* Header */}
            <div className="mb-6">
              <div className="flex items-center gap-3 mb-1">
                <TickerDisplay ticker={filing.ticker} company={filing.company} href={null} className="text-2xl font-bold" />
                <Badge
                  variant="outline"
                  className={`text-xs font-mono ${
                    filing.trade_type === "buy"
                      ? "border-[#22C55E]/30 text-[#22C55E] bg-[#22C55E]/10"
                      : "border-[#EF4444]/30 text-[#EF4444] bg-[#EF4444]/10"
                  }`}
                >
                  {filing.trade_type.toUpperCase()}
                </Badge>
              </div>
              {filing.ticker !== "NONE" && (
                <div className="text-[#8888A0] text-sm">{filing.company}</div>
              )}
            </div>

            {/* Filing Info */}
            <div className="mb-6">
              <SectionLabel>Filing Info</SectionLabel>
              <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 px-4 py-2">
                <InfoRow label="Filed">{filing.filing_date}</InfoRow>
                <InfoRow label="Traded">{filing.trade_date}</InfoRow>
                {filing.accession && (
                  <>
                    <InfoRow label="Accession">
                      <span className="text-xs">{filing.accession}</span>
                    </InfoRow>
                    <div className="flex items-center justify-between py-1.5">
                      <span className="text-[#8888A0] text-sm">SEC Filing</span>
                      <a
                        href={filing.accession ? secEdgarUrl(filing.accession) : "#"}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-1"
                      >
                        View on SEC EDGAR
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                          <path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6M15 3h6v6M10 14L21 3" />
                        </svg>
                      </a>
                    </div>
                  </>
                )}
              </div>
            </div>

            {/* Insider */}
            <div className="mb-6">
              <SectionLabel>Insider</SectionLabel>
              <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 px-4 py-2">
                <InfoRow label="Name">
                  <Link
                    href={`/insider/${filing.cik || filing.insider_id}`}
                    className="text-blue-400 hover:text-blue-300"
                  >
                    {filing.insider_name}
                  </Link>
                </InfoRow>
                <InfoRow label="Title">{filing.title}</InfoRow>
                <div className="flex items-center justify-between py-1.5">
                  <span className="text-[#8888A0] text-sm">Tier</span>
                  <TierBadge tier={filing.tier} />
                </div>
                <InfoRow label="Score">{filing.score?.toFixed(2) ?? "\u2014"}</InfoRow>
              </div>
            </div>

            {/* Trade Details */}
            <div className="mb-6">
              <SectionLabel>Trade Details</SectionLabel>
              <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 px-4 py-2">
                <InfoRow label="Price per Share">${filing.price?.toFixed(2) ?? "\u2014"}</InfoRow>
                <InfoRow label="Shares">{filing.qty?.toLocaleString() ?? "\u2014"}</InfoRow>
                <InfoRow label="Total Value">{formatCurrency(filing.value)}</InfoRow>
              </div>
            </div>

            {/* Outcomes */}
            {(filing.return_7d != null || filing.return_30d != null || filing.return_90d != null) && (
              <div className="mb-6">
                <SectionLabel>Outcomes</SectionLabel>
                <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 px-4 py-2">
                  <ReturnRow label="7d Stock" value={filing.return_7d} tradeType={filing.trade_type} />
                  <ReturnRow label="30d Stock" value={filing.return_30d} tradeType={filing.trade_type} />
                  <ReturnRow label="90d Stock" value={filing.return_90d} tradeType={filing.trade_type} />
                  <ReturnRow label="7d vs SPY" value={filing.abnormal_7d} tradeType={filing.trade_type} />
                </div>
              </div>
            )}

            {/* Actions */}
            <div className="flex flex-col gap-2 pt-2">
              <Link
                href={`/filing/${filing.trade_id}`}
                className="flex items-center justify-center rounded-lg border border-[#2A2A3A] bg-[#1A1A26] px-4 py-2.5 text-sm font-medium text-[#E8E8ED] hover:bg-[#2A2A3A]/60 transition-colors"
              >
                View Full Filing
              </Link>
              <Link
                href={filing.ticker === "NONE" ? `/company/private/${companyToSlug(filing.company)}` : `/company/${filing.ticker}`}
                className="flex items-center justify-center rounded-lg border border-[#2A2A3A] bg-[#1A1A26] px-4 py-2.5 text-sm font-medium text-[#E8E8ED] hover:bg-[#2A2A3A]/60 transition-colors"
              >
                View Company
              </Link>
              <Link
                href={`/insider/${filing.cik || filing.insider_id}`}
                className="flex items-center justify-center rounded-lg border border-[#2A2A3A] bg-[#1A1A26] px-4 py-2.5 text-sm font-medium text-[#E8E8ED] hover:bg-[#2A2A3A]/60 transition-colors"
              >
                View Insider
              </Link>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
