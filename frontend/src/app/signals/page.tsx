export const dynamic = "force-dynamic";

export const metadata = {
  title: "Trade Signals — Notable Insider Events",
  description:
    "Tagged insider trade signals: quality momentum buys, 10b5-1 surprises, deep reversals, dip buys. Filtered and graded by confidence.",
};

import Link from "next/link";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency, formatPercent } from "@/lib/format";
import { TierBadge } from "@/components/ui/tier-badge";
import { Badge } from "@/components/ui/badge";
import { TickerDisplay } from "@/components/ui/ticker-display";
import { SignalBadges } from "@/components/signal-badge";
import type { Filing, PaginatedResponse } from "@/lib/types";

interface SignalItem extends Filing {
  signal_id: number;
  signal_type: string;
  signal_label: string;
  signal_class: "bullish" | "bearish" | "noise" | "neutral";
  confidence: number;
  metadata?: Record<string, unknown>;
  pit_grade?: string;
  pit_blended_score?: number;
}

interface SignalTypeInfo {
  signal_type: string;
  signal_class: string;
  count: number;
  composite?: boolean;
  description?: string;
  strategy?: string;
}

interface Props {
  searchParams: Promise<{
    signal_type?: string;
    signal_class?: string;
    ticker?: string;
    page?: string;
  }>;
}

const PAGE_SIZE = 50;

const CLASS_STYLES: Record<string, string> = {
  bullish: "border-[#22C55E]/50 bg-[#22C55E]/10 text-[#22C55E]",
  bearish: "border-[#EF4444]/50 bg-[#EF4444]/10 text-[#EF4444]",
  noise: "border-[#6B7280]/50 bg-[#6B7280]/10 text-[#6B7280]",
  neutral: "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]",
};

const COMPOSITE_CHIP = "border-[#F59E0B]/50 bg-[#F59E0B]/10 text-[#F59E0B]";

export default async function SignalsPage({ searchParams }: Props) {
  const sp = await searchParams;
  const signalType = sp.signal_type || "";
  const signalClass = sp.signal_class || "";
  const ticker = sp.ticker || "";
  const page = parseInt(sp.page || "1", 10);
  const offset = (page - 1) * PAGE_SIZE;

  const params: Record<string, string> = {
    limit: String(PAGE_SIZE),
    offset: String(offset),
  };
  if (signalType) params.signal_type = signalType;
  if (signalClass) params.signal_class = signalClass;
  if (ticker) params.ticker = ticker;

  let data: PaginatedResponse<SignalItem>;
  let signalTypes: SignalTypeInfo[] = [];

  try {
    [data, { types: signalTypes }] = await Promise.all([
      fetchAPIAuth<PaginatedResponse<SignalItem>>("/signals/tagged", params),
      fetchAPIAuth<{ types: SignalTypeInfo[] }>("/signals/types"),
    ]);
  } catch {
    data = { total: 0, limit: PAGE_SIZE, offset: 0, items: [] };
  }

  const totalPages = Math.ceil(data.total / PAGE_SIZE);

  function buildUrl(overrides: Record<string, string>) {
    const merged = { signal_type: signalType, signal_class: signalClass, ticker, ...overrides };
    const sp = new URLSearchParams();
    Object.entries(merged).forEach(([k, v]) => {
      if (v) sp.set(k, v);
    });
    return `/signals?${sp.toString()}`;
  }

  return (
    <div>
      <div className="flex flex-col gap-4 mb-6 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#E8E8ED]">Trade Signals</h1>
          <p className="text-sm text-[#55556A] mt-1">
            {data.total.toLocaleString()} tagged signals
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {/* Signal class filters */}
          {["bullish", "bearish", "noise"].map((cls) => (
            <Link
              key={cls}
              href={buildUrl({ signal_class: signalClass === cls ? "" : cls, page: "1" })}
              className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
                signalClass === cls
                  ? CLASS_STYLES[cls]
                  : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
              }`}
            >
              {cls.charAt(0).toUpperCase() + cls.slice(1)}
            </Link>
          ))}

          {(signalType || signalClass || ticker) && (
            <>
              <span className="w-px h-5 bg-[#2A2A3A]" />
              <Link href="/signals" className="text-xs text-[#55556A] hover:text-[#8888A0]">
                Clear
              </Link>
            </>
          )}
        </div>
      </div>

      {/* Notable Events (composite signals) */}
      {signalTypes.some((st) => st.composite) && (
        <div className="mb-6">
          <h2 className="text-xs font-semibold text-[#F59E0B] uppercase tracking-wider mb-3">Notable Events</h2>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {signalTypes
              .filter((st) => st.composite)
              .map((st) => (
                <Link
                  key={st.signal_type}
                  href={buildUrl({
                    signal_type: signalType === st.signal_type ? "" : st.signal_type,
                    signal_class: "",
                    page: "1",
                  })}
                  className={`rounded-lg border p-3 transition-colors ${
                    signalType === st.signal_type
                      ? "border-[#F59E0B]/50 bg-[#F59E0B]/10"
                      : "border-[#2A2A3A] hover:border-[#F59E0B]/30 bg-[#12121A]"
                  }`}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-sm font-medium text-[#F59E0B]">
                      {st.signal_type.replace(/_/g, " ").replace("tenb51", "10b5-1")}
                    </span>
                    <span className="text-xs font-mono text-[#8888A0]">{st.count}</span>
                  </div>
                  {st.description && (
                    <p className="text-[11px] text-[#8888A0] leading-snug">{st.description}</p>
                  )}
                </Link>
              ))}
          </div>
        </div>
      )}

      {/* Signal type chips */}
      {signalTypes.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-6">
          {signalTypes
            .filter((st) => !st.composite)
            .map((st) => (
              <Link
                key={`${st.signal_type}-${st.signal_class}`}
                href={buildUrl({
                  signal_type: signalType === st.signal_type ? "" : st.signal_type,
                  page: "1",
                })}
                className={`rounded-full border px-3 py-1 text-xs transition-colors ${
                  signalType === st.signal_type
                    ? CLASS_STYLES[st.signal_class] || CLASS_STYLES.neutral
                    : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED] hover:border-[#3A3A4A]"
                }`}
              >
                {st.signal_type.replace(/_/g, " ")}
                <span className="ml-1.5 opacity-60">{st.count}</span>
              </Link>
            ))}
        </div>
      )}

      {/* Signal cards */}
      <div className="space-y-2">
        {data.items.map((s) => {
          const gated = s.gated;
          const Wrapper = gated ? "div" : Link;
          const wrapperProps = gated ? {} : { href: `/filing/${s.trade_id}` };

          return (
            <Wrapper
              key={`${s.trade_id}-${s.signal_type}`}
              {...(wrapperProps as any)}
              className={`flex items-center gap-4 rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4 transition-colors group ${
                gated ? "select-none" : "hover:bg-[#1A1A26]/60"
              }`}
            >
              {/* Signal badge + ticker */}
              <div className="w-28 shrink-0">
                <div className={`font-mono font-bold ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED] group-hover:text-blue-400"}`}>
                  <TickerDisplay ticker={s.ticker} company={s.company} href={null} />
                </div>
                <Badge
                  variant="outline"
                  className={`text-[10px] font-mono mt-1 ${
                    s.trade_type === "buy"
                      ? "border-[#22C55E]/30 text-[#22C55E]"
                      : "border-[#EF4444]/30 text-[#EF4444]"
                  }`}
                >
                  {s.trade_type.toUpperCase()}
                </Badge>
              </div>

              {/* Signal label + insider */}
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span
                    className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-medium ${
                      (s as any).composite || ["quality_momentum_buy", "tenb51_surprise_buy", "deep_reversal_dip_buy"].includes(s.signal_type)
                        ? COMPOSITE_CHIP
                        : CLASS_STYLES[s.signal_class] || CLASS_STYLES.neutral
                    }`}
                  >
                    {s.signal_label}
                  </span>
                  {s.pit_grade && <TierBadge pitGrade={s.pit_grade} />}
                  {!s.pit_grade && s.score_tier != null && <TierBadge tier={s.score_tier} />}
                </div>
                <div className={`text-xs mt-1 ${gated ? "text-[#55556A]/40 blur-[3px]" : "text-[#55556A]"}`}>
                  {s.insider_name} · {s.title}
                </div>
              </div>

              {/* Value + confidence */}
              <div className="text-right shrink-0 w-24">
                <div className={`font-mono text-sm ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>
                  {formatCurrency(s.value)}
                </div>
                <div className="text-[10px] text-[#55556A]">
                  {Math.round(s.confidence * 100)}% conf
                </div>
              </div>

              {/* Returns */}
              <div className="hidden md:block text-right shrink-0 w-16">
                {s.return_7d != null ? (
                  <>
                    <div className={`font-mono text-sm ${
                      (s.trade_type === "buy" ? s.return_7d >= 0 : s.return_7d <= 0)
                        ? "text-[#22C55E]" : "text-[#EF4444]"
                    }`}>
                      {formatPercent(s.return_7d)}
                    </div>
                    <div className="text-[10px] text-[#55556A]">7d</div>
                  </>
                ) : (
                  <span className="text-[#55556A]">&mdash;</span>
                )}
              </div>

              {/* Date */}
              <div className="hidden md:block text-right shrink-0 w-24">
                <div className={`text-xs ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>
                  {s.trade_date}
                </div>
              </div>
            </Wrapper>
          );
        })}
      </div>

      {data.items.length === 0 && (
        <div className="flex h-40 items-center justify-center text-[#55556A] rounded-lg border border-[#2A2A3A]">
          {signalTypes.length === 0
            ? "No signals computed yet. Run compute_signals.py to generate tags."
            : "No signals match your filters"}
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-6">
          <p className="text-sm text-[#55556A]">
            Showing {offset + 1}–{Math.min(offset + PAGE_SIZE, data.total)} of{" "}
            {data.total.toLocaleString()}
          </p>
          <div className="flex items-center gap-2">
            {page > 1 && (
              <Link
                href={buildUrl({ page: String(page - 1) })}
                className="rounded-md border border-[#2A2A3A] px-3 py-1.5 text-sm text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
              >
                Previous
              </Link>
            )}
            <span className="text-sm text-[#8888A0] px-2">Page {page} of {totalPages}</span>
            {page < totalPages && (
              <Link
                href={buildUrl({ page: String(page + 1) })}
                className="rounded-md border border-[#2A2A3A] px-3 py-1.5 text-sm text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26]/50"
              >
                Next
              </Link>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
