export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency, formatPercent } from "@/lib/format";
import { formatTitle } from "@/lib/title-format";
import { TierBadge } from "@/components/ui/tier-badge";
import { TradeOutcomeTimeline } from "@/components/trade-outcome-timeline";
import { UpgradePrompt } from "@/components/upgrade-prompt";
import { InsiderTradesTable } from "@/components/insider-trades-table";
import { InsiderScoreChart } from "@/components/insider-score-chart";
import { TickerDisplay, companyToSlug } from "@/components/ui/ticker-display";
import type { InsiderProfile, InsiderCompany, Filing, PaginatedResponse } from "@/lib/types";

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
      return { title: "Insider Not Found" };
    }
    const profile: InsiderProfile = await res.json();
    const tr = profile.track_record;
    const grade = (profile as any).best_pit_grade;
    const parts: string[] = [];
    if (grade) parts.push(`Grade ${grade}`);
    if (tr) parts.push(`${tr.buy_count + tr.sell_count} trades across ${tr.n_tickers} companies`);
    const description = parts.length > 0
      ? `${profile.name} insider trading profile. ${parts.join(". ")}. SEC Form 4 analysis on Form4.app.`
      : `${profile.name} insider trading profile on Form4.app.`;
    return {
      title: `${profile.name} — Insider Profile`,
      description,
      openGraph: { title: `${profile.name} — Insider Profile`, description },
    };
  } catch {
    return { title: "Insider Not Found" };
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
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-1">
        {label}
      </div>
      <div className={`text-xl font-mono font-bold ${color || "text-[#E8E8ED]"}`}>{value}</div>
      {sub && <div className="text-xs text-[#55556A] mt-0.5">{sub}</div>}
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
      {children}
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
          <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
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

  const tr = profile.track_record;

  return (
    <div>
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-[#55556A] mb-6">
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
      <div className="flex items-center gap-4 mb-2">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">{profile.name}</h1>
        {(profile as any).best_pit_grade ? (
          <TierBadge pitGrade={(profile as any).best_pit_grade} bestTicker={(profile as any).best_pit_ticker} tickerCount={(profile as any).n_scored_tickers} />
        ) : tr ? (
          <TierBadge tier={tr.score_tier} />
        ) : null}
      </div>
      {(() => {
        const cos = companies.companies;
        const primary = cos.length > 0
          ? [...cos].sort((a, b) => b.trade_count - a.trade_count)[0]
          : null;
        const title = formatTitle((primary as any)?.normalized_title || primary?.title) || formatTitle(tr?.primary_title);
        const otherCount = cos.length > 1 ? cos.length - 1 : 0;
        const skipTitle = !title;
        return (
          <>
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
                  <span className="text-[#55556A]">
                    {" "}(+{otherCount} {otherCount === 1 ? "company" : "companies"})
                  </span>
                )}
              </p>
            )}
            <p className="text-xs text-[#55556A] mb-8">
              {profile.cik && `CIK: ${profile.cik}`}
              {profile.cik && tr && " · "}
              {tr && `${tr.n_tickers} companies traded`}
            </p>
          </>
        );
      })()}

      {/* Entity Relationships */}
      {profile.entity_group && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 px-4 py-3 mb-6">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-2">
            Entity Group
          </div>
          <div className="text-sm text-[#8888A0]">
            {profile.is_entity ? (
              <span>
                Controlled by{" "}
                <Link
                  href={`/insider/${profile.entity_group.primary_insider_id}`}
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
                    href={`/insider/${m.insider_id}`}
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

      {/* Score */}
      {tr && (() => {
        const fc = profile.filing_counts;
        const buyCount = fc?.buy ?? tr.buy_count;
        const sellCount = fc?.sell ?? tr.sell_count;
        return (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
            <StatBox label="Score" value={tr.score != null ? tr.score.toFixed(2) : "\u2014"} sub={tr.percentile != null ? `${(tr.percentile * 100).toFixed(0)}th percentile` : undefined} />
            <StatBox label="Best Window" value={tr.best_window || "\u2014"} />
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
          <h3 className="text-xs font-semibold uppercase tracking-widest text-[#55556A] mb-3">
            Grade by Ticker
          </h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2">
            {((profile as any).ticker_grades as { ticker: string; grade: string; score: number; trade_count: number }[]).map(
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
                    <TierBadge pitGrade={tg.grade} />
                    <span className="text-sm font-mono text-[#E8E8ED] truncate">{tg.ticker}</span>
                  </div>
                  <span className="text-[10px] text-[#55556A] font-mono shrink-0 ml-2">
                    {tg.trade_count} {tg.trade_count === 1 ? "trade" : "trades"}
                  </span>
                </Link>
              ),
            )}
          </div>
        </div>
      )}

      {/* Track Records + Transaction Volume */}
      {tr && (() => {
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
                    <tr className="text-[#55556A]">
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
                        <td key={i} className={`text-right py-1 ${wr != null && wr >= 0.6 ? "text-[#22C55E]" : wr != null ? "text-[#E8E8ED]" : "text-[#55556A]"}`}>
                          {wr != null ? `${(wr * 100).toFixed(0)}%` : "\u2014"}
                        </td>
                      ))}
                    </tr>
                    <tr>
                      <td className="text-[#8888A0] py-1">Avg Move</td>
                      {[buyAvgReturn, tr.buy_avg_return_30d, tr.buy_avg_return_90d].map((r, i) => (
                        <td key={i} className={`text-right py-1 ${r != null ? (r >= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#55556A]"}`}>
                          {r != null ? formatPercent(r) : "\u2014"}
                        </td>
                      ))}
                    </tr>
                    <tr>
                      <td className="text-[#8888A0] py-1">Alpha</td>
                      {[buyAvgAlpha, tr.buy_avg_abnormal_30d, tr.buy_avg_abnormal_90d].map((r, i) => (
                        <td key={i} className={`text-right py-1 ${r != null ? (r >= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#55556A]"}`}>
                          {r != null ? formatPercent(r) : "\u2014"}
                        </td>
                      ))}
                    </tr>
                  </tbody>
                </table>
                {tr.best_window && (
                  <div className="text-[10px] text-[#55556A] mt-2">* Best window</div>
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
                      <tr className="text-[#55556A]">
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
                          <td key={i} className={`text-right py-1 ${wr != null && wr >= 0.6 ? "text-[#22C55E]" : wr != null ? "text-[#E8E8ED]" : "text-[#55556A]"}`}>
                            {wr != null ? `${(wr * 100).toFixed(0)}%` : "\u2014"}
                          </td>
                        ))}
                      </tr>
                      <tr>
                        <td className="text-[#8888A0] py-1">Avg Move</td>
                        {[sellAvgReturn, tr.sell_avg_return_30d, tr.sell_avg_return_90d].map((r, i) => (
                          <td key={i} className={`text-right py-1 ${r != null ? (r <= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#55556A]"}`}>
                            {r != null ? formatPercent(r) : "\u2014"}
                          </td>
                        ))}
                      </tr>
                    </tbody>
                  </table>
                ) : (
                  <div className="text-xs text-[#55556A]">Not yet computed</div>
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
                        "border-[#2A2A3A] text-[#55556A]"
                      }`}>{v.trans_code}</span>
                      <span className="text-[#8888A0] text-xs">{v.label}</span>
                    </div>
                    <div className="text-right">
                      <div className="font-mono text-xs text-[#E8E8ED]">{formatCurrency(v.total_value)}</div>
                      <div className="text-[10px] text-[#55556A]">{v.count} filings</div>
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
                    <span className="text-xs text-[#55556A]">{c.trade_count} trades</span>
                  </div>
                  {!isPrivate && (
                    <div className="text-xs text-[#8888A0] truncate">{c.company}</div>
                  )}
                  <div className="text-xs text-[#55556A] mt-1">
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
      <InsiderTradesTable identifier={id} initialData={trades} />
    </div>
  );
}
