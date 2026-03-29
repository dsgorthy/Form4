"use client";

import { CalendarHeatmap } from "@/components/calendar-heatmap";
import { formatCurrency } from "@/lib/format";
import type { HeatmapDay } from "@/lib/types";

interface CongressSummary {
  total_trades: number;
  total_value: number;
  buys: number;
  sells: number;
  buy_value: number;
  sell_value: number;
  active_politicians: number;
  unique_tickers: number;
  avg_filing_delay: number | null;
}

interface TopTicker {
  ticker: string;
  trade_count: number;
  total_value: number;
  buys: number;
  sells: number;
  politicians: number;
}

interface TopPolitician {
  name: string;
  party: string;
  chamber: string;
  trade_count: number;
  total_value: number;
  buys: number;
  sells: number;
}

interface CongressAnalyticsProps {
  summary: CongressSummary;
  heatmap: HeatmapDay[];
  topTickers: TopTicker[];
  topPoliticians: TopPolitician[];
}

const PARTY_DOT: Record<string, string> = {
  D: "bg-[#3B82F6]",
  R: "bg-[#EF4444]",
  I: "bg-[#8888A0]",
};

export function CongressAnalytics({ summary, heatmap, topTickers, topPoliticians }: CongressAnalyticsProps) {
  const buyPct = summary.total_trades > 0 ? Math.round((summary.buys / summary.total_trades) * 100) : 0;
  const sellPct = 100 - buyPct;

  return (
    <div className="space-y-6 mb-8">
      {/* Stat cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Trades (90d)" value={summary.total_trades.toLocaleString()} />
        <StatCard label="Est. Value" value={formatCurrency(summary.total_value)} />
        <StatCard label="Politicians" value={String(summary.active_politicians)} />
        <StatCard
          label="Avg Filing Delay"
          value={summary.avg_filing_delay ? `${summary.avg_filing_delay}d` : "\u2014"}
        />
      </div>

      {/* Buy/Sell bar */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
        <div className="flex items-center justify-between text-xs text-[#8888A0] mb-2">
          <span>
            <span className="text-[#22C55E] font-mono font-medium">{summary.buys}</span> buys ({formatCurrency(summary.buy_value)})
          </span>
          <span>
            <span className="text-[#EF4444] font-mono font-medium">{summary.sells}</span> sells ({formatCurrency(summary.sell_value)})
          </span>
        </div>
        <div className="h-2 rounded-full bg-[#1A1A26] overflow-hidden flex">
          <div
            className="bg-[#22C55E] transition-all"
            style={{ width: `${buyPct}%` }}
          />
          <div
            className="bg-[#EF4444] transition-all"
            style={{ width: `${sellPct}%` }}
          />
        </div>
      </div>

      {/* Heatmap — full width */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
          Trading Activity (1 year)
        </div>
        <CalendarHeatmap data={heatmap} days={365} />
      </div>

      {/* Top Tickers */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
            Most Traded Tickers
          </div>
          <div className="space-y-2">
            {topTickers.slice(0, 8).map((t) => {
              const maxCount = topTickers[0]?.trade_count ?? 1;
              const barWidth = Math.max((t.trade_count / maxCount) * 100, 4);
              return (
                <div key={t.ticker} className="flex items-center gap-2">
                  <span className="font-mono text-xs text-[#E8E8ED] w-12 shrink-0">{t.ticker}</span>
                  <div className="flex-1 h-4 bg-[#1A1A26] rounded overflow-hidden relative">
                    <div
                      className="h-full bg-[#3B82F6]/30 rounded"
                      style={{ width: `${barWidth}%` }}
                    />
                    <span className="absolute inset-y-0 right-1 flex items-center text-[10px] font-mono text-[#8888A0]">
                      {t.trade_count}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Top Politicians */}
        {topPoliticians.length > 0 && (
          <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-3">
              Most Active Politicians
            </div>
            <div className="space-y-2">
              {topPoliticians.map((p) => (
                <div
                  key={p.name}
                  className="flex items-center gap-2 rounded-md border border-[#2A2A3A]/50 bg-[#1A1A26]/40 px-3 py-2"
                >
                  <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${PARTY_DOT[p.party] ?? PARTY_DOT.I}`} />
                  <div className="min-w-0 flex-1">
                    <div className="text-xs text-[#E8E8ED] font-medium truncate">{p.name}</div>
                    <div className="text-[10px] text-[#55556A]">
                      {p.trade_count} trades · {formatCurrency(p.total_value)}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-3">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">{label}</div>
      <div className="text-lg font-mono font-bold text-[#E8E8ED] mt-1">{value}</div>
    </div>
  );
}
