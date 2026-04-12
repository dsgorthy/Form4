"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import Link from "next/link";
import dynamic from "next/dynamic";
import { formatCurrency } from "@/lib/format";
import { registerForm4Theme, baseGrid, COLORS } from "@/lib/echarts-theme";
import { PortfolioOverlay } from "@/components/portfolio-overlay";

registerForm4Theme();
const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });
import { isPro } from "@/lib/subscription";
import { Badge } from "@/components/ui/badge";
import { Pagination } from "@/components/pagination";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

function StarsPill({ stars }: { stars: number }) {
  const color = stars >= 4 ? "text-[#22C55E]" : stars >= 3 ? "text-[#3B82F6]" : "text-[#8888A0]";
  const bg = stars >= 4 ? "bg-[#22C55E]/15" : stars >= 3 ? "bg-[#3B82F6]/15" : "bg-[#8888A0]/10";
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-mono font-semibold ${color} ${bg}`}>
      {"★".repeat(stars)}{"☆".repeat(5 - stars)}
    </span>
  );
}

interface Summary {
  strategy: string;
  starting_capital: number;
  current_equity: number;
  total_pnl: number;
  cagr: number;
  total_trades: number;
  wins: number;
  win_rate: number;
  stops_hit: number;
  max_drawdown: number;
  max_drawdown_all_time?: number;
  max_drawdown_note?: string | null;
  avg_return: number;
  first_trade: string;
  last_trade: string;
}

interface CurvePoint {
  date: string;
  equity: number;
  pnl: number;
  ticker: string;
  exit_reason: string;
}

interface Trade {
  id: number;
  trade_id: string | null;
  ticker: string;
  trade_type: string;
  direction: string;
  entry_date: string;
  entry_price: number | null;
  exit_date: string | null;
  exit_price: number | null;
  hold_days: number | null;
  target_hold: number;
  stop_hit: boolean;
  pnl_pct: number | null;
  pnl_dollar: number | null;
  position_size: number;
  insider_name: string | null;
  insider_pit_wr: number | null;
  signal_quality: number | null;
  exit_reason: string | null;
  status: string;
  execution_source?: string;
  is_estimated?: boolean;
  company?: string | null;
  gated?: boolean;
}

interface PaginationInfo {
  page: number;
  per_page: number;
  total: number;
  total_pages: number;
}

interface ExitBreakdown {
  exit_reason: string;
  count: number;
  avg_return: number;
  win_rate: number;
  avg_hold: number;
}

interface AnnualReturn {
  year: string;
  trades: number;
  pnl: number;
  win_rate: number;
}

function StatCard({ label, value, sub, color }: { label: string; value: string; sub?: string; color?: string }) {
  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-1">{label}</div>
      <div className={`text-xl font-mono font-bold ${color || "text-[#E8E8ED]"}`}>{value}</div>
      {sub && <div className="text-xs text-[#55556A] mt-0.5">{sub}</div>}
    </div>
  );
}

const STRATEGIES = [
  { value: "quality_momentum", label: "Quality + Momentum", brief: "A+/A insiders buying in uptrends. Sharpe 1.20, 68.7% WR, ~50 trades/yr, 42td hold." },
  { value: "reversal_dip", label: "Deep Reversal", brief: "Persistent sellers reversing into depressed stocks. Sharpe 1.08, ~20 trades/yr, 21td hold." },
  { value: "tenb51_surprise", label: "10b5-1 Surprise", brief: "Scheduled sellers breaking pattern to buy. Experimental, ~40 trades/yr, 60td hold.", experimental: true },
];

interface LivePosition {
  symbol: string;
  qty: number;
  avg_entry_price: number;
  current_price: number;
  market_value: number;
  unrealized_pl: number;
  unrealized_plpc: number;
}

interface LiveSnapshot {
  name: string;
  label: string;
  current_equity?: number;
  total_pnl?: number;
  total_pnl_pct?: number;
  day_change?: number;
  day_change_pct?: number;
  position_count?: number;
  open_positions?: LivePosition[];
  deviation_status?: string;
  backtest?: { cagr: number; sharpe: number; win_rate: number; max_dd: number; trades: number };
  error?: string;
}

function LiveStatusPanel({ strategy, userIsPro }: { strategy: string; userIsPro: boolean }) {
  const { getToken } = useAuth();
  const [snapshot, setSnapshot] = useState<LiveSnapshot | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      try {
        const token = await getToken();
        const res = await fetch(`${apiBase}/paper-trading/dashboard`, {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        if (res.ok) {
          const data = await res.json();
          const match = data.strategies?.find((s: LiveSnapshot) => s.name === strategy);
          if (!cancelled && match) setSnapshot(match);
        }
      } catch {}
      if (!cancelled) setLoading(false);
    };
    load();
    const interval = setInterval(load, 60000);
    return () => { cancelled = true; clearInterval(interval); };
  }, [strategy, getToken]);

  if (loading) {
    return (
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 animate-pulse">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="h-20 bg-[#1A1A26] rounded-lg border border-[#2A2A3A]" />
        ))}
      </div>
    );
  }

  if (!snapshot || snapshot.error) return null;

  const eq = snapshot.current_equity ?? 0;
  const pnl = snapshot.total_pnl ?? 0;
  const pnlPct = snapshot.total_pnl_pct ?? 0;
  const dayChg = snapshot.day_change ?? 0;
  const dayPct = snapshot.day_change_pct ?? 0;
  const positions = snapshot.open_positions ?? [];

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">Live Paper Account</div>
        <span className="inline-block w-2 h-2 rounded-full bg-[#22C55E] animate-pulse" />
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <StatCard label="Equity" value={formatCurrency(eq)} color="text-[#E8E8ED]" />
        <StatCard
          label="Total P&L"
          value={`${pnl >= 0 ? "+" : ""}${formatCurrency(pnl)}`}
          sub={`${pnlPct >= 0 ? "+" : ""}${pnlPct.toFixed(1)}%`}
          color={pnl >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}
        />
        <StatCard
          label="Today"
          value={`${dayChg >= 0 ? "+" : ""}${formatCurrency(dayChg)}`}
          sub={`${dayPct >= 0 ? "+" : ""}${dayPct.toFixed(2)}%`}
          color={dayChg >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}
        />
        <StatCard label="Open Positions" value={`${snapshot.position_count ?? positions.length}`} />
      </div>

      {userIsPro && positions.length > 0 && (
        <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-3 py-2 text-left text-[#55556A] font-medium">Symbol</th>
                <th className="px-3 py-2 text-right text-[#55556A] font-medium">Shares</th>
                <th className="px-3 py-2 text-right text-[#55556A] font-medium">Entry</th>
                <th className="px-3 py-2 text-right text-[#55556A] font-medium">Current</th>
                <th className="px-3 py-2 text-right text-[#55556A] font-medium">Unrealized</th>
              </tr>
            </thead>
            <tbody>
              {positions.map((p) => (
                <tr key={p.symbol} className="border-b border-[#2A2A3A]/30">
                  <td className="px-3 py-2 font-mono font-semibold text-[#E8E8ED]">{p.symbol}</td>
                  <td className="px-3 py-2 text-right text-[#8888A0] font-mono">{p.qty}</td>
                  <td className="px-3 py-2 text-right text-[#8888A0] font-mono">${p.avg_entry_price.toFixed(2)}</td>
                  <td className="px-3 py-2 text-right text-[#8888A0] font-mono">${p.current_price.toFixed(2)}</td>
                  <td className={`px-3 py-2 text-right font-mono ${p.unrealized_pl >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                    {p.unrealized_pl >= 0 ? "+" : ""}{formatCurrency(p.unrealized_pl)} ({p.unrealized_plpc >= 0 ? "+" : ""}{p.unrealized_plpc.toFixed(1)}%)
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!userIsPro && positions.length > 0 && (
        <div className="rounded-lg border border-[#3B82F6]/20 bg-[#3B82F6]/5 p-3 text-xs text-[#8888A0]">
          {positions.length} open position{positions.length !== 1 ? "s" : ""}. <Link href="/pricing" className="text-[#3B82F6] hover:underline">Upgrade to Pro</Link> to see individual holdings and real-time P&L.
        </div>
      )}
    </div>
  );
}

function StrategySelector({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const current = STRATEGIES.find((s) => s.value === value);
  return (
    <div className="relative inline-block">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="appearance-none bg-[#12121A] border border-[#2A2A3A] rounded-lg px-3 py-2 pr-8 text-sm text-[#E8E8ED] font-medium cursor-pointer hover:border-[#3B82F6]/40 focus:border-[#3B82F6] focus:outline-none transition-colors"
      >
        {STRATEGIES.map((s) => (
          <option key={s.value} value={s.value}>{s.label}</option>
        ))}
      </select>
      <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
        <svg className="h-4 w-4 text-[#55556A]" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </div>
    </div>
  );
}

function RunnerStatus({ strategy }: { strategy: string }) {
  const [status, setStatus] = useState<{ healthy?: boolean; status?: string; timestamp?: string } | null>(null);
  const { getToken } = useAuth();

  useEffect(() => {
    const check = async () => {
      try {
        const token = await getToken();
        const res = await fetch(`${apiBase}/portfolio/runner-status?strategy=${strategy}`, {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        if (res.ok) setStatus(await res.json());
      } catch {}
    };
    check();
    const interval = setInterval(check, 60000);
    return () => clearInterval(interval);
  }, [strategy, getToken]);

  if (!status) return null;

  const isLive = status.healthy === true;
  const dot = isLive ? "bg-[#22C55E]" : "bg-[#55556A]";
  const label = isLive ? "Live" : status.status === "weekend" ? "Weekend" : "Offline";
  const ts = status.timestamp ? new Date(status.timestamp).toLocaleString() : "";

  return (
    <div className="flex items-center gap-1.5 text-[10px] text-[#8888A0]">
      <span className={`inline-block w-2 h-2 rounded-full ${dot} ${isLive ? "animate-pulse" : ""}`} />
      <span>{label}</span>
      {ts && <span className="text-[#55556A]">{ts}</span>}
    </div>
  );
}

export function PortfolioView() {
  const { getToken, isSignedIn } = useAuth();
  const { user } = useUser();
  const userIsPro = isPro(user);

  const [strategy, setStrategy] = useState("quality_momentum");
  const [summary, setSummary] = useState<Summary | null>(null);
  const [equityCurve, setEquityCurve] = useState<CurvePoint[]>([]);
  const [spyBenchmark, setSpyBenchmark] = useState<{ date: string; equity: number }[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [returnDistribution, setReturnDistribution] = useState<number[]>([]);
  const [tradePoints, setTradePoints] = useState<{ exit_date: string; pnl_pct: number; exit_reason: string; hold_days: number; signal_quality: number }[]>([]);
  const [exitBreakdown, setExitBreakdown] = useState<ExitBreakdown[]>([]);
  const [annualReturns, setAnnualReturns] = useState<AnnualReturn[]>([]);
  const [dateRange, setDateRange] = useState<{ from: string | null; to: string | null }>({ from: null, to: null });
  const [paginationInfo, setPaginationInfo] = useState<PaginationInfo | null>(null);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const [tradesLoading, setTradesLoading] = useState(false);

  const limit = 25;

  const loadData = useCallback(async (newOffset: number, initial: boolean = false) => {
    if (initial) setLoading(true);
    else setTradesLoading(true);

    const page = Math.floor(newOffset / limit) + 1;
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/portfolio?strategy=${strategy}&page=${page}&per_page=${limit}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      if (res.ok) {
        const data = await res.json();
        if (initial) {
          setSummary(data.summary);
          setEquityCurve(data.equity_curve);
          setSpyBenchmark(data.spy_benchmark || []);
          setReturnDistribution(data.return_distribution || []);
          setTradePoints(data.trade_points || []);
          setExitBreakdown(data.exit_breakdown || []);
          setAnnualReturns(data.annual_returns || []);
        }
        setTrades(data.trades);
        setPaginationInfo(data.pagination);
      }
    } catch {}

    if (initial) setLoading(false);
    else setTradesLoading(false);
  }, [getToken, strategy]);

  useEffect(() => { setOffset(0); loadData(0, true); }, [loadData]);

  const handlePageChange = (newOffset: number) => {
    setOffset(newOffset);
    loadData(newOffset);
    document.getElementById("trade-log")?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  if (loading) {
    return <div className="space-y-4 animate-pulse">
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        {[1,2,3,4,5].map(i => <div key={i} className="h-20 bg-[#1A1A26] rounded-lg border border-[#2A2A3A]" />)}
      </div>
      <div className="h-64 bg-[#1A1A26] rounded-lg border border-[#2A2A3A]" />
    </div>;
  }

  if (!summary) return <div className="text-[#55556A]">Failed to load portfolio data</div>;

  const s = summary;
  const isPositive = s.total_pnl >= 0;

  return (
    <div className="space-y-6">
      {/* Strategy selector */}
      <div className="flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-4">
        <div className="flex items-center justify-between gap-3">
          <StrategySelector value={strategy} onChange={setStrategy} />
          <RunnerStatus strategy={strategy} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <div className="text-xs sm:text-sm text-[#8888A0]">{STRATEGIES.find(s => s.value === strategy)?.brief}</div>
            {(STRATEGIES.find(s => s.value === strategy) as any)?.experimental && (
              <span className="shrink-0 rounded-full bg-[#F59E0B]/15 px-2 py-0.5 text-[10px] font-semibold text-[#F59E0B]">Experimental</span>
            )}
          </div>
        </div>
      </div>

      {/* Live paper account status */}
      <LiveStatusPanel strategy={strategy} userIsPro={userIsPro} />

      {/* Portfolio Performance — blended equity with idle cash in base ETF */}
      <PortfolioOverlay strategy={strategy} onDateRangeChange={(from, to) => setDateRange({ from, to })} />

      {/* Everything below responds to the chart date range */}
      {(() => {
        const filtered = tradePoints.filter(tp => {
          if (dateRange.from && tp.exit_date < dateRange.from) return false;
          if (dateRange.to && tp.exit_date > dateRange.to) return false;
          return true;
        });
        const filteredReturns = filtered.map(tp => tp.pnl_pct);
        const isFiltered = dateRange.from !== null && (dateRange.from > (tradePoints[0]?.exit_date || ""));

        const fTotal = filtered.length;
        const fWins = filtered.filter(tp => tp.pnl_pct > 0).length;
        const fWR = fTotal > 0 ? (fWins / fTotal * 100) : 0;
        const fAvgRet = fTotal > 0 ? filtered.reduce((acc, tp) => acc + tp.pnl_pct, 0) / fTotal : 0;
        const fStops = filtered.filter(tp => tp.exit_reason === "stop_loss").length;

        // Use API-computed max drawdown (dollar-based equity curve, not additive pct)
        const mdd = isFiltered
          ? (() => {
              // For filtered date range, recompute from equity curve points
              const curveFiltered = equityCurve.filter(ep => {
                if (dateRange.from && ep.date < dateRange.from) return false;
                if (dateRange.to && ep.date > dateRange.to) return false;
                return true;
              });
              let pk = curveFiltered[0]?.equity || s.starting_capital;
              let maxDd = 0;
              for (const ep of curveFiltered) {
                if (ep.equity > pk) pk = ep.equity;
                const dd = pk > 0 ? ((pk - ep.equity) / pk * 100) : 0;
                if (dd > maxDd) maxDd = dd;
              }
              return maxDd;
            })()
          : s.max_drawdown;

        const rangeLabel = isFiltered
          ? `${dateRange.from?.slice(0,4) || ""} – ${dateRange.to?.slice(0,4) || ""}`
          : `${s.first_trade?.slice(0,4)} – ${s.last_trade?.slice(0,4)}`;

        const exitMap = new Map<string, { count: number; wins: number; totalRet: number; totalHold: number }>();
        for (const tp of filtered) {
          const e = exitMap.get(tp.exit_reason) || { count: 0, wins: 0, totalRet: 0, totalHold: 0 };
          e.count++;
          if (tp.pnl_pct > 0) e.wins++;
          e.totalRet += tp.pnl_pct;
          e.totalHold += tp.hold_days || 0;
          exitMap.set(tp.exit_reason, e);
        }
        const filteredExits: ExitBreakdown[] = Array.from(exitMap.entries())
          .map(([reason, e]) => ({
            exit_reason: reason,
            count: e.count,
            win_rate: e.count > 0 ? (e.wins / e.count) * 100 : 0,
            avg_return: e.count > 0 ? e.totalRet / e.count : 0,
            avg_hold: e.count > 0 ? e.totalHold / e.count : 0,
          }))
          .sort((a, b) => b.count - a.count);

        const displayReturns = isFiltered ? filteredReturns : returnDistribution;
        const displayExits = isFiltered ? filteredExits : exitBreakdown;
        const tradeCount = displayReturns.length;

        return (
      <>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <StatCard label="Total Trades" value={`${fTotal}`} sub={isFiltered ? rangeLabel : `${s.first_trade?.slice(0,4)} – ${s.last_trade?.slice(0,4)}`} />
        <StatCard label="Win Rate" value={`${fWR.toFixed(1)}%`} sub={`${fWins}/${fTotal} trades`} color={fWR >= 50 ? "text-[#22C55E]" : "text-[#E8E8ED]"} />
        <StatCard label="Avg Return" value={`${fAvgRet > 0 ? "+" : ""}${fAvgRet.toFixed(2)}%`} sub="Per trade" color={fAvgRet >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"} />
        <StatCard
          label="Max Drawdown"
          value={`${mdd.toFixed(1)}%`}
          sub={s.max_drawdown_note ? `${s.max_drawdown_note} | All-time: ${s.max_drawdown_all_time?.toFixed(1)}%` : `${fStops} stops hit`}
        />
      </div>

      {/* Return Distribution + Exit Breakdown + Annual Returns */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Return Distribution — scatter strip by exit type — 2/3 width */}
        <div className="lg:col-span-2 rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-4">
            Trade Returns {isFiltered ? "(filtered)" : ""} — {tradeCount} Trades
          </div>
          {(() => {
            // Build scatter data from exit breakdown + return distribution
            // We need per-trade exit_reason + return — use the exitBreakdown summary stats
            // and the raw distribution for the histogram overlay
            const exitColors: Record<string, string> = {
              target_gain: "#22C55E",
              time_exit: "#3B82F6",
              trailing_stop: "#F59E0B",
              stop_loss: "#EF4444",
            };

            // Histogram with finer bins for all trades
            const binWidth = 1; // 1% bins
            const minBin = -12;
            const maxBin = 10;
            const bins: string[] = [];
            const winData: number[] = [];
            const loseData: number[] = [];
            for (let b = minBin; b < maxBin; b += binWidth) {
              bins.push(b === minBin ? `≤${b}%` : b === maxBin - binWidth ? `≥${b}%` : `${b}%`);
              const inBin = displayReturns.filter(r =>
                b === minBin ? r < b + binWidth :
                b === maxBin - binWidth ? r >= b :
                r >= b && r < b + binWidth
              );
              winData.push(inBin.filter(r => r >= 0).length);
              loseData.push(-inBin.filter(r => r < 0).length); // negative for stacking below
            }

            return (
              <ReactECharts
                option={{
                  grid: { ...baseGrid(), bottom: 30, left: 45, right: 10, top: 10 },
                  xAxis: {
                    type: "category",
                    data: bins,
                    axisLabel: { fontSize: 9, color: COLORS.muted, interval: 1 },
                    axisLine: { lineStyle: { color: COLORS.border } },
                  },
                  yAxis: {
                    type: "value",
                    splitLine: { lineStyle: { color: COLORS.border, type: "dashed" } },
                    axisLabel: {
                      fontSize: 10, color: COLORS.muted,
                      formatter: (v: number) => `${Math.abs(v)}`,
                    },
                  },
                  series: [
                    {
                      name: "Winning",
                      type: "bar",
                      stack: "dist",
                      data: winData,
                      itemStyle: { color: "#22C55E", opacity: 0.8, borderRadius: [2, 2, 0, 0] },
                    },
                    {
                      name: "Losing",
                      type: "bar",
                      stack: "dist",
                      data: loseData,
                      itemStyle: { color: "#EF4444", opacity: 0.8, borderRadius: [0, 0, 2, 2] },
                    },
                  ],
                  tooltip: {
                    trigger: "axis",
                    axisPointer: { type: "shadow" },
                    formatter: (params: any) => {
                      const ps = Array.isArray(params) ? params : [params];
                      const label = ps[0]?.axisValue || "";
                      const w = Math.abs(ps.find((p: any) => p.seriesName === "Winning")?.value || 0);
                      const l = Math.abs(ps.find((p: any) => p.seriesName === "Losing")?.value || 0);
                      return `<div style="color:#55556A">${label}</div>
                        <div style="color:#22C55E;font-family:monospace">${w} winners</div>
                        <div style="color:#EF4444;font-family:monospace">${l} losers</div>
                        <div style="color:#8888A0;font-family:monospace">${w + l} total</div>`;
                    },
                  },
                }}
                theme="form4"
                className="h-[220px] sm:h-[300px]"
                opts={{ renderer: "svg" }}
              />
            );
          })()}
        </div>

        {/* Exit Breakdown + Annual Returns — 1/3 width */}
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-4">
            Exit Breakdown
          </div>
          <div className="space-y-3">
            {(() => {
              const exitConfig: Record<string, { label: string; color: string }> = {
                target_gain: { label: "Target Gain (+8%)", color: "#22C55E" },
                time_exit: { label: "Time Exit (14d)", color: "#8888A0" },
                stop_loss: { label: "Hard Stop (-10%)", color: "#EF4444" },
                trailing_stop: { label: "Trailing Stop", color: "#F59E0B" },
              };
              const totalExits = displayExits.reduce((s, e) => s + e.count, 0);
              return displayExits.map((eb) => {
                const cfg = exitConfig[eb.exit_reason] || { label: eb.exit_reason, color: "#8888A0" };
                const pct = totalExits > 0 ? eb.count / totalExits : 0;
                return (
                  <div key={eb.exit_reason}>
                    <div className="flex items-center justify-between text-xs mb-1">
                      <span style={{ color: cfg.color }} className="font-medium">{cfg.label}</span>
                      <span className="text-[#55556A]">{eb.count} ({(pct * 100).toFixed(0)}%) · {Math.round(eb.avg_hold)}d avg</span>
                    </div>
                    <div className="flex items-center gap-3">
                      <div className="flex-1 h-2 bg-[#1A1A26] rounded-full overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${pct * 100}%`, backgroundColor: cfg.color, opacity: 0.6 }} />
                      </div>
                      <span className="text-xs font-mono text-[#8888A0] w-24 text-right">
                        WR {eb.win_rate.toFixed(0)}% · {eb.avg_return > 0 ? "+" : ""}{eb.avg_return.toFixed(1)}%
                      </span>
                    </div>
                  </div>
                );
              });
            })()}
          </div>

          {/* Annual Returns */}
          <div className="mt-6">
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-2">
              Annual P&L
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-1 text-[10px]">
              {annualReturns.map(ar => (
                <div key={ar.year} className={`rounded px-1.5 py-1 text-center font-mono ${ar.pnl >= 0 ? "bg-[#22C55E]/10 text-[#22C55E]" : "bg-[#EF4444]/10 text-[#EF4444]"}`}>
                  {ar.year}: {ar.pnl >= 0 ? "+" : ""}{formatCurrency(ar.pnl)}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
      </>
        );
      })()}

      {/* Portfolio Alerts CTA (free users) */}
      {!userIsPro && (
        <div className="rounded-lg border border-[#3B82F6]/20 bg-gradient-to-r from-[#3B82F6]/5 to-[#12121A] p-5">
          <div className="flex items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <svg className="w-4 h-4 text-[#3B82F6]" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
                </svg>
                <span className="text-sm font-semibold text-[#E8E8ED]">Get Portfolio Trade Alerts</span>
              </div>
              <p className="text-xs text-[#55556A]">
                Pro members receive real-time notifications when this portfolio enters or exits positions.
                Follow the same trades that produced {s.cagr > 0 ? "+" : ""}{s.cagr}% CAGR.
              </p>
            </div>
            <Link
              href={isSignedIn ? "/pricing" : "/sign-up"}
              className="shrink-0 rounded-lg bg-[#3B82F6] px-4 py-2 text-sm font-medium text-white hover:bg-[#2563EB] transition-colors"
            >
              {isSignedIn ? "Upgrade to Pro" : "Start Free Trial"}
            </Link>
          </div>
        </div>
      )}

      {/* Trade Log */}
      <div id="trade-log">
        <div className="flex items-center justify-between mb-3">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
            Trade Log ({paginationInfo?.total ?? trades.length} trades)
          </div>
        </div>
        <div className={`overflow-x-auto rounded-lg border border-[#2A2A3A] ${tradesLoading ? "opacity-50" : ""}`}>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-[#2A2A3A] bg-[#1A1A26]/50">
                <th className="px-3 py-2.5 text-left text-[#55556A] font-medium">Ticker</th>
                <th className="px-3 py-2.5 text-left text-[#55556A] font-medium">Insider</th>
                <th className="px-3 py-2.5 text-left text-[#55556A] font-medium">Entry</th>
                <th className="px-3 py-2.5 text-right text-[#55556A] font-medium">Entry $</th>
                <th className="px-3 py-2.5 text-left text-[#55556A] font-medium">Exit</th>
                <th className="px-3 py-2.5 text-right text-[#55556A] font-medium">Exit $</th>
                <th className="px-3 py-2.5 text-right text-[#55556A] font-medium">Hold</th>
                <th className="px-3 py-2.5 text-right text-[#55556A] font-medium">Return</th>
                <th className="px-3 py-2.5 text-right text-[#55556A] font-medium">P&L</th>
                <th className="px-3 py-2.5 text-center text-[#55556A] font-medium">Exit</th>
                <th className="hidden lg:table-cell px-3 py-2.5 text-center text-[#55556A] font-medium">Signal</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t) => {
                const isWin = t.pnl_pct != null && t.pnl_pct > 0;
                const gated = t.gated === true;
                return (
                  <tr
                    key={t.id}
                    className={`border-b border-[#2A2A3A]/30 ${gated ? "select-none" : "hover:bg-[#1A1A26]/50 cursor-pointer"}`}
                    onClick={() => { if (!gated) window.location.href = `/portfolio/trades/${t.id}`; }}
                  >
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-1.5">
                        <span className={`font-mono font-semibold ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>{t.ticker}</span>
                        {!gated && t.execution_source === "paper" && (
                          <span className="inline-block w-1.5 h-1.5 rounded-full bg-[#22C55E]" title="Paper traded (real fills)" />
                        )}
                      </div>
                    </td>
                    <td className={`px-3 py-2 truncate max-w-[140px] ${gated ? "text-[#8888A0]/40 blur-[3px]" : "text-[#8888A0]"}`}>
                      {t.insider_name || "\u2014"}
                    </td>
                    <td className={`px-3 py-2 ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>{t.entry_date}</td>
                    <td className={`px-3 py-2 text-right font-mono ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>
                      {t.entry_price != null ? `$${t.entry_price.toFixed(2)}` : "\u2014"}
                    </td>
                    <td className={`px-3 py-2 ${gated ? "text-[#8888A0]/40 blur-[3px]" : "text-[#8888A0]"}`}>{t.exit_date || "Open"}</td>
                    <td className={`px-3 py-2 text-right font-mono ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>
                      {t.exit_price != null ? `$${t.exit_price.toFixed(2)}` : "\u2014"}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono ${gated ? "text-[#55556A]/40 blur-[3px]" : "text-[#55556A]"}`} title={t.hold_days != null ? `${t.hold_days} calendar days` : ""}>
                      {t.hold_days != null ? `${Math.round(t.hold_days * 5/7)}td` : "\u2014"}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : t.pnl_pct != null ? (isWin ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#55556A]"}`}>
                      {t.pnl_pct != null ? `${t.pnl_pct > 0 ? "+" : ""}${t.pnl_pct.toFixed(1)}%` : "\u2014"}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono ${gated ? "text-[#E8E8ED]/40 blur-[3px]" : t.pnl_dollar != null ? (t.pnl_dollar >= 0 ? "text-[#22C55E]" : "text-[#EF4444]") : "text-[#55556A]"}`}>
                      {t.pnl_dollar != null ? `${t.pnl_dollar >= 0 ? "+" : ""}$${Math.abs(t.pnl_dollar).toFixed(0)}` : "\u2014"}
                    </td>
                    <td className="px-3 py-2 text-center">
                      {t.stop_hit ? (
                        <Badge variant="outline" className="text-[9px] border-[#EF4444]/30 text-[#EF4444]">STOP</Badge>
                      ) : t.exit_reason === "trailing_stop" ? (
                        <Badge variant="outline" className="text-[9px] border-[#F59E0B]/30 text-[#F59E0B]">TRAIL</Badge>
                      ) : t.exit_reason === "time_exit" || t.exit_reason === "eod_time_exit" ? (
                        <span className="text-[#55556A]">T+{t.target_hold}</span>
                      ) : t.status === "open" ? (
                        <Badge variant="outline" className="text-[9px] border-[#3B82F6]/30 text-[#3B82F6]">OPEN</Badge>
                      ) : null}
                    </td>
                    <td className="hidden lg:table-cell px-3 py-2 text-center">
                      {!gated && (t as any).trade_grade_stars != null ? (
                        <StarsPill stars={(t as any).trade_grade_stars} />
                      ) : gated ? (
                        <span className="text-[#55556A]/40 blur-[3px]">—</span>
                      ) : null}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Pagination — shared component */}
        {paginationInfo && (
          <Pagination
            total={paginationInfo.total}
            limit={limit}
            offset={offset}
            onPageChange={handlePageChange}
          />
        )}

        {/* Upgrade CTA in trade log for free users seeing blurred rows */}
        {!userIsPro && paginationInfo && paginationInfo.total > 10 && (
          <div className="mt-3 rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-3 flex items-center justify-between">
            <span className="text-xs text-[#55556A]">
              Showing 10 of {paginationInfo.total} trades. Upgrade to see the full trade log + get real-time entry/exit alerts.
            </span>
            <Link
              href="/pricing"
              className="shrink-0 rounded-md bg-[#3B82F6] px-3 py-1.5 text-xs font-medium text-white hover:bg-[#2563EB] transition-colors"
            >
              Upgrade
            </Link>
          </div>
        )}
      </div>

      {/* Strategy description */}
      <div className="rounded-lg border border-[#2A2A3A]/50 bg-[#1A1A26]/30 p-4 text-xs text-[#55556A] space-y-1">
        <div className="text-[10px] font-semibold uppercase tracking-widest mb-2">Strategy Rules</div>
        <p>Entry: Buy at market open on T+1 after SEC Form 4 filing. Requires proven insider track record (PIT WR &ge; 50%, 3+ prior trades). 10% owners and Chairman-only roles excluded.</p>
        <p>Position size: Variable 5–10% by signal quality. CFO/VP buys and rare reversals get largest size. Max 20 concurrent.</p>
        <p>Quality factors: PIT win rate, role (CFO &gt; VP &gt; CEO &gt; Director), rare reversal, holdings % increase. No look-ahead bias.</p>
        <p>Exit: +8% target gain, 14-day time exit, -10% hard stop, or 5% trailing stop from peak — whichever comes first.</p>
        <p>Past performance is simulated and does not guarantee future results.</p>
      </div>
    </div>
  );
}
