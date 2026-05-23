"use client";

import { useEffect, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { fetchAPIWithAuth } from "@/lib/api";

interface SummaryRow {
  strategy: string;
  drift_type: string;
  severity: string;
  n: number;
  last_seen: string;
}

interface DriftEvent {
  id: number;
  ts: string;
  strategy: string;
  ticker: string;
  entry_date: string;
  drift_type: string;
  severity: string;
  sim_status: string | null;
  paper_status: string | null;
  sim_entry_price: number | null;
  paper_entry_price: number | null;
  sim_dollar_amount: number | null;
  paper_dollar_amount: number | null;
  sim_pnl_pct: number | null;
  paper_pnl_pct: number | null;
  notes: string | null;
}

interface DriftResponse {
  checked_at: string;
  window_days: number;
  summary: SummaryRow[];
  events: DriftEvent[];
}

function severityColor(s: string): string {
  if (s === "critical") return "text-[#EF4444]";
  if (s === "warn") return "text-[#F59E0B]";
  return "text-[#8888A0]";
}

function driftTypeBadge(t: string): { bg: string; label: string } {
  const map: Record<string, { bg: string; label: string }> = {
    sim_only: { bg: "bg-[#F59E0B]/20 text-[#F59E0B]", label: "sim only" },
    paper_only: { bg: "bg-[#3B82F6]/20 text-[#3B82F6]", label: "paper only" },
    size_delta: { bg: "bg-[#8888A0]/20 text-[#8888A0]", label: "size Δ" },
    price_delta: { bg: "bg-[#8888A0]/20 text-[#8888A0]", label: "price Δ" },
    exit_delta: { bg: "bg-[#8888A0]/20 text-[#8888A0]", label: "exit Δ" },
  };
  return map[t] ?? { bg: "bg-[#55556A]/20 text-[#55556A]", label: t };
}

function fmtMoney(n: number | null): string {
  if (n === null || n === undefined) return "—";
  return `$${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function fmtPrice(n: number | null): string {
  if (n === null || n === undefined) return "—";
  return `$${n.toFixed(2)}`;
}

function fmtPct(n: number | null): string {
  if (n === null || n === undefined) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

export default function AdminDriftPage() {
  const { isSignedIn, getToken } = useAuth();
  const [data, setData] = useState<DriftResponse | null>(null);
  const [days, setDays] = useState(7);
  const [strategy, setStrategy] = useState<string>("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const token = await getToken();
        const q = new URLSearchParams({ days: String(days) });
        if (strategy) q.set("strategy", strategy);
        const d = await fetchAPIWithAuth<DriftResponse>(
          `/admin/diagnostics/drift?${q.toString()}`,
          token,
        );
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes("403")) {
          setError("Access denied. Admin only.");
        } else {
          setError(msg);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    if (isSignedIn) load();
  }, [isSignedIn, getToken, days, strategy]);

  if (!isSignedIn) {
    return <div className="text-[#8888A0] py-10">Sign in to view.</div>;
  }
  if (loading) {
    return <div className="text-[#8888A0] py-10">Loading drift events…</div>;
  }
  if (error) {
    return (
      <div className="py-10">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Drift</h1>
        <p className="text-[#EF4444] mt-4">{error}</p>
      </div>
    );
  }

  const totalDrifts = data?.summary.reduce((a, s) => a + s.n, 0) ?? 0;
  const criticalCount = data?.summary
    .filter((s) => s.severity === "critical")
    .reduce((a, s) => a + s.n, 0) ?? 0;
  const warnCount = data?.summary
    .filter((s) => s.severity === "warn")
    .reduce((a, s) => a + s.n, 0) ?? 0;

  return (
    <div className="text-[#E8E8ED] py-6">
      <div className="mb-6 flex flex-wrap items-baseline justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold">Sim vs Paper Drift</h1>
          <p className="text-sm text-[#55556A] mt-1">
            Every (strategy, ticker, entry_date) where the simulator and the
            paper account disagree. Recorded daily by{" "}
            <code className="text-[#8888A0]">scripts/drift_detector.py</code>.
            See also{" "}
            <Link href="/admin/strategies" className="text-[#3B82F6] hover:underline">
              /admin/strategies
            </Link>{" "}
            and{" "}
            <Link href="/admin/pipelines" className="text-[#3B82F6] hover:underline">
              /admin/pipelines
            </Link>
            .
          </p>
        </div>
        <div className="flex items-center gap-2 text-sm">
          <label className="text-[#55556A] text-xs uppercase tracking-wider">Strategy</label>
          <select
            value={strategy}
            onChange={(e) => setStrategy(e.target.value)}
            className="bg-[#1A1A26] border border-[#2A2A3A] rounded px-2 py-1 text-[#E8E8ED]"
          >
            <option value="">all</option>
            <option value="quality_momentum">quality_momentum</option>
            <option value="reversal_dip">reversal_dip</option>
            <option value="tenb51_surprise">tenb51_surprise</option>
          </select>
          <label className="text-[#55556A] text-xs uppercase tracking-wider ml-3">Window</label>
          <select
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="bg-[#1A1A26] border border-[#2A2A3A] rounded px-2 py-1 text-[#E8E8ED]"
          >
            <option value={1}>1d</option>
            <option value={7}>7d</option>
            <option value={30}>30d</option>
            <option value={90}>90d</option>
          </select>
        </div>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3 mb-6">
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
          <div className="text-[10px] uppercase tracking-wider text-[#55556A]">Total drift events</div>
          <div className="text-2xl font-mono font-bold mt-1">{totalDrifts.toLocaleString()}</div>
        </div>
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
          <div className="text-[10px] uppercase tracking-wider text-[#55556A]">Critical</div>
          <div className={`text-2xl font-mono font-bold mt-1 ${criticalCount > 0 ? "text-[#EF4444]" : "text-[#22C55E]"}`}>
            {criticalCount.toLocaleString()}
          </div>
        </div>
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
          <div className="text-[10px] uppercase tracking-wider text-[#55556A]">Warn</div>
          <div className={`text-2xl font-mono font-bold mt-1 ${warnCount > 0 ? "text-[#F59E0B]" : "text-[#22C55E]"}`}>
            {warnCount.toLocaleString()}
          </div>
        </div>
      </div>

      {/* Summary by strategy + drift_type */}
      <h2 className="text-lg font-semibold mb-3">Summary ({data?.window_days}d)</h2>
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] mb-6 overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-[#2A2A3A] text-[#55556A] text-xs uppercase">
            <tr>
              <th className="text-left p-3">Strategy</th>
              <th className="text-left p-3">Drift type</th>
              <th className="text-left p-3">Severity</th>
              <th className="text-right p-3">Count</th>
              <th className="text-right p-3">Last seen</th>
            </tr>
          </thead>
          <tbody>
            {data?.summary.map((s, i) => {
              const badge = driftTypeBadge(s.drift_type);
              return (
                <tr
                  key={`${s.strategy}-${s.drift_type}-${i}`}
                  className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]"
                >
                  <td className="p-3 font-mono">{s.strategy}</td>
                  <td className="p-3">
                    <span className={`text-xs px-2 py-0.5 rounded ${badge.bg}`}>{badge.label}</span>
                  </td>
                  <td className={`p-3 ${severityColor(s.severity)}`}>{s.severity}</td>
                  <td className="p-3 text-right font-mono">{s.n}</td>
                  <td className="p-3 text-right text-xs text-[#55556A]">{s.last_seen?.slice(0, 16)}</td>
                </tr>
              );
            })}
            {data?.summary.length === 0 && (
              <tr>
                <td colSpan={5} className="p-6 text-center text-[#22C55E]">
                  No drift in window. Sim and paper are aligned. ✓
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Recent events */}
      <h2 className="text-lg font-semibold mb-3">Events</h2>
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-[#2A2A3A] text-[#55556A] text-xs uppercase">
            <tr>
              <th className="text-left p-3">Detected</th>
              <th className="text-left p-3">Strategy</th>
              <th className="text-left p-3">Ticker</th>
              <th className="text-left p-3">Entry</th>
              <th className="text-left p-3">Drift</th>
              <th className="text-right p-3">Sim $</th>
              <th className="text-right p-3">Paper $</th>
              <th className="text-right p-3">Sim px</th>
              <th className="text-right p-3">Paper px</th>
              <th className="text-left p-3">Notes</th>
            </tr>
          </thead>
          <tbody>
            {data?.events.map((e) => {
              const badge = driftTypeBadge(e.drift_type);
              return (
                <tr key={e.id} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                  <td className="p-3 font-mono text-xs text-[#8888A0]">
                    {e.ts.slice(0, 16).replace("T", " ")}
                  </td>
                  <td className="p-3 font-mono text-xs">{e.strategy}</td>
                  <td className="p-3 font-mono font-medium">{e.ticker}</td>
                  <td className="p-3 font-mono text-xs">{e.entry_date}</td>
                  <td className="p-3">
                    <span className={`text-xs px-2 py-0.5 rounded ${badge.bg}`}>{badge.label}</span>
                  </td>
                  <td className="p-3 text-right font-mono">{fmtMoney(e.sim_dollar_amount)}</td>
                  <td className="p-3 text-right font-mono">{fmtMoney(e.paper_dollar_amount)}</td>
                  <td className="p-3 text-right font-mono">{fmtPrice(e.sim_entry_price)}</td>
                  <td className="p-3 text-right font-mono">{fmtPrice(e.paper_entry_price)}</td>
                  <td className="p-3 text-xs text-[#8888A0]">{e.notes ?? "—"}</td>
                </tr>
              );
            })}
            {data?.events.length === 0 && (
              <tr>
                <td colSpan={10} className="p-6 text-center text-[#22C55E]">
                  No drift events in window.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
