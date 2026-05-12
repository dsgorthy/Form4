"use client";

import { useEffect, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { fetchAPIWithAuth } from "@/lib/api";

interface JobStatus {
  name: string;
  label: string;
  category: string;
  log_file: string;
  exists: boolean;
  last_run_at: string | null;
  age_seconds: number | null;
  expected_cadence_seconds: number;
  status: "healthy" | "lagging" | "stale" | "missing";
  tail: string[];
}

interface JobsResponse {
  checked_at: string;
  summary: { healthy: number; lagging: number; stale: number; missing: number; total: number };
  jobs: JobStatus[];
}

interface FreshnessContract {
  source: string;
  table: string;
  column: string;
  last_computed_at: string | null;
  age_seconds: number | null;
  last_n_rows_affected: number;
  populated_by: string | null;
}

interface FreshnessResponse {
  checked_at: string;
  n_contracts: number;
  contracts: FreshnessContract[];
}

function formatAge(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}

function statusColor(s: string): string {
  if (s === "healthy") return "text-[#22C55E]";
  if (s === "lagging") return "text-[#F59E0B]";
  if (s === "stale") return "text-[#EF4444]";
  return "text-[#55556A]";
}

function statusDot(s: string): string {
  if (s === "healthy") return "bg-[#22C55E]";
  if (s === "lagging") return "bg-[#F59E0B]";
  if (s === "stale") return "bg-[#EF4444]";
  return "bg-[#55556A]";
}

export default function AdminJobsPage() {
  const { isSignedIn, getToken } = useAuth();
  const [data, setData] = useState<JobsResponse | null>(null);
  const [freshness, setFreshness] = useState<FreshnessResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [lastTick, setLastTick] = useState<Date>(new Date());

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const token = await getToken();
        const [jobs, fr] = await Promise.all([
          fetchAPIWithAuth<JobsResponse>("/admin/diagnostics/jobs", token),
          fetchAPIWithAuth<FreshnessResponse>("/admin/diagnostics/freshness", token),
        ]);
        if (!cancelled) {
          setData(jobs);
          setFreshness(fr);
          setLastTick(new Date());
          setError(null);
        }
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        if (!cancelled) {
          setError(msg.includes("403") ? "Access denied — admin only." : msg);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    if (isSignedIn) {
      load();
      const id = setInterval(load, 30_000);
      return () => { cancelled = true; clearInterval(id); };
    }
  }, [isSignedIn, getToken]);

  if (!isSignedIn) return <div className="text-[#E8E8ED] py-10">Sign in to view.</div>;
  if (loading) return <div className="text-[#55556A] py-10">Loading…</div>;
  if (error) return <div className="text-[#EF4444] py-10">{error}</div>;
  if (!data) return null;

  // Group by category
  const byCategory: Record<string, JobStatus[]> = {};
  for (const j of data.jobs) {
    if (!byCategory[j.category]) byCategory[j.category] = [];
    byCategory[j.category].push(j);
  }

  const categoryOrder = ["ingestion", "live_runner", "simulator", "monitoring"];
  const categoryLabels: Record<string, string> = {
    ingestion: "Data Ingestion",
    live_runner: "Live Paper Trading Runners",
    simulator: "Simulated Portfolio",
    monitoring: "Monitoring & Reconciliation",
  };

  return (
    <div className="py-6 space-y-6 text-[#E8E8ED]">
      <div className="flex items-baseline justify-between">
        <div>
          <h1 className="text-2xl font-bold">System Status</h1>
          <p className="text-[#55556A] text-xs mt-1">
            Polling jobs, simulators, runners. Auto-refreshes every 30s · Last fetched {lastTick.toLocaleTimeString()}
          </p>
        </div>
        <div className="text-xs text-[#8888A0]">
          <Link href="/admin/strategies" className="text-[#3B82F6] hover:underline">strategies</Link>
          {" · "}
          <span className="text-[#22C55E]">{data.summary.healthy} healthy</span>
          {" · "}
          <span className="text-[#F59E0B]">{data.summary.lagging} lagging</span>
          {" · "}
          <span className="text-[#EF4444]">{data.summary.stale} stale</span>
        </div>
      </div>

      {/* Jobs by category */}
      {categoryOrder.map((cat) => {
        const jobs = byCategory[cat] || [];
        if (jobs.length === 0) return null;
        return (
          <div key={cat} className="rounded-lg border border-[#2A2A3A] bg-[#12121A]">
            <div className="px-4 py-2 border-b border-[#2A2A3A]">
              <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
                {categoryLabels[cat] || cat}
              </div>
            </div>
            <div>
              {jobs.map((j) => (
                <div key={j.name} className="border-b border-[#2A2A3A]/40 last:border-b-0">
                  <button
                    onClick={() => setExpanded(expanded === j.name ? null : j.name)}
                    className="w-full px-4 py-3 flex items-center gap-3 text-left hover:bg-[#1A1A26]/50 transition-colors"
                  >
                    <span className={`inline-block w-2 h-2 rounded-full ${statusDot(j.status)}`} />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-baseline gap-3">
                        <code className="text-sm text-[#E8E8ED] font-mono">{j.name}</code>
                        <span className="text-xs text-[#8888A0] truncate">{j.label}</span>
                      </div>
                      <div className="text-[10px] text-[#55556A] mt-0.5">
                        last activity: {j.last_run_at
                          ? new Date(j.last_run_at).toLocaleString()
                          : "never"}
                        {" · "}
                        cadence: {formatAge(j.expected_cadence_seconds)}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className={`text-xs font-semibold ${statusColor(j.status)}`}>
                        {j.status.toUpperCase()}
                      </div>
                      <div className="text-[10px] text-[#55556A]">
                        {formatAge(j.age_seconds)} ago
                      </div>
                    </div>
                  </button>
                  {expanded === j.name && (
                    <div className="px-4 pb-3 pt-1 bg-[#0A0A12] border-t border-[#2A2A3A]/40">
                      <div className="text-[10px] uppercase tracking-wider text-[#55556A] mb-1">
                        Last log lines
                      </div>
                      <pre className="text-[11px] text-[#8888A0] font-mono whitespace-pre-wrap leading-snug">
                        {j.tail.length > 0 ? j.tail.join("\n") : "(empty)"}
                      </pre>
                      <div className="text-[10px] text-[#55556A] mt-2">
                        Log file: <code className="text-[#8888A0]">{j.log_file}</code>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        );
      })}

      {/* Freshness contracts */}
      {freshness && (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A]">
          <div className="px-4 py-2 border-b border-[#2A2A3A]">
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
              Data Freshness Contracts ({freshness.n_contracts})
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-[#2A2A3A]/40 text-[#55556A]">
                  <th className="px-4 py-2 text-left font-medium">Source.Table.Column</th>
                  <th className="px-4 py-2 text-right font-medium">Last Updated</th>
                  <th className="px-4 py-2 text-right font-medium">Age</th>
                  <th className="px-4 py-2 text-right font-medium">Rows Affected</th>
                  <th className="px-4 py-2 text-left font-medium">Populated By</th>
                </tr>
              </thead>
              <tbody>
                {freshness.contracts.map((c, i) => {
                  const ageS = c.age_seconds;
                  const ageColor =
                    ageS === null ? "text-[#55556A]" :
                    ageS < 3600 ? "text-[#22C55E]" :
                    ageS < 86400 ? "text-[#F59E0B]" :
                    "text-[#EF4444]";
                  return (
                    <tr key={i} className="border-b border-[#2A2A3A]/20 last:border-b-0">
                      <td className="px-4 py-2 font-mono text-[#E8E8ED]">
                        {c.source ? `${c.source}.` : ""}{c.table}.{c.column}
                      </td>
                      <td className="px-4 py-2 text-right text-[#8888A0]">
                        {c.last_computed_at
                          ? new Date(c.last_computed_at).toLocaleTimeString()
                          : "—"}
                      </td>
                      <td className={`px-4 py-2 text-right font-mono ${ageColor}`}>
                        {formatAge(ageS)}
                      </td>
                      <td className="px-4 py-2 text-right text-[#8888A0] font-mono">
                        {c.last_n_rows_affected.toLocaleString()}
                      </td>
                      <td className="px-4 py-2 text-[10px] text-[#55556A] font-mono truncate max-w-xs">
                        {c.populated_by || "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="text-[10px] text-[#55556A] text-center pt-4">
        Status legend:&nbsp;
        <span className="text-[#22C55E]">healthy</span> (log mtime ≤ cadence) ·{" "}
        <span className="text-[#F59E0B]">lagging</span> (≤ 2× cadence) ·{" "}
        <span className="text-[#EF4444]">stale</span> (&gt; 2× cadence) ·{" "}
        <span className="text-[#55556A]">missing</span> (log file not found)
      </div>
    </div>
  );
}
