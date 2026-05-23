"use client";

import { useEffect, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import Link from "next/link";
import { fetchAPIWithAuth } from "@/lib/api";

interface ServiceSummary {
  service: string;
  last_run: string | null;
  last_success: string | null;
  last_failure: string | null;
  runs_24h: number;
  failures_24h: number;
  last_duration_ms: number | null;
  last_status: string | null;
  last_rows_written: number | null;
  last_error: string | null;
}

interface RunRow {
  id: number;
  service: string;
  started_at: string;
  ended_at: string | null;
  duration_ms: number | null;
  status: string;
  exit_code: number | null;
  rows_written: number | null;
  rows_deleted: number | null;
  error_message: string | null;
  metadata: Record<string, unknown> | null;
  host: string;
  log_path: string | null;
  run_uuid: string;
}

interface PipelinesResponse {
  checked_at: string;
  n_services: number;
  services: ServiceSummary[];
  n_runs: number;
  runs: RunRow[];
}

function statusColor(s: string | null): string {
  if (s === "ok") return "text-[#22C55E]";
  if (s === "running") return "text-[#3B82F6]";
  if (s === "failed") return "text-[#EF4444]";
  if (s === "timeout" || s === "partial") return "text-[#F59E0B]";
  return "text-[#55556A]";
}

function statusDot(s: string | null): string {
  if (s === "ok") return "bg-[#22C55E]";
  if (s === "running") return "bg-[#3B82F6]";
  if (s === "failed") return "bg-[#EF4444]";
  if (s === "timeout" || s === "partial") return "bg-[#F59E0B]";
  return "bg-[#55556A]";
}

function ageFromIso(iso: string | null): string {
  if (!iso) return "never";
  const ms = Date.now() - new Date(iso).getTime();
  if (ms < 0) return "future";
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${(s / 3600).toFixed(1)}h ago`;
  return `${(s / 86400).toFixed(1)}d ago`;
}

function durMs(ms: number | null): string {
  if (ms === null || ms === undefined) return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

export default function AdminPipelinesPage() {
  const { isSignedIn, getToken } = useAuth();
  const [data, setData] = useState<PipelinesResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const token = await getToken();
        const d = await fetchAPIWithAuth<PipelinesResponse>(
          "/admin/diagnostics/pipelines?limit=200",
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
    const t = setInterval(() => isSignedIn && load(), 30_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, [isSignedIn, getToken]);

  if (!isSignedIn) {
    return <div className="text-[#8888A0] py-10">Sign in to view.</div>;
  }
  if (loading) {
    return <div className="text-[#8888A0] py-10">Loading pipeline runs…</div>;
  }
  if (error) {
    return (
      <div className="py-10">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Pipelines</h1>
        <p className="text-[#EF4444] mt-4">{error}</p>
      </div>
    );
  }

  return (
    <div className="text-[#E8E8ED] py-6">
      <div className="mb-6 flex items-baseline justify-between">
        <div>
          <h1 className="text-2xl font-bold">Pipelines</h1>
          <p className="text-sm text-[#55556A] mt-1">
            Structured run history from <code className="text-[#8888A0]">pipeline_runs</code>.
            Services migrate here as they adopt{" "}
            <code className="text-[#8888A0]">framework.observability.pipeline_run()</code>.
            Pre-migration services still report via{" "}
            <Link href="/admin/jobs" className="text-[#3B82F6] hover:underline">
              /admin/jobs
            </Link>
            .
          </p>
        </div>
        <div className="text-xs text-[#55556A]">
          Refreshed {ageFromIso(data?.checked_at ?? null)}
        </div>
      </div>

      {/* Service summary */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] mb-6 overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-[#2A2A3A] text-[#55556A] text-xs uppercase">
            <tr>
              <th className="text-left p-3">Service</th>
              <th className="text-left p-3">Status</th>
              <th className="text-right p-3">Last run</th>
              <th className="text-right p-3">Last success</th>
              <th className="text-right p-3">Last failure</th>
              <th className="text-right p-3">24h runs</th>
              <th className="text-right p-3">24h fails</th>
              <th className="text-right p-3">Last dur</th>
              <th className="text-right p-3">Last rows</th>
            </tr>
          </thead>
          <tbody>
            {data?.services.map((s) => (
              <tr key={s.service} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                <td className="p-3 font-mono">{s.service}</td>
                <td className={`p-3 font-medium ${statusColor(s.last_status)}`}>
                  <span className={`inline-block w-2 h-2 rounded-full mr-2 ${statusDot(s.last_status)}`} />
                  {s.last_status ?? "—"}
                </td>
                <td className="p-3 text-right font-mono text-[#8888A0]">{ageFromIso(s.last_run)}</td>
                <td className="p-3 text-right font-mono text-[#8888A0]">{ageFromIso(s.last_success)}</td>
                <td className={`p-3 text-right font-mono ${s.last_failure ? "text-[#EF4444]" : "text-[#55556A]"}`}>
                  {ageFromIso(s.last_failure)}
                </td>
                <td className="p-3 text-right font-mono">{s.runs_24h}</td>
                <td className={`p-3 text-right font-mono ${s.failures_24h > 0 ? "text-[#EF4444]" : ""}`}>
                  {s.failures_24h}
                </td>
                <td className="p-3 text-right font-mono">{durMs(s.last_duration_ms)}</td>
                <td className="p-3 text-right font-mono">
                  {s.last_rows_written?.toLocaleString() ?? "—"}
                </td>
              </tr>
            ))}
            {data?.services.length === 0 && (
              <tr>
                <td colSpan={9} className="p-6 text-center text-[#55556A]">
                  No services have reported yet. Wait for the next scheduled run, or instrument
                  more services with <code>framework.observability.pipeline_run()</code>.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Recent runs */}
      <h2 className="text-lg font-semibold mb-3">Recent runs</h2>
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-[#2A2A3A] text-[#55556A] text-xs uppercase">
            <tr>
              <th className="text-left p-3">Started</th>
              <th className="text-left p-3">Service</th>
              <th className="text-left p-3">Status</th>
              <th className="text-right p-3">Dur</th>
              <th className="text-right p-3">Rows</th>
              <th className="text-left p-3">Host</th>
              <th className="text-left p-3"></th>
            </tr>
          </thead>
          <tbody>
            {data?.runs.map((r) => (
              <>
                <tr
                  key={r.id}
                  className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26] cursor-pointer"
                  onClick={() => setExpanded(expanded === r.id ? null : r.id)}
                >
                  <td className="p-3 font-mono text-xs text-[#8888A0]">
                    {r.started_at.slice(0, 19).replace("T", " ")}
                  </td>
                  <td className="p-3 font-mono">{r.service}</td>
                  <td className={`p-3 font-medium ${statusColor(r.status)}`}>
                    <span className={`inline-block w-2 h-2 rounded-full mr-2 ${statusDot(r.status)}`} />
                    {r.status}
                  </td>
                  <td className="p-3 text-right font-mono">{durMs(r.duration_ms)}</td>
                  <td className="p-3 text-right font-mono">
                    {r.rows_written?.toLocaleString() ?? "—"}
                  </td>
                  <td className="p-3 text-xs text-[#55556A]">{r.host}</td>
                  <td className="p-3 text-xs text-[#3B82F6]">
                    {expanded === r.id ? "▾" : "▸"}
                  </td>
                </tr>
                {expanded === r.id && (
                  <tr className="bg-[#0A0A0F]">
                    <td colSpan={7} className="p-4 text-xs">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <div className="text-[#55556A] uppercase tracking-wider mb-1">
                            Metadata
                          </div>
                          <pre className="bg-[#12121A] p-2 rounded text-[10px] text-[#8888A0] overflow-x-auto">
                            {r.metadata ? JSON.stringify(r.metadata, null, 2) : "—"}
                          </pre>
                        </div>
                        <div>
                          <div className="text-[#55556A] uppercase tracking-wider mb-1">
                            {r.error_message ? "Error" : "Detail"}
                          </div>
                          {r.error_message && (
                            <pre className="bg-[#12121A] p-2 rounded text-[10px] text-[#EF4444] overflow-x-auto whitespace-pre-wrap">
                              {r.error_message}
                            </pre>
                          )}
                          <div className="mt-2 text-[10px] text-[#55556A]">
                            <div>run_uuid: <code className="text-[#8888A0]">{r.run_uuid}</code></div>
                            <div>log_path: <code className="text-[#8888A0]">{r.log_path ?? "—"}</code></div>
                            <div>exit_code: <code className="text-[#8888A0]">{r.exit_code ?? "—"}</code></div>
                          </div>
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
            {data?.runs.length === 0 && (
              <tr>
                <td colSpan={7} className="p-6 text-center text-[#55556A]">
                  No runs yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
