"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { useAuth } from "@clerk/nextjs";
import { fetchAPIWithAuth } from "@/lib/api";

interface FreshnessRow {
  table: string;
  column: string;
  max_staleness_hours: number;
  observed_age_hours: number | null;
  last_observed_at: string | null;
  stale: boolean;
  populated_by: string;
}

interface DecisionRow {
  ts: string;
  ticker: string | null;
  trade_id: number | null;
  filing_date: string;
  thesis: string;
  stage: string;
  passed: boolean;
  reason: string | null;
  pit_grade: string | null;
  conviction: number | null;
  source: string;
  feature_snapshot?: Record<string, unknown> | null;
}

interface RejectionRow {
  stage: string;
  reason: string;
  count: number;
}

interface AlertRow {
  ts: string;
  severity: string;
  component: string;
  message: string;
  extra?: Record<string, unknown>;
}

interface DetailResponse {
  strategy: { name: string; label: string; thesis: string };
  decision_summary: {
    total_evaluations: number;
    rejected: number;
    by_stage: Record<string, { passed: number; rejected: number; total: number }>;
  };
  freshness: FreshnessRow[];
  rejection_histogram_30d: RejectionRow[];
  recent_decisions: DecisionRow[];
  recent_alerts: AlertRow[];
}

export default function AdminStrategyDetailPage() {
  const params = useParams();
  const name = String(params?.name ?? "");
  const { isSignedIn, getToken } = useAuth();
  const [data, setData] = useState<DetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const token = await getToken();
        const d = await fetchAPIWithAuth<DetailResponse>(
          `/admin/diagnostics/strategies/${name}`,
          token,
        );
        setData(d);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes("403")) {
          setError("Access denied. Admin only.");
        } else if (msg.includes("404")) {
          setError(`Strategy "${name}" not found.`);
        } else {
          setError(msg);
        }
      } finally {
        setLoading(false);
      }
    }
    if (isSignedIn && name) load();
  }, [isSignedIn, getToken, name]);

  if (!isSignedIn) return null;
  if (loading) return <div className="text-[#8888A0] py-10">Loading…</div>;
  if (error) {
    return (
      <div className="py-10">
        <p className="text-[#EF4444]">{error}</p>
        <p className="text-[#55556A] mt-2">
          <Link href="/admin/strategies" className="text-[#3B82F6] hover:underline">
            ← back to all strategies
          </Link>
        </p>
      </div>
    );
  }
  if (!data) return null;

  const summary = data.decision_summary;
  return (
    <div className="text-[#E8E8ED] py-6 space-y-8">
      {/* Header */}
      <div>
        <p className="text-xs text-[#55556A]">
          <Link href="/admin/strategies" className="text-[#3B82F6] hover:underline">
            admin
          </Link>{" "}
          / strategies / <code>{data.strategy.name}</code>
        </p>
        <h1 className="text-2xl font-bold mt-1">{data.strategy.label}</h1>
        <p className="text-sm text-[#8888A0] mt-1">{data.strategy.thesis}</p>
      </div>

      {/* Summary stats */}
      <div className="grid gap-4 md:grid-cols-4">
        <Stat
          label="Total evaluations"
          value={summary.total_evaluations.toLocaleString()}
        />
        <Stat
          label="Rejections"
          value={summary.rejected.toLocaleString()}
          hint={
            summary.total_evaluations > 0
              ? `${((summary.rejected / summary.total_evaluations) * 100).toFixed(1)}%`
              : "—"
          }
        />
        <Stat
          label="Freshness OK"
          value={`${data.freshness.filter((f) => !f.stale).length} / ${data.freshness.length}`}
          tone={data.freshness.some((f) => f.stale) ? "warn" : "ok"}
        />
        <Stat label="P0 alerts (24h)" value={data.recent_alerts.filter(a => a.severity === "critical").length} tone={data.recent_alerts.some(a => a.severity === "critical") ? "warn" : "ok"} />
      </div>

      {/* Freshness contracts */}
      <Section title="Data Freshness">
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-xs uppercase tracking-wider">
            <tr>
              <th className="py-2">Column</th>
              <th>Max age</th>
              <th>Observed</th>
              <th>Last update</th>
              <th>Status</th>
              <th>Populated by</th>
            </tr>
          </thead>
          <tbody>
            {data.freshness.map((f) => (
              <tr key={`${f.table}.${f.column}`} className="border-t border-[#2A2A3A]">
                <td className="py-2"><code className="text-xs">{f.table}.{f.column}</code></td>
                <td>{f.max_staleness_hours}h</td>
                <td>{f.observed_age_hours != null ? `${f.observed_age_hours.toFixed(1)}h` : "—"}</td>
                <td className="text-xs text-[#8888A0]">{f.last_observed_at ?? "—"}</td>
                <td className={f.stale ? "text-[#EF4444]" : "text-[#22C55E]"}>
                  {f.stale ? "STALE" : "ok"}
                </td>
                <td className="text-xs text-[#55556A]">{f.populated_by}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      {/* Decision summary by stage */}
      <Section title="Decisions by Stage">
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-xs uppercase tracking-wider">
            <tr><th className="py-2">Stage</th><th>Total</th><th>Passed</th><th>Rejected</th><th>Pass rate</th></tr>
          </thead>
          <tbody>
            {Object.entries(summary.by_stage).map(([stage, b]) => (
              <tr key={stage} className="border-t border-[#2A2A3A]">
                <td className="py-2"><code className="text-xs">{stage}</code></td>
                <td>{b.total.toLocaleString()}</td>
                <td className="text-[#22C55E]">{b.passed.toLocaleString()}</td>
                <td className="text-[#EF4444]">{b.rejected.toLocaleString()}</td>
                <td>{b.total > 0 ? `${((b.passed / b.total) * 100).toFixed(1)}%` : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      {/* Top rejection reasons */}
      <Section title="Top Rejection Reasons (30d)">
        {data.rejection_histogram_30d.length === 0 ? (
          <p className="text-[#55556A] text-sm">No rejections in last 30 days.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-left text-[#55556A] text-xs uppercase tracking-wider">
              <tr><th className="py-2">Stage</th><th>Reason</th><th>Count</th></tr>
            </thead>
            <tbody>
              {data.rejection_histogram_30d.map((r, i) => (
                <tr key={i} className="border-t border-[#2A2A3A]">
                  <td className="py-2"><code className="text-xs">{r.stage}</code></td>
                  <td className="text-[#E8E8ED]">{r.reason ?? "—"}</td>
                  <td>{r.count.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Section>

      {/* Recent decisions */}
      <Section title="Recent Decisions (last 50)">
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-xs uppercase tracking-wider">
            <tr>
              <th className="py-2">Time</th><th>Ticker</th><th>Filing</th>
              <th>Stage</th><th>Result</th><th>Reason</th>
              <th>Grade</th><th>Conv</th><th>Source</th>
            </tr>
          </thead>
          <tbody>
            {data.recent_decisions.map((d, i) => (
              <tr key={i} className="border-t border-[#2A2A3A]">
                <td className="py-2 text-xs text-[#8888A0]">{d.ts.slice(0, 19).replace("T", " ")}</td>
                <td className="font-mono text-xs">{d.ticker ?? "—"}</td>
                <td className="text-xs text-[#8888A0]">{d.filing_date}</td>
                <td><code className="text-xs">{d.stage}</code></td>
                <td className={d.passed ? "text-[#22C55E]" : "text-[#EF4444]"}>
                  {d.passed ? "PASS" : "REJECT"}
                </td>
                <td className="text-xs">{d.reason ?? "—"}</td>
                <td className="text-xs">{d.pit_grade ?? "—"}</td>
                <td className="text-xs">{d.conviction != null ? d.conviction.toFixed(1) : "—"}</td>
                <td className="text-xs text-[#55556A]">{d.source}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="mt-3 text-xs text-[#55556A]">
          See also: <Link href={`/admin/strategies/${name}/decisions`} className="text-[#3B82F6] hover:underline">
            full decision audit (paginated)
          </Link>
        </p>
      </Section>

      {/* Alerts */}
      <Section title="Recent Alerts">
        {data.recent_alerts.length === 0 ? (
          <p className="text-[#55556A] text-sm">No alerts from this strategy.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-left text-[#55556A] text-xs uppercase tracking-wider">
              <tr><th className="py-2">Time</th><th>Severity</th><th>Component</th><th>Message</th></tr>
            </thead>
            <tbody>
              {data.recent_alerts.map((a, i) => (
                <tr key={i} className="border-t border-[#2A2A3A]">
                  <td className="py-2 text-xs text-[#8888A0]">{a.ts}</td>
                  <td className={
                    a.severity === "critical" ? "text-[#EF4444]" :
                    a.severity === "error" ? "text-[#F59E0B]" :
                    a.severity === "warn" ? "text-[#FBBF24]" : "text-[#55556A]"
                  }>{a.severity}</td>
                  <td className="text-xs"><code>{a.component}</code></td>
                  <td className="text-xs whitespace-pre-wrap">{a.message}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Section>
    </div>
  );
}

function Stat({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string | number;
  hint?: string;
  tone?: "ok" | "warn";
}) {
  const valueColor =
    tone === "warn" ? "text-[#F59E0B]" : tone === "ok" ? "text-[#22C55E]" : "text-[#E8E8ED]";
  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
      <div className="text-[10px] uppercase tracking-wider text-[#55556A]">{label}</div>
      <div className={`text-xl font-semibold mt-1 ${valueColor}`}>{value}</div>
      {hint && <div className="text-[10px] text-[#55556A] mt-1">{hint}</div>}
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h2 className="text-base font-semibold text-[#E8E8ED] mb-3">{title}</h2>
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4 overflow-x-auto">
        {children}
      </div>
    </div>
  );
}
