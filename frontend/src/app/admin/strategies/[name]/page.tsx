"use client";

import { useEffect, useMemo, useState } from "react";
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

interface EvaluationRow {
  ts: string;
  run_id: string;
  strategy: string;
  ticker: string | null;
  trade_id: number | null;
  filing_date: string;
  thesis: string;
  source: string;
  pit_grade: string | null;
  conviction: number | null;
  // per-stage outcomes
  dedup_evaluated: boolean;
  dedup_passed: boolean | null;
  dedup_reason: string | null;
  filter_evaluated: boolean;
  filter_passed: boolean | null;
  filter_reason: string | null;
  pit_evaluated: boolean;
  pit_passed: boolean | null;
  pit_reason: string | null;
  tenb51_evaluated: boolean;
  tenb51_passed: boolean | null;
  tenb51_reason: string | null;
  conviction_evaluated: boolean;
  conviction_passed: boolean | null;
  conviction_reason: string | null;
  capacity_evaluated: boolean;
  capacity_passed: boolean | null;
  capacity_reason: string | null;
  final_passed: boolean;
  rejected_at: string | null;
  feature_snapshot: Record<string, unknown> | null;
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

interface DivergenceRow {
  id: number;
  ticker: string;
  issue_type: "missing_in_alpaca" | "orphan_in_alpaca" | "qty_mismatch" | "price_mismatch";
  severity: "info" | "warn" | "critical";
  db_qty: number | null;
  alpaca_qty: number | null;
  db_entry_price: number | null;
  alpaca_avg_cost: number | null;
  db_status: string | null;
  portfolio_id: number | null;
  detail: string | null;
  detected_at: string;
}

interface AlpacaPositionRow {
  ticker: string;
  qty: number;
  avg_entry_price: number | null;
  market_value: number | null;
  current_price: number | null;
  unrealized_pl: number | null;
  captured_at: string | null;
}

interface ReconciliationBlock {
  divergences: DivergenceRow[];
  alpaca_positions: AlpacaPositionRow[];
  latest_capture_at: string | null;
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
  recent_evaluations: EvaluationRow[];
  recent_alerts: AlertRow[];
  reconciliation?: ReconciliationBlock;
}

type FilterMode = "all" | "passed" | "rejected";

export default function AdminStrategyDetailPage() {
  const params = useParams();
  const name = String(params?.name ?? "");
  const { isSignedIn, getToken } = useAuth();
  const [data, setData] = useState<DetailResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [filterMode, setFilterMode] = useState<FilterMode>("all");
  const [stageFilter, setStageFilter] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);

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
        if (msg.includes("403")) setError("Access denied. Admin only.");
        else if (msg.includes("404")) setError(`Strategy "${name}" not found.`);
        else setError(msg);
      } finally {
        setLoading(false);
      }
    }
    if (isSignedIn && name) load();
  }, [isSignedIn, getToken, name]);

  const visibleRows = useMemo(() => {
    if (!data) return [];
    let rows = data.recent_evaluations;
    if (filterMode === "passed") rows = rows.filter((r) => r.final_passed);
    else if (filterMode === "rejected") rows = rows.filter((r) => !r.final_passed);
    if (stageFilter) rows = rows.filter((r) => r.rejected_at === stageFilter);
    return rows;
  }, [data, filterMode, stageFilter]);

  if (!isSignedIn) return null;
  if (loading) return <div className="text-[#8888A0] py-10">Loading…</div>;
  if (error) {
    return (
      <div className="py-10">
        <p className="text-[#EF4444]">{error}</p>
        <p className="text-[#55556A] mt-2">
          <Link href="/admin/strategies" className="text-[#3B82F6] hover:underline">← back to all strategies</Link>
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
          <Link href="/admin/strategies" className="text-[#3B82F6] hover:underline">admin</Link> / strategies / <code>{data.strategy.name}</code>
        </p>
        <h1 className="text-2xl font-bold mt-1">{data.strategy.label}</h1>
        <p className="text-sm text-[#8888A0] mt-1">{data.strategy.thesis}</p>
        <div className="mt-3 flex flex-wrap items-center gap-2 text-[10px]">
          <SourceBadge source="live" /><span className="text-[#8888A0]">cw_runner real-time</span>
          <SourceBadge source="simulation" /><span className="text-[#8888A0]">deterministic walk-forward replay (current code, PIT-correct)</span>
          <SourceBadge source="actual" /><span className="text-[#8888A0]">historical entries from strategy_portfolio (mixed code versions)</span>
        </div>
      </div>

      {/* Summary */}
      <div className="grid gap-4 md:grid-cols-4">
        <Stat label="Total evaluations" value={summary.total_evaluations.toLocaleString()} />
        <Stat label="Rejections" value={summary.rejected.toLocaleString()}
          hint={summary.total_evaluations > 0 ? `${((summary.rejected / summary.total_evaluations) * 100).toFixed(1)}%` : "—"} />
        <Stat label="Freshness OK" value={`${data.freshness.filter((f) => !f.stale).length} / ${data.freshness.length}`}
          tone={data.freshness.some((f) => f.stale) ? "warn" : "ok"} />
        <Stat label="P0 alerts (24h)" value={data.recent_alerts.filter((a) => a.severity === "critical").length}
          tone={data.recent_alerts.some((a) => a.severity === "critical") ? "warn" : "ok"} />
      </div>

      {/* Freshness */}
      <Section title="Data Freshness">
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-xs uppercase tracking-wider">
            <tr><th className="py-2">Column</th><th>Max age</th><th>Observed</th><th>Last update</th><th>Status</th><th>Populated by</th></tr>
          </thead>
          <tbody>
            {data.freshness.map((f) => (
              <tr key={`${f.table}.${f.column}`} className="border-t border-[#2A2A3A]">
                <td className="py-2"><code className="text-xs">{f.table}.{f.column}</code></td>
                <td>{f.max_staleness_hours}h</td>
                <td>{f.observed_age_hours != null ? `${f.observed_age_hours.toFixed(1)}h` : "—"}</td>
                <td className="text-xs text-[#8888A0]">{f.last_observed_at ?? "—"}</td>
                <td className={f.stale ? "text-[#EF4444]" : "text-[#22C55E]"}>{f.stale ? "STALE" : "ok"}</td>
                <td className="text-xs text-[#55556A]">{f.populated_by}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      {/* Strategy ↔ Alpaca reconciliation */}
      {data.reconciliation && (
        <Section title="Strategy ↔ Alpaca">
          {data.reconciliation.latest_capture_at ? (
            <p className="text-[10px] text-[#55556A] mb-3">
              Latest snapshot {data.reconciliation.latest_capture_at.slice(0, 19).replace("T", " ")} ·{" "}
              <span className="text-[#8888A0]">
                strategy_portfolio is canonical; Alpaca is a side-channel for tracking.
              </span>
            </p>
          ) : (
            <p className="text-[10px] text-[#55556A] mb-3">
              No Alpaca snapshot yet — run <code>scripts/alpaca_reconcile.py</code>.
            </p>
          )}

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <div className="text-[10px] uppercase tracking-wider text-[#55556A] mb-2">
                Active divergences ({data.reconciliation.divergences.length})
              </div>
              {data.reconciliation.divergences.length === 0 ? (
                <p className="text-sm text-[#22C55E]">No drift. Strategy ↔ Alpaca aligned.</p>
              ) : (
                <table className="w-full text-xs">
                  <thead className="text-left text-[#55556A] text-[10px] uppercase">
                    <tr>
                      <th className="py-1 pr-2">Ticker</th>
                      <th className="pr-2">Issue</th>
                      <th className="pr-2">Sev</th>
                      <th className="pr-2">DB</th>
                      <th className="pr-2">Alpaca</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.reconciliation.divergences.map((d) => (
                      <tr key={d.id} className="border-t border-[#2A2A3A]">
                        <td className="py-1 pr-2 font-mono">{d.ticker}</td>
                        <td className="pr-2"><code className="text-[10px]">{d.issue_type}</code></td>
                        <td className="pr-2">
                          <span className={
                            d.severity === "critical" ? "text-[#EF4444]"
                              : d.severity === "warn" ? "text-[#F59E0B]"
                                : "text-[#55556A]"}>
                            {d.severity}
                          </span>
                        </td>
                        <td className="pr-2">
                          {d.db_qty != null ? `${d.db_qty} sh` : "—"}
                          {d.db_entry_price != null && (
                            <span className="text-[#8888A0]"> @ ${d.db_entry_price.toFixed(2)}</span>
                          )}
                        </td>
                        <td className="pr-2">
                          {d.alpaca_qty != null ? `${d.alpaca_qty} sh` : "—"}
                          {d.alpaca_avg_cost != null && (
                            <span className="text-[#8888A0]"> @ ${d.alpaca_avg_cost.toFixed(2)}</span>
                          )}
                        </td>
                        <td className="text-[10px] text-[#8888A0]">{d.detail}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div>
              <div className="text-[10px] uppercase tracking-wider text-[#55556A] mb-2">
                Alpaca holdings ({data.reconciliation.alpaca_positions.length})
              </div>
              {data.reconciliation.alpaca_positions.length === 0 ? (
                <p className="text-sm text-[#55556A]">No positions in Alpaca.</p>
              ) : (
                <table className="w-full text-xs">
                  <thead className="text-left text-[#55556A] text-[10px] uppercase">
                    <tr>
                      <th className="py-1 pr-2">Ticker</th>
                      <th className="pr-2">Qty</th>
                      <th className="pr-2">Cost</th>
                      <th className="pr-2">Last</th>
                      <th className="pr-2">P&L</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.reconciliation.alpaca_positions.map((p) => (
                      <tr key={p.ticker} className="border-t border-[#2A2A3A]">
                        <td className="py-1 pr-2 font-mono">{p.ticker}</td>
                        <td className="pr-2">{p.qty}</td>
                        <td className="pr-2">{p.avg_entry_price != null ? `$${p.avg_entry_price.toFixed(2)}` : "—"}</td>
                        <td className="pr-2">{p.current_price != null ? `$${p.current_price.toFixed(2)}` : "—"}</td>
                        <td className={`pr-2 ${(p.unrealized_pl ?? 0) >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                          {p.unrealized_pl != null ? `$${p.unrealized_pl.toFixed(0)}` : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </Section>
      )}

      {/* Recent evaluations — ONE ROW PER EVALUATION */}
      <Section title="Recent Evaluations">
        <div className="flex flex-wrap items-center gap-2 mb-3 text-sm">
          <span className="text-[#55556A] mr-2">Filter:</span>
          <FilterBtn active={filterMode === "all"} onClick={() => { setFilterMode("all"); setStageFilter(null); }}>
            All ({data.recent_evaluations.length})
          </FilterBtn>
          <FilterBtn active={filterMode === "passed"} onClick={() => { setFilterMode("passed"); setStageFilter(null); }} tone="green">
            Passed ({data.recent_evaluations.filter((r) => r.final_passed).length})
          </FilterBtn>
          <FilterBtn active={filterMode === "rejected"} onClick={() => { setFilterMode("rejected"); setStageFilter(null); }} tone="red">
            Rejected ({data.recent_evaluations.filter((r) => !r.final_passed).length})
          </FilterBtn>
          {filterMode === "rejected" && (
            <>
              <span className="text-[#55556A] mx-2">at stage:</span>
              {(["dedup", "filter", "pit_lookup", "min_10b5_1", "conviction", "capacity"] as const).map((s) => {
                const count = data.recent_evaluations.filter((r) => r.rejected_at === s).length;
                if (count === 0) return null;
                return (
                  <FilterBtn key={s} active={stageFilter === s} onClick={() => setStageFilter(stageFilter === s ? null : s)}>
                    {s} ({count})
                  </FilterBtn>
                );
              })}
            </>
          )}
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-left text-[#55556A] text-[10px] uppercase tracking-wider">
              <tr>
                <th className="py-2 pr-3">Time</th>
                <th className="pr-3">Ticker</th>
                <th className="pr-3">Filing</th>
                <th className="pr-3">Dedup</th>
                <th className="pr-3">Filter</th>
                <th className="pr-3">PIT</th>
                <th className="pr-3">10b5-1</th>
                <th className="pr-3">Conviction</th>
                <th className="pr-3">Capacity</th>
                <th className="pr-3">Result</th>
                <th className="pr-3">Grade</th>
                <th className="pr-3">Conv</th>
                <th className="pr-3">Source</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((r) => {
                const key = `${r.run_id}:${r.trade_id}`;
                const isOpen = expanded === key;
                return (
                  <>
                    <tr key={key} className="border-t border-[#2A2A3A] hover:bg-[#1A1A26] cursor-pointer"
                        onClick={() => setExpanded(isOpen ? null : key)}>
                      <td className="py-2 pr-3 text-[10px] text-[#8888A0]">{r.ts.slice(0, 19).replace("T", " ")}</td>
                      <td className="pr-3 font-mono text-xs">{r.ticker ?? "—"}</td>
                      <td className="pr-3 text-[10px] text-[#8888A0]">{r.filing_date}</td>
                      <td className="pr-3"><StageDot evaluated={r.dedup_evaluated} passed={r.dedup_passed} /></td>
                      <td className="pr-3"><StageDot evaluated={r.filter_evaluated} passed={r.filter_passed} /></td>
                      <td className="pr-3"><StageDot evaluated={r.pit_evaluated} passed={r.pit_passed} /></td>
                      <td className="pr-3"><StageDot evaluated={r.tenb51_evaluated} passed={r.tenb51_passed} /></td>
                      <td className="pr-3"><StageDot evaluated={r.conviction_evaluated} passed={r.conviction_passed} /></td>
                      <td className="pr-3"><StageDot evaluated={r.capacity_evaluated} passed={r.capacity_passed} /></td>
                      <td className="pr-3">
                        <span className={r.final_passed
                          ? "inline-block px-2 py-0.5 rounded text-[10px] font-bold bg-[#22C55E22] text-[#22C55E]"
                          : "inline-block px-2 py-0.5 rounded text-[10px] font-bold bg-[#EF444422] text-[#EF4444]"}>
                          {r.final_passed ? "PASS" : "REJECT"}
                        </span>
                      </td>
                      <td className="pr-3 text-xs">{r.pit_grade ?? "—"}</td>
                      <td className="pr-3 text-xs">{r.conviction != null ? r.conviction.toFixed(1) : "—"}</td>
                      <td className="pr-3"><SourceBadge source={r.source} /></td>
                      <td className="text-[#55556A] text-xs">{isOpen ? "▼" : "▶"}</td>
                    </tr>
                    {isOpen && (
                      <tr className="border-t border-[#2A2A3A] bg-[#0A0A0F]">
                        <td colSpan={14} className="py-3 px-4">
                          <div className="space-y-2 text-xs">
                            <Reason label="Dedup"      passed={r.dedup_passed}      reason={r.dedup_reason} />
                            <Reason label="Filter"     passed={r.filter_passed}     reason={r.filter_reason} />
                            <Reason label="PIT lookup" passed={r.pit_passed}        reason={r.pit_reason} />
                            <Reason label="10b5-1"     passed={r.tenb51_passed}     reason={r.tenb51_reason} />
                            <Reason label="Conviction" passed={r.conviction_passed} reason={r.conviction_reason} />
                            <Reason label="Capacity"   passed={r.capacity_passed}   reason={r.capacity_reason} />
                            {r.feature_snapshot && (
                              <details className="mt-2">
                                <summary className="cursor-pointer text-[#3B82F6] text-[11px]">Feature snapshot</summary>
                                <pre className="mt-1 p-2 bg-[#000] rounded text-[10px] overflow-x-auto">{JSON.stringify(r.feature_snapshot, null, 2)}</pre>
                              </details>
                            )}
                          </div>
                        </td>
                      </tr>
                    )}
                  </>
                );
              })}
              {visibleRows.length === 0 && (
                <tr>
                  <td colSpan={14} className="py-6 text-center text-[#55556A] text-xs">
                    No evaluations match the current filter.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
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
                  <td className={a.severity === "critical" ? "text-[#EF4444]" : a.severity === "error" ? "text-[#F59E0B]" : a.severity === "warn" ? "text-[#FBBF24]" : "text-[#55556A]"}>{a.severity}</td>
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

function Stat({ label, value, hint, tone }: { label: string; value: string | number; hint?: string; tone?: "ok" | "warn" }) {
  const valueColor = tone === "warn" ? "text-[#F59E0B]" : tone === "ok" ? "text-[#22C55E]" : "text-[#E8E8ED]";
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
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">{children}</div>
    </div>
  );
}

function StageDot({ evaluated, passed }: { evaluated: boolean; passed: boolean | null }) {
  if (!evaluated || passed == null) {
    return <span className="inline-block w-3 h-3 rounded-full bg-[#1F1F2E] border border-[#2A2A3A]" title="not reached" />;
  }
  if (passed) {
    return <span className="inline-block w-3 h-3 rounded-full bg-[#22C55E]" title="passed" />;
  }
  return <span className="inline-block w-3 h-3 rounded-full bg-[#EF4444]" title="rejected" />;
}

function SourceBadge({ source }: { source: string }) {
  const cls =
    source === "live"
      ? "bg-[#22C55E22] text-[#22C55E] border-[#22C55E55]"
      : source === "simulation"
        ? "bg-[#3B82F622] text-[#3B82F6] border-[#3B82F655]"
        : source === "actual"
          ? "bg-[#A855F722] text-[#A855F7] border-[#A855F755]"
          : "bg-[#55556A22] text-[#8888A0] border-[#55556A55]";
  const label =
    source === "live" ? "LIVE"
      : source === "simulation" ? "SIM"
        : source === "actual" ? "ACTUAL"
          : source.toUpperCase();
  return <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-bold border ${cls}`}>
    {label}
  </span>;
}

function FilterBtn({
  active, onClick, children, tone,
}: { active: boolean; onClick: () => void; children: React.ReactNode; tone?: "green" | "red" }) {
  const baseColor =
    tone === "green" ? "border-[#22C55E55]" :
    tone === "red"   ? "border-[#EF444455]" : "border-[#2A2A3A]";
  const activeColor =
    tone === "green" ? "bg-[#22C55E22] text-[#22C55E]" :
    tone === "red"   ? "bg-[#EF444422] text-[#EF4444]" : "bg-[#3B82F622] text-[#3B82F6]";
  return (
    <button onClick={onClick}
      className={`px-3 py-1 text-xs rounded border ${baseColor} ${active ? activeColor : "text-[#8888A0] hover:text-[#E8E8ED]"}`}>
      {children}
    </button>
  );
}

function Reason({ label, passed, reason }: { label: string; passed: boolean | null; reason: string | null }) {
  if (passed == null) {
    return (
      <div className="flex items-baseline gap-3">
        <span className="text-[#55556A] w-24">{label}</span>
        <span className="text-[#55556A]">— not reached —</span>
      </div>
    );
  }
  return (
    <div className="flex items-baseline gap-3">
      <span className="text-[#8888A0] w-24">{label}</span>
      <span className={passed ? "text-[#22C55E]" : "text-[#EF4444]"}>{passed ? "PASS" : "REJECT"}</span>
      <span className="text-[#E8E8ED] flex-1">{reason ?? "(no reason recorded)"}</span>
    </div>
  );
}
