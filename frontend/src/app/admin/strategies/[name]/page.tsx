"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { useAuth } from "@clerk/nextjs";
import { fetchAPIWithAuth } from "@/lib/api";
import { Pagination } from "@/components/pagination";

// ── Types ────────────────────────────────────────────────────────────────

interface FreshnessRow {
  table: string;
  column: string;
  max_staleness_hours: number;
  observed_age_hours: number | null;
  business_age_hours: number | null;
  last_observed_at: string | null;
  stale: boolean;
  effective_stale: boolean;
  status_label: "fresh" | "weekend_ok" | "stale" | "unknown";
  business_hours_only: boolean;
  populated_by: string;
}

interface EvalOutcomeNotEntered {
  kind: "not_entered";
  rejected_at: string | null;
  reason: string | null;
}
interface EvalOutcomeOpen {
  kind: "open";
  entry_price: number | null;
  current_price: number | null;
  price_source: string;
  shares: number;
  pnl_pct: number | null;
  pnl_dollar: number | null;
  entry_date: string | null;
}
interface EvalOutcomeClosed {
  kind: "closed";
  entry_price: number | null;
  exit_price: number | null;
  pnl_pct: number | null;
  pnl_dollar: number | null;
  exit_reason: string | null;
}
interface EvalOutcomeUntracked {
  kind: "entered_untracked";
}
type EvalOutcome =
  | EvalOutcomeNotEntered
  | EvalOutcomeOpen
  | EvalOutcomeClosed
  | EvalOutcomeUntracked;

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
  // per-stage
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
  // enrichment
  accession: string | null;
  insider_name: string | null;
  insider_title: string | null;
  lots_in_filing: number;
  outcome: EvalOutcome;
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

interface StrategyRules {
  execution_mode: string;
  exit_strategy: string | null;
  hold_days: number | null;
  stop_loss_pct: number | null;
  max_concurrent: number | null;
  soft_cap: number | null;
  min_conviction: number | null;
  min_conviction_above_soft: number | null;
  position_size_pct: number | null;
}

interface DetailResponse {
  strategy: { name: string; label: string; thesis: string; execution_mode?: string };
  decision_summary: {
    total_evaluations: number;
    rejected: number;
    by_stage: Record<string, { passed: number; rejected: number; total: number }>;
  };
  freshness: FreshnessRow[];
  rejection_histogram_30d: RejectionRow[];
  recent_evaluations: EvaluationRow[];
  recent_alerts: AlertRow[];
  reconciliation?: ReconciliationBlock | null;
  rules?: StrategyRules;
}

interface OpenPositionRow {
  id: number;
  trade_id: number | null;
  ticker: string;
  execution_source: string | null;
  insider_name: string | null;
  insider_title: string | null;
  signal_grade: string | null;
  entry_date: string;
  entry_price: number;
  shares: number;
  dollar_amount: number;
  portfolio_value: number | null;
  position_size_pct: number | null;
  current_price: number | null;
  current_price_at: string | null;
  price_source: string;
  unrealized_pnl_pct: number | null;
  unrealized_pnl_dollar: number | null;
  days_held: number | null;
  planned_exit_date: string | null;
  trading_days_remaining: number | null;
}

interface ClosedPositionRow {
  id: number;
  trade_id: number | null;
  ticker: string;
  execution_source: string | null;
  insider_name: string | null;
  insider_title: string | null;
  signal_grade: string | null;
  entry_date: string;
  entry_price: number | null;
  exit_date: string | null;
  exit_price: number | null;
  shares: number;
  dollar_amount: number | null;
  portfolio_value: number | null;
  position_size_pct: number | null;
  pnl_pct: number | null;
  pnl_dollar: number | null;
  exit_reason: string | null;
  hold_days: number | null;
}

interface PositionsResponse {
  strategy: string;
  rules: StrategyRules;
  open: {
    rows: OpenPositionRow[];
    count: number;
    total_cost: number;
    total_unrealized_pnl_dollar: number;
    total_unrealized_pnl_pct: number | null;
  };
  closed: {
    rows: ClosedPositionRow[];
    page: number;
    per_page: number;
    total: number;
  };
}

type FilterMode = "all" | "passed" | "rejected";

// ── Page ─────────────────────────────────────────────────────────────────

export default function AdminStrategyDetailPage() {
  const params = useParams();
  const name = String(params?.name ?? "");
  const { isSignedIn, getToken } = useAuth();
  const [data, setData] = useState<DetailResponse | null>(null);
  const [positions, setPositions] = useState<PositionsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  // Filings filter state
  const [filterMode, setFilterMode] = useState<FilterMode>("all");
  const [stageFilter, setStageFilter] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);

  // Closed-positions pagination state
  const [closedPage, setClosedPage] = useState(1);
  const [closedPerPage, setClosedPerPage] = useState(25);

  // Detail fetch
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

  // Positions fetch — refires when closed pagination changes
  const loadPositions = useCallback(async () => {
    try {
      const token = await getToken();
      const p = await fetchAPIWithAuth<PositionsResponse>(
        `/admin/diagnostics/strategies/${name}/positions?page=${closedPage}&per_page=${closedPerPage}`,
        token,
      );
      setPositions(p);
    } catch (e) {
      console.warn("positions fetch failed", e);
    }
  }, [getToken, name, closedPage, closedPerPage]);

  useEffect(() => {
    if (isSignedIn && name) loadPositions();
  }, [isSignedIn, name, loadPositions]);

  // Auto-refresh open positions every 60s while page is visible
  useEffect(() => {
    if (!isSignedIn || !name) return;
    const id = window.setInterval(() => {
      if (document.visibilityState === "visible") loadPositions();
    }, 60_000);
    return () => window.clearInterval(id);
  }, [isSignedIn, name, loadPositions]);

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
  const executionMode = data.strategy.execution_mode || data.rules?.execution_mode || "paper";
  const isAlertOnly = executionMode === "alert_only";

  // Stat: open P&L from positions (overrides "P0 alerts" when positions loaded)
  const openPnL = positions?.open.total_unrealized_pnl_dollar ?? null;
  const openCount = positions?.open.count ?? 0;
  const freshnessOkCount = data.freshness.filter((f) => !f.effective_stale && f.status_label !== "unknown").length;

  return (
    <div className="text-[#E8E8ED] py-6 space-y-8">
      {/* Header */}
      <div>
        <p className="text-xs text-[#55556A]">
          <Link href="/admin/strategies" className="text-[#3B82F6] hover:underline">admin</Link> / strategies / <code>{data.strategy.name}</code>
        </p>
        <h1 className="text-2xl font-bold mt-1">{data.strategy.label}</h1>
        <p className="text-sm text-[#8888A0] mt-1">{data.strategy.thesis}</p>
        <div className="mt-2 text-[10px] text-[#55556A]">
          execution mode: <code className="text-[#8888A0]">{executionMode}</code>
        </div>
      </div>

      {/* Summary */}
      <div className="grid gap-4 md:grid-cols-4">
        <Stat label="Open positions" value={openCount}
          hint={openPnL !== null ? formatDollarSigned(openPnL) + " unrealized" : "—"} />
        <Stat label="Freshness OK"
          value={`${freshnessOkCount} / ${data.freshness.length}`}
          tone={data.freshness.some((f) => f.effective_stale) ? "warn" : "ok"} />
        <Stat label="Total evaluations" value={summary.total_evaluations.toLocaleString()} />
        <Stat label="P0 alerts (24h)" value={data.recent_alerts.filter((a) => a.severity === "critical").length}
          tone={data.recent_alerts.some((a) => a.severity === "critical") ? "warn" : "ok"} />
      </div>

      {/* 1. Data freshness — business-hours-aware */}
      <FreshnessSection rows={data.freshness} />

      {/* 2. Positions — open + paginated closed */}
      <PositionsSection
        positions={positions}
        rules={data.rules}
        closedPage={closedPage}
        closedPerPage={closedPerPage}
        onClosedPageChange={(offset) => setClosedPage(Math.floor(offset / closedPerPage) + 1)}
        onClosedPerPageChange={(n) => { setClosedPerPage(n); setClosedPage(1); }}
      />

      {/* 3. Filings evaluated — deduped + outcome column */}
      <FilingsSection
        rows={visibleRows}
        allRows={data.recent_evaluations}
        filterMode={filterMode}
        setFilterMode={setFilterMode}
        stageFilter={stageFilter}
        setStageFilter={setStageFilter}
        expanded={expanded}
        setExpanded={setExpanded}
      />

      {/* Conditional: Strategy ↔ Alpaca (only when runner actually places orders) */}
      {!isAlertOnly && data.reconciliation && (
        <ReconciliationSection block={data.reconciliation} />
      )}

      {/* Compact: Recent alerts */}
      <AlertsSection alerts={data.recent_alerts} />
    </div>
  );
}

// ── Section: Freshness ───────────────────────────────────────────────────

function FreshnessSection({ rows }: { rows: FreshnessRow[] }) {
  return (
    <Section title="Data freshness">
      <p className="text-[10px] text-[#55556A] mb-3">
        Status uses business-hours-adjusted age (excludes weekends + US market holidays).
        Raw clock age shown alongside for context.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-[10px] uppercase tracking-wider">
            <tr>
              <th className="py-2 pr-3">Column</th>
              <th className="pr-3">SLA</th>
              <th className="pr-3">Business age</th>
              <th className="pr-3">Clock age</th>
              <th className="pr-3">Last update</th>
              <th className="pr-3">Status</th>
              <th>Populated by</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((f) => (
              <tr key={`${f.table}.${f.column}`} className="border-t border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                <td className="py-2 pr-3"><code className="text-xs">{f.table}.{f.column}</code></td>
                <td className="pr-3 font-mono text-xs">{f.max_staleness_hours}h</td>
                <td className="pr-3 font-mono text-xs">
                  {f.business_age_hours != null ? `${f.business_age_hours.toFixed(1)}h` : "—"}
                </td>
                <td className="pr-3 font-mono text-xs text-[#8888A0]">
                  {f.observed_age_hours != null ? `${f.observed_age_hours.toFixed(1)}h` : "—"}
                </td>
                <td className="pr-3 text-xs text-[#8888A0]">
                  {f.last_observed_at ? f.last_observed_at.slice(0, 19).replace("T", " ") : "—"}
                </td>
                <td className="pr-3"><FreshnessStatus label={f.status_label} /></td>
                <td className="text-[10px] text-[#55556A]">{f.populated_by}</td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr><td colSpan={7} className="py-4 text-center text-[#55556A] text-xs">No contracts for this strategy.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </Section>
  );
}

function SizeCell({
  dollarAmount, shares, pct,
}: { dollarAmount: number | null; shares: number; pct: number | null }) {
  if (dollarAmount == null) return <span className="text-[#55556A]">—</span>;
  const k = dollarAmount >= 1000
    ? `$${(dollarAmount / 1000).toFixed(1)}k`
    : `$${dollarAmount.toFixed(0)}`;
  return (
    <span>
      <span className="text-[#E8E8ED]">{k}</span>
      <span className="ml-1 text-[#55556A]">({shares} sh)</span>
      {pct != null && (
        <span className="ml-1 text-[#8888A0]">· {(pct * 100).toFixed(1)}%</span>
      )}
    </span>
  );
}

function ExecSourceBadge({ source }: { source: string | null }) {
  // Provenance per row: where this position originated.
  //   simulated → strategy_simulator (deterministic walk-forward replay)
  //   alert     → cw_runner alert_only mode (real signal, no Alpaca order)
  //   paper     → cw_runner paper mode (real Alpaca paper order)
  //   live      → cw_runner live mode (real Alpaca live order)
  //   backtest  → historical backtest result (legacy)
  const s = (source || "").toLowerCase();
  const palette: Record<string, string> = {
    alert:     "bg-[#22C55E22] text-[#22C55E] border-[#22C55E55]",
    paper:     "bg-[#3B82F622] text-[#3B82F6] border-[#3B82F655]",
    live:      "bg-[#A855F722] text-[#A855F7] border-[#A855F755]",
    simulated: "bg-[#55556A33] text-[#8888A0] border-[#55556A55]",
    backtest:  "bg-[#55556A33] text-[#55556A] border-[#55556A55]",
  };
  const cls = palette[s] || "bg-[#55556A33] text-[#55556A] border-[#55556A55]";
  const label = s === "simulated" ? "SIM"
    : s === "backtest" ? "BT"
    : s.toUpperCase() || "?";
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-bold border ${cls}`}
          title={`execution_source = ${source || "unknown"}`}>
      {label}
    </span>
  );
}

function FreshnessStatus({ label }: { label: FreshnessRow["status_label"] }) {
  const style: Record<typeof label, string> = {
    fresh:      "bg-[#22C55E22] text-[#22C55E] border-[#22C55E55]",
    weekend_ok: "bg-[#3B82F622] text-[#3B82F6] border-[#3B82F655]",
    stale:      "bg-[#EF444422] text-[#EF4444] border-[#EF444455]",
    unknown:    "bg-[#55556A22] text-[#8888A0] border-[#55556A55]",
  };
  const text: Record<typeof label, string> = {
    fresh: "FRESH",
    weekend_ok: "WEEKEND OK",
    stale: "STALE",
    unknown: "UNKNOWN",
  };
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-bold border ${style[label]}`}>
      {text[label]}
    </span>
  );
}

// ── Section: Positions ───────────────────────────────────────────────────

function PositionsSection({
  positions, rules, closedPage, closedPerPage,
  onClosedPageChange, onClosedPerPageChange,
}: {
  positions: PositionsResponse | null;
  rules?: StrategyRules;
  closedPage: number;
  closedPerPage: number;
  onClosedPageChange: (offset: number) => void;
  onClosedPerPageChange: (n: number) => void;
}) {
  if (!positions) {
    return (
      <Section title="Positions">
        <div className="text-[#55556A] text-sm">Loading positions…</div>
      </Section>
    );
  }

  const r = positions.rules || rules;
  const totalPnL = positions.open.total_unrealized_pnl_dollar;
  const totalPnLPct = positions.open.total_unrealized_pnl_pct;
  const totalCost = positions.open.total_cost;

  return (
    <Section title="Positions">
      {/* Strategy rules header */}
      <div className="text-[10px] text-[#55556A] mb-4">
        {r?.exit_strategy && <>exit <code className="text-[#8888A0]">{r.exit_strategy}</code></>}
        {r?.hold_days != null && <> · hold <code className="text-[#8888A0]">{r.hold_days} td</code></>}
        <> · stop <code className="text-[#8888A0]">
          {r?.stop_loss_pct != null ? `${(r.stop_loss_pct * 100).toFixed(0)}%` : "none"}
        </code></>
        {r?.position_size_pct != null && <> · size <code className="text-[#8888A0]">{(r.position_size_pct * 100).toFixed(0)}%</code></>}
        {r?.max_concurrent != null && <> · cap <code className="text-[#8888A0]">{r.max_concurrent}</code></>}
      </div>

      {/* Open positions */}
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-[#E8E8ED]">Open ({positions.open.count})</h3>
        {positions.open.count > 0 && (
          <div className="text-xs text-[#8888A0]">
            Cost <span className="font-mono">${totalCost.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
            <span className="mx-2 text-[#55556A]">·</span>
            Unrealized <span className={`font-mono ${totalPnL >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
              {formatDollarSigned(totalPnL)}
            </span>
            {totalPnLPct != null && (
              <span className={`ml-1 font-mono ${totalPnLPct >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                ({formatPctSigned(totalPnLPct)})
              </span>
            )}
          </div>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-[10px] uppercase tracking-wider">
            <tr>
              <th className="py-2 pr-3">Ticker</th>
              <th className="pr-3">Src</th>
              <th className="pr-3 text-right">Size</th>
              <th className="pr-3 text-right">Entry</th>
              <th className="pr-3 text-right">Now</th>
              <th className="pr-3 text-right">+/- %</th>
              <th className="pr-3 text-right">+/- $</th>
              <th className="pr-3">Held</th>
              <th className="pr-3">Remain</th>
              <th className="pr-3">Planned exit</th>
              <th>Insider</th>
            </tr>
          </thead>
          <tbody>
            {positions.open.rows.map((p) => (
              <tr key={p.id} className="border-t border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                <td className="py-2 pr-3 font-mono font-bold text-[#E8E8ED]">{p.ticker}</td>
                <td className="pr-3"><ExecSourceBadge source={p.execution_source} /></td>
                <td className="pr-3 text-right font-mono text-xs">
                  <SizeCell dollarAmount={p.dollar_amount} shares={p.shares} pct={p.position_size_pct} />
                </td>
                <td className="pr-3 text-right font-mono text-xs">${p.entry_price.toFixed(2)}</td>
                <td className="pr-3 text-right font-mono text-xs">
                  {p.current_price != null ? `$${p.current_price.toFixed(2)}` : "—"}
                  {p.price_source === "eod_fallback" && (
                    <span className="ml-1 text-[10px] text-[#F59E0B]" title="Last EOD close — live quote unavailable">·EOD</span>
                  )}
                </td>
                <td className={`pr-3 text-right font-mono text-xs ${(p.unrealized_pnl_pct ?? 0) >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                  {p.unrealized_pnl_pct != null ? formatPctSigned(p.unrealized_pnl_pct) : "—"}
                </td>
                <td className={`pr-3 text-right font-mono text-xs ${(p.unrealized_pnl_dollar ?? 0) >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                  {p.unrealized_pnl_dollar != null ? formatDollarSigned(p.unrealized_pnl_dollar) : "—"}
                </td>
                <td className="pr-3 text-xs text-[#8888A0]">{p.days_held != null ? `${p.days_held}d` : "—"}</td>
                <td className="pr-3 text-xs text-[#8888A0]">
                  {p.trading_days_remaining != null ? `${p.trading_days_remaining} td` : "—"}
                </td>
                <td className="pr-3 text-xs text-[#8888A0]">{p.planned_exit_date ?? "—"}</td>
                <td className="text-xs text-[#8888A0] truncate max-w-[200px]">
                  {p.insider_name ?? "—"}
                  {p.insider_title && <span className="text-[#55556A]"> ({p.insider_title})</span>}
                </td>
              </tr>
            ))}
            {positions.open.rows.length === 0 && (
              <tr>
                <td colSpan={11} className="py-4 text-center text-[#55556A] text-xs">No open positions.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Closed positions */}
      <div className="mt-6 mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-[#E8E8ED]">
          Closed ({positions.closed.total})
        </h3>
        <div className="text-xs text-[#55556A] flex items-center gap-2">
          <span>per page:</span>
          <select
            value={closedPerPage}
            onChange={(e) => onClosedPerPageChange(parseInt(e.target.value, 10))}
            className="bg-[#1A1A26] border border-[#2A2A3A] rounded px-2 py-1 text-xs text-[#E8E8ED]"
          >
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-[10px] uppercase tracking-wider">
            <tr>
              <th className="py-2 pr-3">Ticker</th>
              <th className="pr-3">Src</th>
              <th className="pr-3 text-right">Size</th>
              <th className="pr-3 text-right">Entry</th>
              <th className="pr-3 text-right">Exit</th>
              <th className="pr-3 text-right">+/- %</th>
              <th className="pr-3 text-right">+/- $</th>
              <th className="pr-3">Held</th>
              <th className="pr-3">Entry date</th>
              <th className="pr-3">Exit date</th>
              <th>Exit reason</th>
            </tr>
          </thead>
          <tbody>
            {positions.closed.rows.map((p) => (
              <tr key={p.id} className="border-t border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                <td className="py-2 pr-3 font-mono font-bold text-[#E8E8ED]">{p.ticker}</td>
                <td className="pr-3"><ExecSourceBadge source={p.execution_source} /></td>
                <td className="pr-3 text-right font-mono text-xs">
                  <SizeCell dollarAmount={p.dollar_amount} shares={p.shares} pct={p.position_size_pct} />
                </td>
                <td className="pr-3 text-right font-mono text-xs">
                  {p.entry_price != null ? `$${p.entry_price.toFixed(2)}` : "—"}
                </td>
                <td className="pr-3 text-right font-mono text-xs">
                  {p.exit_price != null ? `$${p.exit_price.toFixed(2)}` : "—"}
                </td>
                <td className={`pr-3 text-right font-mono text-xs ${(p.pnl_pct ?? 0) >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                  {p.pnl_pct != null ? formatPctSigned(p.pnl_pct) : "—"}
                </td>
                <td className={`pr-3 text-right font-mono text-xs ${(p.pnl_dollar ?? 0) >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
                  {p.pnl_dollar != null ? formatDollarSigned(p.pnl_dollar) : "—"}
                </td>
                <td className="pr-3 text-xs text-[#8888A0]">{p.hold_days != null ? `${p.hold_days}d` : "—"}</td>
                <td className="pr-3 text-xs text-[#8888A0]">{p.entry_date}</td>
                <td className="pr-3 text-xs text-[#8888A0]">{p.exit_date ?? "—"}</td>
                <td className="text-xs text-[#8888A0]"><code>{p.exit_reason ?? "—"}</code></td>
              </tr>
            ))}
            {positions.closed.rows.length === 0 && (
              <tr>
                <td colSpan={11} className="py-4 text-center text-[#55556A] text-xs">No closed positions yet.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <Pagination
        total={positions.closed.total}
        limit={closedPerPage}
        offset={(closedPage - 1) * closedPerPage}
        onPageChange={onClosedPageChange}
      />
    </Section>
  );
}

// ── Section: Filings evaluated ───────────────────────────────────────────

function FilingsSection({
  rows, allRows, filterMode, setFilterMode, stageFilter, setStageFilter,
  expanded, setExpanded,
}: {
  rows: EvaluationRow[];
  allRows: EvaluationRow[];
  filterMode: FilterMode;
  setFilterMode: (m: FilterMode) => void;
  stageFilter: string | null;
  setStageFilter: (s: string | null) => void;
  expanded: string | null;
  setExpanded: (s: string | null) => void;
}) {
  return (
    <Section title="Filings evaluated">
      <p className="text-[10px] text-[#55556A] mb-3">
        One row per SEC filing (multi-lot Form 4s collapsed to the highest-conviction lot).
        Click a row to see per-stage decisions.
      </p>
      <div className="flex flex-wrap items-center gap-2 mb-3 text-sm">
        <span className="text-[#55556A] mr-2">Show:</span>
        <FilterBtn active={filterMode === "all"} onClick={() => { setFilterMode("all"); setStageFilter(null); }}>
          All ({allRows.length})
        </FilterBtn>
        <FilterBtn active={filterMode === "passed"} onClick={() => { setFilterMode("passed"); setStageFilter(null); }} tone="green">
          Entered ({allRows.filter((r) => r.final_passed).length})
        </FilterBtn>
        <FilterBtn active={filterMode === "rejected"} onClick={() => { setFilterMode("rejected"); setStageFilter(null); }} tone="red">
          Not entered ({allRows.filter((r) => !r.final_passed).length})
        </FilterBtn>
        {filterMode === "rejected" && (
          <>
            <span className="text-[#55556A] mx-2">because of:</span>
            {(["dedup", "filter", "pit_lookup", "min_10b5_1", "conviction", "capacity"] as const).map((s) => {
              const count = allRows.filter((r) => r.rejected_at === s).length;
              if (count === 0) return null;
              return (
                <FilterBtn key={s} active={stageFilter === s} onClick={() => setStageFilter(stageFilter === s ? null : s)}>
                  {humanizeStage(s)} ({count})
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
              <th className="py-2 pr-3">Filed</th>
              <th className="pr-3">Ticker</th>
              <th className="pr-3">Insider</th>
              <th className="pr-3 text-right">Conv</th>
              <th className="pr-3">Grade</th>
              <th className="pr-3">Outcome</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const key = `${r.run_id}:${r.trade_id}`;
              const isOpen = expanded === key;
              return (
                <>
                  <tr key={key} className="border-t border-[#2A2A3A]/50 hover:bg-[#1A1A26] cursor-pointer"
                      onClick={() => setExpanded(isOpen ? null : key)}>
                    <td className="py-2 pr-3 text-[10px] text-[#8888A0]">{r.filing_date}</td>
                    <td className="pr-3 font-mono font-bold text-xs">{r.ticker ?? "—"}</td>
                    <td className="pr-3 text-xs text-[#8888A0] truncate max-w-[200px]">
                      {r.insider_name ?? "—"}
                      {r.lots_in_filing > 1 && (
                        <span className="ml-1 text-[10px] text-[#55556A]">·{r.lots_in_filing} lots</span>
                      )}
                    </td>
                    <td className="pr-3 text-right font-mono text-xs">
                      {r.conviction != null ? r.conviction.toFixed(1) : "—"}
                    </td>
                    <td className="pr-3 text-xs">{r.pit_grade ?? "—"}</td>
                    <td className="pr-3"><OutcomeCell outcome={r.outcome} /></td>
                    <td className="text-[#55556A] text-xs">{isOpen ? "▼" : "▶"}</td>
                  </tr>
                  {isOpen && (
                    <tr className="border-t border-[#2A2A3A] bg-[#0A0A0F]">
                      <td colSpan={7} className="py-3 px-4">
                        <div className="space-y-2 text-xs">
                          <Reason label="Held/Seen" passed={r.dedup_passed}      reason={r.dedup_reason} />
                          <Reason label="Filter"    passed={r.filter_passed}     reason={r.filter_reason} />
                          <Reason label="PIT"       passed={r.pit_passed}        reason={r.pit_reason} />
                          <Reason label="10b5-1"    passed={r.tenb51_passed}     reason={r.tenb51_reason} />
                          <Reason label="Conviction" passed={r.conviction_passed} reason={r.conviction_reason} />
                          <Reason label="Capacity"  passed={r.capacity_passed}   reason={r.capacity_reason} />
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
            {rows.length === 0 && (
              <tr>
                <td colSpan={7} className="py-4 text-center text-[#55556A] text-xs">No filings match the current filter.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Section>
  );
}

function OutcomeCell({ outcome }: { outcome: EvalOutcome }) {
  if (outcome.kind === "not_entered") {
    return (
      <span className="text-xs">
        <span className="text-[#EF4444]">✗</span>{" "}
        <span className="text-[#8888A0]">{outcome.rejected_at ? humanizeStage(outcome.rejected_at) : "rejected"}</span>
        {outcome.reason && (
          <span className="text-[#55556A] ml-1">— {outcome.reason}</span>
        )}
      </span>
    );
  }
  if (outcome.kind === "open") {
    const pct = outcome.pnl_pct;
    const dol = outcome.pnl_dollar;
    if (pct == null || dol == null) {
      return <span className="text-xs text-[#22C55E]">✓ entered <span className="text-[#55556A]">(no quote)</span></span>;
    }
    const color = pct >= 0 ? "text-[#22C55E]" : "text-[#EF4444]";
    return (
      <span className="text-xs">
        <span className="text-[#22C55E]">✓ entered</span>{" "}
        <span className={`font-mono ${color}`}>{formatPctSigned(pct)}</span>{" "}
        <span className={`font-mono ${color}`}>({formatDollarSigned(dol)})</span>
      </span>
    );
  }
  if (outcome.kind === "closed") {
    const pct = outcome.pnl_pct;
    const color = (pct ?? 0) >= 0 ? "text-[#22C55E]" : "text-[#EF4444]";
    return (
      <span className="text-xs">
        <span className="text-[#8888A0]">closed</span>{" "}
        <span className={`font-mono ${color}`}>{pct != null ? formatPctSigned(pct) : "—"}</span>{" "}
        {outcome.exit_reason && (
          <span className="text-[#55556A]">· {outcome.exit_reason}</span>
        )}
      </span>
    );
  }
  return <span className="text-xs text-[#22C55E]">✓ entered</span>;
}

// ── Section: Reconciliation (only when not alert_only) ───────────────────

function ReconciliationSection({ block }: { block: ReconciliationBlock }) {
  return (
    <Section title="Strategy ↔ Alpaca">
      {block.latest_capture_at ? (
        <p className="text-[10px] text-[#55556A] mb-3">
          Latest snapshot {block.latest_capture_at.slice(0, 19).replace("T", " ")} ·{" "}
          <span className="text-[#8888A0]">strategy_portfolio is canonical; Alpaca is side-channel.</span>
        </p>
      ) : (
        <p className="text-[10px] text-[#55556A] mb-3">
          No Alpaca snapshot yet — run <code>scripts/alpaca_reconcile.py</code>.
        </p>
      )}

      <div className="grid gap-4 md:grid-cols-2">
        <div>
          <div className="text-[10px] uppercase tracking-wider text-[#55556A] mb-2">
            Active divergences ({block.divergences.length})
          </div>
          {block.divergences.length === 0 ? (
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
                {block.divergences.map((d) => (
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
            Alpaca holdings ({block.alpaca_positions.length})
          </div>
          {block.alpaca_positions.length === 0 ? (
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
                {block.alpaca_positions.map((p) => (
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
  );
}

// ── Section: Alerts (compact) ────────────────────────────────────────────

function AlertsSection({ alerts }: { alerts: AlertRow[] }) {
  const recent = alerts.slice(-10).reverse();
  return (
    <Section title="Recent alerts">
      {recent.length === 0 ? (
        <p className="text-[#55556A] text-sm">No alerts from this strategy.</p>
      ) : (
        <table className="w-full text-sm">
          <thead className="text-left text-[#55556A] text-[10px] uppercase tracking-wider">
            <tr><th className="py-2 pr-3">Time</th><th className="pr-3">Sev</th><th className="pr-3">Component</th><th>Message</th></tr>
          </thead>
          <tbody>
            {recent.map((a, i) => (
              <tr key={i} className="border-t border-[#2A2A3A]/50">
                <td className="py-1 pr-3 text-[10px] text-[#8888A0]">{a.ts}</td>
                <td className={`pr-3 text-xs ${a.severity === "critical" ? "text-[#EF4444]" : a.severity === "error" ? "text-[#F59E0B]" : a.severity === "warn" ? "text-[#FBBF24]" : "text-[#55556A]"}`}>{a.severity}</td>
                <td className="pr-3 text-xs"><code>{a.component}</code></td>
                <td className="text-xs whitespace-pre-wrap">{a.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Section>
  );
}

// ── Helpers ──────────────────────────────────────────────────────────────

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

function humanizeStage(s: string): string {
  return {
    dedup: "Held/Seen",
    filter: "Filter",
    pit_lookup: "PIT lookup",
    min_10b5_1: "10b5-1",
    conviction: "Conviction",
    capacity: "Capacity",
  }[s] ?? s;
}

function formatPctSigned(x: number): string {
  return `${x >= 0 ? "+" : ""}${(x * 100).toFixed(2)}%`;
}

function formatDollarSigned(x: number): string {
  const sign = x >= 0 ? "+" : "-";
  return `${sign}$${Math.abs(x).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}
