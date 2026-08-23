export const dynamic = "force-dynamic";

import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { formatCurrency } from "@/lib/format";
import { Badge } from "@/components/ui/badge";
import { insiderPath } from "@/lib/insider-url";
import { INSIDER_RATING_BLURB, insiderRating } from "@/lib/ratings";

interface SignalBreakdown {
  baseline: number;
  pit_win_rate_bonus: number;
  csuite_bonus: number;
  holdings_bonus: number;
  owner_10pct_penalty: number;
}

interface EntryReasoning {
  // The CURRENT shape written by simulate_strategy_portfolio:
  //   {thesis, filing_date, conviction, career_grade, pit_grade,
  //    is_csuite, is_rare_reversal}
  // The nested signal/insider/filing blocks below are the OLD shape, produced
  // by portfolio_simulator.compute_signal_quality — the PIT-violating scorer
  // archived on 2026-08-21. Rows written before then still carry it, so both
  // are typed and the page renders whichever it finds.
  thesis?: string;
  career_grade?: string | null;
  pit_grade?: string | null;
  conviction?: number | null;
  is_csuite?: boolean;
  is_rare_reversal?: boolean;

  signal?: {
    quality: number;
    grade: string | null;
    breakdown: SignalBreakdown;
  };
  insider?: {
    name: string;
    title: string | null;
    is_csuite: boolean;
    pit_win_rate_7d: number | null;
    pit_n_trades: number | null;
    pit_avg_abnormal_7d: number | null;
    pit_win_rate_30d: number | null;
    switch_rate: number | null;
    is_rare_reversal: boolean;
  };
  filing?: {
    filing_date: string | null;
    trade_date: string | null;
    trade_value: number | null;
    shares_bought: number | null;
    shares_after: number | null;
    holdings_pct_change: number | null;
    is_10b5_1: boolean;
  };
  company?: {
    ticker: string;
    name: string | null;
  };
  market_context?: {
    entry_price: number;
    week52_proximity: number | null;
  };
  position?: {
    size_pct: number;
    dollar_amount: number | null;
    shares: number | null;
    portfolio_equity: number | null;
    target_hold_days: number;
    hard_stop_pct: number;
    trailing_stop_drop: number;
  };
  cluster?: {
    is_cluster: boolean;
    cluster_size: number;
    other_insiders: string[];
  };
}

interface ExitReasoning {
  exit_reason: string;
  exit_price: number;
  pnl_pct: number;
  peak_return: number;
  peak_date: string | null;
  hold_days: number;
  spy_return_during_hold: number | null;
}

interface TradeDetail {
  id: number;
  trade_id: string | null;
  ticker: string;
  company: string | null;
  trade_type: string;
  direction: string;
  status: string;
  entry_date: string;
  exit_date: string | null;
  filing_date: string | null;
  trade_date: string | null;
  entry_price: number;
  exit_price: number | null;
  actual_fill_price: number | null;
  pnl_pct: number | null;
  pnl_dollar: number | null;
  peak_return: number | null;
  hold_days: number | null;
  target_hold: number;
  exit_reason: string | null;
  stop_hit: boolean;
  position_size: number;
  shares: number | null;
  dollar_amount: number | null;
  portfolio_value: number | null;
  stop_pct: number;
  insider_name: string;
  insider_id: number | null;
  insider_title: string | null;
  insider_pit_n: number | null;
  insider_pit_wr: number | null;
  trade_value: number | null;
  signal_quality: number | null;   // internal only — never rendered
  strategy_label?: string | null;
  insider_slug?: string | null;
  signal_grade: string | null;
  is_csuite: boolean;
  holdings_pct_change: number | null;
  is_rare_reversal: boolean;
  is_cluster: boolean;
  cluster_size: number | null;
  execution_source: string;
  /** Still sent by the API and still true of every simulated row; no longer
   *  rendered per-trade — see the Execution section. */
  is_estimated: boolean;
  slippage_applied: number | null;
  entry_reasoning: EntryReasoning | null;
  exit_reasoning: ExitReasoning | null;
}

// ---------------------------------------------------------------------------

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-4">{title}</div>
      {children}
    </div>
  );
}

function Row({ label, value, color, mono = false, sub }: {
  label: string;
  value: React.ReactNode;
  color?: string;
  mono?: boolean;
  sub?: string;
}) {
  return (
    <div className="flex items-baseline justify-between py-1.5 border-b border-[#2A2A3A]/30 last:border-0">
      <span className="text-xs text-[#55556A]">{label}</span>
      <div className="text-right">
        <span className={`text-sm ${mono ? "font-mono" : ""} ${color || "text-[#E8E8ED]"}`}>{value}</span>
        {sub && <div className="text-[10px] text-[#55556A]">{sub}</div>}
      </div>
    </div>
  );
}

function ScoreBar({ label, value, maxVal = 3 }: { label: string; value: number; maxVal?: number }) {
  const pct = Math.min(100, Math.max(0, ((value + maxVal) / (maxVal * 2)) * 100));
  const isPositive = value > 0;
  const isNeg = value < 0;
  return (
    <div className="flex items-center gap-3 py-1">
      <span className="text-xs text-[#55556A] w-28 shrink-0">{label}</span>
      <div className="flex-1 h-2.5 bg-[#1A1A26] rounded-full overflow-hidden relative">
        <div className="absolute left-1/2 top-0 bottom-0 w-px bg-[#2A2A3A]" />
        {isPositive && (
          <div
            className="absolute top-0 bottom-0 bg-[#22C55E] rounded-full"
            style={{ left: "50%", width: `${(value / maxVal) * 50}%` }}
          />
        )}
        {isNeg && (
          <div
            className="absolute top-0 bottom-0 bg-[#EF4444] rounded-full"
            style={{ right: "50%", width: `${(Math.abs(value) / maxVal) * 50}%` }}
          />
        )}
      </div>
      <span className={`text-xs font-mono w-10 text-right ${isPositive ? "text-[#22C55E]" : isNeg ? "text-[#EF4444]" : "text-[#55556A]"}`}>
        {value > 0 ? "+" : ""}{value.toFixed(1)}
      </span>
    </div>
  );
}

function ExitReasonBadge({ reason }: { reason: string }) {
  const map: Record<string, { label: string; color: string; border: string }> = {
    time_exit: { label: "TIME EXIT (T+30)", color: "text-[#8888A0]", border: "border-[#8888A0]/30" },
    trailing_stop: { label: "TRAILING STOP", color: "text-[#F59E0B]", border: "border-[#F59E0B]/30" },
    stop_loss: { label: "HARD STOP (-15%)", color: "text-[#EF4444]", border: "border-[#EF4444]/30" },
    missing_position: { label: "MISSING POSITION", color: "text-[#EF4444]", border: "border-[#EF4444]/30" },
  };
  const cfg = map[reason] || { label: reason.toUpperCase(), color: "text-[#8888A0]", border: "border-[#8888A0]/30" };
  return <Badge variant="outline" className={`text-[10px] ${cfg.color} ${cfg.border}`}>{cfg.label}</Badge>;
}

function ExecutionBadge({ source }: { source: string }) {
  if (source === "paper" || source === "live") {
    return <Badge variant="outline" className="text-[10px] border-[#22C55E]/30 text-[#22C55E]">LIVE</Badge>;
  }
  // "ESTIMATED FILL", not "SIMULATED".
  //
  // Everything on this page is real except one thing: the filing, the insider,
  // the grade, the rules and the prices are all actual, and what is modelled
  // is the price we would have transacted at. "SIMULATED" put the doubt on all
  // of it; this puts it on the part that is genuinely an estimate.
  //
  // Paired against LIVE above, so the distinction a reader needs — did money
  // move — is still carried by the two labels.
  return (
    <Badge variant="outline" className="text-[10px] border-[#55556A]/30 text-[#55556A]">
      ESTIMATED FILL
    </Badge>
  );
}

function gradeColor(grade: string | null): string {
  if (!grade) return "text-[#55556A]";
  if (grade === "A") return "text-[#22C55E]";
  if (grade === "B") return "text-[#3B82F6]";
  if (grade === "C") return "text-[#F59E0B]";
  return "text-[#EF4444]";
}

// ---------------------------------------------------------------------------

export default async function TradeDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;

  let trade: TradeDetail;
  try {
    trade = await fetchAPI<TradeDetail>(`/portfolio/trades/${id}`);
  } catch {
    notFound();
  }

  const er = trade.entry_reasoning;
  const xr = trade.exit_reasoning;
  const isOpen = trade.status === "open";
  const isWin = trade.pnl_pct != null && trade.pnl_pct > 0;

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      {/* Back link */}
      <Link href="/portfolio" className="text-xs text-[#55556A] hover:text-[#8888A0] transition-colors">
        ← Back to Portfolio
      </Link>

      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2 sm:gap-3">
            <h1 className="text-2xl font-bold font-mono text-[#E8E8ED]">{trade.ticker}</h1>
            {/* Two different measures of two different things, shown side by
                side with no labels until 2026-08-18: a bare "1.5 /10" next to
                a bare "Grade A" reads as a contradiction. The number is the
                conviction score, whose entry floor is 1.5, so a value at the
                bottom of the scale means the grade carried the trade and
                conviction merely cleared — not that we rated it 1.5 out of 10.
                The letter is the insider's career grade. Both get a word. */}
            {trade.signal_grade && (
              <span className="inline-flex items-baseline gap-1.5 rounded-md bg-[#1A1A26] px-2.5 py-1">
                <span className="text-[10px] uppercase tracking-wider text-[#55556A]">Career grade</span>
                <span className={`text-sm font-semibold ${gradeColor(trade.signal_grade)}`}>{trade.signal_grade}</span>
              </span>
            )}
            {/* Conviction is NOT rendered. api/ratings.py lists it in
                INTERNAL_ONLY_FIELDS and says why: it is a strategy's own entry
                threshold with a floor of 1.5, so "1.5" means CLEARED THE BAR,
                not "scored 1.5 out of 10". Printed as "1.5/10" beside a career
                grade of A it read as near-zero confidence in the strategy's
                best trades, and the obvious question was why we entered at
                all. The answer — an A+ insider on a validated thesis — was in
                entry_reasoning the whole time and is now what the page shows. */}
            <ExecutionBadge source={trade.execution_source} />
            {isOpen && <Badge variant="outline" className="text-[10px] border-[#3B82F6]/30 text-[#3B82F6]">OPEN</Badge>}
          </div>
          <div className="text-sm text-[#8888A0] mt-1">
            <Link href={`/company/${trade.ticker}`} className="hover:text-[#E8E8ED] transition-colors">
              {trade.company || trade.ticker}
            </Link>
            {trade.insider_name && (
              <span className="text-[#55556A]">
                {" — "}
                {trade.insider_id ? (
                  <Link href={insiderPath(trade.insider_name, trade.insider_id)} className="hover:text-[#8888A0]">{trade.insider_name}</Link>
                ) : trade.insider_name}
              </span>
            )}
            {trade.insider_title && <span className="text-[#55556A]">, {trade.insider_title}</span>}
          </div>
        </div>

        {/* P&L badge */}
        {trade.pnl_pct != null && (
          <div className={`rounded-lg px-5 py-3 text-center ${isWin ? "bg-[#22C55E]/10" : "bg-[#EF4444]/10"}`}>
            <div className={`text-2xl font-mono font-bold ${isWin ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
              {trade.pnl_pct > 0 ? "+" : ""}{trade.pnl_pct.toFixed(2)}%
            </div>
            {trade.pnl_dollar != null && (
              <div className={`text-xs font-mono ${isWin ? "text-[#22C55E]/70" : "text-[#EF4444]/70"}`}>
                {trade.pnl_dollar >= 0 ? "+" : ""}${Math.abs(trade.pnl_dollar).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
              </div>
            )}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Signal Quality Breakdown */}
        <Section title="Why This Trade Was Taken">
          {er?.signal?.breakdown ? (
            <div className="space-y-0.5">
              {/* The score bars below ARE the explanation — what each factor
                  contributed. The total that used to head them was conviction,
                  which is internal (api/ratings.INTERNAL_ONLY_FIELDS) and
                  reads as its own opposite: the floor is 1.5, so the strategy's
                  best trades displayed "1.5/10". */}
              {er.career_grade && (
                <div className="mb-3">
                  <Row
                    label="Insider rating"
                    value={`${er.career_grade} — ${INSIDER_RATING_BLURB[insiderRating(er.career_grade)]}`}
                  />
                </div>
              )}
              <ScoreBar label="Baseline" value={0} />
              <ScoreBar label="PIT Win Rate" value={er.signal.breakdown.pit_win_rate_bonus} />
              <ScoreBar label="C-Suite" value={er.signal.breakdown.csuite_bonus} />
              <ScoreBar label="Holdings Chg" value={er.signal.breakdown.holdings_bonus} />
              {er.signal.breakdown.owner_10pct_penalty !== 0 && (
                <ScoreBar label="10% Owner" value={er.signal.breakdown.owner_10pct_penalty} />
              )}
            </div>
          ) : (
            /* The reasons, not the score. entry_reasoning carries the thesis
               and the grades the entry was actually made on; the old fallback
               printed conviction alone, which answered a question nobody asked
               and raised one nobody could answer from the page. */
            <div className="space-y-0.5">
              {er?.career_grade && (
                <Row
                  label="Insider rating"
                  value={`${er.career_grade} — ${INSIDER_RATING_BLURB[insiderRating(er.career_grade)]}`}
                />
              )}
              {er?.thesis && (
                <Row label="Strategy" value={trade.strategy_label || er.thesis} />
              )}
              {!er?.career_grade && !er?.thesis && (
                <div className="text-sm text-[#55556A]">
                  No entry reasoning was recorded for this trade.
                </div>
              )}
            </div>
          )}

          {/* Badges row */}
          <div className="flex flex-wrap gap-1.5 mt-4">
            {trade.is_csuite && (
              <Badge variant="outline" className="text-[10px] border-[#F59E0B]/30 text-[#F59E0B]">C-SUITE</Badge>
            )}
            {trade.is_rare_reversal && (
              <Badge variant="outline" className="text-[10px] border-[#A855F7]/30 text-[#A855F7]">RARE REVERSAL</Badge>
            )}
            {trade.is_cluster && (
              <Badge variant="outline" className="text-[10px] border-[#3B82F6]/30 text-[#3B82F6]">
                CLUSTER ({trade.cluster_size} insiders)
              </Badge>
            )}
            {trade.holdings_pct_change != null && trade.holdings_pct_change >= 10 && (
              <Badge variant="outline" className="text-[10px] border-[#22C55E]/30 text-[#22C55E]">
                +{trade.holdings_pct_change.toFixed(0)}% HOLDINGS
              </Badge>
            )}
          </div>
        </Section>

        {/* Insider Track Record */}
        <Section title="Insider at Time of Entry">
          <Row label="Name" value={
            /* slug first, encoded id as the fallback. This linked the raw
               numeric row id, which /insider/{identifier} does not resolve —
               every one of these links was a 404. */
            (trade.insider_slug || trade.insider_id) ? (
              <Link href={`/insider/${trade.insider_slug || trade.insider_id}`} className="text-[#3B82F6] hover:text-[#60A5FA]">
                {trade.insider_name}
              </Link>
            ) : trade.insider_name
          } />
          <Row label="Title" value={trade.insider_title || "—"} />
          <Row
            label="PIT Win Rate (7d)"
            value={trade.insider_pit_wr != null ? `${trade.insider_pit_wr}%` : "—"}
            mono
            color={trade.insider_pit_wr != null && trade.insider_pit_wr >= 60 ? "text-[#22C55E]" : undefined}
          />
          <Row label="PIT Trade Count" value={er?.insider?.pit_n_trades ?? trade.insider_pit_n ?? "—"} mono />
          {er?.insider?.pit_win_rate_30d != null && (
            <Row label="PIT Win Rate (30d)" value={`${(er.insider.pit_win_rate_30d * 100).toFixed(0)}%`} mono />
          )}
          {er?.insider?.pit_avg_abnormal_7d != null && (
            <Row
              label="PIT Avg Abnormal (7d)"
              value={`${er.insider.pit_avg_abnormal_7d > 0 ? "+" : ""}${(er.insider.pit_avg_abnormal_7d * 100).toFixed(1)}%`}
              mono
              color={er.insider.pit_avg_abnormal_7d > 0 ? "text-[#22C55E]" : "text-[#EF4444]"}
            />
          )}
          {er?.insider?.switch_rate != null && (
            <Row label="Direction Switch Rate" value={`${(er.insider.switch_rate * 100).toFixed(0)}%`} mono />
          )}
        </Section>

        {/* Filing Details */}
        <Section title="SEC Filing">
          <Row label="Insider traded" value={trade.trade_date || "—"} />
          <Row label="Filed with SEC" value={trade.filing_date || "—"} />
          <Row
            label="Trade Value"
            value={trade.trade_value ? formatCurrency(trade.trade_value) : "—"}
            mono
          />
          {er?.filing?.shares_bought != null && (
            <Row label="Shares Bought" value={er.filing.shares_bought.toLocaleString()} mono />
          )}
          {er?.filing?.shares_after != null && (
            <Row label="Shares After" value={er.filing.shares_after.toLocaleString()} mono />
          )}
          {trade.holdings_pct_change != null && (
            <Row
              label="Holdings Change"
              value={`+${trade.holdings_pct_change.toFixed(1)}%`}
              mono
              color={trade.holdings_pct_change >= 10 ? "text-[#22C55E]" : undefined}
            />
          )}
          {er?.filing?.is_10b5_1 && (
            <Row label="10b5-1 Plan" value="Yes" />
          )}
          {trade.trade_id && (
            <div className="mt-3">
              <Link href={`/filing/${trade.trade_id}`} className="text-xs text-[#3B82F6] hover:text-[#60A5FA]">
                View SEC Filing →
              </Link>
            </div>
          )}
        </Section>

        {/* Position Details */}
        <Section title="Position">
          <Row label="Entry Date" value={trade.entry_date} />
          <Row label="Entry Price" value={`$${trade.entry_price.toFixed(2)}`} mono />
          <Row label="Position Size" value={`${(trade.position_size * 100).toFixed(0)}%`} mono />
          {trade.shares != null && <Row label="Shares" value={trade.shares.toLocaleString()} mono />}
          {trade.dollar_amount != null && <Row label="Dollar Amount" value={formatCurrency(trade.dollar_amount)} mono />}
          {trade.portfolio_value != null && (
            <Row label="Portfolio at Entry" value={formatCurrency(trade.portfolio_value)} mono />
          )}
          <Row label="Target Hold" value={`${trade.target_hold} days`} />
          {/* Signed. stop_pct is stored as a magnitude, and "50%" next to a
              green return read as a target rather than a floor. */}
          <Row label="Hard Stop" value={`-${(Math.abs(trade.stop_pct) * 100).toFixed(0)}%`} mono color="text-[#EF4444]" />
          {er?.position?.trailing_stop_drop != null && (
            <Row label="Trailing Stop" value={`${(er.position.trailing_stop_drop * 100).toFixed(0)}% from peak`} mono color="text-[#F59E0B]" />
          )}
          {er?.market_context?.week52_proximity != null && (
            <Row
              label="52-Week Position"
              value={`${(er.market_context.week52_proximity * 100).toFixed(0)}% of high`}
              mono
            />
          )}
        </Section>
      </div>

      {/* Outcome (only for closed trades) */}
      {!isOpen && (
        <Section title="Outcome">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8">
            <div>
              <Row label="Exit Date" value={trade.exit_date || "—"} />
              <Row label="Exit Price" value={trade.exit_price != null ? `$${trade.exit_price.toFixed(2)}` : "—"} mono />
              <Row label="Hold Days" value={trade.hold_days != null ? `${trade.hold_days}d (${Math.round((trade.hold_days ?? 0) * 5/7)}td)` : "—"} mono />
              <Row
                label="Exit Reason"
                value={trade.exit_reason ? <ExitReasonBadge reason={trade.exit_reason} /> : "—"}
              />
            </div>
            <div>
              <Row
                label="Return"
                value={trade.pnl_pct != null ? `${trade.pnl_pct > 0 ? "+" : ""}${trade.pnl_pct.toFixed(2)}%` : "—"}
                mono
                color={isWin ? "text-[#22C55E]" : "text-[#EF4444]"}
              />
              <Row
                label="P&L"
                value={trade.pnl_dollar != null ? `${trade.pnl_dollar >= 0 ? "+" : ""}${formatCurrency(Math.abs(trade.pnl_dollar))}` : "—"}
                mono
                color={trade.pnl_dollar != null && trade.pnl_dollar >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}
              />
              {trade.peak_return != null && (
                <Row
                  label="Peak Return"
                  value={`+${trade.peak_return.toFixed(2)}%`}
                  mono
                  color="text-[#22C55E]"
                  sub={xr?.peak_date ? `on ${xr.peak_date}` : undefined}
                />
              )}
              {xr?.spy_return_during_hold != null && (
                <Row
                  label="SPY During Hold"
                  value={`${xr.spy_return_during_hold > 0 ? "+" : ""}${(xr.spy_return_during_hold * 100).toFixed(2)}%`}
                  mono
                  color={xr.spy_return_during_hold >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}
                />
              )}
            </div>
          </div>
        </Section>
      )}

      {/* Cluster detail */}
      {er?.cluster?.is_cluster && er.cluster.other_insiders.length > 0 && (
        <Section title={`Cluster — ${er.cluster.cluster_size} Insiders Same Day`}>
          <div className="space-y-1">
            {er.cluster.other_insiders.map((name, i) => (
              <div key={i} className="text-sm text-[#8888A0]">
                {name}
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* The Execution section is gone. It had one row left — Source —
          and it rendered the same badge already shown beside the ticker at the
          top of the page, so it existed to repeat itself.

          Fill provenance is still disclosed where it sits next to a
          performance claim: under the strategy numbers on the homepage, under
          the equity curve on /portfolio, and in full on /performance and
          /disclaimer. */}
    </div>
  );
}
