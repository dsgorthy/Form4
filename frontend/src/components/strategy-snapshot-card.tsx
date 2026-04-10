import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatCurrency } from "@/lib/format";

export interface StrategyPosition {
  symbol: string;
  qty: number;
  avg_entry_price: number;
  current_price: number;
  market_value: number;
  unrealized_pl: number;
  unrealized_plpc: number;
}

export interface StrategySnapshot {
  name: string;
  label: string;
  starting_capital: number;
  started_at: string;
  backtest: {
    cagr: number;
    sharpe: number;
    win_rate: number;
    max_dd: number;
    trades: number;
  };
  // Optional fields when present (absent if Alpaca call failed)
  current_equity?: number;
  cash?: number;
  buying_power?: number;
  status?: string;
  day_change?: number;
  day_change_pct?: number;
  total_pnl?: number;
  total_pnl_pct?: number;
  expected_equity?: number;
  delta_from_expected_pct?: number;
  deviation_status?: "on_track" | "below" | "well_below";
  position_count?: number;
  open_positions?: StrategyPosition[];
  error?: string;
}

function pct(value: number | undefined, decimals = 2): string {
  if (value == null) return "—";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(decimals)}%`;
}

function colorForPct(value: number | undefined): string {
  if (value == null) return "text-[#55556A]";
  if (value > 0) return "text-[#22C55E]";
  if (value < 0) return "text-[#EF4444]";
  return "text-[#8888A0]";
}

function statusBadge(status?: string): { label: string; cls: string } {
  switch (status) {
    case "on_track":
      return { label: "On track", cls: "bg-[#22C55E]/10 text-[#22C55E] border-[#22C55E]/30" };
    case "below":
      return { label: "Below expectation", cls: "bg-[#F59E0B]/10 text-[#F59E0B] border-[#F59E0B]/30" };
    case "well_below":
      return { label: "Well below", cls: "bg-[#EF4444]/10 text-[#EF4444] border-[#EF4444]/30" };
    default:
      return { label: status ?? "—", cls: "bg-[#1A1A26] text-[#8888A0] border-[#2A2A3A]" };
  }
}

export function StrategySnapshotCard({ snapshot }: { snapshot: StrategySnapshot }) {
  const badge = statusBadge(snapshot.deviation_status);
  const hasError = !!snapshot.error;

  return (
    <Card className="bg-[#12121A] border-[#2A2A3A]">
      <CardHeader>
        <div className="flex items-start justify-between gap-2">
          <div>
            <CardTitle className="text-base text-[#E8E8ED]">{snapshot.label}</CardTitle>
            <div className="text-xs text-[#55556A] font-mono">{snapshot.name}</div>
          </div>
          {!hasError && (
            <span className={`rounded px-2 py-0.5 text-[10px] font-medium border ${badge.cls}`}>
              {badge.label}
            </span>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {hasError ? (
          <div className="rounded border border-[#EF4444]/30 bg-[#EF4444]/5 p-3 text-xs text-[#EF4444]">
            Error fetching live data: {snapshot.error}
          </div>
        ) : (
          <>
            {/* Equity row */}
            <div className="grid grid-cols-2 gap-3">
              <div>
                <div className="text-[10px] uppercase tracking-wider text-[#55556A]">Current equity</div>
                <div className="text-2xl font-mono text-[#E8E8ED]">
                  {formatCurrency(snapshot.current_equity)}
                </div>
                <div className={`text-xs font-mono ${colorForPct(snapshot.total_pnl_pct)}`}>
                  {pct(snapshot.total_pnl_pct)} since start
                </div>
              </div>
              <div>
                <div className="text-[10px] uppercase tracking-wider text-[#55556A]">vs Backtest</div>
                <div className={`text-2xl font-mono ${colorForPct(snapshot.delta_from_expected_pct)}`}>
                  {pct(snapshot.delta_from_expected_pct)}
                </div>
                <div className="text-xs text-[#55556A]">
                  expected {formatCurrency(snapshot.expected_equity)}
                </div>
              </div>
            </div>

            {/* Detail row */}
            <div className="grid grid-cols-3 gap-3 border-t border-[#2A2A3A] pt-3 text-xs">
              <div>
                <div className="text-[#55556A]">Started</div>
                <div className="font-mono text-[#E8E8ED]">{formatCurrency(snapshot.starting_capital)}</div>
              </div>
              <div>
                <div className="text-[#55556A]">Cash</div>
                <div className="font-mono text-[#E8E8ED]">{formatCurrency(snapshot.cash)}</div>
              </div>
              <div>
                <div className="text-[#55556A]">Day</div>
                <div className={`font-mono ${colorForPct(snapshot.day_change_pct)}`}>
                  {pct(snapshot.day_change_pct)}
                </div>
              </div>
            </div>

            {/* Backtest baseline */}
            <div className="rounded border border-[#2A2A3A] bg-[#0A0A0F] p-2 text-[10px] text-[#8888A0]">
              Backtest baseline:
              <span className="ml-2 font-mono">CAGR {snapshot.backtest.cagr}%</span>
              <span className="ml-2 font-mono">Sharpe {snapshot.backtest.sharpe}</span>
              <span className="ml-2 font-mono">WR {snapshot.backtest.win_rate}%</span>
              <span className="ml-2 font-mono">MaxDD {snapshot.backtest.max_dd}%</span>
            </div>

            {/* Open positions */}
            {snapshot.open_positions && snapshot.open_positions.length > 0 ? (
              <div>
                <div className="text-[10px] uppercase tracking-wider text-[#55556A] mb-2">
                  Open positions ({snapshot.position_count})
                </div>
                <div className="space-y-1">
                  {snapshot.open_positions.map((p) => (
                    <div
                      key={p.symbol}
                      className="flex items-center justify-between text-xs font-mono border-b border-[#2A2A3A]/50 pb-1"
                    >
                      <span className="text-[#E8E8ED] w-16">{p.symbol}</span>
                      <span className="text-[#8888A0] w-16 text-right">{p.qty.toFixed(0)}</span>
                      <span className="text-[#8888A0] w-20 text-right">${p.current_price.toFixed(2)}</span>
                      <span className={`w-20 text-right ${colorForPct(p.unrealized_plpc)}`}>
                        {pct(p.unrealized_plpc)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="text-xs text-[#55556A] italic">No open positions</div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
