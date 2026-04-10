export const dynamic = "force-dynamic";

import { redirect } from "next/navigation";
import { fetchAPIAuth } from "@/lib/auth";
import { StrategySnapshotCard, type StrategySnapshot } from "@/components/strategy-snapshot-card";

export const metadata = {
  title: "Paper Trading — Form4 Admin",
  description: "Live state of the 3 paper trading strategies vs backtest expectations.",
};

interface PaperTradingResponse {
  as_of: string;
  strategies: StrategySnapshot[];
  summary: {
    total_strategies: number;
    on_track: number;
    below: number;
    well_below: number;
    errored: number;
  };
}

async function getDashboardData(): Promise<PaperTradingResponse | { error: string }> {
  try {
    return await fetchAPIAuth<PaperTradingResponse>("/paper-trading/dashboard");
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { error: msg };
  }
}

export default async function PaperTradingPage() {
  const data = await getDashboardData();

  // 403 from the API (non-admin) → redirect to dashboard
  if ("error" in data) {
    if (data.error.includes("403")) {
      redirect("/dashboard");
    }
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Paper Trading</h1>
        <div className="rounded border border-[#EF4444]/30 bg-[#EF4444]/5 p-4 text-sm text-[#EF4444]">
          Failed to load: {data.error}
        </div>
      </div>
    );
  }

  const { as_of, strategies, summary } = data;
  const summaryLine = (() => {
    if (summary.errored > 0) {
      return `${summary.errored} strategy${summary.errored > 1 ? "ies" : ""} failed to load`;
    }
    if (summary.well_below > 0) {
      return `${summary.well_below} strategy${summary.well_below > 1 ? "ies" : ""} well below expectation`;
    }
    if (summary.below > 0) {
      return `${summary.below} strategy${summary.below > 1 ? "ies" : ""} below expectation`;
    }
    return `All ${summary.total_strategies} strategies on track`;
  })();
  const summaryColor =
    summary.errored > 0 || summary.well_below > 0
      ? "text-[#EF4444]"
      : summary.below > 0
        ? "text-[#F59E0B]"
        : "text-[#22C55E]";

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between flex-wrap gap-2">
        <div>
          <h1 className="text-2xl font-bold text-[#E8E8ED]">Paper Trading</h1>
          <p className="text-sm text-[#55556A] mt-1">
            Live state of the 3 paper trading strategies vs backtest expectations.
          </p>
        </div>
        <div className="text-right">
          <div className={`text-sm font-medium ${summaryColor}`}>{summaryLine}</div>
          <div className="text-xs text-[#55556A] font-mono mt-1">
            as of {new Date(as_of).toLocaleString()}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {strategies.map((s) => (
          <StrategySnapshotCard key={s.name} snapshot={s} />
        ))}
      </div>
    </div>
  );
}
