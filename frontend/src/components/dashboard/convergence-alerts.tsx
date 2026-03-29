import Link from "next/link";
import { fetchAPI } from "@/lib/api";
import { formatCurrency } from "@/lib/format";
import type { ConvergenceItem } from "@/lib/types";

export async function ConvergenceAlerts() {
  const data = await fetchAPI<{ items: ConvergenceItem[]; total: number }>(
    "/congress/convergence",
    { days: "90" },
  );

  // Sort by most recent signal first
  const sorted = [...data.items].sort(
    (a, b) => b.last_date.localeCompare(a.last_date),
  );
  const top5 = sorted.slice(0, 5);

  if (top5.length === 0) {
    return (
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
        <h3 className="text-sm font-medium text-[#8888A0] mb-3">Convergence Alerts</h3>
        <p className="text-xs text-[#55556A]">No convergence signals in the last 90 days</p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
      <h3 className="text-sm font-medium text-[#8888A0] mb-3">Convergence Alerts</h3>
      <div className="space-y-2">
        {top5.map((item) => {
          const combinedBuys = (item.insider_buys ?? 0) + (item.politician_buys ?? 0);
          return (
            <Link
              key={item.ticker}
              href={`/company/${item.ticker}`}
              className="flex items-center justify-between rounded-md border border-[#2A2A3A]/50 bg-[#1A1A26]/40 px-3 py-2.5 hover:bg-[#1A1A26] transition-colors group"
            >
              <div className="flex items-center gap-2 min-w-0">
                <span className="font-mono font-bold text-[#E8E8ED] group-hover:text-blue-400 transition-colors text-sm">
                  {item.ticker}
                </span>
                <span
                  className={`rounded-full px-1.5 py-0.5 text-[10px] font-bold font-mono shrink-0 ${
                    combinedBuys >= 10
                      ? "bg-[#22C55E]/15 text-[#22C55E] border border-[#22C55E]/30"
                      : combinedBuys >= 5
                        ? "bg-[#F59E0B]/15 text-[#F59E0B] border border-[#F59E0B]/30"
                        : "bg-[#3B82F6]/15 text-[#3B82F6] border border-[#3B82F6]/30"
                  }`}
                >
                  {combinedBuys} buys
                </span>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <span className="text-[10px] text-[#55556A] font-mono hidden xl:inline">
                  {formatCurrency((item.insider_total_value ?? 0) + (item.politician_total_value_estimate ?? 0))}
                </span>
                <span className="text-[10px] text-[#55556A] font-mono">
                  {item.last_date.slice(5)}
                </span>
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
