import Link from "next/link";
import { fetchAPIAuth } from "@/lib/auth";
import { formatCurrency } from "@/lib/format";
import type { Inflection } from "@/lib/types";

function ratioColor(ratio: number, tradeType: string): string {
  if (ratio >= 3) return tradeType === "buy" ? "#22C55E" : "#EF4444";
  return "#F59E0B";
}

function ratioBgColor(ratio: number, tradeType: string): string {
  if (ratio >= 3) return tradeType === "buy" ? "rgba(34,197,94,0.1)" : "rgba(239,68,68,0.1)";
  return "rgba(245,158,11,0.1)";
}

function ratioBorderColor(ratio: number, tradeType: string): string {
  if (ratio >= 3) return tradeType === "buy" ? "rgba(34,197,94,0.3)" : "rgba(239,68,68,0.3)";
  return "rgba(245,158,11,0.3)";
}

export async function ActivitySpikes() {
  let data: { items: Inflection[]; total: number; gated?: boolean };
  try {
    data = await fetchAPIAuth<{ items: Inflection[]; total: number; gated?: boolean }>(
      "/dashboard/inflections",
    );
  } catch {
    return (
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
        <h3 className="text-sm font-medium text-[#8888A0] mb-3">Activity Spikes</h3>
        <p className="text-xs text-[#55556A]">Sign in with Pro to view activity spikes</p>
      </div>
    );
  }

  const isGated = data.gated === true;
  const top6 = data.items.slice(0, 6);

  if (top6.length === 0) {
    return (
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
        <h3 className="text-sm font-medium text-[#8888A0] mb-3">Activity Spikes</h3>
        <p className="text-xs text-[#55556A]">No inflection signals detected</p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
      <h3 className="text-sm font-medium text-[#8888A0] mb-3">Activity Spikes</h3>
      <div className="grid grid-cols-1 gap-2">
        {top6.map((item) => {
          const color = ratioColor(item.ratio, item.trade_type);
          const bgColor = ratioBgColor(item.ratio, item.trade_type);
          const borderColor = ratioBorderColor(item.ratio, item.trade_type);

          return (
            <Link
              key={`${item.ticker}-${item.trade_type}`}
              href={`/company/${item.ticker}`}
              className={`flex items-center justify-between rounded-md border border-[#2A2A3A]/50 bg-[#1A1A26]/40 px-3 py-2.5 hover:bg-[#1A1A26] transition-colors group ${isGated ? "select-none" : ""}`}
            >
              <div className="flex items-center gap-2 min-w-0">
                <span className={`font-mono font-bold group-hover:text-blue-400 transition-colors text-sm ${isGated ? "text-[#E8E8ED]/40 blur-[3px]" : "text-[#E8E8ED]"}`}>
                  {item.ticker}
                </span>
                <span
                  className="rounded px-1.5 py-0.5 text-[10px] font-mono font-medium shrink-0"
                  style={{
                    color: item.trade_type === "buy" ? "#22C55E" : "#EF4444",
                    backgroundColor: item.trade_type === "buy" ? "rgba(34,197,94,0.1)" : "rgba(239,68,68,0.1)",
                  }}
                >
                  {item.trade_type.toUpperCase()}
                </span>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <span className={`text-[10px] font-mono hidden xl:inline ${isGated ? "text-[#55556A]/40 blur-[3px]" : "text-[#55556A]"}`}>
                  {formatCurrency(item.recent_value)}
                </span>
                <span
                  className="rounded-full px-1.5 py-0.5 text-[10px] font-bold font-mono whitespace-nowrap"
                  style={{ color, backgroundColor: bgColor, border: `1px solid ${borderColor}` }}
                >
                  {item.ratio.toFixed(1)}x
                </span>
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
