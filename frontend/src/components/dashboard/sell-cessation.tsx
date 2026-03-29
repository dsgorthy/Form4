import Link from "next/link";
import { fetchAPIAuth } from "@/lib/auth";
import { TierBadge } from "@/components/ui/tier-badge";
import type { SellCessationItem } from "@/lib/types";

export async function SellCessation() {
  let data: { items: SellCessationItem[]; total: number; gated?: boolean };
  try {
    data = await fetchAPIAuth<{ items: SellCessationItem[]; total: number; gated?: boolean }>(
      "/signals/sell-cessation",
    );
  } catch {
    return (
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
        <h3 className="text-sm font-medium text-[#8888A0] mb-3">Sell Cessation</h3>
        <p className="text-xs text-[#55556A]">Sign in with Pro to view sell cessation signals</p>
      </div>
    );
  }

  const isGated = data.gated === true;
  const top10 = data.items.slice(0, 10);

  if (top10.length === 0) {
    return (
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
        <h3 className="text-sm font-medium text-[#8888A0] mb-3">Sell Cessation</h3>
        <p className="text-xs text-[#55556A]">No sell cessation signals detected</p>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4">
      <h3 className="text-sm font-medium text-[#8888A0] mb-3">Sell Cessation</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[#2A2A3A]">
              <th className="pb-2 text-left text-[#55556A] font-medium">Insider</th>
              <th className="pb-2 text-center text-[#55556A] font-medium">Tier</th>
              <th className="pb-2 text-right text-[#55556A] font-medium">Silent</th>
              <th className="pb-2 text-left text-[#55556A] font-medium pl-3">Tickers</th>
            </tr>
          </thead>
          <tbody>
            {top10.map((item) => (
              <tr
                key={item.insider_id}
                className={`border-b border-[#2A2A3A]/30 hover:bg-[#1A1A26]/30 transition-colors ${isGated ? "select-none" : ""}`}
              >
                <td className="py-1.5">
                  {isGated ? (
                    <span className="text-[#E8E8ED]/40 blur-[3px] truncate block max-w-[120px]">{item.name}</span>
                  ) : (
                    <Link
                      href={`/insider/${item.cik || item.insider_id}`}
                      className="text-[#E8E8ED] hover:text-blue-400 transition-colors truncate block max-w-[120px]"
                    >
                      {item.name}
                    </Link>
                  )}
                </td>
                <td className="py-1.5 text-center">
                  {item.score_tier != null ? (
                    <TierBadge tier={item.score_tier} />
                  ) : (
                    <span className="text-[#55556A]">{"\u2014"}</span>
                  )}
                </td>
                <td className="py-1.5 text-right font-mono">
                  <span
                    className={
                      item.days_silent >= 90
                        ? "text-[#22C55E]"
                        : "text-[#F59E0B]"
                    }
                  >
                    {item.days_silent}d
                  </span>
                </td>
                <td className="py-1.5 pl-3">
                  <div className={`flex flex-wrap gap-1 ${isGated ? "blur-[3px]" : ""}`}>
                    {item.tickers.split(",").slice(0, 3).map((ticker) => (
                      <span
                        key={ticker}
                        className="rounded bg-[#1A1A26] px-1 py-0.5 text-[10px] font-mono text-[#8888A0]"
                      >
                        {ticker}
                      </span>
                    ))}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
