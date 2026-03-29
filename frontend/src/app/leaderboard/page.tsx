export const dynamic = "force-dynamic";

import Link from "next/link";
import { LeaderboardTable } from "@/components/leaderboard-table";

interface Props {
  searchParams: Promise<{
    sort?: string;
    order?: string;
    min_trades?: string;
    min_tier?: string;
    active?: string;
  }>;
}

export default async function LeaderboardPage({ searchParams }: Props) {
  const sp = await searchParams;
  const sort = sp.sort || "score";
  const order = sp.order || "desc";
  const minTrades = sp.min_trades || "";
  const minTier = sp.min_tier || "";
  const active = sp.active || "12m";

  // Compute active_since date
  const activeMonths: Record<string, number> = { "3m": 3, "6m": 6, "12m": 12, "all": 0 };
  const months = activeMonths[active] || 12;
  let activeSince = "";
  if (months > 0) {
    const d = new Date();
    d.setMonth(d.getMonth() - months);
    activeSince = d.toISOString().slice(0, 10);
  }

  const filters: Record<string, string> = {};
  if (minTrades) filters.min_trades = minTrades;
  if (minTier) filters.min_tier = minTier;
  if (activeSince) filters.active_since = activeSince;

  function buildUrl(overrides: Record<string, string> = {}) {
    const merged = { sort, order, min_trades: minTrades, min_tier: minTier, active, ...overrides };
    const sp = new URLSearchParams();
    Object.entries(merged).forEach(([k, v]) => { if (v) sp.set(k, v); });
    return `/leaderboard?${sp.toString()}`;
  }

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6 md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[#E8E8ED]">Leaderboard</h1>
          <p className="text-sm text-[#55556A] mt-1">
            Ranked insiders by track record
            <span className="mx-2">&middot;</span>
            <a
              href="/scoring"
              target="_blank"
              rel="noopener noreferrer"
              className="text-[#3B82F6] hover:text-[#60A5FA] transition-colors"
            >
              How scoring works &rarr;
            </a>
          </p>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-2 md:gap-3">
          {/* Activity recency */}
          {(["3m", "6m", "12m", "all"] as const).map((period) => (
            <Link
              key={period}
              href={buildUrl({ active: period })}
              className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
                active === period
                  ? "border-[#22C55E]/50 bg-[#22C55E]/10 text-[#22C55E]"
                  : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
              }`}
            >
              {period === "all" ? "All Time" : `Active ${period}`}
            </Link>
          ))}

          <span className="w-px h-5 bg-[#2A2A3A]" />

          {/* Trade count */}
          <Link
            href={buildUrl({ min_trades: minTrades === "5" ? "" : "5" })}
            className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
              minTrades === "5"
                ? "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]"
                : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
            }`}
          >
            5+ Trades
          </Link>
          <Link
            href={buildUrl({ min_trades: minTrades === "10" ? "" : "10" })}
            className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
              minTrades === "10"
                ? "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]"
                : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
            }`}
          >
            10+ Trades
          </Link>

          <span className="w-px h-5 bg-[#2A2A3A]" />

          {/* Tier */}
          {[2, 3].map((t) => (
            <Link
              key={t}
              href={buildUrl({ min_tier: minTier === String(t) ? "" : String(t) })}
              className={`rounded-md px-3 py-1.5 text-xs font-medium border transition-colors ${
                minTier === String(t)
                  ? "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]"
                  : "border-[#2A2A3A] text-[#8888A0] hover:text-[#E8E8ED]"
              }`}
            >
              Tier {t}+
            </Link>
          ))}

          {(minTrades || minTier || active !== "12m") && (
            <Link
              href="/leaderboard"
              className="text-xs text-[#55556A] hover:text-[#8888A0]"
            >
              Reset
            </Link>
          )}
        </div>
      </div>

      {/* Table — client-side data loading */}
      <LeaderboardTable filters={filters} sort={sort} order={order} />
    </div>
  );
}
