export const dynamic = "force-dynamic";

import { PortfolioView } from "@/components/portfolio-view";

export default function PortfolioPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Live Strategies</h1>
        {/* Methodology claims belong on /research/methodology, not here.
            "No look-ahead bias" answers an objection the reader did not have
            and plants it — the page's job is to show what the strategies did.
            Every trade being public is the one process fact that is also a
            result, so it stays. */}
        <p className="text-sm text-[#55556A] mt-1">
          Three insider-trading strategies, running live. Every trade public.
        </p>
      </div>
      <PortfolioView />
    </div>
  );
}
