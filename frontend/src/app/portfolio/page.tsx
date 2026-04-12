export const dynamic = "force-dynamic";

import { PortfolioView } from "@/components/portfolio-view";

export default function PortfolioPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Live Strategies</h1>
        <p className="text-sm text-[#55556A] mt-1">
          Three insider-trading strategies running on real paper accounts. Every trade is public.
          Research-backed, point-in-time scoring. No look-ahead bias.
        </p>
      </div>
      <PortfolioView />
    </div>
  );
}
