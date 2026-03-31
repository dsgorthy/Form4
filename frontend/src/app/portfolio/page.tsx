export const dynamic = "force-dynamic";

import { PortfolioView } from "@/components/portfolio-view";

export default function PortfolioPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-[#E8E8ED]">Strategy Portfolio</h1>
        <p className="text-sm text-[#55556A] mt-1">
          Simulated strategies based on insider trading signals with strict point-in-time scoring.
          No look-ahead bias. Entry at T+1 open after SEC filing.
        </p>
      </div>
      <PortfolioView />
    </div>
  );
}
