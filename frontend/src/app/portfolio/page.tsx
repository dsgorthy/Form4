export const dynamic = "force-dynamic";

import { Suspense } from "react";
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
      {/* PortfolioView reads ?strategy= (set by onboarding) via
          useSearchParams, which Next requires a Suspense boundary around. The
          fallback matches the component's own loading skeleton so the swap is
          invisible. */}
      <Suspense
        fallback={
          <div className="space-y-4 animate-pulse">
            <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
              {[1, 2, 3, 4, 5].map((i) => (
                <div key={i} className="h-20 rounded-lg border border-[#2A2A3A] bg-[#1A1A26]" />
              ))}
            </div>
            <div className="h-64 rounded-lg border border-[#2A2A3A] bg-[#1A1A26]" />
          </div>
        }
      >
        <PortfolioView />
      </Suspense>
    </div>
  );
}
