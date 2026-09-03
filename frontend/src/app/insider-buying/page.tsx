import type { Metadata } from "next";
import Link from "next/link";
import { fetchAPI } from "@/lib/api";
import { formatCurrency } from "@/lib/format";

/**
 * /insider-buying — the hub the site did not have.
 *
 * Every one of the 17 static URLs in the core sitemap was /pricing, /privacy,
 * /terms or a research note. Not one targeted an informational query, while
 * Googlebot spent ~4,300 requests a week on leaf pages that target none
 * either — "Erez Chimovits" is not a search anyone runs.
 *
 * This page and its eleven children target searches that exist ("stocks
 * insiders are buying", "healthcare insider buying") and, just as usefully,
 * point at several hundred leaf pages that currently get crawled with no
 * topical page linking to them.
 */
export const revalidate = 3600;

export const metadata: Metadata = {
  title: "Stocks Insiders Are Buying — Insider Buying by Sector",
  description:
    "Open-market insider buying across all 11 sectors, from SEC Form 4 filings. " +
    "Largest purchases, most-bought companies and the insiders behind them, updated daily.",
  alternates: { canonical: "https://form4.app/insider-buying" },
};

interface SectorRow {
  sector: string;
  slug: string;
  buy_filings: number;
  tickers: number;
  insiders: number;
  total_value: number | null;
}

export default async function InsiderBuyingHub() {
  let sectors: SectorRow[] = [];
  let windowDays = 90;
  try {
    const data = await fetchAPI<{ sectors: SectorRow[]; window_days: number }>(
      "/sectors",
    );
    sectors = data.sectors || [];
    windowDays = data.window_days ?? 90;
  } catch {}

  const totalFilings = sectors.reduce((a, s) => a + (s.buy_filings || 0), 0);

  return (
    <div className="mx-auto max-w-5xl px-4 py-8">
      <nav className="flex items-center gap-2 text-sm text-[#81819A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">
          Dashboard
        </Link>
        <span>/</span>
        <span className="text-[#E8E8ED]">Insider Buying</span>
      </nav>

      <h1 className="text-3xl font-bold text-[#E8E8ED] mb-3">
        Stocks Insiders Are Buying
      </h1>
      <p className="text-[#8888A0] max-w-3xl mb-2">
        Every open-market purchase disclosed on an SEC Form 4, grouped by
        sector. {totalFilings.toLocaleString()} buy filings across{" "}
        {sectors.length} sectors in the last {windowDays} days.
      </p>
      {/* The exclusion is the interesting part and belongs above the fold: it
          is the difference between this and a raw Form 4 dump. 10b5-1 plan
          purchases are pre-scheduled and, measured over the full history,
          return -2.22% abnormal at 30 days against +1.71% for discretionary
          ones — so counting them would actively mislead. */}
      <p className="text-sm text-[#81819A] max-w-3xl mb-8">
        Discretionary purchases only. Scheduled 10b5-1 plan buys, compensation
        grants, option exercises and tax withholding are excluded — they are
        the majority of Form 4 activity and none of them is a decision to buy.
      </p>

      {sectors.length === 0 ? (
        <p className="text-[#8888A0]">Sector data is unavailable right now.</p>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {sectors.map((s) => (
            <Link
              key={s.slug}
              href={`/insider-buying/${s.slug}`}
              className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-5 hover:bg-[#2A2A3A]/40 transition-colors"
            >
              <div className="flex items-baseline justify-between mb-2">
                <span className="text-lg font-semibold text-[#E8E8ED]">
                  {s.sector}
                </span>
                <span className="font-mono text-sm text-[#22C55E]">
                  {formatCurrency(s.total_value)}
                </span>
              </div>
              <div className="text-sm text-[#8888A0]">
                {s.buy_filings.toLocaleString()} buy{" "}
                {s.buy_filings === 1 ? "filing" : "filings"} ·{" "}
                {s.tickers.toLocaleString()} companies ·{" "}
                {s.insiders.toLocaleString()} insiders
              </div>
            </Link>
          ))}
        </div>
      )}

      <div className="mt-10 flex flex-wrap gap-4 text-sm">
        <Link href="/leaderboard" className="text-[#3B82F6] hover:text-[#60A5FA]">
          Top insiders by track record &rarr;
        </Link>
        <Link href="/clusters" className="text-[#3B82F6] hover:text-[#60A5FA]">
          Cluster buying &rarr;
        </Link>
        <Link href="/feed" className="text-[#3B82F6] hover:text-[#60A5FA]">
          Latest filings &rarr;
        </Link>
      </div>
    </div>
  );
}
