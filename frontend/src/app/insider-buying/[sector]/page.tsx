import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchAPI } from "@/lib/api";
import { formatCurrency } from "@/lib/format";
import { SectionLabel } from "@/components/ui/section-label";

/** One sector's insider buying. See ../page.tsx for why these pages exist. */
export const revalidate = 3600;

interface SectorDetail {
  sector: string;
  slug: string;
  window_days: number;
  summary: { buy_filings: number; tickers: number; insiders: number; total_value: number | null };
  top_buys: {
    trade_id: string; ticker: string; company: string | null;
    insider_id: string | null; insider_name: string | null; insider_slug: string | null;
    title: string | null; value: number | null; qty: number | null; price: number | null;
    filing_date: string; trade_date: string;
  }[];
  top_companies: {
    ticker: string; company: string | null; buy_filings: number;
    insiders: number; total_value: number | null;
  }[];
  top_insiders: {
    insider_id: string; name: string | null; slug: string | null; is_entity: number;
    buy_filings: number; tickers: number; total_value: number | null;
  }[];
  all_sectors: { sector: string; slug: string; buy_filings: number }[];
}

async function load(slug: string): Promise<SectorDetail | null> {
  try {
    return await fetchAPI<SectorDetail>(`/sectors/${slug}`);
  } catch {
    return null;
  }
}

export async function generateMetadata(
  { params }: { params: Promise<{ sector: string }> },
): Promise<Metadata> {
  const { sector } = await params;
  const d = await load(sector);
  if (!d) return { title: "Sector Not Found", robots: { index: false, follow: true } };
  const url = `https://form4.app/insider-buying/${d.slug}`;
  return {
    title: `${d.sector} Insider Buying — ${d.summary.tickers} Companies`,
    // The description carries the numbers on purpose: it is what shows in the
    // result, and a count is the one thing a competing scraper page cannot
    // claim without having done the work.
    description:
      `Open-market insider buying in ${d.sector}: ${d.summary.buy_filings} discretionary ` +
      `purchase filings by ${d.summary.insiders} insiders across ${d.summary.tickers} ` +
      `companies in the last ${d.window_days} days, from SEC Form 4 data.`,
    alternates: { canonical: url },
    openGraph: { title: `${d.sector} Insider Buying`, url, type: "website" },
  };
}

function Section({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="mb-10">
      <SectionLabel>{label}</SectionLabel>
      {children}
    </div>
  );
}

export default async function SectorPage(
  { params }: { params: Promise<{ sector: string }> },
) {
  const { sector } = await params;
  const d = await load(sector);
  if (!d) notFound();

  const s = d.summary;

  return (
    <div className="mx-auto max-w-5xl px-4 py-8">
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{
          __html: JSON.stringify({
            "@context": "https://schema.org",
            "@type": "CollectionPage",
            name: `${d.sector} Insider Buying`,
            url: `https://form4.app/insider-buying/${d.slug}`,
            description:
              `${s.buy_filings} discretionary insider purchase filings in ${d.sector} ` +
              `over the last ${d.window_days} days.`,
          }),
        }}
      />

      <nav className="flex items-center gap-2 text-sm text-[#81819A] mb-6">
        <Link href="/" className="hover:text-[#8888A0] transition-colors">Dashboard</Link>
        <span>/</span>
        <Link href="/insider-buying" className="hover:text-[#8888A0] transition-colors">
          Insider Buying
        </Link>
        <span>/</span>
        <span className="text-[#E8E8ED]">{d.sector}</span>
      </nav>

      <h1 className="text-3xl font-bold text-[#E8E8ED] mb-3">
        {d.sector} Insider Buying
      </h1>
      <p className="text-[#8888A0] max-w-3xl mb-6">
        {s.buy_filings.toLocaleString()} discretionary purchase{" "}
        {s.buy_filings === 1 ? "filing" : "filings"} by{" "}
        {s.insiders.toLocaleString()} insiders across{" "}
        {s.tickers.toLocaleString()} {d.sector} companies in the last{" "}
        {d.window_days} days, totalling {formatCurrency(s.total_value)}.
        Scheduled 10b5-1 plan buys, compensation grants and option exercises
        are excluded.
      </p>

      {d.top_buys.length > 0 && (
        <Section label={`Largest ${d.sector} Insider Buys`}>
          <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[#2A2A3A] bg-[#12121A]">
                  {["Company", "Insider", "Value", "Traded", "Filed"].map((h, i) => (
                    <th
                      key={h}
                      className={`px-2 py-2 text-[10px] font-medium uppercase tracking-wider text-[#81819A] sm:px-4 ${
                        i >= 2 ? "text-right" : "text-left"
                      } ${i === 1 ? "hidden sm:table-cell" : ""} ${
                        i >= 3 ? "hidden md:table-cell" : ""
                      }`}
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {d.top_buys.map((b) => (
                  <tr key={b.trade_id} className="border-b border-[#2A2A3A]/40 last:border-0">
                    <td className="px-2 py-2 sm:px-4">
                      <Link href={`/company/${b.ticker}`} className="font-mono font-semibold text-[#E8E8ED] hover:text-blue-400">
                        {b.ticker}
                      </Link>
                      {b.company && (
                        <div className="max-w-[11rem] truncate text-xs text-[#8888A0] sm:max-w-[220px]">
                          {b.company}
                        </div>
                      )}
                      {/* The insider, folded in below sm where its own column
                          does not fit. */}
                      <div className="max-w-[11rem] truncate text-xs text-[#81819A] sm:hidden">
                        {b.insider_name || "Unknown"}
                        {b.title ? ` · ${b.title}` : ""}
                      </div>
                    </td>
                    <td className="hidden px-2 py-2 sm:table-cell sm:px-4">
                      <div className="max-w-[8rem] truncate sm:max-w-[220px]">
                        {b.insider_id ? (
                          <Link href={`/insider/${b.insider_slug || b.insider_id}`} className="text-blue-400 hover:text-blue-300">
                            {b.insider_name || "Unknown"}
                          </Link>
                        ) : (
                          <span className="text-[#8888A0]">{b.insider_name || "Unknown"}</span>
                        )}
                      </div>
                      {b.title && (
                        <div className="max-w-[8rem] truncate text-xs text-[#81819A] sm:max-w-[200px]">
                          {b.title}
                        </div>
                      )}
                    </td>
                    <td className="whitespace-nowrap px-2 py-2 text-right font-mono text-[#22C55E] sm:px-4">
                      <Link href={`/filing/${b.trade_id}`} className="hover:underline">
                        {formatCurrency(b.value)}
                      </Link>
                    </td>
                    <td className="hidden px-4 py-2 text-right font-mono text-[#81819A] md:table-cell">{b.trade_date}</td>
                    <td className="hidden px-4 py-2 text-right font-mono text-[#81819A] md:table-cell">{b.filing_date}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      )}

      {d.top_companies.length > 0 && (
        <Section label={`Most-Bought ${d.sector} Companies`}>
          <ul className="m-0 list-none p-0">
            {d.top_companies.map((c) => (
              <li key={c.ticker} className="border-b border-[#1D1D26] last:border-0">
                <Link href={`/company/${c.ticker}`}
                  className="flex items-baseline gap-4 py-3 transition-colors hover:bg-[#14141C]/60">
                  <span className="w-[4.5rem] shrink-0 font-mono font-semibold text-[#E8E8ED]">{c.ticker}</span>
                  <span className="min-w-0 flex-1 truncate text-[13.5px] text-[#8A8A9E]">{c.company}</span>
                  <span className="shrink-0 font-mono text-[13.5px] tabular-nums text-[#46CC8D]">
                    {formatCurrency(c.total_value)}
                  </span>
                  <span className="w-[7rem] shrink-0 text-right font-mono text-[12px] text-[#63636F]">
                    {c.buy_filings} {c.buy_filings === 1 ? "buy" : "buys"} · {c.insiders}{" "}
                    {c.insiders === 1 ? "insider" : "insiders"}
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        </Section>
      )}

      {d.top_insiders.length > 0 && (
        <Section label={`Most Active ${d.sector} Buyers`}>
          {/* Ranked by how much they bought, NOT by how well it worked. A list
              of people ordered by past returns under a "buying" headline is a
              tip sheet, and our grades do not predict forward returns. Each
              insider's accuracy is on their own page with its denominator. */}
          <p className="text-xs text-[#81819A] mb-3">
            Ranked by value purchased in the window, not by past performance.
            Each insider&rsquo;s track record is on their profile.
          </p>
          <ul className="m-0 list-none p-0">
            {d.top_insiders.map((p) => (
              <li key={p.insider_id} className="border-b border-[#1D1D26] last:border-0">
                <Link href={`/insider/${p.slug || p.insider_id}`}
                  className="flex items-baseline gap-4 py-3 transition-colors hover:bg-[#14141C]/60">
                  <span className="min-w-0 flex-1 truncate text-[14px] text-[#E8E8ED]">
                    {p.name || "Unknown"}
                    {p.is_entity === 1 && (
                      <span className="ml-2 rounded-[2px] border border-[#63636F]/30 bg-[#63636F]/10 px-1 py-[1px] align-middle text-[10px] text-[#8A8A9E]">
                        Entity
                      </span>
                    )}
                  </span>
                  <span className="shrink-0 font-mono text-[13.5px] tabular-nums text-[#46CC8D]">
                    {formatCurrency(p.total_value)}
                  </span>
                  <span className="w-[7rem] shrink-0 text-right font-mono text-[12px] text-[#63636F]">
                    {p.buy_filings} {p.buy_filings === 1 ? "buy" : "buys"} · {p.tickers}{" "}
                    {p.tickers === 1 ? "co." : "cos."}
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        </Section>
      )}

      <Section label="Other Sectors">
        <div className="flex flex-wrap gap-2">
          {d.all_sectors.filter((o) => o.slug !== d.slug).map((o) => (
            <Link key={o.slug} href={`/insider-buying/${o.slug}`}
              className="rounded-full border border-[#2A2A3A] bg-[#1A1A26]/50 px-3 py-1.5 text-xs text-[#8888A0] hover:bg-[#2A2A3A]/40 hover:text-[#E8E8ED] transition-colors">
              {o.sector} <span className="text-[#81819A]">({o.buy_filings})</span>
            </Link>
          ))}
        </div>
      </Section>
    </div>
  );
}
