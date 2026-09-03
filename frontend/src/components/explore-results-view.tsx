import Link from "next/link";
import { InsiderGradeBadge } from "@/components/insider-grade-badge";
import { formatCurrency } from "@/lib/format";
import { SectionLabel } from "@/components/ui/section-label";

/**
 * Full results for a search term — tier 2 of the search.
 *
 * The dropdown answers "take me to the thing I already have in mind". It
 * cannot answer "show me everything matching", because it holds 8 rows and a
 * term like "smith" matches ~780 insiders. It advertised that gap honestly
 * ("8 of 6,257") while having nowhere to send you; this is that destination.
 *
 * Companies and insiders get different row shapes rather than a shared one.
 * A company is identified by ticker and sized by trade volume; an insider by
 * name, role, and track record. Forcing both through one column layout is
 * what makes a dropdown row feel cramped, and there is no reason to inherit
 * that constraint on a full page.
 */

export interface ResultsTicker {
  ticker: string;
  company: string | null;
  trade_count?: number;
  total_value?: number;
}

export interface ResultsInsider {
  insider_id: string;
  name: string;
  slug?: string | null;
  score?: number | null;
  best_pit_grade?: string | null;
  best_career_grade?: string | null;
  primary_title: string | null;
  primary_ticker: string | null;
}


export function ExploreResultsView({
  term,
  tickers,
  insiders,
  tickerTotal,
  insiderTotal,
}: {
  term: string;
  tickers: ResultsTicker[];
  insiders: ResultsInsider[];
  tickerTotal: number;
  insiderTotal: number;
}) {
  const nothing = tickers.length === 0 && insiders.length === 0;

  // Lead with whichever type actually carries the match. Searching "smith"
  // and getting three companies above 780 people buries the answer.
  const companiesFirst = tickerTotal >= insiderTotal;

  const companySection = tickers.length > 0 && (
    <section key="companies" className="mb-10">
      <SectionLabel>
        Companies ({tickerTotal.toLocaleString()})
      </SectionLabel>
      <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] text-xs uppercase text-[#81819A]">
              <th className="px-4 py-2 text-left font-medium">Ticker</th>
              <th className="px-4 py-2 text-left font-medium">Company</th>
              <th className="px-4 py-2 text-right font-medium">Trades</th>
              <th className="px-4 py-2 text-right font-medium">Value</th>
            </tr>
          </thead>
          <tbody>
            {tickers.map((t) => (
              <tr key={t.ticker} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                <td className="px-4 py-2">
                  <Link
                    href={`/explore?ticker=${encodeURIComponent(t.ticker)}`}
                    className="font-mono font-bold text-[#3B82F6] hover:text-[#60A5FA]"
                  >
                    {t.ticker}
                  </Link>
                </td>
                <td className="px-4 py-2 text-[#8888A0]">{t.company || "—"}</td>
                <td className="px-4 py-2 text-right font-mono text-[#E8E8ED]">
                  {t.trade_count ?? "—"}
                </td>
                <td className="px-4 py-2 text-right font-mono text-[#E8E8ED]">
                  {t.total_value != null ? formatCurrency(t.total_value) : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );

  const insiderSection = insiders.length > 0 && (
    <section key="insiders" className="mb-10">
      <SectionLabel>
        Insiders ({insiderTotal.toLocaleString()})
      </SectionLabel>
      <div className="overflow-x-auto rounded-lg border border-[#2A2A3A]">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2A2A3A] text-xs uppercase text-[#81819A]">
              <th className="px-4 py-2 text-left font-medium">Name</th>
              <th className="px-4 py-2 text-left font-medium">Role</th>
              <th className="px-4 py-2 text-left font-medium">Company</th>
              <th className="px-4 py-2 text-right font-medium">Grade</th>
            </tr>
          </thead>
          <tbody>
            {insiders.map((ins) => (
              <tr key={ins.insider_id} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                <td className="px-4 py-2">
                  <Link
                    href={`/explore?insider=${encodeURIComponent(ins.slug || ins.insider_id)}`}
                    className="text-[#3B82F6] hover:text-[#60A5FA]"
                  >
                    {ins.name}
                  </Link>
                </td>
                <td className="px-4 py-2 text-[#8888A0]">{ins.primary_title || "—"}</td>
                <td className="px-4 py-2 font-mono text-xs text-[#81819A]">
                  {ins.primary_ticker || "—"}
                </td>
                <td className="px-4 py-2 text-right">
                  {ins.best_career_grade ? (
                    <InsiderGradeBadge grade={ins.best_career_grade} />
                  ) : (
                    <span className="text-[#81819A]">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );

  return (
    <div>
      <div className="mb-8">
        <h1 className="mb-1 text-2xl font-bold text-[#E8E8ED]">
          Results for &ldquo;{term}&rdquo;
        </h1>
        <p className="text-sm text-[#81819A]">
          {insiderTotal.toLocaleString()} insider
          {insiderTotal === 1 ? "" : "s"} &middot;{" "}
          {tickerTotal.toLocaleString()} compan{tickerTotal === 1 ? "y" : "ies"}
        </p>
      </div>

      {nothing ? (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-10 text-center">
          <p className="text-sm text-[#8888A0]">
            No companies or insiders match &ldquo;{term}&rdquo;
          </p>
          <p className="mt-1 text-xs text-[#81819A]">
            Try a ticker symbol, a company name, or a person&rsquo;s full name.
          </p>
        </div>
      ) : companiesFirst ? (
        <>{companySection}{insiderSection}</>
      ) : (
        <>{insiderSection}{companySection}</>
      )}
    </div>
  );
}
