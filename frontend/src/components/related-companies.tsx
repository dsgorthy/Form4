import Link from "next/link";
import { SectionLabel } from "@/components/ui/section-label";

/**
 * "Related companies" — topical navigation for the second-most-crawled surface.
 *
 * Company pages took 1,482 Googlebot requests in 7 days (against 508 for
 * insider pages) and carried twenty outbound links to insiders and NOT ONE to
 * another company. A reader who arrived from search had nowhere to go, and a
 * crawler had no sector signal to read.
 *
 * Two relations, and the card says which, because they are not equally strong:
 *
 *   shared_insiders  people file on both. Countable, and two or more is a
 *                    genuine link between the companies.
 *   sector_peer      same sector, ranked by recent insider buying.
 *
 * NOT A COMPARISON AND NOT A SIGNAL. Two companies sharing a director tells
 * you about one person's calendar, not about their stocks. There is no price
 * correlation here, nothing is ranked by performance, and no figure on this
 * card says either company is a good or bad holding.
 */
export interface RelatedCompany {
  ticker: string;
  company: string | null;
  sector: string | null;
  rank: number;
  reason: "shared_insiders" | "sector_peer";
  shared_insiders: number;
  same_sector: boolean;
  recent_buys: number;
}

function reasonLine(r: RelatedCompany): string {
  if (r.reason === "shared_insiders") {
    const n = r.shared_insiders;
    return `${n} shared ${n === 1 ? "insider" : "insiders"}`;
  }
  return r.sector ? `Also in ${r.sector}` : "Same sector";
}

export function RelatedCompanies({
  items,
  ticker,
}: {
  items: RelatedCompany[];
  ticker: string;
}) {
  if (!items.length) return null;

  return (
    <div className="mt-10">
      {/* A ruled list, matching related-insiders. These two sit at the bottom
          of sibling pages and should not be two different shapes. */}
      <SectionLabel>Related Companies</SectionLabel>
      <p className="mb-1 max-w-[62ch] text-xs text-[#63636F]">
        Companies that share insiders with {ticker}, and sector peers with
        recent insider buying. Related by who files, not by how the stocks move.
      </p>
      <ul className="m-0 list-none p-0">
        {items.map((r) => (
          <li key={r.ticker} className="border-b border-[#1D1D26] last:border-0">
            <Link
              href={`/company/${r.ticker}`}
              className="flex items-baseline gap-4 py-3 transition-colors hover:bg-[#14141C]/60"
            >
              <span className="w-[4.5rem] shrink-0 font-mono font-semibold text-[#E8E8ED]">
                {r.ticker}
              </span>
              <span className="min-w-0 flex-1 truncate text-[13.5px] text-[#8A8A9E]">
                {r.company || "\u2014"}
              </span>
              <span className="hidden shrink-0 text-[13px] text-[#8A8A9E] sm:block">
                {reasonLine(r)}
              </span>
              <span className="w-[8rem] shrink-0 text-right font-mono text-[12px] text-[#63636F]">
                {r.recent_buys > 0
                  ? `${r.recent_buys} ${r.recent_buys === 1 ? "buy" : "buys"} / yr`
                  : "no recent buys"}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
