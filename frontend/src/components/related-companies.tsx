import Link from "next/link";

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
      <div className="text-[10px] font-semibold uppercase tracking-widest text-[#81819A] mb-2">
        Related Companies
      </div>
      <p className="text-xs text-[#81819A] mb-4">
        Companies that share insiders with {ticker}, and sector peers with
        recent insider buying. Related by who files, not by how the stocks move.
      </p>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {items.map((r) => (
          <Link
            key={r.ticker}
            href={`/company/${r.ticker}`}
            className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4 hover:bg-[#2A2A3A]/40 transition-colors"
          >
            <div className="flex items-center justify-between gap-2 mb-1 min-w-0">
              <span className="font-mono font-semibold text-[#E8E8ED]">
                {r.ticker}
              </span>
              <span className="text-[11px] text-[#81819A] shrink-0">
                {reasonLine(r)}
              </span>
            </div>
            {r.company && (
              <div className="text-xs text-[#8888A0] truncate">{r.company}</div>
            )}
            <div className="text-[11px] text-[#81819A] mt-1">
              {r.recent_buys > 0
                ? `${r.recent_buys} insider ${r.recent_buys === 1 ? "buy" : "buys"} in the last year`
                : "No insider buys in the last year"}
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
