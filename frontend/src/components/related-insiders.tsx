import Link from "next/link";

/**
 * "Related insiders" — a navigation aid, not a ranking.
 *
 * WHY THE COPY IS WORDED THE WAY IT IS
 *
 * Two insiders land here for one of two reasons and the card says WHICH,
 * because the two are not equally strong and a reader deserves to weigh them
 * separately:
 *
 *   co_investment    they file on the same company. Concrete, checkable, and
 *                    the tickers are named.
 *   similar_profile  they file at a similar cadence, size and buy/sell mix,
 *                    within the same sector.
 *
 * NOTHING HERE IS A QUALITY CLAIM. The behavioural clustering behind
 * `similar_profile` was tested against forward returns and failed —
 * permutation p=0.208, observed spread 2.33pp against a null median of
 * 1.75pp. So there is no grade on these cards, no "top", no ordering language,
 * and the heading says "Related", not "Best" or "Recommended". If a later
 * change wants to sort or badge these by performance, it needs an experiment
 * that works first.
 *
 * The list is deliberately short and sometimes very short. Padding it out with
 * weak matches is what produced a small-town bank director as the eighth
 * "related insider" on a biotech investor's page; see
 * scripts/insider_similarity.py.
 */
export interface RelatedInsider {
  insider_id: string;
  name: string;
  slug: string | null;
  is_entity: number;
  filing_count: number;
  rank: number;
  shared_tickers: number;
  reason: "co_investment" | "similar_profile";
  reason_tickers: string[];
}

function reasonLine(r: RelatedInsider): string {
  if (r.reason === "co_investment" && r.reason_tickers.length) {
    const shown = r.reason_tickers.slice(0, 3).join(", ");
    const extra = r.shared_tickers - Math.min(3, r.reason_tickers.length);
    return extra > 0 ? `Also files on ${shown} +${extra}` : `Also files on ${shown}`;
  }
  if (r.reason === "co_investment") return "Files on the same company";
  return "Similar filing profile";
}

export function RelatedInsiders({ items }: { items: RelatedInsider[] }) {
  if (!items.length) return null;

  return (
    <div className="mt-10">
      {/* Ruled list, not a card grid. The page's problem was thirteen
          identical bordered boxes; adding a fourteenth grid of them was the
          version this replaces. */}
      <h2 className="mb-3 border-b border-[#24242F] pb-2 font-mono text-[10.5px] font-medium uppercase tracking-[0.15em] text-[#63636F]">
        Related Insiders
      </h2>
      <p className="mb-1 max-w-[62ch] text-xs text-[#63636F]">
        Grouped by shared companies and similar filing behaviour. This is a
        similarity measure, not a ranking or a performance signal.
      </p>
      <ul className="m-0 list-none p-0">
        {items.map((r) => (
          <li key={r.insider_id} className="border-b border-[#1D1D26] last:border-0">
            <Link
              href={`/insider/${r.slug || r.insider_id}`}
              className="flex items-baseline gap-4 py-3 transition-colors hover:bg-[#14141C]/60"
            >
              <span className="min-w-0 flex-1 truncate text-[14px] text-[#E8E8ED]">
                {r.name}
                {r.is_entity === 1 && (
                  <span className="ml-2 rounded-[2px] border border-[#63636F]/30 bg-[#63636F]/10 px-1 py-[1px] align-middle text-[10px] text-[#8A8A9E]">
                    Entity
                  </span>
                )}
              </span>
              <span className="hidden shrink-0 text-[13px] text-[#8A8A9E] sm:block">
                {reasonLine(r)}
              </span>
              <span className="w-[5.5rem] shrink-0 text-right font-mono text-[12px] tabular-nums text-[#63636F]">
                {r.filing_count.toLocaleString()}{" "}
                {r.filing_count === 1 ? "filing" : "filings"}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
