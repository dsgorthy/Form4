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
      <div className="flex items-baseline justify-between mb-2">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#81819A]">
          Related Insiders
        </div>
      </div>
      {/* Said plainly and once. The alternative is a reader assuming this is a
          leaderboard, which is the single most likely misreading. */}
      <p className="text-xs text-[#81819A] mb-4">
        Grouped by shared companies and similar filing behaviour. This is a
        similarity measure, not a ranking or a performance signal.
      </p>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {items.map((r) => (
          <Link
            key={r.insider_id}
            href={`/insider/${r.slug || r.insider_id}`}
            className="rounded-lg border border-[#2A2A3A] bg-[#1A1A26]/50 p-4 hover:bg-[#2A2A3A]/40 transition-colors"
          >
            <div className="flex items-center gap-2 mb-1 min-w-0">
              <span className="text-sm font-medium text-[#E8E8ED] truncate">
                {r.name}
              </span>
              {r.is_entity === 1 && (
                <span className="shrink-0 rounded px-1 py-0.5 text-[10px] font-medium border border-[#81819A]/30 bg-[#81819A]/10 text-[#8888A0]">
                  Entity
                </span>
              )}
            </div>
            <div className="text-xs text-[#8888A0] truncate">{reasonLine(r)}</div>
            <div className="text-[11px] text-[#81819A] mt-1">
              {r.filing_count.toLocaleString()}{" "}
              {r.filing_count === 1 ? "filing" : "filings"}
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
