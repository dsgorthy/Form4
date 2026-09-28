/**
 * A complete, server-rendered list of the entities linked from a page.
 *
 * WHY THIS EXISTS, AND WHY IT IS NOT THE ROSTER
 *
 * A sitemap is a weak crawl signal next to a link. On 2026-09-20 Google held
 * 59,022 URLs as "Discovered - currently not indexed" — found, queued, never
 * fetched — and 67% of its crawl budget went to Discovery rather than Refresh.
 * Internal links are what changes that ordering.
 *
 * The company page is our best-performing page type (2.80 search views per
 * 1,000 URLs, against 1.53 for insiders) and the insider page is the one we are
 * trying to lift. So company -> insider is the edge that matters, and it was
 * mostly missing: `InsiderRoster` paginates client-side at 10 rows, so the
 * delivered HTML carried the first 10 links and no more, whatever the roster's
 * real size. Measured 2026-09-27 across the 10,684 company pages we submit:
 *
 *   median roster        21 insiders
 *   p90                  49
 *   max                 183
 *   rosters over 10   8,553 of 10,684  (80%)
 *   links rendered   99,570 of 265,605 (37%)
 *
 * Two thirds of the edges existed in the data and not in the HTML.
 *
 * This renders the rest. It is deliberately NOT a second roster: no grades, no
 * values, no sorting, nothing gated — a `<details>` element holding names and
 * links. `<details>` content is in the DOM and is crawled; it is collapsed
 * because a reader who wants the ranked, valued view already has it above, and
 * 183 duplicate rows in the middle of the page would be a worse page.
 */
import Link from "next/link";

export interface EntityLink {
  href: string;
  label: string;
  /** Optional second line, e.g. a role or a filing count. */
  detail?: string | null;
}

export function EntityLinkList({
  items,
  summary,
  /** How many of `items` the page already links above this block. */
  alreadyShown = 0,
}: {
  items: EntityLink[];
  summary: string;
  alreadyShown?: number;
}) {
  // Nothing to add when the page already links everything.
  if (items.length <= alreadyShown) return null;

  return (
    <details className="mt-3 rounded-lg border border-[#2A2A3A] bg-[#12121A]/60">
      <summary className="cursor-pointer px-4 py-2.5 text-xs text-[#8888A0] hover:text-[#E8E8ED] transition-colors">
        {summary}
      </summary>
      <ul className="flex flex-wrap gap-x-4 gap-y-1.5 px-4 pb-3.5 pt-1">
        {items.map((it) => (
          <li key={it.href} className="text-[13px] leading-snug">
            <Link href={it.href} className="text-[#7FA8F0] hover:text-[#A8C4F5]">
              {it.label}
            </Link>
            {it.detail && (
              <span className="text-[#63636F]"> · {it.detail}</span>
            )}
          </li>
        ))}
      </ul>
    </details>
  );
}
