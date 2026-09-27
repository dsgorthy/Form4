/**
 * Shared sitemap sourcing and section definitions.
 *
 * The sitemap protocol caps a single <urlset> at 50,000 URLs. form4.app
 * emitted ONE file holding all of them and measured 52,327 on 2026-08-15.
 * Google does not truncate an oversized sitemap — it rejects the file — so
 * from the day we crossed the line nothing in it was processed. Last
 * successful index: 2026-04-03. Every SEO change since (insider slugs, the
 * public /insider tier, the canonical work) has been landing in a document
 * search engines were discarding.
 *
 * Deliberately hand-authored route handlers rather than Next's `sitemap.ts`
 * metadata convention with generateSitemaps(). That was tried first
 * (1fbe4c8) and failed in production in two ways at once: no index was
 * generated at /sitemap.xml, and the `id` argument did not resolve as a
 * section index, so every child served the same 16 URLs. Explicit routes are
 * more code and behave the way the protocol says.
 */

const BASE = "https://form4.app";
const API =
  process.env.API_URL_INTERNAL ||
  process.env.NEXT_PUBLIC_API_URL ||
  "http://localhost:8000/api/v1";

// Well under the 50,000 cap, so a section can grow substantially before it
// needs another file.
export const CHUNK = 20000;

// ── INDIVIDUAL FILINGS ARE NO LONGER SUBMITTED FOR INDEXING (2026-09-27) ──
//
// Derek's call, and the numbers back it. Clicks per thousand submitted URLs over
// the 28 days to 2026-09-20: companies 2.7, insiders 2.0, individual filings
// 0.77. Filings are a third as productive per URL and were a third of
// everything submitted — 30,014 of 99,690.
//
// WHY THAT MATTERS MORE THAN THE CLICK COUNT. Search Console, 2026-09-25:
// 255,350 crawl requests in 90 days against those 99,690 URLs, split
// Discovery 67% / Refresh 33%. Two thirds of a ~2,800/day budget was going to
// finding new filing URLs while the company pages that actually ranked went
// unvisited — /company/BVH had ranked at position 16 and its last successful
// crawl was 2026-09-05, twenty-two days before this change. Submitting fewer
// URLs is how those get re-crawled sooner. Counterintuitive, and it is the
// mechanism.
//
// THIS IS DELIBERATE, NOT THE SILENT-SHRINK BUG. The header above and the
// code deleted in db2b849 both warn about filings vanishing from the sitemap by
// accident, because an empty chunk is indistinguishable from a failed API fetch.
// That guard still applies to companies and insiders. Filings are now absent BY
// DECLARATION, which is a different thing: flip PUBLISH_FILINGS to restore them
// and nothing else needs to change.
//
// Filing pages remain crawlable and indexable — they are linked from every
// company and insider page and carry a self-canonical. We have stopped PUSHING
// them, not hidden them. A `noindex` would be the stronger version of this
// decision and would forfeit the 12% of clicks they still produce.
export const PUBLISH_FILINGS = false;

// Retained at 4 even while unpublished: Google already knows
// /sitemaps/filings-0..3.xml, and those children still resolve and serve an
// empty urlset rather than 404. A 404 on a sitemap it read yesterday is an
// error state that sits in the report for weeks; an empty urlset is the
// protocol's way of saying "nothing here now".
export const FILING_CHUNKS = 4;

// INSIDERS IS CHUNKED TOO, AS OF 2026-09-10.
//
// It was a single file, and the header above used to claim insiders were
// "bounded by construction". They are not: the bound is `buy_count >= 2` in
// api/routers/sitemap.py, and that population GREW THROUGH THE PROTOCOL CAP.
// It was 42,195 when the limit was raised to 45,000 on 2026-09-03; measured
// 2026-09-10 it is 51,747 — past the 50,000 ceiling a single <urlset> may
// hold, almost certainly from the Form 4 ingestion-loss reload landing.
//
// So this section was one generation away from repeating the exact failure
// documented at the top of this file: an oversized sitemap is REJECTED, not
// truncated, and nothing in it gets processed. The 45,000 limit was the only
// thing holding it under, at the cost of dropping 6,747 qualifying insiders.
//
// Three files gives 60,000 of capacity against 51,747 eligible. Adding a
// fourth is a one-line change here — which is the point of chunking it.
export const INSIDER_CHUNKS = 3;

// Ask the API for exactly what the chunks can hold. Keep this and
// INSIDER_CHUNKS in step; the API clamps to its own ceiling independently.
export const INSIDER_LIMIT = INSIDER_CHUNKS * CHUNK;

/** The filings children, published or not. */
export const FILING_SECTIONS = Array.from(
  { length: FILING_CHUNKS }, (_, i) => `filings-${i}`);

export const SECTIONS = [
  "core",
  "companies",
  // /sitemaps/insiders.xml is deliberately gone rather than kept as an alias.
  // The index is the only document that names its children, Google re-reads it
  // on every fetch, and a stale alias would be a second URL serving the same
  // 20,000 entries as insiders-0.
  ...Array.from({ length: INSIDER_CHUNKS }, (_, i) => `insiders-${i}`),
  ...(PUBLISH_FILINGS ? FILING_SECTIONS : []),
];

/**
 * Sections Google already knows that we no longer publish. The route resolves
 * them and serves an empty urlset, so a crawler that still holds the old index
 * gets a valid answer instead of a 404.
 */
export const RETIRED_SECTIONS = PUBLISH_FILINGS ? [] : FILING_SECTIONS;

export interface SitemapEntry {
  loc: string;
  lastmod?: string;
  changefreq?: string;
  priority?: number;
}

interface SitemapData {
  tickers: string[];
  // The API returns {id, name, slug}. Older deploys returned bare id strings,
  // and a version skew during a rolling deploy must not publish
  // /insider/undefined into Google, so both shapes are accepted.
  insiders: ({ id: string; name: string; slug?: string } | string)[];
  filings: string[];
}

export async function fetchSitemapData(): Promise<SitemapData> {
  try {
    const resp = await fetch(
      `${API}/sitemap/urls?limit_insiders=${INSIDER_LIMIT}&filing_days=90`,
      { next: { revalidate: 3600 } },
    );
    if (resp.ok) return await resp.json();
  } catch {
    // A failed fetch yields an empty section rather than a broken document.
    // An empty <urlset> is valid; a 500 tells Google the sitemap is unhealthy.
  }
  return { tickers: [], insiders: [], filings: [] };
}

/**
 * A ticker we are willing to publish.
 *
 * The trades table carries 23 values that are not tickers — "(CALX)",
 * "[NONE]", "$FEED", bare CIKs like "1314152". Their pages render, so they are
 * not broken links, but they are thin content and there is no reason to invite
 * a crawler to them.
 */
export function isPublishableTicker(t: string): boolean {
  return /^[A-Z][A-Z0-9]{0,5}(\.[A-Z]{1,2})?$/.test(t);
}

function xmlEscape(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

export function renderUrlset(entries: SitemapEntry[]): string {
  const urls = entries
    .map((e) => {
      const parts = [`    <loc>${xmlEscape(e.loc)}</loc>`];
      if (e.lastmod) parts.push(`    <lastmod>${e.lastmod}</lastmod>`);
      if (e.changefreq) parts.push(`    <changefreq>${e.changefreq}</changefreq>`);
      if (e.priority != null) parts.push(`    <priority>${e.priority.toFixed(1)}</priority>`);
      return `  <url>\n${parts.join("\n")}\n  </url>`;
    })
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls}\n</urlset>\n`;
}

export function renderIndex(sections: string[], lastmod: string): string {
  const items = sections
    .map(
      (s) =>
        `  <sitemap>\n    <loc>${BASE}/sitemaps/${s}.xml</loc>\n    <lastmod>${lastmod}</lastmod>\n  </sitemap>`,
    )
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${items}\n</sitemapindex>\n`;
}

export { BASE };


/**
 * The sector hub children, sourced live rather than hard-coded.
 *
 * A sector with no insider buying in the window has no page worth submitting,
 * and the slug is defined once on the API side (api/routers/sectors.slugify)
 * so a second copy here could drift into 404s in the sitemap.
 */
export async function getSectorPaths(): Promise<string[]> {
  try {
    const res = await fetch(`${API}/sectors`, { next: { revalidate: 3600 } });
    if (!res.ok) return [];
    const data = (await res.json()) as { sectors?: { slug: string }[] };
    return (data.sectors || [])
      .filter((s) => s && s.slug)
      .map((s) => `${BASE}/insider-buying/${s.slug}`);
  } catch {
    return [];
  }
}
