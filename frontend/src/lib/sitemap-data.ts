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

// Filings dominate and grow daily (32,935 of the 52,327). Companies and
// insiders are bounded by construction — one row per ticker, top-N insiders —
// so only this section scales by adding files.
export const FILING_CHUNKS = 4;

export const SECTIONS = [
  "core",
  "companies",
  "insiders",
  ...Array.from({ length: FILING_CHUNKS }, (_, i) => `filings-${i}`),
];

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
      `${API}/sitemap/urls?limit_insiders=45000&filing_days=90`,
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
