import { getAllResearch, RESEARCH_TYPES } from "@/lib/research";
import { insiderPath } from "@/lib/insider-url";
import {
  BASE,
  CHUNK,
  SECTIONS,
  fetchSitemapData,
  isPublishableTicker,
  renderUrlset,
  type SitemapEntry,
} from "@/lib/sitemap-data";

export const revalidate = 3600;

const STATIC_PATHS = [
  "/", "/feed", "/leaderboard", "/clusters", "/congress",
  "/explore", "/pricing",
  "/research",
  ...RESEARCH_TYPES.map((t) => `/research/${t}`),
  "/privacy", "/terms", "/disclaimer", "/performance",
];

/**
 * One child sitemap per section, at /sitemaps/{section}.xml.
 *
 * Each section carries its own change cadence, which the previous single file
 * could not express: a filing is immutable once filed, a company page moves as
 * trades land, an insider page moves when that person trades. Previously 16 of
 * 52,327 URLs carried lastmod at all, so even a parseable document gave Google
 * almost nothing to prioritise a recrawl on.
 */
export async function GET(
  _req: Request,
  { params }: { params: Promise<{ section: string }> },
) {
  const { section: raw } = await params;
  const section = raw.replace(/\.xml$/, "");

  if (!SECTIONS.includes(section)) {
    return new Response("Not found", { status: 404 });
  }

  const today = new Date().toISOString().slice(0, 10);
  let entries: SitemapEntry[] = [];

  if (section === "core") {
    entries = [
      ...STATIC_PATHS.map((p) => ({
        loc: `${BASE}${p}`,
        lastmod: today,
        changefreq: p === "/" ? "daily" : "weekly",
        priority: p === "/" ? 1.0 : 0.7,
      })),
      ...getAllResearch().map((p) => ({
        loc: `${BASE}${p.url}`,
        lastmod: p.frontmatter.date
          ? new Date(p.frontmatter.date).toISOString().slice(0, 10)
          : today,
        changefreq: "monthly",
        priority: p.type === "whitepapers" ? 0.9 : 0.7,
      })),
    ];
  } else {
    const data = await fetchSitemapData();

    if (section === "companies") {
      // Drop the 23 values that are not tickers — "(CALX)", "[NONE]",
      // "$FEED", bare CIKs. Their pages render, so they are not broken links,
      // but they are thin and there is no reason to invite a crawler.
      entries = data.tickers.filter(isPublishableTicker).map((t) => ({
        loc: `${BASE}/company/${t}`,
        lastmod: today,
        changefreq: "daily",
        priority: 0.8,
      }));
    } else if (section === "insiders") {
      // insiderPath prefers the STORED slug, which is the canonical URL the
      // page itself declares. Deriving one from the name instead would publish
      // a URL that 301s to the real one — a redirect chain in the sitemap,
      // pointing at the surface this whole SEO push was for.
      entries = data.insiders
        .map((i) => (typeof i === "string" ? { id: i, name: "" } : i))
        .filter((i) => i && i.id)          // never emit /insider/undefined
        .map((i) => ({
          loc: `${BASE}${insiderPath(i.name, i.id, i.slug)}`,
          lastmod: today,
          changefreq: "weekly",
          priority: 0.6,
        }));
    } else if (section.startsWith("filings-")) {
      const n = Number(section.slice("filings-".length));
      entries = data.filings
        .slice(n * CHUNK, (n + 1) * CHUNK)
        .map((id) => ({
          loc: `${BASE}/filing/${id}`,
          // The one section where "never" is literally true: a filing's
          // disclosure does not change once filed.
          changefreq: "never",
          priority: 0.5,
        }));
    }
  }

  return new Response(renderUrlset(entries), {
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=3600, s-maxage=3600",
    },
  });
}
