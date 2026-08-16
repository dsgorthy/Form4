import type { MetadataRoute } from "next";
import { getAllResearch, RESEARCH_TYPES } from "@/lib/research";
import { insiderPath } from "@/lib/insider-url";

const BASE = "https://form4.app";
const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

// Regenerate sitemap every hour
export const revalidate = 3600;

/**
 * The sitemap protocol caps a single <urlset> at 50,000 URLs. This site emitted
 * ONE file containing all of them, and crossed that ceiling as filing pages
 * accumulated — 52,327 URLs when measured on 2026-08-15. Google does not
 * truncate an oversized sitemap, it rejects the file, so from the moment we
 * crossed the line nothing in it was being processed. Last successful index:
 * 2026-04-03.
 *
 * Splitting by section keeps every child well under the cap and, more
 * usefully, lets each section carry its own change cadence: filings never
 * change once filed, companies move weekly, insider pages move when the person
 * trades. One undifferentiated file could not express any of that.
 *
 * Filings are chunked because they are the section that grows without bound —
 * 32,935 of the 52,327. Everything else is naturally bounded (one row per
 * ticker, top-N insiders), so only this one needs to scale by adding files.
 */
const CHUNK = 20000;

// Filings dominate and grow daily; the others are bounded by construction.
// Deliberately generous: 90 days of filings is ~33k today, and headroom here
// is cheaper than rediscovering the 50k ceiling in production a second time.
const FILING_CHUNKS = 4;

const STATIC_PATHS = [
  "/", "/feed", "/leaderboard", "/clusters", "/congress",
  "/explore", "/pricing",
  "/research",
  ...RESEARCH_TYPES.map((t) => `/research/${t}`),
  "/privacy", "/terms", "/disclaimer",
];

interface SitemapData {
  tickers: string[];
  // The API returns {id, name} so URLs can carry the insider's legal name.
  // Older deploys returned bare id strings, and this file is statically
  // generated at BUILD time — so during a rolling deploy the new frontend
  // can render against the old API and emit /insider/undefined straight into
  // Google. Accept both shapes so a version skew degrades to an ugly-but-
  // valid URL instead of a poisoned sitemap.
  insiders: ({ id: string; name: string; slug?: string } | string)[];
  filings: string[];
}

async function fetchSitemapData(): Promise<SitemapData> {
  try {
    const resp = await fetch(`${API}/sitemap/urls?limit_insiders=10000&filing_days=90`, {
      next: { revalidate: 86400 },
    });
    if (resp.ok) return await resp.json();
  } catch {}
  return { tickers: [], insiders: [], filings: [] };
}

/**
 * One child sitemap per section. Next serves these at /sitemap/{id}.xml and
 * generates the index at /sitemap.xml, which is the URL robots.txt already
 * points at — so no robots change is needed and the existing submission in
 * Search Console keeps working.
 */
// Next passes `id` through as the value given here and expects it to be a
// number — string ids arrive as something else entirely and blew up the
// prerender with "e.startsWith is not a function". So the sections are indexed
// and named in one place instead.
const SECTIONS = [
  "core",
  "companies",
  "insiders",
  ...Array.from({ length: FILING_CHUNKS }, (_, i) => `filings-${i}`),
] as const;

export async function generateSitemaps() {
  return SECTIONS.map((_, id) => ({ id }));
}

export default async function sitemap(
  { id }: { id: number },
): Promise<MetadataRoute.Sitemap> {
  const now = new Date();
  const section: string = SECTIONS[id] ?? "core";

  if (section === "core") {
    const staticPages: MetadataRoute.Sitemap = STATIC_PATHS.map((path) => ({
      url: `${BASE}${path}`,
      lastModified: now,
      changeFrequency: path === "/" ? "daily" as const : "weekly" as const,
      priority: path === "/" ? 1.0 : 0.7,
    }));
    const researchPages: MetadataRoute.Sitemap = getAllResearch().map((p) => ({
      url: `${BASE}${p.url}`,
      lastModified: p.frontmatter.date ? new Date(p.frontmatter.date) : now,
      changeFrequency: "monthly" as const,
      priority: p.type === "whitepapers" ? 0.9 : 0.7,
    }));
    return [...staticPages, ...researchPages];
  }

  const data = await fetchSitemapData();

  if (section === "companies") {
    return data.tickers.map((ticker) => ({
      url: `${BASE}/company/${ticker}`,
      lastModified: now,
      changeFrequency: "weekly" as const,
      priority: 0.8,
    }));
  }

  if (section === "insiders") {
    return data.insiders
      .map((ins) => (typeof ins === "string" ? { id: ins, name: "" } : ins))
      .filter((ins) => ins && ins.id)          // never emit /insider/undefined
      .map((ins) => ({
        url: `${BASE}${insiderPath(ins.name, ins.id, ins.slug)}`,
        lastModified: now,
        changeFrequency: "weekly" as const,
        priority: 0.6,
      }));
  }

  if (section.startsWith("filings-")) {
    const n = Number(section.slice("filings-".length));
    const slice = data.filings.slice(n * CHUNK, (n + 1) * CHUNK);
    return slice.map((fid) => ({
      url: `${BASE}/filing/${fid}`,
      // A filing is immutable once filed — the disclosure does not change —
      // so this is the one section where "never" is literally true.
      changeFrequency: "never" as const,
      priority: 0.5,
    }));
  }

  return [];
}
