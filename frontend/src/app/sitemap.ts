import type { MetadataRoute } from "next";

const BASE = "https://form4.app";
const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

// Regenerate sitemap every hour
export const revalidate = 3600;

const STATIC_PATHS = [
  "/", "/feed", "/leaderboard", "/signals", "/clusters", "/congress",
  "/screener", "/scoring", "/pricing", "/sells", "/convergence",
  "/inflections", "/privacy", "/terms", "/disclaimer",
];

interface SitemapData {
  tickers: string[];
  insiders: string[];
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

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const now = new Date();
  const data = await fetchSitemapData();

  // Static pages
  const staticPages: MetadataRoute.Sitemap = STATIC_PATHS.map((path) => ({
    url: `${BASE}${path}`,
    lastModified: now,
    changeFrequency: path === "/" ? "daily" as const : "weekly" as const,
    priority: path === "/" ? 1.0 : 0.7,
  }));

  // Company pages (~5K)
  const companyPages: MetadataRoute.Sitemap = data.tickers.map((ticker) => ({
    url: `${BASE}/company/${ticker}`,
    changeFrequency: "weekly" as const,
    priority: 0.8,
  }));

  // Insider pages (top 10K by trade count)
  const insiderPages: MetadataRoute.Sitemap = data.insiders.map((id) => ({
    url: `${BASE}/insider/${id}`,
    changeFrequency: "monthly" as const,
    priority: 0.6,
  }));

  // Recent filing pages (last 90 days)
  const filingPages: MetadataRoute.Sitemap = data.filings.map((id) => ({
    url: `${BASE}/filing/${id}`,
    changeFrequency: "never" as const,
    priority: 0.5,
  }));

  return [...staticPages, ...companyPages, ...insiderPages, ...filingPages];
}
