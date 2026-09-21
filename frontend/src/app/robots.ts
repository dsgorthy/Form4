import type { MetadataRoute } from "next";

// AhrefsBot ran 6,176 requests in one day against 200 KB server-rendered
// pages (2026-09-21 caddy log), SemrushBot 127; between them and the AI
// search crawlers the edge saw more bot than human traffic, and the ssh
// forward that fronts the site ran out of file descriptors under the
// bursts. Both honour Crawl-delay. A user-agent with its own block gets
// ONLY that block, so the disallows are repeated, not inherited.
const DISALLOW = ["/portfolio/", "/settings/", "/onboarding/", "/admin/", "/api/"];

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      { userAgent: "*", allow: "/", disallow: DISALLOW },
      { userAgent: "AhrefsBot", allow: "/", disallow: DISALLOW, crawlDelay: 10 },
      { userAgent: "SemrushBot", allow: "/", disallow: DISALLOW, crawlDelay: 10 },
    ],
    sitemap: "https://form4.app/sitemap.xml",
  };
}
