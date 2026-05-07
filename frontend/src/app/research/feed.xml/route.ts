import { getAllResearch, RESEARCH_TYPE_SINGULAR } from "@/lib/research";

const BASE_URL = "https://form4.app";

function escapeXml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function toRfc822(iso: string): string {
  const d = iso ? new Date(iso) : new Date();
  if (Number.isNaN(d.getTime())) return new Date().toUTCString();
  return d.toUTCString();
}

export async function GET() {
  const posts = getAllResearch();
  const buildDate = new Date().toUTCString();

  const items = posts
    .map((p) => {
      const url = `${BASE_URL}${p.url}`;
      const category = RESEARCH_TYPE_SINGULAR[p.type];
      return `    <item>
      <title>${escapeXml(p.frontmatter.title)}</title>
      <link>${url}</link>
      <guid isPermaLink="true">${url}</guid>
      <pubDate>${toRfc822(p.frontmatter.date)}</pubDate>
      <description>${escapeXml(p.frontmatter.summary)}</description>
      <author>research@form4.app (${escapeXml(p.frontmatter.author)})</author>
      <category>${escapeXml(category)}</category>
    </item>`;
    })
    .join("\n");

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>Form4 Research</title>
    <link>${BASE_URL}/research</link>
    <atom:link href="${BASE_URL}/research/feed.xml" rel="self" type="application/rss+xml" />
    <description>Thesis papers, portfolio updates, and research notes from Form4. Every strategy we trade is documented here.</description>
    <language>en-US</language>
    <lastBuildDate>${buildDate}</lastBuildDate>
    <generator>Form4</generator>
${items}
  </channel>
</rss>`;

  return new Response(xml, {
    headers: {
      "Content-Type": "application/rss+xml; charset=utf-8",
      "Cache-Control": "public, max-age=3600, s-maxage=3600",
    },
  });
}
