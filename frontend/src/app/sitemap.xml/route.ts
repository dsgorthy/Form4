import { SECTIONS, fetchSitemapData, renderIndex } from "@/lib/sitemap-data";

/**
 * The sitemap index. robots.txt already declares this exact URL and it is what
 * is submitted in Search Console, so the split is invisible from outside —
 * only the contents change, from one oversized <urlset> to a list of children.
 */
export const revalidate = 3600;

export async function GET() {
  const lastmod = new Date().toISOString().slice(0, 10);

  // Trim filing chunks that are empty: the chunk count is sized for growth, so
  // the last ones hold nothing today, and Google flags an empty <urlset> as a
  // warning and refetches it every crawl.
  //
  // Fails OPEN. If the fetch returns nothing we cannot tell an empty chunk
  // from an unavailable API, and guessing "empty" would quietly drop 33,000
  // filing URLs from the index — the same silent-shrink failure that made this
  // sitemap useless in the first place. A warning about an empty child is
  // recoverable; a sitemap that lost two thirds of the site is not.
  const data = await fetchSitemapData();
  const sections =
    data.filings.length === 0
      ? SECTIONS
      : SECTIONS.filter((s) => {
          if (!s.startsWith("filings-")) return true;
          const n = Number(s.slice("filings-".length));
          return data.filings.length > n * 20000;
        });

  return new Response(renderIndex(sections, lastmod), {
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=3600, s-maxage=3600",
    },
  });
}
