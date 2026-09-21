import { SECTIONS, renderIndex } from "@/lib/sitemap-data";

export const revalidate = 3600;

// The index lists every section, unconditionally. It used to pull the whole
// 5 MB URL list just to decide how many filings chunks to advertise; an
// empty <urlset> is valid and costs a crawler one small fetch, while the
// data pull cost the API three queries per crawler per hour (2026-09-20).
export async function GET() {
  const lastmod = new Date().toISOString().slice(0, 10);

  return new Response(renderIndex(SECTIONS, lastmod), {
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=3600, s-maxage=3600",
    },
  });
}
