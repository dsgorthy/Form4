import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import {
  formatResearchDate,
  getResearchByType,
  isValidResearchType,
  RESEARCH_TYPES,
  RESEARCH_TYPE_DESCRIPTIONS,
  RESEARCH_TYPE_LABELS,
  RESEARCH_TYPE_SINGULAR,
} from "@/lib/research";

const BASE_URL = "https://form4.app";

export async function generateStaticParams() {
  return RESEARCH_TYPES.map((type) => ({ type }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ type: string }>;
}): Promise<Metadata> {
  const { type } = await params;
  if (!isValidResearchType(type)) return {};
  const label = RESEARCH_TYPE_LABELS[type];
  const description = RESEARCH_TYPE_DESCRIPTIONS[type];
  return {
    title: `${label} — Form4 Research`,
    description,
    alternates: { canonical: `${BASE_URL}/research/${type}` },
    openGraph: {
      type: "website",
      url: `${BASE_URL}/research/${type}`,
      title: `${label} — Form4 Research`,
      description,
      siteName: "Form4",
      images: [{ url: "/og-image.png", width: 1200, height: 630 }],
    },
  };
}

export default async function ResearchTypeListingPage({
  params,
}: {
  params: Promise<{ type: string }>;
}) {
  const { type } = await params;
  if (!isValidResearchType(type)) notFound();

  const posts = getResearchByType(type);

  return (
    <div className="mx-auto max-w-4xl py-8 md:py-12">
      <nav className="mb-6 text-sm text-[#55556A]">
        <Link href="/research" className="hover:text-[#E8E8ED] transition-colors">
          Research
        </Link>
      </nav>

      <header className="mb-10">
        <div className="text-xs font-medium uppercase tracking-widest text-[#3B82F6] mb-2">
          {RESEARCH_TYPE_SINGULAR[type]}s
        </div>
        <h1 className="text-3xl md:text-4xl font-bold text-[#E8E8ED] tracking-tight">
          {RESEARCH_TYPE_LABELS[type]}
        </h1>
        <p className="mt-3 text-base text-[#8888A0] max-w-2xl">
          {RESEARCH_TYPE_DESCRIPTIONS[type]}
        </p>
      </header>

      {posts.length === 0 ? (
        <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-8 text-center">
          <p className="text-[#8888A0]">
            Nothing here yet — first {RESEARCH_TYPE_SINGULAR[type].toLowerCase()} coming soon.
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {posts.map((p) => (
            <Link
              key={p.slug}
              href={p.url}
              className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5 md:p-6 hover:border-[#3B82F6]/50 transition-colors"
            >
              {p.frontmatter.subtitle && (
                <div className="text-xs font-medium uppercase tracking-widest text-[#55556A] mb-1">
                  {p.frontmatter.subtitle}
                </div>
              )}
              <h2 className="text-xl md:text-2xl font-semibold text-[#E8E8ED] tracking-tight">
                {p.frontmatter.title}
              </h2>
              {p.frontmatter.summary && (
                <p className="mt-2 text-sm md:text-base text-[#8888A0] leading-relaxed line-clamp-3">
                  {p.frontmatter.summary}
                </p>
              )}
              <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-[#55556A]">
                <span>{p.frontmatter.author}</span>
                {p.frontmatter.date && (
                  <>
                    <span>·</span>
                    <time dateTime={p.frontmatter.date}>
                      {formatResearchDate(p.frontmatter.date)}
                    </time>
                  </>
                )}
                <span>·</span>
                <span>{p.readingTimeMinutes} min read</span>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
