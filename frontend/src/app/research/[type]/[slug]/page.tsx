import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import {
  formatResearchDate,
  getResearchByType,
  getResearchBySlug,
  getStaticParamsForResearch,
  isValidResearchType,
  RESEARCH_TYPE_LABELS,
  RESEARCH_TYPE_SINGULAR,
} from "@/lib/research";

const BASE_URL = "https://form4.app";

export async function generateStaticParams() {
  return getStaticParamsForResearch();
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ type: string; slug: string }>;
}): Promise<Metadata> {
  const { type, slug } = await params;
  if (!isValidResearchType(type)) return {};
  const post = await getResearchBySlug(type, slug);
  if (!post) return {};

  const url = `${BASE_URL}${post.url}`;
  const title = post.frontmatter.subtitle
    ? `${post.frontmatter.subtitle}: ${post.frontmatter.title}`
    : post.frontmatter.title;

  return {
    title,
    description: post.frontmatter.summary,
    alternates: { canonical: url },
    openGraph: {
      type: "article",
      url,
      title,
      description: post.frontmatter.summary,
      publishedTime: post.frontmatter.date,
      authors: [post.frontmatter.author],
      siteName: "Form4",
      images: [{ url: "/og-image.png", width: 1200, height: 630 }],
    },
    twitter: {
      card: "summary_large_image",
      title,
      description: post.frontmatter.summary,
      images: ["/og-image.png"],
    },
  };
}

function buildJsonLd(post: NonNullable<Awaited<ReturnType<typeof getResearchBySlug>>>) {
  const url = `${BASE_URL}${post.url}`;
  const schemaType = post.type === "whitepapers" ? "ScholarlyArticle" : "Article";
  return {
    "@context": "https://schema.org",
    "@type": schemaType,
    headline: post.frontmatter.title,
    description: post.frontmatter.summary,
    datePublished: post.frontmatter.date,
    dateModified: post.frontmatter.date,
    author: {
      "@type": "Organization",
      name: post.frontmatter.author,
      url: BASE_URL,
    },
    publisher: {
      "@type": "Organization",
      name: "Form4",
      url: BASE_URL,
      logo: {
        "@type": "ImageObject",
        url: `${BASE_URL}/logo.png`,
      },
    },
    mainEntityOfPage: { "@type": "WebPage", "@id": url },
    url,
    inLanguage: "en-US",
    ...(post.frontmatter.tags ? { keywords: post.frontmatter.tags.join(", ") } : {}),
  };
}

export default async function ResearchPostPage({
  params,
}: {
  params: Promise<{ type: string; slug: string }>;
}) {
  const { type, slug } = await params;
  if (!isValidResearchType(type)) notFound();
  const post = await getResearchBySlug(type, slug);
  if (!post) notFound();

  const related = getResearchByType(type)
    .filter((p) => p.slug !== post.slug)
    .slice(0, 3);
  const jsonLd = buildJsonLd(post);

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
      <article className="mx-auto max-w-3xl py-8 md:py-12">
        <nav className="mb-8 text-sm text-[#55556A]">
          <Link href="/research" className="hover:text-[#E8E8ED] transition-colors">
            Research
          </Link>
          <span className="mx-2">/</span>
          <Link
            href={`/research/${post.type}`}
            className="hover:text-[#E8E8ED] transition-colors"
          >
            {RESEARCH_TYPE_LABELS[post.type]}
          </Link>
        </nav>

        <header className="mb-10 pb-8 border-b border-[#2A2A3A]">
          <div className="text-xs font-medium uppercase tracking-widest text-[#3B82F6] mb-3">
            {RESEARCH_TYPE_SINGULAR[post.type]}
          </div>
          {post.frontmatter.subtitle && (
            <div className="text-sm font-medium text-[#8888A0] mb-2">
              {post.frontmatter.subtitle}
            </div>
          )}
          <h1 className="text-3xl md:text-4xl lg:text-5xl font-bold text-[#E8E8ED] tracking-tight leading-tight">
            {post.frontmatter.title}
          </h1>
          {post.frontmatter.summary && (
            <p className="mt-4 text-lg text-[#8888A0] leading-relaxed">
              {post.frontmatter.summary}
            </p>
          )}
          <div className="mt-6 flex flex-wrap items-center gap-x-4 gap-y-2 text-sm text-[#55556A]">
            <span className="text-[#8888A0]">{post.frontmatter.author}</span>
            {post.frontmatter.date && (
              <>
                <span>·</span>
                <time dateTime={post.frontmatter.date}>
                  {formatResearchDate(post.frontmatter.date)}
                </time>
              </>
            )}
            <span>·</span>
            <span>{post.readingTimeMinutes} min read</span>
          </div>
        </header>

        <div
          className="research-prose"
          dangerouslySetInnerHTML={{ __html: post.html }}
        />

        {related.length > 0 && (
          <section className="mt-16 pt-8 border-t border-[#2A2A3A]">
            <h2 className="text-sm font-medium uppercase tracking-widest text-[#8888A0] mb-4">
              More {RESEARCH_TYPE_LABELS[post.type]}
            </h2>
            <div className="space-y-3">
              {related.map((p) => (
                <Link
                  key={p.slug}
                  href={p.url}
                  className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-4 hover:border-[#3B82F6]/50 transition-colors"
                >
                  <div className="text-base font-semibold text-[#E8E8ED]">
                    {p.frontmatter.title}
                  </div>
                  {p.frontmatter.summary && (
                    <p className="mt-1 text-sm text-[#8888A0] line-clamp-2">
                      {p.frontmatter.summary}
                    </p>
                  )}
                  <div className="mt-2 text-xs text-[#55556A]">
                    {formatResearchDate(p.frontmatter.date)} · {p.readingTimeMinutes} min read
                  </div>
                </Link>
              ))}
            </div>
          </section>
        )}
      </article>
    </>
  );
}
