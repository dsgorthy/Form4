import type { Metadata } from "next";
import Link from "next/link";
import {
  formatResearchDate,
  getResearchByType,
  RESEARCH_TYPES,
  RESEARCH_TYPE_DESCRIPTIONS,
  RESEARCH_TYPE_LABELS,
  type ResearchPostMeta,
  type ResearchType,
} from "@/lib/research";

const BASE_URL = "https://form4.app";

export const metadata: Metadata = {
  title: "Research — Form4",
  description:
    "Thesis papers, portfolio updates, and research notes from Form4. Every strategy we trade is documented here — academic foundation, walk-forward validation, and live operational metrics.",
  alternates: { canonical: `${BASE_URL}/research` },
  openGraph: {
    type: "website",
    url: `${BASE_URL}/research`,
    title: "Research — Form4",
    description:
      "Thesis papers, portfolio updates, and research notes. Every strategy we trade is documented here.",
    siteName: "Form4",
    images: [{ url: "/og-image.png", width: 1200, height: 630 }],
  },
};

function FeaturedCard({ post }: { post: ResearchPostMeta }) {
  return (
    <Link
      href={post.url}
      className="block rounded-2xl border border-[#2A2A3A] bg-gradient-to-br from-[#12121A] to-[#0F0F18] p-6 md:p-8 hover:border-[#3B82F6]/50 transition-colors"
    >
      <div className="flex items-center gap-2 mb-3">
        <span className="text-xs font-medium uppercase tracking-widest text-[#3B82F6]">
          Featured · {RESEARCH_TYPE_LABELS[post.type].slice(0, -1)}
        </span>
      </div>
      {post.frontmatter.subtitle && (
        <div className="text-sm font-medium uppercase tracking-wider text-[#8888A0] mb-2">
          {post.frontmatter.subtitle}
        </div>
      )}
      <h2 className="text-2xl md:text-3xl font-bold text-[#E8E8ED] tracking-tight leading-tight">
        {post.frontmatter.title}
      </h2>
      {post.frontmatter.summary && (
        <p className="mt-3 text-base md:text-lg text-[#8888A0] leading-relaxed">
          {post.frontmatter.summary}
        </p>
      )}
      <div className="mt-5 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-[#55556A]">
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
    </Link>
  );
}

function PostCard({ post }: { post: ResearchPostMeta }) {
  return (
    <Link
      href={post.url}
      className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5 hover:border-[#3B82F6]/50 transition-colors"
    >
      {post.frontmatter.subtitle && (
        <div className="text-xs font-medium uppercase tracking-widest text-[#55556A] mb-1">
          {post.frontmatter.subtitle}
        </div>
      )}
      <h3 className="text-lg font-semibold text-[#E8E8ED] tracking-tight leading-snug">
        {post.frontmatter.title}
      </h3>
      {post.frontmatter.summary && (
        <p className="mt-2 text-sm text-[#8888A0] line-clamp-2">
          {post.frontmatter.summary}
        </p>
      )}
      <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-[#55556A]">
        {post.frontmatter.date && (
          <time dateTime={post.frontmatter.date}>
            {formatResearchDate(post.frontmatter.date)}
          </time>
        )}
        <span>·</span>
        <span>{post.readingTimeMinutes} min read</span>
      </div>
    </Link>
  );
}

function TypeSection({ type, posts }: { type: ResearchType; posts: ResearchPostMeta[] }) {
  const recent = posts.slice(0, 3);
  return (
    <section className="space-y-4">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight">
            {RESEARCH_TYPE_LABELS[type]}
          </h2>
          <p className="mt-1 text-sm text-[#8888A0] max-w-2xl">
            {RESEARCH_TYPE_DESCRIPTIONS[type]}
          </p>
        </div>
        {posts.length > 3 && (
          <Link
            href={`/research/${type}`}
            className="text-sm text-[#3B82F6] hover:text-[#60A5FA] transition-colors whitespace-nowrap"
          >
            View all {posts.length} →
          </Link>
        )}
      </div>

      {recent.length === 0 ? (
        <div className="rounded-lg border border-dashed border-[#2A2A3A] bg-[#0F0F18] p-6 text-center">
          <p className="text-sm text-[#55556A]">
            First {RESEARCH_TYPE_LABELS[type].toLowerCase().replace(/s$/, "")} coming soon.
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {recent.map((p) => (
            <PostCard key={p.slug} post={p} />
          ))}
        </div>
      )}
    </section>
  );
}

export default function ResearchHubPage() {
  const byType = Object.fromEntries(
    RESEARCH_TYPES.map((t) => [t, getResearchByType(t)])
  ) as Record<ResearchType, ResearchPostMeta[]>;

  const allPosts = RESEARCH_TYPES.flatMap((t) => byType[t]).sort((a, b) =>
    a.frontmatter.date < b.frontmatter.date ? 1 : -1
  );
  const featured = allPosts[0];

  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    name: "Research — Form4",
    description:
      "Thesis papers, portfolio updates, and research notes from Form4. Every strategy we trade is documented here.",
    url: `${BASE_URL}/research`,
    publisher: {
      "@type": "Organization",
      name: "Form4",
      url: BASE_URL,
    },
  };

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
      <div className="mx-auto max-w-6xl py-8 md:py-12 space-y-12">
        <header className="text-center max-w-3xl mx-auto">
          <div className="text-xs font-medium uppercase tracking-widest text-[#3B82F6] mb-3">
            Form4 Research
          </div>
          <h1 className="text-4xl md:text-5xl font-bold text-[#E8E8ED] tracking-tight leading-tight">
            The methodology behind every strategy we trade.
          </h1>
          <p className="mt-5 text-lg text-[#8888A0] leading-relaxed">
            Academic foundation, walk-forward validation, and live operational metrics.
            Every paper documents a strategy that runs against a real-money portfolio in public view.
          </p>
          <div className="mt-6 flex items-center justify-center gap-4 text-sm">
            <Link
              href="/research/feed.xml"
              className="text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
            >
              RSS feed
            </Link>
            <span className="text-[#2A2A3A]">·</span>
            <Link
              href="/portfolio"
              className="text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
            >
              Live portfolios
            </Link>
          </div>
        </header>

        {featured && <FeaturedCard post={featured} />}

        <section className="space-y-4">
          <div>
            <h2 className="text-2xl font-bold text-[#E8E8ED] tracking-tight">Methodology</h2>
            <p className="mt-1 text-sm text-[#8888A0] max-w-2xl">
              How we compute Insider Grades and Trade Grades — the two-tier system behind every signal.
            </p>
          </div>
          <Link
            href="/research/methodology"
            className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5 hover:border-[#3B82F6]/50 transition-colors"
          >
            <h3 className="text-lg font-semibold text-[#E8E8ED] tracking-tight">How Scoring Works</h3>
            <p className="mt-2 text-sm text-[#8888A0] leading-relaxed">
              Insider Grade (A+ to D) measures the person&apos;s historical track record.
              Trade Grade (1-5★) scores each transaction on 13 factors. Bayesian analysis across 196K+ insider trades.
            </p>
            <div className="mt-3 text-xs text-[#55556A]">
              Reference · How scoring works
            </div>
          </Link>
        </section>

        <div className="space-y-12">
          {RESEARCH_TYPES.map((type) => (
            <TypeSection key={type} type={type} posts={byType[type]} />
          ))}
        </div>
      </div>
    </>
  );
}
