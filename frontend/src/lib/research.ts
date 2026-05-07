import fs from "node:fs";
import path from "node:path";
import matter from "gray-matter";
import { remark } from "remark";
import remarkGfm from "remark-gfm";
import remarkHtml from "remark-html";

const CONTENT_ROOT = path.join(process.cwd(), "content", "research");

export type ResearchType = "whitepapers" | "portfolio-updates" | "notes";

export const RESEARCH_TYPES: ResearchType[] = ["whitepapers", "portfolio-updates", "notes"];

export const RESEARCH_TYPE_LABELS: Record<ResearchType, string> = {
  whitepapers: "Whitepapers",
  "portfolio-updates": "Portfolio Updates",
  notes: "Notes",
};

export const RESEARCH_TYPE_SINGULAR: Record<ResearchType, string> = {
  whitepapers: "Whitepaper",
  "portfolio-updates": "Portfolio Update",
  notes: "Note",
};

export const RESEARCH_TYPE_DESCRIPTIONS: Record<ResearchType, string> = {
  whitepapers:
    "Long-form thesis papers documenting strategies that work. Each paper covers academic foundation, validation, risk factors, and live operational metrics.",
  "portfolio-updates":
    "Per-trade and monthly recaps from our live paper portfolios. What we entered, why, what we exited, and what we'd do differently.",
  notes:
    "Short observations, single-chart pieces, and market commentary that don't yet warrant a full paper.",
};

export interface ResearchFrontmatter {
  title: string;
  subtitle?: string;
  slug: string;
  type: string;
  date: string;
  author: string;
  summary: string;
  tags?: string[];
}

export interface ResearchPostMeta {
  type: ResearchType;
  slug: string;
  frontmatter: ResearchFrontmatter;
  readingTimeMinutes: number;
  url: string;
}

export interface ResearchPost extends ResearchPostMeta {
  content: string;
  html: string;
}

export function isValidResearchType(t: string): t is ResearchType {
  return RESEARCH_TYPES.includes(t as ResearchType);
}

function listFilesInType(type: ResearchType): string[] {
  const dir = path.join(CONTENT_ROOT, type);
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).filter((f) => f.endsWith(".md"));
}

async function renderMarkdown(content: string): Promise<string> {
  const file = await remark().use(remarkGfm).use(remarkHtml).process(content);
  return String(file);
}

function estimateReadingMinutes(text: string): number {
  const words = text.trim().split(/\s+/).length;
  return Math.max(1, Math.round(words / 220));
}

function toIsoDate(v: unknown): string {
  if (!v) return "";
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  return String(v);
}

function normalizeFrontmatter(
  data: Record<string, unknown>,
  type: ResearchType,
  slug: string
): ResearchFrontmatter {
  return {
    title: String(data.title ?? slug),
    subtitle: data.subtitle ? String(data.subtitle) : undefined,
    slug: String(data.slug ?? slug),
    type: String(data.type ?? type),
    date: toIsoDate(data.date),
    author: String(data.author ?? "Form4 Research"),
    summary: String(data.summary ?? ""),
    tags: Array.isArray(data.tags) ? data.tags.map(String) : undefined,
  };
}

function buildMeta(type: ResearchType, slug: string, raw: string): ResearchPostMeta {
  const parsed = matter(raw);
  const frontmatter = normalizeFrontmatter(parsed.data, type, slug);
  return {
    type,
    slug,
    frontmatter,
    readingTimeMinutes: estimateReadingMinutes(parsed.content),
    url: `/research/${type}/${slug}`,
  };
}

export async function getResearchBySlug(
  type: ResearchType,
  slug: string
): Promise<ResearchPost | null> {
  const filepath = path.join(CONTENT_ROOT, type, `${slug}.md`);
  if (!fs.existsSync(filepath)) return null;
  const raw = fs.readFileSync(filepath, "utf-8");
  const parsed = matter(raw);
  const frontmatter = normalizeFrontmatter(parsed.data, type, slug);
  const html = await renderMarkdown(parsed.content);
  return {
    type,
    slug,
    frontmatter,
    content: parsed.content,
    html,
    readingTimeMinutes: estimateReadingMinutes(parsed.content),
    url: `/research/${type}/${slug}`,
  };
}

export function getResearchByType(type: ResearchType): ResearchPostMeta[] {
  return listFilesInType(type)
    .map((f) => {
      const slug = f.replace(/\.md$/, "");
      const filepath = path.join(CONTENT_ROOT, type, f);
      const raw = fs.readFileSync(filepath, "utf-8");
      return buildMeta(type, slug, raw);
    })
    .sort((a, b) => (a.frontmatter.date < b.frontmatter.date ? 1 : -1));
}

export function getAllResearch(): ResearchPostMeta[] {
  return RESEARCH_TYPES.flatMap((t) => getResearchByType(t)).sort((a, b) =>
    a.frontmatter.date < b.frontmatter.date ? 1 : -1
  );
}

export function getStaticParamsForResearch(): { type: string; slug: string }[] {
  return RESEARCH_TYPES.flatMap((type) =>
    listFilesInType(type).map((f) => ({ type, slug: f.replace(/\.md$/, "") }))
  );
}

export function formatResearchDate(iso: string): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}
