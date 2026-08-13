/**
 * Insider URL construction.
 *
 * Insider pages are an SEO surface, so the legal name belongs in the path:
 *   /insider/jensen-huang-x7hq9r
 *
 * The trailing segment is the authoritative ID (sqid or CIK). The name is
 * decoration for search engines and humans and is never parsed server-side —
 * `identifier_from_slug` in api/id_encoding.py takes the last hyphen-delimited
 * segment.
 *
 * Chosen over a dedicated slug column because it needs no migration, no
 * uniqueness handling for duplicate names (the ID guarantees it), and no
 * redirect table: a bare `/insider/x7hq9r` still resolves, so existing links,
 * bookmarks and previously-indexed URLs keep working.
 */

/** Lowercase, ASCII, hyphen-separated. Diacritics folded, punctuation dropped. */
export function slugifyName(name: string): string {
  return (name || "")
    .normalize("NFKD")                  // split accents off their base letters
    .replace(/[̀-ͯ]/g, "")    // ...and drop them
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")        // punctuation, spaces, "/" -> hyphen
    .replace(/^-+|-+$/g, "")            // no leading/trailing hyphens
    .slice(0, 60)                       // keep URLs sane for very long entities
    .replace(/-+$/g, "");               // slice may have left a trailing hyphen
}

/**
 * Build the canonical path for an insider.
 *
 * Falls back to the bare ID when no usable name is available (entities with
 * blank names, gated rows where the name is redacted to bullets) — a working
 * ugly URL beats a broken pretty one.
 */
export function insiderPath(
  name: string | null | undefined,
  id: string | number,
  slug?: string | null,
): string {
  // Prefer the slug stored on the row. It is write-once and authoritative:
  // 99.3% of insiders hold a clean /insider/roger-s-penske, and the ~0.7%
  // with a name collision hold a disambiguated one. Deriving from the name
  // instead would silently disagree with what is stored the moment a name
  // is normalised, which is exactly how URLs rot.
  if (slug) return `/insider/${slug}`;

  // No stored slug (older API, or a row backfill has not reached): fall back
  // to name+id, which still resolves — the router tries the trailing segment.
  const derived = slugifyName(String(name ?? ""));
  return derived ? `/insider/${derived}-${id}` : `/insider/${id}`;
}

/**
 * Extract the authoritative ID from a slugged path segment.
 *
 * Mirrors `identifier_from_slug` in api/id_encoding.py. Needed anywhere the
 * route param is fed back into insiderPath() — otherwise the name gets
 * prefixed twice and you emit a canonical like
 * /insider/stephen-a-wynn-stephen-a-wynn-f2jrvh.
 */
export function idFromSlug(param: string): string {
  if (!param) return param;
  const i = param.lastIndexOf("-");
  return i === -1 ? param : param.slice(i + 1);
}
