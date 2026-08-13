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
export function insiderPath(name: string | null | undefined, id: string | number): string {
  const slug = slugifyName(String(name ?? ""));
  return slug ? `/insider/${slug}-${id}` : `/insider/${id}`;
}
