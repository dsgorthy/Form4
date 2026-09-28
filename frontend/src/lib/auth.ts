import { auth } from "@clerk/nextjs/server";

// Server-side: use internal Docker network URL if available (runtime env var)
const API_BASE = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

/**
 * The API answered, with a status we did not want.
 *
 * The MESSAGE FORMAT IS LOAD-BEARING and must stay `API error: <status>`: the
 * insider page renders its upgrade prompt on `e.message?.includes("403")`.
 * `status` is the field new code should read.
 */
export class ApiError extends Error {
  readonly status: number;
  constructor(status: number) {
    super(`API error: ${status}`);
    this.name = "ApiError";
    this.status = status;
  }
}

/** The API did not answer at all, or answered unparseably. NEVER a 404. */
export class ApiUnreachable extends Error {
  constructor(cause?: unknown) {
    super("API unreachable");
    this.name = "ApiUnreachable";
    this.cause = cause;
  }
}

/**
 * Is this error "there is no such entity", as opposed to "the backend broke"?
 *
 * The ONLY condition under which an indexable page may render a not-found
 * response. Every other failure has to become a 500 so the URL survives in the
 * index. Anything that is not a definite 404 is treated as a backend problem,
 * which is the safe direction: a spurious 500 costs one retry, a spurious
 * not-found costs the page.
 */
export function isEntityMissing(e: unknown): boolean {
  return e instanceof ApiError && e.status === 404;
}

/**
 * Server-component version of fetchAPI that injects the Clerk JWT.
 * Falls back to unauthenticated (free-tier) fetch if no session exists.
 */
export async function fetchAPIAuth<T>(
  endpoint: string,
  params?: Record<string, string>,
): Promise<T> {
  const url = new URL(`${API_BASE}${endpoint}`);
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  }

  const headers: Record<string, string> = {};

  try {
    const { getToken } = await auth();
    const token = await getToken();
    if (token) {
      headers["Authorization"] = `Bearer ${token}`;
    }
  } catch {
    // No auth context (e.g., during build) — continue unauthenticated
  }

  // WHY THE ERROR IS TYPED, AND WHY IT MATTERS MORE THAN IT LOOKS
  //
  // This used to be `if (!res.ok) throw new Error(...)`, one error for every
  // failure. Every indexable page wrapped the call in `catch { notFound() }`,
  // and the not-found route answers 200 with `<meta robots="noindex">`. So a
  // backend blip made the page tell a crawler: "200 OK, this page exists, do
  // not index it." Google complies at once and has no reason to retry, because
  // the fetch SUCCEEDED.
  //
  // That emptied the index. Impressions were climbing 149 -> ~3,000/day through
  // early September; on 09-17, the day after a 5.5h outage and during four days
  // of dropped connections from a 256-fd limit, they fell to 258 and then to 45.
  // Crawl backoff cannot do that — indexed pages keep ranking when crawling
  // slows. Only removal can. Google and Bing both went to zero while Applebot
  // and GPTBot kept pulling ~2,400 pages a day, because those two honour
  // noindex and the others do not rank anything.
  //
  // A 404 means "no such entity" and may render a not-found page. ANY other
  // failure must propagate, so Next serves a 500 — which Google retries and
  // which leaves the URL in the index. Full account:
  // the memory `project_2026-09-27_noindex_deindexed_the_site`.
  let res: Response;
  try {
    res = await fetch(url.toString(), {
      headers,
      cache: "no-store",
    });
  } catch (cause) {
    // Connection refused, reset, timeout. The backend is down, the entity is
    // not missing.
    throw new ApiUnreachable(cause);
  }
  if (!res.ok) throw new ApiError(res.status);
  try {
    return (await res.json()) as T;
  } catch (cause) {
    // 200 with a body we cannot parse is a broken upstream, not an absence.
    throw new ApiUnreachable(cause);
  }
}
