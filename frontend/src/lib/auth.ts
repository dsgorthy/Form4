import { auth } from "@clerk/nextjs/server";

// Server-side: use internal Docker network URL if available (runtime env var)
const API_BASE = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

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

  const res = await fetch(url.toString(), {
    headers,
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
