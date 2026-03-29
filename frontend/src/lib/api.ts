// Server-side: use internal Docker network URL if available (runtime env var)
// Client-side: use the public URL baked at build time
const isServer = typeof window === "undefined";
const API_BASE = (isServer && process.env.API_URL_INTERNAL) || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export async function fetchAPI<T>(endpoint: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(`${API_BASE}${endpoint}`);
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  }
  const res = await fetch(url.toString(), { next: { revalidate: 60 } });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

/**
 * Client-component version: fetches with Clerk session token.
 * Uses @clerk/nextjs useAuth() token — call this from client components.
 */
export async function fetchAPIWithAuth<T>(
  endpoint: string,
  token: string | null,
  params?: Record<string, string>,
): Promise<T> {
  const url = new URL(`${API_BASE}${endpoint}`);
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  }
  const headers: Record<string, string> = {};
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  const res = await fetch(url.toString(), { headers });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
