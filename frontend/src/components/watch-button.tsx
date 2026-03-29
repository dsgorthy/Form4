"use client";

import { useState, useEffect, useCallback } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { isPro } from "@/lib/subscription";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export function WatchButton({ ticker }: { ticker: string }) {
  const { getToken } = useAuth();
  const { user } = useUser();
  const [watched, setWatched] = useState(false);
  const [loading, setLoading] = useState(false);

  const userIsPro = isPro(user);

  const checkWatched = useCallback(async () => {
    try {
      const token = await getToken();
      if (!token) return;
      const res = await fetch(`${apiBase}/notifications/watchlist`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.ok) {
        const data = await res.json();
        setWatched(data.items?.some((w: { ticker: string }) => w.ticker === ticker.toUpperCase()) ?? false);
      }
    } catch {
      // silent
    }
  }, [getToken, ticker]);

  useEffect(() => {
    if (userIsPro) checkWatched();
  }, [userIsPro, checkWatched]);

  if (!userIsPro) return null;

  async function toggle() {
    setLoading(true);
    try {
      const token = await getToken();
      if (!token) return;
      if (watched) {
        await fetch(`${apiBase}/notifications/watchlist/${ticker.toUpperCase()}`, {
          method: "DELETE",
          headers: { Authorization: `Bearer ${token}` },
        });
        setWatched(false);
      } else {
        const res = await fetch(`${apiBase}/notifications/watchlist`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({ ticker: ticker.toUpperCase() }),
        });
        if (res.ok) setWatched(true);
      }
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  }

  return (
    <button
      onClick={toggle}
      disabled={loading}
      title={watched ? "Remove from watchlist" : "Add to watchlist"}
      className={`inline-flex items-center gap-1 rounded-md border px-2 py-1 text-xs font-medium transition-colors ${
        watched
          ? "border-[#F59E0B]/30 bg-[#F59E0B]/10 text-[#F59E0B]"
          : "border-[#2A2A3A] text-[#55556A] hover:text-[#E8E8ED] hover:border-[#55556A]"
      } ${loading ? "opacity-50" : ""}`}
    >
      <svg
        className="w-3.5 h-3.5"
        fill={watched ? "currentColor" : "none"}
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M11.049 2.927c.3-.921 1.603-.921 1.902 0l1.519 4.674a1 1 0 00.95.69h4.915c.969 0 1.371 1.24.588 1.81l-3.976 2.888a1 1 0 00-.363 1.118l1.518 4.674c.3.922-.755 1.688-1.538 1.118l-3.976-2.888a1 1 0 00-1.176 0l-3.976 2.888c-.783.57-1.838-.197-1.538-1.118l1.518-4.674a1 1 0 00-.363-1.118l-3.976-2.888c-.784-.57-.38-1.81.588-1.81h4.914a1 1 0 00.951-.69l1.519-4.674z"
        />
      </svg>
      {watched ? "Watching" : "Watch"}
    </button>
  );
}
