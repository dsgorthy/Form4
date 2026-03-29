"use client";

import { useEffect, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

interface SyncData {
  last_sync_at: string | null;
  latest_filed_at: string | null;
  latest_filing_date: string;
  filings_today: number;
  total_trades: number;
}

function formatSyncAge(ts: string): string {
  const now = new Date();
  const date = new Date(ts.replace(" ", "T") + (ts.includes("Z") ? "" : "Z"));
  const diffMs = now.getTime() - date.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDays = Math.floor(diffHr / 24);
  return `${diffDays}d ago`;
}

export function SyncStatus() {
  const [data, setData] = useState<SyncData | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    let mounted = true;

    async function fetchStatus() {
      try {
        const res = await fetch(`${API_BASE}/dashboard/sync-status`);
        if (!res.ok) throw new Error();
        const json = await res.json();
        if (mounted) {
          setData(json);
          setError(false);
        }
      } catch {
        if (mounted) setError(true);
      }
    }

    fetchStatus();
    const interval = setInterval(fetchStatus, 300_000); // refresh every 5min
    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, []);

  if (error) {
    return (
      <div className="flex items-center gap-1.5 text-[11px] text-[#55556A]">
        <span className="inline-block w-1.5 h-1.5 rounded-full bg-red-500" />
        Sync unavailable
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center gap-1.5 text-[11px] text-[#55556A]">
        <span className="inline-block w-1.5 h-1.5 rounded-full bg-[#55556A] animate-pulse" />
        Loading...
      </div>
    );
  }

  const syncTs = data.last_sync_at;
  const syncAge = syncTs ? formatSyncAge(syncTs) : null;

  // Green if synced within 10 min, yellow within 30 min, red otherwise
  let dotColor = "bg-[#55556A]";
  if (syncTs) {
    const diffMs = Date.now() - new Date(syncTs.replace(" ", "T") + "Z").getTime();
    const diffMin = diffMs / 60000;
    if (diffMin < 10) dotColor = "bg-emerald-500";
    else if (diffMin < 30) dotColor = "bg-yellow-500";
    else dotColor = "bg-red-500";
  }

  return (
    <div className="flex items-center gap-1.5 text-[11px] text-[#8888A0]">
      <span className={`inline-block w-1.5 h-1.5 rounded-full ${dotColor}`} />
      {syncAge ? (
        <>
          Synced {syncAge}
          {data.filings_today > 0 && (
            <span className="text-[#55556A]">
              &middot; {data.filings_today.toLocaleString()} filed today
            </span>
          )}
        </>
      ) : (
        "No sync data"
      )}
    </div>
  );
}
