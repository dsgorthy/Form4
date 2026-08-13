"use client";

/**
 * Entity search for /explore.
 *
 * Deliberately one input, not a company/insider toggle: a visitor types
 * "penske" or "AAPL" without first deciding which kind of thing that is.
 * Making them pick a mode before typing is friction that buys nothing.
 *
 * Shares the /search endpoint with the nav SearchBar, which already returns
 * tickers and insiders grouped. The difference is only where results go —
 * the nav sends you to the public pages (/company, /insider), this stays
 * inside the tool (/explore?ticker=, /explore?insider=).
 */

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";

interface TickerResult {
  ticker: string;
  company: string | null;
  trade_count?: number;
}

interface InsiderResult {
  insider_id: string;
  name: string;
  cik: string | null;
  slug?: string | null;
  best_pit_grade?: string | null;
  primary_title: string | null;
  primary_ticker: string | null;
}

interface SearchResponse {
  tickers: TickerResult[];
  insiders: InsiderResult[];
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export function ExploreSearch({ initial = "" }: { initial?: string }) {
  const router = useRouter();
  const [q, setQ] = useState(initial);
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (q.trim().length < 1) {
      setResults(null);
      return;
    }
    const t = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q.trim())}`);
        if (res.ok) {
          setResults(await res.json());
          setOpen(true);
        }
      } catch {
        setResults(null);
      } finally {
        setLoading(false);
      }
    }, 180);
    return () => clearTimeout(t);
  }, [q]);

  // Close on outside click so the panel doesn't sit over the page content.
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  const go = (path: string) => {
    setOpen(false);
    router.push(path);
  };

  // Enter with no selection: treat a short all-caps-ish token as a ticker,
  // which is what someone typing "NVDA" and hitting return expects.
  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key !== "Enter") return;
    const first = results?.tickers?.[0];
    const firstIns = results?.insiders?.[0];
    if (first) go(`/explore?ticker=${encodeURIComponent(first.ticker)}`);
    else if (firstIns) go(`/explore?insider=${encodeURIComponent(firstIns.slug || firstIns.insider_id)}`);
    else if (q.trim()) go(`/explore?ticker=${encodeURIComponent(q.trim().toUpperCase())}`);
  };

  const hasResults =
    results && (results.tickers.length > 0 || results.insiders.length > 0);

  return (
    <div ref={boxRef} className="relative w-full max-w-xl">
      <input
        value={q}
        onChange={(e) => setQ(e.target.value)}
        onFocus={() => results && setOpen(true)}
        onKeyDown={onKeyDown}
        placeholder="Search a company or insider — e.g. NVDA, Roger S. Penske"
        className="w-full rounded-lg border border-[#2A2A3A] bg-[#12121A] px-4 py-2.5 text-sm text-[#E8E8ED] placeholder:text-[#55556A] focus:border-[#3B82F6]/60 focus:outline-none"
      />
      {loading && (
        <div className="absolute right-3 top-3 text-xs text-[#55556A]">…</div>
      )}

      {open && hasResults && (
        <div className="absolute left-0 right-0 top-full z-50 mt-1 overflow-hidden rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-xl">
          {results!.tickers.length > 0 && (
            <div className="py-1">
              <div className="px-3 py-1 text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
                Companies
              </div>
              {results!.tickers.map((t) => (
                <button
                  key={t.ticker}
                  onClick={() => go(`/explore?ticker=${encodeURIComponent(t.ticker)}`)}
                  className="flex w-full items-center gap-3 px-3 py-2 text-left transition-colors hover:bg-[#1A1A26]/60"
                >
                  <span className="font-mono text-sm text-[#E8E8ED]">{t.ticker}</span>
                  <span className="truncate text-xs text-[#8888A0]">{t.company}</span>
                </button>
              ))}
            </div>
          )}
          {results!.insiders.length > 0 && (
            <div className="border-t border-[#2A2A3A] py-1">
              <div className="px-3 py-1 text-[10px] font-semibold uppercase tracking-widest text-[#55556A]">
                Insiders
              </div>
              {results!.insiders.map((ins) => (
                <button
                  key={ins.insider_id}
                  onClick={() =>
                    go(`/explore?insider=${encodeURIComponent(ins.slug || ins.insider_id)}`)
                  }
                  className="flex w-full items-center gap-3 px-3 py-2 text-left transition-colors hover:bg-[#1A1A26]/60"
                >
                  <span className="truncate text-sm text-[#E8E8ED]">{ins.name}</span>
                  {ins.primary_title && (
                    <span className="truncate text-xs text-[#8888A0]">{ins.primary_title}</span>
                  )}
                  {ins.primary_ticker && (
                    <span className="ml-auto font-mono text-xs text-[#55556A]">
                      {ins.primary_ticker}
                    </span>
                  )}
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
