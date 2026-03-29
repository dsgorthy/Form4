"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import { TierBadge } from "@/components/ui/tier-badge";
import { formatCurrency } from "@/lib/format";

interface TickerResult {
  ticker: string;
  company: string;
  trade_count: number;
  total_value: number;
}

interface InsiderResult {
  insider_id: string;
  name: string;
  cik: string | null;
  score: number | null;
  score_tier: number | null;
  primary_title: string | null;
  primary_ticker: string | null;
}

interface SearchResponse {
  tickers: TickerResult[];
  insiders: InsiderResult[];
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export function SearchBar() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const router = useRouter();
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  const search = useCallback(async (q: string) => {
    if (q.length < 1) {
      setResults(null);
      setIsOpen(false);
      return;
    }
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/search?q=${encodeURIComponent(q)}`);
      if (res.ok) {
        const data: SearchResponse = await res.json();
        setResults(data);
        setIsOpen(true);
      }
    } catch {
      // Silently fail
    } finally {
      setLoading(false);
    }
  }, []);

  const handleInput = (value: string) => {
    setQuery(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => search(value), 200);
  };

  const navigate = (path: string) => {
    setIsOpen(false);
    setQuery("");
    router.push(path);
  };

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // Keyboard shortcut: Cmd+K or Ctrl+K
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        inputRef.current?.focus();
      }
      if (e.key === "Escape") {
        setIsOpen(false);
        inputRef.current?.blur();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, []);

  const hasResults = results && (results.tickers.length > 0 || results.insiders.length > 0);

  return (
    <div ref={containerRef} className="relative">
      <div className="relative">
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => handleInput(e.target.value)}
          onFocus={() => {
            if (hasResults) setIsOpen(true);
          }}
          placeholder="Search tickers, insiders..."
          className="w-full md:w-48 rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-3 py-1.5 pl-8 text-sm text-[#E8E8ED] placeholder:text-[#55556A] focus:border-[#3B82F6] focus:outline-none transition-colors"
        />
        <svg
          className="absolute left-2.5 top-1/2 -translate-y-1/2 text-[#55556A]"
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
        >
          <circle cx="11" cy="11" r="8" />
          <path d="M21 21l-4.35-4.35" />
        </svg>
        {!query && (
          <kbd className="absolute right-2 top-1/2 -translate-y-1/2 rounded border border-[#2A2A3A] bg-[#12121A] px-1.5 py-0.5 text-[10px] text-[#55556A] font-mono">
            {"\u2318"}K
          </kbd>
        )}
        {loading && (
          <div className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 border border-[#3B82F6] border-t-transparent rounded-full animate-spin" />
        )}
      </div>

      {/* Dropdown */}
      {isOpen && hasResults && (
        <div className="absolute top-full mt-1 left-0 right-0 w-full md:w-80 rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-2xl z-50 overflow-hidden">
          {/* Tickers */}
          {results!.tickers.length > 0 && (
            <div>
              <div className="px-3 py-2 text-[10px] font-semibold uppercase tracking-widest text-[#55556A] bg-[#1A1A26]/50">
                Companies
              </div>
              {results!.tickers.map((t) => (
                <button
                  key={t.ticker}
                  onClick={() => navigate(`/company/${t.ticker}`)}
                  className="w-full flex items-center gap-3 px-3 py-2 hover:bg-[#1A1A26]/60 transition-colors text-left"
                >
                  <span className="font-mono font-bold text-sm text-[#E8E8ED] w-14">
                    {t.ticker}
                  </span>
                  <span className="text-xs text-[#8888A0] truncate flex-1">
                    {t.company}
                  </span>
                  <span className="text-[10px] text-[#55556A] shrink-0">
                    {t.trade_count} trades
                  </span>
                </button>
              ))}
            </div>
          )}

          {/* Insiders */}
          {results!.insiders.length > 0 && (
            <div>
              <div className="px-3 py-2 text-[10px] font-semibold uppercase tracking-widest text-[#55556A] bg-[#1A1A26]/50">
                Insiders
              </div>
              {results!.insiders.map((ins) => (
                <button
                  key={ins.insider_id}
                  onClick={() => navigate(`/insider/${ins.cik || ins.insider_id}`)}
                  className="w-full flex items-center gap-3 px-3 py-2 hover:bg-[#1A1A26]/60 transition-colors text-left"
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-[#E8E8ED] truncate">{ins.name}</span>
                      {ins.score_tier != null && <TierBadge tier={ins.score_tier} />}
                    </div>
                    {ins.primary_title && (
                      <div className="text-[10px] text-[#55556A] truncate">
                        {ins.primary_title}
                        {ins.primary_ticker && ` at ${ins.primary_ticker}`}
                      </div>
                    )}
                  </div>
                  {ins.score != null && (
                    <span className="text-xs font-mono text-[#8888A0] shrink-0">
                      {ins.score.toFixed(2)}
                    </span>
                  )}
                </button>
              ))}
            </div>
          )}
        </div>
      )}

      {/* No results */}
      {isOpen && results && !hasResults && query.length > 0 && (
        <div className="absolute top-full mt-1 left-0 right-0 w-full md:w-80 rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-2xl z-50 p-4 text-center text-sm text-[#55556A]">
          No results for &ldquo;{query}&rdquo;
        </div>
      )}
    </div>
  );
}
