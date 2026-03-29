"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { useRouter, useSearchParams } from "next/navigation";

interface TickerResult {
  ticker: string;
  company: string;
  trade_count: number;
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export function TickerInput() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const currentTicker = searchParams.get("ticker") || "AAPL";

  const [query, setQuery] = useState(currentTicker);
  const [suggestions, setSuggestions] = useState<TickerResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  const navigate = useCallback(
    (ticker: string) => {
      setQuery(ticker);
      setSuggestions([]);
      setIsOpen(false);
      setSelectedIdx(-1);
      router.push(`/screener?ticker=${encodeURIComponent(ticker.toUpperCase())}`);
    },
    [router],
  );

  const search = useCallback(async (q: string) => {
    if (q.length < 1) {
      setSuggestions([]);
      setIsOpen(false);
      return;
    }
    try {
      const res = await fetch(
        `${API_BASE}/search?q=${encodeURIComponent(q)}`,
      );
      if (res.ok) {
        const data = await res.json();
        setSuggestions(data.tickers?.slice(0, 8) || []);
        setIsOpen(true);
        setSelectedIdx(-1);
      }
    } catch {}
  }, []);

  const handleInput = (value: string) => {
    setQuery(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => search(value), 150);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIdx((prev) => Math.min(prev + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIdx((prev) => Math.max(prev - 1, -1));
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (selectedIdx >= 0 && suggestions[selectedIdx]) {
        navigate(suggestions[selectedIdx].ticker);
      } else if (query.trim()) {
        navigate(query.trim());
      }
    } else if (e.key === "Escape") {
      setIsOpen(false);
      inputRef.current?.blur();
    }
  };

  // Sync input when URL changes externally
  useEffect(() => {
    setQuery(currentTicker);
  }, [currentTicker]);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <div ref={containerRef} className="relative w-full max-w-sm">
      <div className="relative">
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => handleInput(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => {
            if (suggestions.length > 0) setIsOpen(true);
          }}
          placeholder="Enter ticker (e.g. AAPL, MSFT, NVDA)"
          className="w-full rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-4 py-2.5 pl-10 text-sm text-[#E8E8ED] placeholder:text-[#55556A] focus:border-[#3B82F6] focus:outline-none font-mono transition-colors"
        />
        <svg
          className="absolute left-3 top-1/2 -translate-y-1/2 text-[#55556A]"
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
        >
          <circle cx="11" cy="11" r="8" />
          <path d="M21 21l-4.35-4.35" />
        </svg>
        {currentTicker && (
          <button
            onClick={() => {
              setQuery("");
              setSuggestions([]);
              setIsOpen(false);
              router.push("/screener");
            }}
            className="absolute right-3 top-1/2 -translate-y-1/2 text-[#55556A] hover:text-[#8888A0] transition-colors"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M18 6L6 18M6 6l12 12" />
            </svg>
          </button>
        )}
      </div>

      {/* Autocomplete dropdown */}
      {isOpen && suggestions.length > 0 && (
        <div className="absolute top-full mt-1 left-0 right-0 rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-2xl z-50 overflow-hidden">
          {suggestions.map((s, i) => (
            <button
              key={s.ticker}
              onClick={() => navigate(s.ticker)}
              className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
                i === selectedIdx
                  ? "bg-[#1A1A26]"
                  : "hover:bg-[#1A1A26]/60"
              }`}
            >
              <span className="font-mono font-bold text-sm text-[#E8E8ED] w-16">
                {s.ticker}
              </span>
              <span className="text-xs text-[#8888A0] truncate flex-1">
                {s.company}
              </span>
              <span className="text-[10px] text-[#55556A] shrink-0">
                {s.trade_count} trades
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
