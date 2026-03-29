"use client";

import { useState, useRef, useEffect, useCallback } from "react";

interface TickerResult {
  ticker: string;
  company: string;
  trade_count: number;
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

interface TickerAutocompleteProps {
  value: string;
  onChange: (value: string) => void;
  onSelect: (ticker: string) => void;
  placeholder?: string;
  className?: string;
}

export function TickerAutocomplete({
  value,
  onChange,
  onSelect,
  placeholder = "Add ticker (e.g. AAPL)",
  className = "",
}: TickerAutocompleteProps) {
  const [suggestions, setSuggestions] = useState<TickerResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const containerRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

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
    } catch {
      // silent
    }
  }, []);

  const handleInput = (val: string) => {
    onChange(val.toUpperCase());
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => search(val), 150);
  };

  const select = (ticker: string) => {
    onChange("");
    setSuggestions([]);
    setIsOpen(false);
    setSelectedIdx(-1);
    onSelect(ticker);
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
        select(suggestions[selectedIdx].ticker);
      } else if (value.trim()) {
        select(value.trim().toUpperCase());
      }
    } else if (e.key === "Escape") {
      setIsOpen(false);
    }
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

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <input
        type="text"
        value={value}
        onChange={(e) => handleInput(e.target.value)}
        onKeyDown={handleKeyDown}
        onFocus={() => {
          if (suggestions.length > 0) setIsOpen(true);
        }}
        placeholder={placeholder}
        maxLength={10}
        className="w-full rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-3 py-2 text-sm text-[#E8E8ED] placeholder-[#55556A] focus:outline-none focus:border-[#3B82F6] uppercase font-mono"
      />

      {isOpen && suggestions.length > 0 && (
        <div className="absolute top-full mt-1 left-0 right-0 rounded-lg border border-[#2A2A3A] bg-[#12121A] shadow-2xl z-50 overflow-hidden">
          {suggestions.map((s, i) => (
            <button
              key={s.ticker}
              onClick={() => select(s.ticker)}
              className={`w-full flex items-center gap-3 px-3 py-2 text-left transition-colors ${
                i === selectedIdx
                  ? "bg-[#1A1A26]"
                  : "hover:bg-[#1A1A26]/60"
              }`}
            >
              <span className="font-mono font-bold text-sm text-[#E8E8ED] w-14">
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
