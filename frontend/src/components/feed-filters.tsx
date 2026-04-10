"use client";

import { useState, useCallback } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FilterState {
  trade_type: string;       // "" | "buy" | "sell"
  min_value: string;        // "" | "100000" | "500000" | "1000000"
  min_tier: string;         // "" | "2" | "3"
  min_grade: string;        // "" | "A" | "B"
  hide_routine: string;     // "" | "true"
  ticker: string;           // "" | "AAPL"
}

const EMPTY_FILTERS: FilterState = {
  trade_type: "",
  min_value: "",
  min_tier: "",
  min_grade: "",
  hide_routine: "",
  ticker: "",
};

// ---------------------------------------------------------------------------
// Presets
// ---------------------------------------------------------------------------

interface Preset {
  key: string;
  label: string;
  filters: Partial<FilterState>;
}

const PRESETS: Preset[] = [
  { key: "all", label: "All Filings", filters: {} },
  { key: "high_buys", label: "High Signal Buys", filters: { trade_type: "buy", min_grade: "B", min_value: "100000" } },
  { key: "sells", label: "Notable Sells", filters: { trade_type: "sell", hide_routine: "true" } },
  { key: "big", label: "$1M+ Trades", filters: { min_value: "1000000" } },
  { key: "top", label: "Top Insiders", filters: { min_tier: "2" } },
  { key: "a_grade", label: "A-Grade Insiders", filters: { min_grade: "A" } },
];

// ---------------------------------------------------------------------------
// Filter chip definitions
// ---------------------------------------------------------------------------

interface FilterOption {
  key: keyof FilterState;
  label: string;
  options: { value: string; label: string; color?: string }[];
}

const FILTER_OPTIONS: FilterOption[] = [
  {
    key: "trade_type",
    label: "Type",
    options: [
      { value: "buy", label: "Buys", color: "border-[#22C55E]/50 bg-[#22C55E]/10 text-[#22C55E]" },
      { value: "sell", label: "Sells", color: "border-[#EF4444]/50 bg-[#EF4444]/10 text-[#EF4444]" },
    ],
  },
  {
    key: "min_value",
    label: "Min Value",
    options: [
      { value: "100000", label: "$100K+" },
      { value: "500000", label: "$500K+" },
      { value: "1000000", label: "$1M+" },
      { value: "5000000", label: "$5M+" },
    ],
  },
  {
    key: "min_grade",
    label: "Insider Grade",
    options: [
      { value: "A", label: "A+ / A Insiders", color: "border-[#22C55E]/50 bg-[#22C55E]/10 text-[#22C55E]" },
      { value: "B", label: "B+ Insiders", color: "border-[#3B82F6]/50 bg-[#3B82F6]/10 text-[#3B82F6]" },
    ],
  },
  {
    key: "min_tier",
    label: "Insider Tier",
    options: [
      { value: "2", label: "Tier 2+" },
      { value: "3", label: "Tier 3+" },
    ],
  },
  {
    key: "hide_routine",
    label: "Noise Filter",
    options: [
      { value: "true", label: "Discretionary Only" },
    ],
  },
];

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface FeedFiltersProps {
  filters: FilterState;
  onChange: (filters: FilterState) => void;
  resultCount?: number;
}

export function FeedFilters({ filters, onChange, resultCount }: FeedFiltersProps) {
  const [showAddFilter, setShowAddFilter] = useState(false);
  const [activePreset, setActivePreset] = useState("all");

  const setFilter = useCallback(
    (key: keyof FilterState, value: string) => {
      const next = { ...filters, [key]: filters[key] === value ? "" : value };
      onChange(next);
      setActivePreset(""); // clear preset when manually filtering
    },
    [filters, onChange],
  );

  const removeFilter = useCallback(
    (key: keyof FilterState) => {
      const next = { ...filters, [key]: "" };
      onChange(next);
      setActivePreset("");
    },
    [filters, onChange],
  );

  const applyPreset = useCallback(
    (preset: Preset) => {
      const next = { ...EMPTY_FILTERS, ticker: filters.ticker, ...preset.filters };
      onChange(next);
      setActivePreset(preset.key);
      setShowAddFilter(false);
    },
    [filters.ticker, onChange],
  );

  const clearAll = useCallback(() => {
    onChange({ ...EMPTY_FILTERS, ticker: filters.ticker });
    setActivePreset("all");
    setShowAddFilter(false);
  }, [filters.ticker, onChange]);

  // Determine which filters are active (non-empty, excluding ticker)
  const activeFilters = FILTER_OPTIONS.filter((f) => filters[f.key] !== "");
  const inactiveFilters = FILTER_OPTIONS.filter((f) => filters[f.key] === "");
  const hasFilters = activeFilters.length > 0;

  // Find display label for an active filter value
  const getActiveLabel = (opt: FilterOption): string => {
    const val = filters[opt.key];
    const match = opt.options.find((o) => o.value === val);
    return match ? match.label : val;
  };

  return (
    <div className="space-y-3">
      {/* Row 1: Presets */}
      <div className="flex flex-wrap items-center gap-1.5">
        {PRESETS.map((p) => (
          <button
            key={p.key}
            onClick={() => applyPreset(p)}
            className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
              activePreset === p.key
                ? "bg-[#3B82F6]/15 text-[#3B82F6] border border-[#3B82F6]/40"
                : "text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26] border border-transparent"
            }`}
          >
            {p.label}
          </button>
        ))}
      </div>

      {/* Row 2: Active filter chips + Add Filter */}
      <div className="flex flex-wrap items-center gap-1.5">
        {/* Active filter chips */}
        {activeFilters.map((f) => {
          const match = f.options.find((o) => o.value === filters[f.key]);
          const chipColor = match?.color || "border-[#55556A]/50 bg-[#1A1A26] text-[#E8E8ED]";
          return (
            <span
              key={f.key}
              className={`inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium ${chipColor}`}
            >
              {getActiveLabel(f)}
              <button
                onClick={() => removeFilter(f.key)}
                className="ml-0.5 opacity-60 hover:opacity-100"
              >
                ×
              </button>
            </span>
          );
        })}

        {/* Ticker chip */}
        {filters.ticker && (
          <span className="inline-flex items-center gap-1.5 rounded-md border border-[#55556A]/50 bg-[#1A1A26] px-2.5 py-1 text-xs font-mono font-medium text-[#E8E8ED]">
            {filters.ticker}
            <button
              onClick={() => removeFilter("ticker")}
              className="ml-0.5 opacity-60 hover:opacity-100"
            >
              ×
            </button>
          </span>
        )}

        {/* Add Filter button */}
        {inactiveFilters.length > 0 && (
          <div className="relative">
            <button
              onClick={() => setShowAddFilter(!showAddFilter)}
              className="rounded-md border border-dashed border-[#2A2A3A] px-2.5 py-1 text-xs text-[#55556A] hover:text-[#8888A0] hover:border-[#55556A] transition-colors"
            >
              + Filter
            </button>

            {/* Dropdown */}
            {showAddFilter && (
              <div className="absolute left-0 top-full mt-1 z-50 w-56 rounded-lg border border-[#2A2A3A] bg-[#12121A] p-2 shadow-xl">
                {inactiveFilters.map((f) => (
                  <div key={f.key} className="mb-2 last:mb-0">
                    <div className="text-[10px] text-[#55556A] uppercase tracking-wider px-2 py-1">
                      {f.label}
                    </div>
                    <div className="flex flex-wrap gap-1 px-1">
                      {f.options.map((opt) => (
                        <button
                          key={opt.value}
                          onClick={() => {
                            setFilter(f.key, opt.value);
                            setShowAddFilter(false);
                          }}
                          className="rounded px-2 py-1 text-xs text-[#8888A0] hover:text-[#E8E8ED] hover:bg-[#1A1A26] transition-colors"
                        >
                          {opt.label}
                        </button>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Clear all */}
        {hasFilters && (
          <button
            onClick={clearAll}
            className="text-[10px] text-[#55556A] hover:text-[#8888A0] ml-1"
          >
            Clear all
          </button>
        )}

        {/* Result count */}
        {resultCount != null && (
          <span className="text-[10px] text-[#55556A] ml-auto">
            {resultCount.toLocaleString()} results
          </span>
        )}
      </div>
    </div>
  );
}

export { EMPTY_FILTERS };
export type { FilterState as FeedFilterState };
