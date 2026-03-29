"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatCurrency, tierLabel } from "@/lib/format";

interface TradeMarker {
  date: string;
  price: number;
  trade_type: "buy" | "sell";
  insider_name: string;
  value: number;
  score_tier: number | null;
  gated?: boolean;
}

interface Candle {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
}

interface TradingGap {
  start: string;
  end: string;
  days: number;
  price_before: number;
  price_after: number;
}

interface ChartData {
  candles: Candle[];
  trades: TradeMarker[];
  gaps?: TradingGap[];
  gated?: boolean;
}

interface InsiderTradeChartProps {
  ticker: string;
}

interface Filters {
  trade_type: "buy" | "sell" | null;
  csuite: boolean;
  director: boolean;
  owner_10pct: boolean;
  top_performer: boolean;
}

const DEFAULT_FILTERS: Filters = {
  trade_type: null,
  csuite: false,
  director: false,
  owner_10pct: false,
  top_performer: false,
};

function buildQueryString(filters: Filters): string {
  const params = new URLSearchParams();
  if (filters.trade_type) params.set("trade_type", filters.trade_type);
  if (filters.csuite) params.set("csuite", "true");
  if (filters.director) params.set("director", "true");
  if (filters.owner_10pct) params.set("owner_10pct", "true");
  if (filters.top_performer) params.set("top_performer", "true");
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

function FilterButton({
  active,
  onClick,
  color,
  children,
}: {
  active: boolean;
  onClick: () => void;
  color?: string;
  children: React.ReactNode;
}) {
  const activeColor = color || "#3B82F6";
  return (
    <button
      onClick={onClick}
      className={`rounded-md px-2.5 py-1 text-[11px] font-medium border transition-colors ${
        active
          ? `border-[${activeColor}]/50 bg-[${activeColor}]/10 text-[${activeColor}]`
          : "border-[#2A2A3A] text-[#55556A] hover:text-[#8888A0]"
      }`}
      style={
        active
          ? {
              borderColor: `${activeColor}80`,
              backgroundColor: `${activeColor}1A`,
              color: activeColor,
            }
          : undefined
      }
    >
      {children}
    </button>
  );
}

export function InsiderTradeChart({ ticker }: InsiderTradeChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof import("lightweight-charts").createChart> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ChartData | null>(null);
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);

  const { getToken } = useAuth();
  const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

  // Fetch data whenever ticker or filters change
  useEffect(() => {
    setLoading(true);
    setError(null);
    const qs = buildQueryString(filters);
    getToken().then((token) => {
      const headers: Record<string, string> = {};
      if (token) headers["Authorization"] = `Bearer ${token}`;
      return fetch(`${apiBase}/companies/${ticker}/chart-data${qs}`, { headers });
    })
      .then((res) => {
        if (!res.ok) throw new Error(`${res.status}`);
        return res.json();
      })
      .then((d: ChartData) => {
        setData(d);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, [ticker, filters, apiBase, getToken]);

  const toggleFilter = useCallback(
    <K extends keyof Filters>(key: K, value?: Filters[K]) => {
      setFilters((prev) => {
        if (key === "trade_type") {
          return { ...prev, trade_type: prev.trade_type === value ? null : (value as Filters["trade_type"]) };
        }
        return { ...prev, [key]: !prev[key] };
      });
    },
    []
  );

  const hasActiveFilters =
    filters.trade_type !== null || filters.csuite || filters.director || filters.owner_10pct || filters.top_performer;

  // Create chart
  useEffect(() => {
    if (!data || !containerRef.current) return;

    let chart: ReturnType<typeof import("lightweight-charts").createChart> | null = null;
    let resizeObserver: ResizeObserver | null = null;

    const init = async () => {
      const lc = await import("lightweight-charts");
      if (!containerRef.current) return;

      const isMobile = containerRef.current.clientWidth < 640;
      chart = lc.createChart(containerRef.current, {
        width: containerRef.current.clientWidth,
        height: isMobile ? 280 : 400,
        layout: {
          background: { type: lc.ColorType.Solid, color: "#12121A" },
          textColor: "#8888A0",
          fontFamily: "ui-monospace, SFMono-Regular, monospace",
        },
        grid: {
          vertLines: { color: "#1E1E2E" },
          horzLines: { color: "#1E1E2E" },
        },
        crosshair: {
          mode: lc.CrosshairMode.Normal,
          vertLine: { color: "#3A3A4A", width: 1, style: lc.LineStyle.Dashed },
          horzLine: { color: "#3A3A4A", width: 1, style: lc.LineStyle.Dashed },
        },
        timeScale: {
          borderColor: "#2A2A3A",
          timeVisible: false,
          rightOffset: 5,
          minBarSpacing: 2,
        },
        rightPriceScale: {
          borderColor: "#2A2A3A",
        },
      });
      chartRef.current = chart;

      // Build trade lookup by date for tooltip
      const tradesByDate = new Map<string, TradeMarker[]>();
      for (const t of data.trades) {
        const existing = tradesByDate.get(t.date) || [];
        existing.push(t);
        tradesByDate.set(t.date, existing);
      }

      // Build consolidated marker groups once — reused on zoom changes
      const markerGroups = new Map<string, TradeMarker[]>();
      for (const t of data.trades) {
        const key = `${t.date}|${t.trade_type}`;
        const group = markerGroups.get(key) || [];
        group.push(t);
        markerGroups.set(key, group);
      }

      // Threshold: show text labels only when this many bars or fewer are visible
      const TEXT_BAR_THRESHOLD = 90;

      function buildMarkers(showText: boolean): any[] {
        const markers: any[] = [];
        for (const [, group] of markerGroups) {
          const first = group[0];
          let text = "";
          if (showText) {
            const names = group.map((t) => t.insider_name.split(/[\s,]+/).filter(Boolean)[0] || "");
            if (names.length === 1) {
              text = names[0];
            } else if (names.length === 2) {
              text = `${names[0]}, ${names[1]}`;
            } else {
              text = `${names[0]}, ${names[1]} +${names.length - 2}`;
            }
          }
          const isGated = group.every((t) => t.gated);
          markers.push({
            time: first.date,
            position: first.trade_type === "buy" ? "belowBar" : "aboveBar",
            shape: isGated ? "circle" : first.trade_type === "buy" ? "arrowUp" : "arrowDown",
            color: isGated
              ? (first.trade_type === "buy" ? "#22C55E40" : "#EF444440")
              : (first.trade_type === "buy" ? "#22C55E" : "#EF4444"),
            text: isGated ? "" : text,
          });
        }
        markers.sort((a, b) => a.time.localeCompare(b.time));
        return markers;
      }

      let seriesRef: ReturnType<typeof chart.addSeries> | null = null;
      let markerHandle: ReturnType<typeof lc.createSeriesMarkers> | null = null;
      let lastShowText: boolean | null = null;

      if (data.candles.length > 0) {
        const series = chart.addSeries(lc.CandlestickSeries, {
          upColor: "#22C55E",
          downColor: "#EF4444",
          borderUpColor: "#22C55E",
          borderDownColor: "#EF4444",
          wickUpColor: "#22C55E80",
          wickDownColor: "#EF444480",
        });
        series.setData(data.candles);
        seriesRef = series;
      } else {
        const series = chart.addSeries(lc.LineSeries, {
          color: "#8888A0",
          lineWidth: 1,
        });

        const priceByDate = new Map<string, { sum: number; count: number }>();
        for (const t of data.trades) {
          const entry = priceByDate.get(t.date) || { sum: 0, count: 0 };
          entry.sum += t.price;
          entry.count += 1;
          priceByDate.set(t.date, entry);
        }
        const lineData = Array.from(priceByDate.entries())
          .map(([date, { sum, count }]) => ({ time: date, value: sum / count }))
          .sort((a, b) => a.time.localeCompare(b.time));

        series.setData(lineData);
        seriesRef = series;
      }

      // Initial markers (no text at full zoom-out)
      if (seriesRef) {
        const initialMarkers = buildMarkers(false);
        markerHandle = (lc.createSeriesMarkers as any)(seriesRef, initialMarkers);
        lastShowText = false;
      }

      // Update markers when zoom level changes
      chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        if (!range || !markerHandle) return;
        const visibleBars = Math.round(range.to - range.from);
        const showText = visibleBars <= TEXT_BAR_THRESHOLD;
        if (showText === lastShowText) return;
        lastShowText = showText;
        markerHandle.setMarkers(buildMarkers(showText));
      });

      // Tooltip on crosshair move
      chart.subscribeCrosshairMove((param) => {
        const tooltip = tooltipRef.current;
        if (!tooltip || !containerRef.current) return;

        if (!param.time || !param.point) {
          tooltip.style.display = "none";
          return;
        }

        const dateStr = param.time as string;
        const dayTrades = tradesByDate.get(dateStr);
        if (!dayTrades || dayTrades.length === 0) {
          tooltip.style.display = "none";
          return;
        }

        const containerRect = containerRef.current.getBoundingClientRect();
        let left = param.point.x + 16;
        let top = param.point.y - 16;

        if (left + 220 > containerRect.width) {
          left = param.point.x - 230;
        }
        if (top < 0) top = 4;
        if (top + 150 > containerRect.height) {
          top = containerRect.height - 150;
        }

        tooltip.style.display = "block";
        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;

        const shown = dayTrades.slice(0, 5);
        const more = dayTrades.length > 5 ? dayTrades.length - 5 : 0;
        const allGated = dayTrades.every((t) => t.gated);
        if (allGated) {
          tooltip.innerHTML = `<div style="color:#55556A;font-size:11px">${dayTrades.length} trade${dayTrades.length > 1 ? "s" : ""} · <a href="/pricing" style="color:#3B82F6">Upgrade to Pro</a></div>`;
        } else {
          tooltip.innerHTML = shown
            .map((t) => {
              if (t.gated) {
                const typeColor = t.trade_type === "buy" ? "#22C55E" : "#EF4444";
                return `<div style="margin-bottom:4px;filter:blur(4px);user-select:none">
                  <div style="font-weight:500;color:#E8E8ED">${t.insider_name}</div>
                  <div style="color:#8888A0">
                    <span style="color:${typeColor};font-family:monospace;font-weight:600">${t.trade_type.toUpperCase()}</span>
                    · $${t.price.toFixed(2)} · ${formatCurrency(t.value)}
                  </div>
                </div>`;
              }
              const typeColor = t.trade_type === "buy" ? "#22C55E" : "#EF4444";
              const tierStr = t.score_tier != null ? ` · ${tierLabel(t.score_tier)}` : "";
              return `<div style="margin-bottom:4px">
                <div style="font-weight:500;color:#E8E8ED">${t.insider_name}</div>
                <div style="color:#8888A0">
                  <span style="color:${typeColor};font-family:monospace;font-weight:600">${t.trade_type.toUpperCase()}</span>
                  · $${t.price.toFixed(2)} · ${formatCurrency(t.value)}${tierStr}
                </div>
              </div>`;
            })
            .join("") + (more > 0 ? `<div style="color:#55556A">+${more} more</div>` : "");
        }
      });

      // Default to last 1 year visible (not full 10-year range)
      if (data.candles.length > 252) {
        chart.timeScale().setVisibleLogicalRange({
          from: data.candles.length - 252,
          to: data.candles.length,
        });
      } else {
        chart.timeScale().fitContent();
      }

      resizeObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          if (chart) {
            chart.applyOptions({ width: entry.contentRect.width });
          }
        }
      });
      resizeObserver.observe(containerRef.current);
    };

    init();

    return () => {
      resizeObserver?.disconnect();
      if (chart) {
        chart.remove();
        chartRef.current = null;
      }
    };
  }, [data]);

  if (loading) {
    return (
      <Card className="bg-[#12121A] border-[#2A2A3A]">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm font-medium text-[#8888A0]">
            Price & Insider Activity
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-[400px] items-center justify-center text-[#55556A]">
            Loading chart...
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !data) {
    return (
      <Card className="bg-[#12121A] border-[#2A2A3A]">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm font-medium text-[#8888A0]">
            Price & Insider Activity
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-[400px] items-center justify-center text-[#55556A]">
            No trade data available
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="bg-[#12121A] border-[#2A2A3A]">
      <CardHeader className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2">
        <CardTitle className="text-sm font-medium text-[#8888A0]">
          Price & Insider Activity
        </CardTitle>
        <div className="flex items-center gap-1.5 flex-wrap">
          <FilterButton
            active={filters.trade_type === "buy"}
            onClick={() => toggleFilter("trade_type", "buy")}
            color="#22C55E"
          >
            Buys
          </FilterButton>
          <FilterButton
            active={filters.trade_type === "sell"}
            onClick={() => toggleFilter("trade_type", "sell")}
            color="#EF4444"
          >
            Sells
          </FilterButton>
          <span className="w-px h-4 bg-[#2A2A3A]" />
          <FilterButton
            active={filters.csuite}
            onClick={() => toggleFilter("csuite")}
          >
            C-Suite
          </FilterButton>
          <FilterButton
            active={filters.director}
            onClick={() => toggleFilter("director")}
          >
            Directors
          </FilterButton>
          <FilterButton
            active={filters.owner_10pct}
            onClick={() => toggleFilter("owner_10pct")}
          >
            10% Owners
          </FilterButton>
          <FilterButton
            active={filters.top_performer}
            onClick={() => toggleFilter("top_performer")}
            color="#F59E0B"
          >
            Top Performers
          </FilterButton>
          {hasActiveFilters && (
            <>
              <span className="w-px h-4 bg-[#2A2A3A]" />
              <button
                onClick={() => setFilters(DEFAULT_FILTERS)}
                className="text-[11px] text-[#55556A] hover:text-[#8888A0]"
              >
                Clear
              </button>
            </>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {data.trades.length === 0 && data.candles.length === 0 ? (
          <div className="flex h-[400px] items-center justify-center text-[#55556A]">
            No trade data available
          </div>
        ) : (
          <div className="relative h-[280px] sm:h-[400px]">
            <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
            <div
              ref={tooltipRef}
              className="absolute z-50 hidden rounded-md border border-[#2A2A3A] bg-[#1A1A26] px-3 py-2 text-xs shadow-xl pointer-events-none"
              style={{ maxWidth: 250, minWidth: 160 }}
            />
          </div>
        )}
        {/* Legend */}
        <div className="flex items-center gap-4 mt-3 text-[10px] text-[#55556A]">
          <span className="flex items-center gap-1">
            <span className="inline-block w-0 h-0 border-l-[4px] border-l-transparent border-r-[4px] border-r-transparent border-b-[6px] border-b-[#22C55E]" />
            Buy
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block w-0 h-0 border-l-[4px] border-l-transparent border-r-[4px] border-r-transparent border-t-[6px] border-t-[#EF4444]" />
            Sell
          </span>
          <span className="w-px h-3 bg-[#2A2A3A]" />
          <span>{data.candles.length > 0 ? "OHLC daily candles" : "Trade price line"}</span>
          <span className="w-px h-3 bg-[#2A2A3A]" />
          <span>Scroll to zoom · Drag to pan</span>
        </div>
        {data.gated && (
          <div className="mt-2 text-[11px] text-[#8888A0] border border-[#2A2A3A] rounded-md px-3 py-2 bg-[#1A1A26]/50">
            Insider filings older than 90 days are hidden.{" "}
            <a href="/pricing" className="text-[#3B82F6] hover:text-[#2563EB]">
              Upgrade to Pro
            </a>{" "}
            to see the full history.
          </div>
        )}
        {data.gaps && data.gaps.length > 0 && (
          <div className="mt-2 space-y-1">
            {data.gaps.map((gap, i) => {
              const pctChange = ((gap.price_after - gap.price_before) / gap.price_before * 100).toFixed(0);
              const months = Math.round(gap.days / 30);
              return (
                <div key={i} className="text-[11px] text-[#F59E0B] border border-[#F59E0B]/20 rounded-md px-3 py-2 bg-[#F59E0B]/5 flex items-center gap-2">
                  <svg className="w-3.5 h-3.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
                  </svg>
                  <span>
                    Trading gap: {gap.start} to {gap.end} ({months} months, no price data).
                    Price went from ${gap.price_before.toFixed(2)} to ${gap.price_after.toFixed(2)} ({pctChange}%).
                    Likely halted, delisted, or underwent corporate action.
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
