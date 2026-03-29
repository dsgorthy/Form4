"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { useAuth } from "@clerk/nextjs";
import { formatCurrency } from "@/lib/format";

const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

interface OverlayPoint {
  date: string;
  insider_equity: number;
  insider_alloc_pct: number;
  n_positions: number;
  [key: string]: string | number;
}

interface OverlayData {
  starting_capital: number;
  base_assets: string[];
  data: OverlayPoint[];
}

const ASSET_LABELS: Record<string, string> = {
  SPY: "S&P 500 (SPY)",
  QQQ: "Nasdaq 100 (QQQ)",
  IWM: "Russell 2000 (IWM)",
  TLT: "20+ Year Treasury (TLT)",
  GLD: "Gold (GLD)",
  CASH: "Cash (0% return)",
};

const RANGES = [
  { label: "1Y", days: 252 },
  { label: "3Y", days: 756 },
  { label: "5Y", days: 1260 },
  { label: "ALL", days: 0 },
] as const;

interface PortfolioOverlayProps {
  onDateRangeChange?: (from: string | null, to: string | null) => void;
}

export function PortfolioOverlay({ onDateRangeChange }: PortfolioOverlayProps) {
  const { getToken } = useAuth();
  const [data, setData] = useState<OverlayData | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedBase, setSelectedBase] = useState("SPY");
  const [activeRange, setActiveRange] = useState("ALL");

  const equityRef = useRef<HTMLDivElement>(null);
  const allocRef = useRef<HTMLDivElement>(null);
  const equityChartRef = useRef<any>(null);
  const allocChartRef = useRef<any>(null);
  const [ready, setReady] = useState(false);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const token = await getToken();
      const res = await fetch(`${apiBase}/portfolio/overlay`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      if (res.ok) setData(await res.json());
    } catch {}
    setLoading(false);
  }, [getToken]);

  useEffect(() => { loadData(); }, [loadData]);

  // Build charts when data or selectedBase changes
  useEffect(() => {
    if (!data || data.data.length === 0 || !equityRef.current || !allocRef.current) return;

    let equityChart: any = null;
    let allocChart: any = null;
    let ro1: ResizeObserver | null = null;
    let ro2: ResizeObserver | null = null;

    const init = async () => {
      const lc = await import("lightweight-charts");
      if (!equityRef.current || !allocRef.current) return;

      // Clean up old charts
      if (equityChartRef.current) { try { equityChartRef.current.remove(); } catch {} }
      if (allocChartRef.current) { try { allocChartRef.current.remove(); } catch {} }

      const points = data.data;
      const blendedKey = `blended_${selectedBase}`;
      const pureKey = `pure_${selectedBase}`;

      const baseColor = selectedBase === "QQQ" ? "#8B5CF6" : selectedBase === "IWM" ? "#F59E0B"
        : selectedBase === "TLT" ? "#06B6D4" : selectedBase === "GLD" ? "#EAB308"
        : selectedBase === "CASH" ? "#55556A" : "#3B82F6";

      const isMobile = equityRef.current.clientWidth < 640;

      const chartOpts = {
        layout: {
          background: { type: lc.ColorType.Solid, color: "#12121A" },
          textColor: "#55556A",
          fontFamily: "ui-monospace, SFMono-Regular, monospace",
          fontSize: 11,
        },
        grid: {
          vertLines: { visible: false },
          horzLines: { color: "#1E1E2E", style: lc.LineStyle.Dotted },
        },
        rightPriceScale: {
          borderColor: "#2A2A3A",
          scaleMargins: { top: 0.08, bottom: 0.05 },
        },
        timeScale: {
          borderColor: "#2A2A3A",
          timeVisible: false,
          fixLeftEdge: true,
          fixRightEdge: true,
        },
        crosshair: {
          mode: lc.CrosshairMode.Normal,
          vertLine: { color: "#3B82F6", width: 1 as any, style: lc.LineStyle.Dashed, labelVisible: false },
          horzLine: { color: "#3B82F6", width: 1 as any, style: lc.LineStyle.Dashed, labelVisible: true },
        },
        handleScale: { axisPressedMouseMove: { time: true, price: true } },
        handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      };

      // ---- Equity Chart ----
      equityChart = lc.createChart(equityRef.current, {
        ...chartOpts,
        width: equityRef.current.clientWidth,
        height: isMobile ? 260 : 360,
      });
      equityChartRef.current = equityChart;

      // Blended line (green)
      const blendedSeries = equityChart.addSeries(lc.AreaSeries, {
        lineColor: "#22C55E",
        topColor: "rgba(34,197,94,0.12)",
        bottomColor: "rgba(34,197,94,0.01)",
        lineWidth: 2,
        priceFormat: { type: "custom" as const, formatter: (p: number) => `$${(p/1000).toFixed(0)}K` },
        crosshairMarkerVisible: true,
        crosshairMarkerRadius: 3,
      });
      blendedSeries.setData(
        points.map(p => ({ time: p.date as string, value: (p[blendedKey] as number) || 0 }))
      );

      // Pure base line (dashed)
      const pureSeries = equityChart.addSeries(lc.LineSeries, {
        color: baseColor,
        lineWidth: 1,
        lineStyle: lc.LineStyle.Dashed,
        crosshairMarkerVisible: false,
        lastValueVisible: true,
        priceLineVisible: false,
        priceFormat: { type: "custom" as const, formatter: (p: number) => `$${(p/1000).toFixed(0)}K` },
      });
      pureSeries.setData(
        points.map(p => ({ time: p.date as string, value: (p[pureKey] as number) || 0 }))
      );

      // Insider-only line (dotted grey)
      const insiderSeries = equityChart.addSeries(lc.LineSeries, {
        color: "#55556A",
        lineWidth: 1,
        lineStyle: lc.LineStyle.Dotted,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
        priceFormat: { type: "custom" as const, formatter: (p: number) => `$${(p/1000).toFixed(0)}K` },
      });
      insiderSeries.setData(
        points.map(p => ({ time: p.date as string, value: p.insider_equity || 0 }))
      );

      // Starting capital line
      blendedSeries.createPriceLine({
        price: data.starting_capital,
        color: "#55556A",
        lineWidth: 1,
        lineStyle: lc.LineStyle.Dashed,
        axisLabelVisible: false,
        title: "",
      });

      equityChart.timeScale().fitContent();

      // ---- Allocation Chart ----
      allocChart = lc.createChart(allocRef.current, {
        ...chartOpts,
        width: allocRef.current.clientWidth,
        height: isMobile ? 100 : 140,
        rightPriceScale: {
          borderColor: "#2A2A3A",
          scaleMargins: { top: 0.05, bottom: 0.05 },
        },
      });
      allocChartRef.current = allocChart;

      const allocSeries = allocChart.addSeries(lc.AreaSeries, {
        lineColor: "#22C55E",
        topColor: "rgba(34,197,94,0.25)",
        bottomColor: "rgba(34,197,94,0.02)",
        lineWidth: 1,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceFormat: { type: "custom" as const, formatter: (p: number) => `${p.toFixed(0)}%` },
      });
      allocSeries.setData(
        points.map(p => ({ time: p.date as string, value: p.insider_alloc_pct || 0 }))
      );

      allocChart.timeScale().fitContent();

      // Sync time scales + emit date range to parent
      const emitRange = () => {
        if (!onDateRangeChange || !equityChart) return;
        const logRange = equityChart.timeScale().getVisibleLogicalRange();
        if (!logRange) return;
        const pts = points;
        const fromIdx = Math.max(0, Math.floor(logRange.from));
        const toIdx = Math.min(pts.length - 1, Math.ceil(logRange.to));
        if (fromIdx < pts.length && toIdx >= 0) {
          onDateRangeChange(pts[fromIdx]?.date || null, pts[toIdx]?.date || null);
        }
      };

      equityChart.timeScale().subscribeVisibleLogicalRangeChange((range: any) => {
        if (range && allocChart) allocChart.timeScale().setVisibleLogicalRange(range);
        emitRange();
      });
      allocChart.timeScale().subscribeVisibleLogicalRangeChange((range: any) => {
        if (range && equityChart) equityChart.timeScale().setVisibleLogicalRange(range);
      });

      // Resize
      ro1 = new ResizeObserver(entries => {
        for (const e of entries) if (equityChart) equityChart.applyOptions({ width: e.contentRect.width });
      });
      ro1.observe(equityRef.current);
      ro2 = new ResizeObserver(entries => {
        for (const e of entries) if (allocChart) allocChart.applyOptions({ width: e.contentRect.width });
      });
      ro2.observe(allocRef.current);

      setReady(true);
    };

    init();

    return () => {
      if (ro1) ro1.disconnect();
      if (ro2) ro2.disconnect();
      if (equityChartRef.current) { try { equityChartRef.current.remove(); } catch {} equityChartRef.current = null; }
      if (allocChartRef.current) { try { allocChartRef.current.remove(); } catch {} allocChartRef.current = null; }
    };
  }, [data, selectedBase]);

  const handleRange = (label: string, days: number) => {
    setActiveRange(label);
    const chart = equityChartRef.current;
    if (!chart || !data) return;
    if (days === 0) {
      chart.timeScale().fitContent();
    } else {
      const dates = data.data.map(p => p.date).sort();
      const last = dates[dates.length - 1];
      const startIdx = Math.max(0, dates.length - Math.floor(days / 5)); // weekly data
      const start = dates[startIdx];
      chart.timeScale().setVisibleRange({ from: start, to: last });
    }
  };

  if (loading) {
    return (
      <div className="space-y-4 animate-pulse">
        <div className="h-10 w-48 bg-[#1A1A26] rounded-lg" />
        <div className="h-80 bg-[#1A1A26] rounded-lg border border-[#2A2A3A]" />
        <div className="h-24 bg-[#1A1A26] rounded-lg border border-[#2A2A3A]" />
      </div>
    );
  }

  if (!data || data.data.length === 0) return null;

  const points = data.data;
  const last = points[points.length - 1];
  const blendedFinal = (last[`blended_${selectedBase}`] as number) || 0;
  const pureFinal = (last[`pure_${selectedBase}`] as number) || 0;
  const starting = data.starting_capital;
  const years = Math.max(0.5, (new Date(last.date).getTime() - new Date(points[0].date).getTime()) / (365.25 * 86400000));
  const blendedCAGR = ((blendedFinal / starting) ** (1 / years) - 1) * 100;
  const pureCAGR = ((pureFinal / starting) ** (1 / years) - 1) * 100;
  const alpha = blendedCAGR - pureCAGR;
  const baseColor = selectedBase === "QQQ" ? "#8B5CF6" : selectedBase === "IWM" ? "#F59E0B"
    : selectedBase === "TLT" ? "#06B6D4" : selectedBase === "GLD" ? "#EAB308"
    : selectedBase === "CASH" ? "#55556A" : "#3B82F6";

  return (
    <div className="space-y-2">
      {/* Header: selector + stats + range buttons */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div className="flex items-center gap-3">
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-1">
              Idle Cash Vehicle
            </div>
            <select
              value={selectedBase}
              onChange={(e) => setSelectedBase(e.target.value)}
              className="rounded-md border border-[#2A2A3A] bg-[#12121A] text-[#E8E8ED] text-sm px-3 py-1.5 focus:outline-none focus:ring-1 focus:ring-[#3B82F6]"
            >
              {data.base_assets.map(a => (
                <option key={a} value={a}>{ASSET_LABELS[a] || a}</option>
              ))}
            </select>
          </div>
          <div className="flex gap-1 ml-3 mt-4">
            {RANGES.map(({ label, days }) => (
              <button
                key={label}
                onClick={() => handleRange(label, days)}
                className={`px-2.5 py-1 text-[10px] font-semibold rounded transition-colors ${
                  activeRange === label
                    ? "bg-[#3B82F6]/20 text-[#3B82F6]"
                    : "text-[#55556A] hover:text-[#8888A0]"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>

        <div className="flex gap-5 text-xs">
          <div>
            <div className="text-[10px] text-[#55556A] uppercase">Portfolio Value</div>
            <div className="text-xl font-mono font-bold text-[#E8E8ED]">{formatCurrency(blendedFinal)}</div>
            <div className="text-[#55556A]">from {formatCurrency(starting)}</div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-[#55556A] uppercase">CAGR</div>
            <div className="text-lg font-mono font-bold text-[#22C55E]">{blendedCAGR.toFixed(1)}%</div>
            <div className="text-[#55556A]">Insider + {selectedBase}</div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-[#55556A] uppercase">{selectedBase} Only</div>
            <div className="text-lg font-mono font-bold" style={{ color: baseColor }}>{pureCAGR.toFixed(1)}%</div>
            <div className="text-[#55556A]">{formatCurrency(pureFinal)}</div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-[#55556A] uppercase">Alpha</div>
            <div className={`text-lg font-mono font-bold ${alpha >= 0 ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
              {alpha >= 0 ? "+" : ""}{alpha.toFixed(1)}%
            </div>
            <div className="text-[#55556A]">annual</div>
          </div>
        </div>
      </div>

      {/* Equity chart */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-2 pt-3">
        <div ref={equityRef} />
      </div>

      {/* Allocation chart */}
      <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-2">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-1 px-2">
          Insider Allocation %
        </div>
        <div ref={allocRef} />
      </div>
    </div>
  );
}
