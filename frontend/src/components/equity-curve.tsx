"use client";

import { useEffect, useRef, useState } from "react";

interface CurvePoint {
  date: string;
  equity: number;
}

interface EquityCurveProps {
  data: CurvePoint[];
  benchmark?: CurvePoint[];
  startingCapital: number;
}

const RANGES = [
  { label: "3M", days: 63 },
  { label: "6M", days: 126 },
  { label: "1Y", days: 252 },
  { label: "ALL", days: 0 },
] as const;

export function EquityCurve({ data, benchmark, startingCapital }: EquityCurveProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartObjRef = useRef<any>(null);
  const [activeRange, setActiveRange] = useState("ALL");
  const [hoverInfo, setHoverInfo] = useState<{
    date: string;
    equity: number;
    spy?: number;
  } | null>(null);
  const [ready, setReady] = useState(false);

  // Build benchmark lookup once
  const benchByDate = useRef(new Map<string, number>());
  useEffect(() => {
    const m = new Map<string, number>();
    if (benchmark) {
      for (const p of benchmark) m.set(p.date, p.equity);
    }
    benchByDate.current = m;
  }, [benchmark]);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    let chart: any = null;
    let ro: ResizeObserver | null = null;

    const init = async () => {
      const lc = await import("lightweight-charts");
      if (!containerRef.current) return;

      // Remove old chart
      if (chartObjRef.current) {
        try { chartObjRef.current.remove(); } catch {}
        chartObjRef.current = null;
      }

      const container = containerRef.current;
      const isMobile = container.clientWidth < 640;

      chart = lc.createChart(container, {
        width: container.clientWidth,
        height: isMobile ? 260 : 380,
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
          vertLine: { color: "#3B82F6", width: 1, style: lc.LineStyle.Dashed, labelVisible: false },
          horzLine: { color: "#3B82F6", width: 1, style: lc.LineStyle.Dashed, labelVisible: true },
        },
        handleScale: { axisPressedMouseMove: { time: true, price: true } },
        handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      });

      chartObjRef.current = chart;

      // Portfolio line
      const equitySeries = chart.addSeries(lc.AreaSeries, {
        lineColor: "#3B82F6",
        topColor: "rgba(59,130,246,0.15)",
        bottomColor: "rgba(59,130,246,0.02)",
        lineWidth: 2,
        priceFormat: {
          type: "custom" as const,
          formatter: (price: number) => `$${(price / 1000).toFixed(0)}K`,
        },
        crosshairMarkerVisible: true,
        crosshairMarkerRadius: 4,
      });

      // Deduplicate: multiple trades can exit on the same date.
      // Take the last equity value per date (cumulative, so last is correct).
      const byDate = new Map<string, number>();
      for (const p of data) {
        byDate.set(p.date, p.equity);
      }
      const equityData = Array.from(byDate.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([time, value]) => ({ time, value }));
      equitySeries.setData(equityData);

      // Starting capital line
      equitySeries.createPriceLine({
        price: startingCapital,
        color: "#55556A",
        lineWidth: 1,
        lineStyle: lc.LineStyle.Dashed,
        axisLabelVisible: true,
        title: "",
      });

      // SPY benchmark
      if (benchmark && benchmark.length > 0) {
        const benchSeries = chart.addSeries(lc.LineSeries, {
          color: "#55556A",
          lineWidth: 1,
          lineStyle: lc.LineStyle.Dashed,
          crosshairMarkerVisible: false,
          lastValueVisible: true,
          priceLineVisible: false,
          priceFormat: {
            type: "custom" as const,
            formatter: (price: number) => `$${(price / 1000).toFixed(0)}K`,
          },
        });
        benchSeries.setData(
          benchmark.map((p) => ({ time: p.date as string, value: p.equity }))
        );
      }

      // Crosshair
      chart.subscribeCrosshairMove((param: any) => {
        if (!param.time || !param.point) {
          setHoverInfo(null);
          return;
        }
        const dateStr = String(param.time);
        const sd = param.seriesData.get(equitySeries);
        if (!sd || !("value" in sd)) {
          setHoverInfo(null);
          return;
        }
        const equity = (sd as any).value as number;
        const spy = benchByDate.current.get(dateStr);
        setHoverInfo({ date: dateStr, equity, spy });
      });

      // Resize
      ro = new ResizeObserver((entries) => {
        for (const entry of entries) {
          if (chart) chart.applyOptions({ width: entry.contentRect.width });
        }
      });
      ro.observe(container);

      // Lock Y-axis to full data range so it doesn't jump when panning
      const allValues = data.map((p) => p.equity);
      if (benchmark) {
        for (const p of benchmark) allValues.push(p.equity);
      }
      const minVal = Math.min(...allValues) * 0.95;
      const maxVal = Math.max(...allValues) * 1.03;
      equitySeries.applyOptions({
        autoscaleInfoProvider: () => ({
          priceRange: { minValue: minVal, maxValue: maxVal },
        }),
      });

      chart.timeScale().fitContent();
      setReady(true);
    };

    init();

    return () => {
      if (ro) ro.disconnect();
      if (chartObjRef.current) {
        try { chartObjRef.current.remove(); } catch {}
        chartObjRef.current = null;
      }
    };
  }, [data, benchmark, startingCapital]);

  const handleRange = (label: string, days: number) => {
    setActiveRange(label);
    const chart = chartObjRef.current;
    if (!chart || data.length === 0) return;
    if (days === 0) {
      chart.timeScale().fitContent();
    } else {
      // Calculate target start date, then use setVisibleRange with actual dates
      const uniqueDates = [...new Set(data.map((p) => p.date))].sort();
      const lastDate = uniqueDates[uniqueDates.length - 1];
      const startIdx = Math.max(0, uniqueDates.length - days);
      const startDate = uniqueDates[startIdx];
      chart.timeScale().setVisibleRange({
        from: startDate,
        to: lastDate,
      });
    }
  };

  const displayEquity = hoverInfo?.equity ?? (data.length > 0 ? data[data.length - 1].equity : startingCapital);
  const displayPnl = displayEquity - startingCapital;
  const displayPnlPct = ((displayEquity / startingCapital) - 1) * 100;
  const isPositive = displayPnl >= 0;

  return (
    <div className="rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-2 mb-3">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-widest text-[#55556A] mb-1">
            Portfolio Equity
          </div>
          <div className="flex items-baseline gap-3">
            <span className="text-2xl font-mono font-bold text-[#E8E8ED]">
              ${displayEquity.toLocaleString(undefined, { maximumFractionDigits: 0 })}
            </span>
            <span className={`text-sm font-mono font-semibold ${isPositive ? "text-[#22C55E]" : "text-[#EF4444]"}`}>
              {isPositive ? "+" : ""}{displayPnlPct.toFixed(1)}%
            </span>
            <span className={`text-xs font-mono ${isPositive ? "text-[#22C55E]/70" : "text-[#EF4444]/70"}`}>
              {isPositive ? "+" : ""}${Math.abs(displayPnl).toLocaleString(undefined, { maximumFractionDigits: 0 })}
            </span>
          </div>
          <div className="text-[10px] text-[#55556A] mt-0.5 h-4">
            {hoverInfo ? (
              <>
                {hoverInfo.date}
                {hoverInfo.spy != null && (
                  <span className="ml-2">
                    SPY: ${hoverInfo.spy.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    {" "}({((hoverInfo.spy / startingCapital - 1) * 100).toFixed(1)}%)
                  </span>
                )}
              </>
            ) : (
              <span>&nbsp;</span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-1">
          {RANGES.map(({ label, days }) => (
            <button
              key={label}
              onClick={() => handleRange(label, days)}
              className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
                activeRange === label
                  ? "bg-[#3B82F6]/20 text-[#3B82F6]"
                  : "text-[#55556A] hover:text-[#8888A0] hover:bg-[#1A1A26]"
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* Chart container */}
      <div ref={containerRef} className="w-full h-[260px] sm:h-[380px]" />

      {/* Legend */}
      <div className="flex items-center gap-4 mt-2 text-[10px] text-[#55556A]">
        <span className="flex items-center gap-1.5">
          <span className="w-4 h-0.5 bg-[#3B82F6] inline-block rounded" />
          Portfolio
        </span>
        {benchmark && benchmark.length > 0 && (
          <span className="flex items-center gap-1.5">
            <span className="w-4 h-0.5 bg-[#55556A] inline-block rounded" style={{ borderTop: "1px dashed #55556A" }} />
            S&P 500 (SPY)
          </span>
        )}
        <span className="ml-auto">Scroll to zoom · Drag to pan</span>
      </div>
    </div>
  );
}
