/**
 * The live book, drawn as SVG on the server.
 *
 * This is the landing page's opening argument, so it has to render in the
 * first paint with no JavaScript: a crawler and a first-time visitor on a
 * phone both need to see the line before they see anything else. The product
 * already has an ECharts equity curve (equity-curve.tsx) with a range selector
 * and tooltips — that is the right component for /portfolio, where someone is
 * interrogating the record, and the wrong one here, where they are deciding
 * whether to care at all.
 *
 * Two series on purpose. A rising line alone means nothing; the same capital
 * in SPY over the same window is what makes it an argument.
 */

type Point = { date: string; equity: number };

const W = 720;
const H = 220;
const PAD_T = 16;
const PAD_B = 8;

function toPath(points: Point[], x: (d: string) => number, y: (v: number) => number) {
  return points
    .map((p, i) => `${i === 0 ? "M" : "L"}${x(p.date).toFixed(1)},${y(p.equity).toFixed(1)}`)
    .join("");
}

export function EquitySparkline({
  strategy,
  benchmark,
  label,
  benchmarkLabel = "S&P 500",
}: {
  strategy: Point[];
  benchmark: Point[];
  label: string;
  benchmarkLabel?: string;
}) {
  if (!strategy?.length) return null;

  // Both series are mapped by date onto one x-scale rather than by index. They
  // can have different point counts over the same window, and indexing them
  // separately would make the two lines tell time differently — which would
  // make the comparison a lie rather than a chart.
  const t0 = new Date(strategy[0].date).getTime();
  const t1 = new Date(strategy[strategy.length - 1].date).getTime();
  const span = Math.max(t1 - t0, 1);
  const bench = (benchmark ?? []).filter((p) => {
    const t = new Date(p.date).getTime();
    return t >= t0 && t <= t1;
  });

  const values = [...strategy, ...bench].map((p) => p.equity);
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const range = Math.max(hi - lo, 1);

  const x = (d: string) => ((new Date(d).getTime() - t0) / span) * W;
  const y = (v: number) => PAD_T + (1 - (v - lo) / range) * (H - PAD_T - PAD_B);

  const stratPath = toPath(strategy, x, y);
  const benchPath = bench.length > 1 ? toPath(bench, x, y) : "";
  const areaPath = `${stratPath}L${W},${H}L0,${H}Z`;

  const last = strategy[strategy.length - 1];
  const benchLast = bench.length ? bench[bench.length - 1] : null;

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      className="w-full h-auto block"
      role="img"
      aria-label={`${label} grew $${Math.round(strategy[0].equity).toLocaleString()} to $${Math.round(last.equity).toLocaleString()}. ${benchmarkLabel} reached $${benchLast ? Math.round(benchLast.equity).toLocaleString() : "n/a"} over the same period.`}
      preserveAspectRatio="none"
    >
      <defs>
        <linearGradient id="eq-fill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#22C55E" stopOpacity="0.22" />
          <stop offset="100%" stopColor="#22C55E" stopOpacity="0" />
        </linearGradient>
      </defs>

      {/* Starting capital — the reader needs to see where zero is. */}
      <line
        x1="0" x2={W} y1={y(strategy[0].equity)} y2={y(strategy[0].equity)}
        stroke="#2A2A3A" strokeWidth="1" strokeDasharray="3 4"
      />

      {benchPath && (
        <path d={benchPath} fill="none" stroke="#81819A" strokeWidth="1.5"
              vectorEffect="non-scaling-stroke" />
      )}
      <path d={areaPath} fill="url(#eq-fill)" stroke="none" />
      <path d={stratPath} fill="none" stroke="#22C55E" strokeWidth="2.25"
            vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
      <circle cx={x(last.date)} cy={y(last.equity)} r="3.5" fill="#22C55E" />
    </svg>
  );
}
