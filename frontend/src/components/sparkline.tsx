"use client";

interface SparklineProps {
  returns: number[];
  width?: number;
  height?: number;
}

export default function Sparkline({
  returns,
  width = 80,
  height = 24,
}: SparklineProps) {
  if (!returns || returns.length < 2) {
    return <span className="text-muted-foreground text-xs">--</span>;
  }

  const padding = 2;
  const innerW = width - padding * 2;
  const innerH = height - padding * 2;

  const min = Math.min(...returns);
  const max = Math.max(...returns);
  const range = max - min || 1;

  const points = returns.map((v, i) => {
    const x = padding + (i / (returns.length - 1)) * innerW;
    const y = padding + innerH - ((v - min) / range) * innerH;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const polylinePoints = points.join(" ");

  // Fill polygon: same points but close along the bottom edge
  const firstX = padding;
  const lastX = padding + innerW;
  const bottomY = height - padding;
  const fillPoints = `${firstX},${bottomY} ${polylinePoints} ${lastX},${bottomY}`;

  // Green if last value >= first value, red otherwise
  const trend = returns[returns.length - 1] >= returns[0];
  const lineColor = trend ? "#22C55E" : "#EF4444";
  const fillColor = trend ? "#22C55E" : "#EF4444";

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className="inline-block align-middle"
    >
      <polygon points={fillPoints} fill={fillColor} opacity={0.15} />
      <polyline
        points={polylinePoints}
        fill="none"
        stroke={lineColor}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
