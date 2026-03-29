import * as echarts from "echarts";

const COLORS = {
  bg: "transparent",
  cardBg: "#12121A",
  border: "#2A2A3A",
  muted: "#55556A",
  text: "#8888A0",
  bright: "#E8E8ED",
  blue: "#3B82F6",
  green: "#22C55E",
  red: "#EF4444",
  amber: "#F59E0B",
};

let registered = false;

export function registerForm4Theme() {
  if (registered) return;
  registered = true;
  echarts.registerTheme("form4", {
    backgroundColor: COLORS.bg,
    textStyle: { color: COLORS.text, fontFamily: "inherit" },
    categoryAxis: {
      axisLine: { lineStyle: { color: COLORS.border } },
      axisTick: { show: false },
      axisLabel: { color: COLORS.muted, fontSize: 10 },
      splitLine: { lineStyle: { color: COLORS.border, type: "dashed" } },
    },
    valueAxis: {
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: COLORS.muted, fontSize: 11 },
      splitLine: { lineStyle: { color: COLORS.border, type: "dashed" } },
    },
    timeAxis: {
      axisLine: { lineStyle: { color: COLORS.border } },
      axisTick: { show: false },
      axisLabel: { color: COLORS.muted, fontSize: 10 },
      splitLine: { lineStyle: { color: COLORS.border, type: "dashed" } },
    },
    legend: { textStyle: { color: COLORS.text, fontSize: 11 } },
    tooltip: {
      backgroundColor: "#1A1A26",
      borderColor: COLORS.border,
      borderWidth: 1,
      textStyle: { color: COLORS.bright, fontSize: 12 },
      extraCssText: "border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.5);",
    },
    color: [COLORS.blue, COLORS.green, COLORS.red, COLORS.amber, COLORS.text],
  });
}

export function baseGrid() {
  return { top: 10, right: 16, bottom: 30, left: 50, containLabel: false };
}

export function timeSeriesDataZoom() {
  return [
    {
      type: "inside" as const,
      xAxisIndex: 0,
      zoomOnMouseWheel: true,
      moveOnMouseMove: false,
      moveOnTouch: true,
    },
    {
      type: "slider" as const,
      xAxisIndex: 0,
      height: 18,
      bottom: 0,
      borderColor: COLORS.border,
      backgroundColor: "#0A0A0F",
      fillerColor: "rgba(59,130,246,0.15)",
      handleStyle: { color: COLORS.blue, borderColor: COLORS.blue },
      textStyle: { color: COLORS.muted, fontSize: 9 },
      dataBackground: {
        lineStyle: { color: COLORS.border },
        areaStyle: { color: "rgba(59,130,246,0.05)" },
      },
    },
  ];
}

export function tooltipFormatter(params: any): string {
  if (Array.isArray(params)) {
    const header = `<div style="color:#8888A0;margin-bottom:4px">${params[0]?.axisValueLabel || params[0]?.name || ""}</div>`;
    const rows = params
      .map(
        (p: any) =>
          `<div style="display:flex;justify-content:space-between;gap:12px"><span>${p.marker}${p.seriesName}</span><span style="font-family:monospace;font-weight:600">${p.value?.[1] ?? p.value ?? ""}</span></div>`
      )
      .join("");
    return header + rows;
  }
  return "";
}

export { COLORS };
