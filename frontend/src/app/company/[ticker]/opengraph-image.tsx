import { ImageResponse } from "next/og";
import { ogMoney as money } from "@/lib/og-format";

export const runtime = "edge";
export const alt = "Form4 Company Insider Trading Data";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

// Product palette, same values the app uses.
const BG = "#0A0A0F";
const INK = "#E8E8ED";
const MUTED = "#8888A0";
const FAINT = "#55556A";
const RULE = "#23232E";
const ACCENT = "#3B82F6";
const POS = "#22C55E";
const NEG = "#EF4444";

export default async function Image({ params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;

  let company = ticker;
  let trades = "—";
  let insiders = "—";
  let net6 = 0;
  let hasNet = false;
  let totalValue = 0;

  try {
    const resp = await fetch(`${API}/companies/${ticker}`, { next: { revalidate: 3600 } });
    if (resp.ok) {
      const data = await resp.json();
      company = data.company || ticker;
      trades = String(data.total_trades ?? "—");
      insiders = String(data.distinct_insiders ?? data.insiders?.length ?? "—");
      totalValue = Number(data.total_value ?? 0);
      const b = Number(data.buy_value_6mo ?? 0);
      const s = Number(data.sell_value_6mo ?? 0);
      if (b || s) { net6 = b - s; hasNet = true; }
    }
  } catch {}

  // Long names wrap badly at this size and push the stat row off the canvas.
  const name = company.length > 46 ? `${company.slice(0, 44)}…` : company;

  const stats: { value: string; label: string; color: string }[] = [
    { value: trades, label: "filings", color: INK },
    { value: insiders, label: "insiders", color: INK },
    { value: money(totalValue), label: "total value", color: INK },
  ];
  if (hasNet) {
    stats.push({
      value: `${net6 >= 0 ? "+" : "−"}${money(Math.abs(net6))}`,
      label: "net 6-month flow",
      color: net6 >= 0 ? POS : NEG,
    });
  }

  return new ImageResponse(
    (
      // space-between, not center. The old card centred ~210px of content in a
      // 630px frame and left roughly 300px of dead space, which reads as an
      // unfinished template once a feed crops it.
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          width: "100%",
          height: "100%",
          backgroundColor: BG,
          padding: "54px 72px",
          fontFamily: "system-ui, sans-serif",
        }}
      >
        {/* Brand sits in the flow, top-left, aligned with everything else. It
            used to be absolutely positioned bottom-right while the rest of the
            card was left-aligned top, which made an L nothing resolved. */}
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          <div style={{ display: "flex", width: "10px", height: "30px", backgroundColor: ACCENT, borderRadius: "2px" }} />
          <span style={{ fontSize: "24px", fontWeight: 700, color: INK, letterSpacing: "-0.5px" }}>Form4</span>
          <span style={{ fontSize: "20px", color: FAINT }}>SEC Form 4 insider trading</span>
        </div>

        <div style={{ display: "flex", flexDirection: "column" }}>
          {/* alignItems flex-end, not baseline: Satori baseline-aligns 128px
              and 34px text inconsistently and the label floated. */}
          <div style={{ display: "flex", alignItems: "flex-end", gap: "20px" }}>
            <span style={{ fontSize: "128px", fontWeight: 800, color: INK, letterSpacing: "-5px", lineHeight: 1 }}>
              {ticker}
            </span>
            <span style={{ fontSize: "34px", color: ACCENT, fontWeight: 600, paddingBottom: "10px" }}>
              Insider Trading
            </span>
          </div>
          <span style={{ fontSize: "32px", color: MUTED, marginTop: "14px" }}>{name}</span>
        </div>

        <div style={{ display: "flex", flexDirection: "column" }}>
          <div style={{ display: "flex", width: "100%", height: "1px", backgroundColor: RULE, marginBottom: "26px" }} />
          <div style={{ display: "flex", justifyContent: "space-between", width: "100%" }}>
            {stats.map((s) => (
              <div key={s.label} style={{ display: "flex", flexDirection: "column" }}>
                <span style={{ fontSize: "50px", fontWeight: 700, color: s.color, lineHeight: 1 }}>{s.value}</span>
                <span style={{ fontSize: "17px", color: FAINT, textTransform: "uppercase", letterSpacing: "2.5px", marginTop: "10px" }}>
                  {s.label}
                </span>
              </div>
            ))}
            <div style={{ display: "flex", flexDirection: "column", justifyContent: "flex-end" }}>
              <span style={{ fontSize: "22px", color: MUTED }}>form4.app</span>
            </div>
          </div>
        </div>
      </div>
    ),
    { ...size },
  );
}
