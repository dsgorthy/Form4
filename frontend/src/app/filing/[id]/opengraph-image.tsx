import { ImageResponse } from "next/og";
import { ogMoney } from "@/lib/og-format";

export const runtime = "edge";
export const alt = "Form4 SEC Filing Detail";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

const BG = "#0A0A0F";
const INK = "#E8E8ED";
const MUTED = "#8888A0";
const FAINT = "#81819A";
const RULE = "#23232E";
const ACCENT = "#3B82F6";

export default async function Image({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let ticker = "—";
  let company = "";
  let tradeType = "BUY";
  let insiderName = "Insider";
  let insiderTitle = "";
  let value = "";
  let date = "";

  try {
    const resp = await fetch(`${API}/filings/${id}`, { next: { revalidate: 86400 } });
    if (resp.ok) {
      const data = await resp.json();
      ticker = data.ticker || "—";
      company = data.company || "";
      tradeType = (data.trade_type || "buy").toUpperCase();
      insiderName = data.insider_name || "Insider";
      // normalized_title first. The raw `title` is whatever the filer typed
      // and is frequently a fragment — ARES Management's reads "10%", which
      // renders as "ARES Management LLC · 10%". normalized_title already
      // holds the cleaned "10% Owner".
      insiderTitle = data.normalized_title || data.insider_title || data.title || "";
      value = data.value ? ogMoney(Number(data.value)) : "";
      date = data.filing_date || "";
    }
  } catch {}

  const isBuy = tradeType === "BUY";
  const typeColor = isBuy ? "#22C55E" : "#EF4444";
  const typeBg = isBuy ? "rgba(34, 197, 94, 0.15)" : "rgba(239, 68, 68, 0.15)";

  const name = company.length > 44 ? `${company.slice(0, 42)}…` : company;
  const who = insiderTitle
    ? `${insiderName} · ${insiderTitle.length > 34 ? `${insiderTitle.slice(0, 32)}…` : insiderTitle}`
    : insiderName;

  return new ImageResponse(
    (
      // space-between rather than centre. Centring left the bottom ~45% of the
      // canvas empty while the one number anyone cares about sat in 20px grey
      // at the end of a row — the hierarchy was upside down.
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
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          <div style={{ display: "flex", width: "10px", height: "30px", backgroundColor: ACCENT, borderRadius: "2px" }} />
          <span style={{ fontSize: "24px", fontWeight: 700, color: INK, letterSpacing: "-0.5px" }}>Form4</span>
          <span style={{ fontSize: "20px", color: FAINT }}>SEC Form 4 filing</span>
        </div>

        <div style={{ display: "flex", flexDirection: "column" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "22px" }}>
            <span style={{ fontSize: "86px", fontWeight: 800, color: INK, letterSpacing: "-3px", lineHeight: 1 }}>
              {ticker}
            </span>
            <span
              style={{
                display: "flex",
                fontSize: "30px",
                fontWeight: 700,
                color: typeColor,
                backgroundColor: typeBg,
                borderRadius: "8px",
                padding: "8px 20px",
                border: `1px solid ${typeColor}`,
                letterSpacing: "1px",
              }}
            >
              {tradeType}
            </span>
          </div>
          {name && <span style={{ fontSize: "28px", color: MUTED, marginTop: "12px" }}>{name}</span>}
        </div>

        <div style={{ display: "flex", flexDirection: "column" }}>
          <div style={{ display: "flex", width: "100%", height: "1px", backgroundColor: RULE, marginBottom: "26px" }} />
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", width: "100%" }}>
            <div style={{ display: "flex", flexDirection: "column" }}>
              {/* The value is the headline fact of a filing card and now reads
                  like one. It was 20px grey next to the date. */}
              <span style={{ fontSize: "76px", fontWeight: 800, color: typeColor, lineHeight: 1, letterSpacing: "-2px" }}>
                {value}
              </span>
              <span style={{ fontSize: "24px", color: MUTED, marginTop: "14px" }}>{who}</span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end" }}>
              <span style={{ fontSize: "22px", color: FAINT, marginBottom: "14px" }}>{date}</span>
              <div style={{ display: "flex", alignItems: "center", backgroundColor: ACCENT, color: "#FFFFFF",
                            fontSize: "24px", fontWeight: 700, padding: "14px 28px", borderRadius: "10px" }}>
                See the full filing →
              </div>
            </div>
          </div>
        </div>
      </div>
    ),
    { ...size },
  );
}
