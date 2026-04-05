import { ImageResponse } from "next/og";

export const runtime = "edge";
export const alt = "Form4 SEC Filing Detail";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export default async function Image({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let ticker = "—";
  let company = "";
  let tradeType = "BUY";
  let insiderName = "Insider";
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
      value = data.value ? `$${(data.value / 1000).toFixed(0)}K` : "";
      date = data.filing_date || "";
    }
  } catch {}

  const isBuy = tradeType === "BUY";
  const typeColor = isBuy ? "#22C55E" : "#EF4444";
  const typeBg = isBuy ? "rgba(34, 197, 94, 0.15)" : "rgba(239, 68, 68, 0.15)";

  return new ImageResponse(
    (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          width: "100%",
          height: "100%",
          backgroundColor: "#0A0A0F",
          padding: "60px 80px",
          fontFamily: "system-ui, sans-serif",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "20px", marginBottom: "8px" }}>
          <span style={{ fontSize: "72px", fontWeight: "800", color: "#E8E8ED", letterSpacing: "-2px" }}>
            {ticker}
          </span>
          <span
            style={{
              fontSize: "28px",
              fontWeight: "700",
              color: typeColor,
              backgroundColor: typeBg,
              borderRadius: "8px",
              padding: "6px 16px",
              border: `1px solid ${typeColor}`,
            }}
          >
            {tradeType}
          </span>
        </div>
        {company && (
          <div style={{ fontSize: "24px", color: "#8888A0", marginBottom: "24px" }}>
            {company}
          </div>
        )}
        <div style={{ fontSize: "32px", color: "#E8E8ED", marginBottom: "8px" }}>
          {insiderName}
        </div>
        <div style={{ display: "flex", gap: "24px", fontSize: "20px", color: "#55556A" }}>
          {value && <span>{value}</span>}
          {date && <span>{date}</span>}
        </div>
        <div style={{ position: "absolute", bottom: "40px", right: "60px", display: "flex", alignItems: "center", gap: "8px" }}>
          <span style={{ fontSize: "20px", fontWeight: "700", color: "#3B82F6" }}>Form4</span>
          <span style={{ fontSize: "16px", color: "#55556A" }}>form4.app</span>
        </div>
      </div>
    ),
    { ...size },
  );
}
