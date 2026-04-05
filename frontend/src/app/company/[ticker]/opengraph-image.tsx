import { ImageResponse } from "next/og";

export const runtime = "edge";
export const alt = "Form4 Company Insider Trading Data";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export default async function Image({ params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;

  let company = ticker;
  let trades = "—";
  let insiders = "—";

  try {
    const resp = await fetch(`${API}/companies/${ticker}`, { next: { revalidate: 3600 } });
    if (resp.ok) {
      const data = await resp.json();
      company = data.company || ticker;
      trades = String(data.total_trades ?? "—");
      insiders = String(data.insiders?.length ?? "—");
    }
  } catch {}

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
        <div style={{ display: "flex", alignItems: "baseline", gap: "16px", marginBottom: "16px" }}>
          <span style={{ fontSize: "72px", fontWeight: "800", color: "#E8E8ED", letterSpacing: "-2px" }}>
            {ticker}
          </span>
          <span style={{ fontSize: "28px", color: "#3B82F6", fontWeight: "600" }}>
            Insider Trading
          </span>
        </div>
        <div style={{ fontSize: "28px", color: "#8888A0", marginBottom: "40px" }}>
          {company}
        </div>
        <div style={{ display: "flex", gap: "48px" }}>
          <div style={{ display: "flex", flexDirection: "column" }}>
            <span style={{ fontSize: "40px", fontWeight: "700", color: "#E8E8ED" }}>{trades}</span>
            <span style={{ fontSize: "16px", color: "#55556A", textTransform: "uppercase", letterSpacing: "2px" }}>trades</span>
          </div>
          <div style={{ display: "flex", flexDirection: "column" }}>
            <span style={{ fontSize: "40px", fontWeight: "700", color: "#E8E8ED" }}>{insiders}</span>
            <span style={{ fontSize: "16px", color: "#55556A", textTransform: "uppercase", letterSpacing: "2px" }}>insiders</span>
          </div>
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
