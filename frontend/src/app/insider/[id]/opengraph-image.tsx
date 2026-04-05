import { ImageResponse } from "next/og";

export const runtime = "edge";
export const alt = "Form4 Insider Profile";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

const GRADE_COLORS: Record<string, string> = {
  "A+": "#D97706", A: "#F59E0B", B: "#94A3B8", C: "#CD7F32", D: "#55556A",
};

export default async function Image({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let name = "Insider";
  let grade = "";
  let tickers = "—";
  let trades = "—";

  try {
    const resp = await fetch(`${API}/insiders/${id}`, { next: { revalidate: 3600 } });
    if (resp.ok) {
      const data = await resp.json();
      name = data.name || "Insider";
      grade = data.best_pit_grade || "";
      tickers = String(data.track_record?.n_tickers ?? "—");
      trades = String((data.track_record?.buy_count ?? 0) + (data.track_record?.sell_count ?? 0));
    }
  } catch {}

  const gradeColor = GRADE_COLORS[grade] || "#55556A";

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
        <div style={{ display: "flex", alignItems: "center", gap: "20px", marginBottom: "12px" }}>
          <span style={{ fontSize: "52px", fontWeight: "800", color: "#E8E8ED" }}>
            {name}
          </span>
          {grade && (
            <span
              style={{
                fontSize: "28px",
                fontWeight: "700",
                color: "#fff",
                backgroundColor: gradeColor,
                borderRadius: "8px",
                padding: "4px 14px",
              }}
            >
              {grade}
            </span>
          )}
        </div>
        <div style={{ fontSize: "24px", color: "#3B82F6", marginBottom: "40px" }}>
          Insider Profile
        </div>
        <div style={{ display: "flex", gap: "48px" }}>
          <div style={{ display: "flex", flexDirection: "column" }}>
            <span style={{ fontSize: "40px", fontWeight: "700", color: "#E8E8ED" }}>{trades}</span>
            <span style={{ fontSize: "16px", color: "#55556A", textTransform: "uppercase", letterSpacing: "2px" }}>trades</span>
          </div>
          <div style={{ display: "flex", flexDirection: "column" }}>
            <span style={{ fontSize: "40px", fontWeight: "700", color: "#E8E8ED" }}>{tickers}</span>
            <span style={{ fontSize: "16px", color: "#55556A", textTransform: "uppercase", letterSpacing: "2px" }}>companies</span>
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
