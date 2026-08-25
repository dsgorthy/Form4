import { ImageResponse } from "next/og";
import { ogMoney } from "@/lib/og-format";

export const runtime = "edge";
export const alt = "Form4 Insider Profile";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

const API = process.env.API_URL_INTERNAL || process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

const BG = "#0A0A0F";
const INK = "#E8E8ED";
const MUTED = "#8888A0";
const FAINT = "#81819A";
const RULE = "#23232E";
const ACCENT = "#3B82F6";
const POS = "#22C55E";
const NEG = "#EF4444";

const GRADE_COLORS: Record<string, string> = {
  "A+": "#D97706", A: "#F59E0B", B: "#94A3B8", C: "#CD7F32", D: "#81819A",
};

export default async function Image({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let name = "Insider";
  let grade = "";
  let role = "";
  let tickers = "—";
  let buys = 0;
  let sells = 0;

  try {
    const resp = await fetch(`${API}/insiders/${id}`, { next: { revalidate: 3600 } });
    if (resp.ok) {
      const data = await resp.json();
      name = data.name || "Insider";
      // career_grade first, and never pit_grade as a substitute. The order was
      // reversed until 2026-08-21, so a share card could show a letter the
      // insider's own page did not — and pit_grade is not monotonic, so it
      // could show a worse letter than the truth. An insider with no career
      // grade is Unrated; the card renders no badge rather than borrowing one.
      grade = data.best_career_grade || "";
      role = data.primary_title || data.title || "";
      const tr = data.track_record || {};
      tickers = String(tr.n_tickers ?? "—");
      buys = Number(tr.buy_count ?? 0);
      sells = Number(tr.sell_count ?? 0);
    }
  } catch {}

  const gradeColor = GRADE_COLORS[grade] || FAINT;
  // Long entity names ("Control Empresarial de Capitales S.A. de C.V.") wrap
  // and shove the stat row off the canvas.
  const display = name.length > 34 ? `${name.slice(0, 32)}…` : name;

  // Conversion text. The card carried labelled numbers and no sentence, so a
  // reader had to assemble the story themselves — and a card that requires
  // assembly does not get clicked. "0 buys, 833 sells across 2 companies" is
  // the story; the stat row below is the evidence for it.
  const nCo = tickers === "—" ? "" : ` across ${tickers} ${tickers === "1" ? "company" : "companies"}`;
  const hook = buys || sells
    ? `${buys.toLocaleString()} ${buys === 1 ? "buy" : "buys"} · ${sells.toLocaleString()} ${sells === 1 ? "sell" : "sells"}${nCo}`
    : "SEC Form 4 filing history";

  const stats: { value: string; label: string; color: string }[] = [
    { value: String(buys + sells), label: "trades", color: INK },
    { value: tickers, label: "companies", color: INK },
  ];
  // Buys and sells split out, because "833 trades" reads as activity while
  // "0 buys / 833 sells" reads as a position — and that is the whole story on
  // an entity like Magnetar.
  if (buys || sells) {
    stats.push({ value: String(buys), label: "buys", color: buys ? POS : FAINT });
    stats.push({ value: String(sells), label: "sells", color: sells ? NEG : FAINT });
  }

  return new ImageResponse(
    (
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
          <span style={{ fontSize: "20px", color: FAINT }}>Insider profile</span>
        </div>

        <div style={{ display: "flex", flexDirection: "column" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "20px" }}>
            <span style={{ fontSize: "68px", fontWeight: 800, color: INK, letterSpacing: "-2px", lineHeight: 1.1 }}>
              {display}
            </span>
            {grade && (
              <span
                style={{
                  display: "flex",
                  fontSize: "30px",
                  fontWeight: 800,
                  color: gradeColor,
                  border: `2px solid ${gradeColor}`,
                  borderRadius: "10px",
                  padding: "6px 18px",
                }}
              >
                {grade}
              </span>
            )}
          </div>
          <span style={{ fontSize: "34px", color: INK, marginTop: "16px" }}>{hook}</span>
          {role && <span style={{ fontSize: "24px", color: MUTED, marginTop: "8px" }}>{role}</span>}
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
            {/* Reads as a button. It is not clickable — the whole card is —
                but the affordance is what tells a scroller there is somewhere
                to go, which is the single thing the card was missing. */}
            <div style={{ display: "flex", flexDirection: "column", justifyContent: "flex-end" }}>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  backgroundColor: ACCENT,
                  color: "#FFFFFF",
                  fontSize: "24px",
                  fontWeight: 700,
                  padding: "14px 28px",
                  borderRadius: "10px",
                }}
              >
                See every filing →
              </div>
            </div>
          </div>
        </div>
      </div>
    ),
    { ...size },
  );
}
