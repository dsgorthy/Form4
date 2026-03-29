"use client";

import { useEffect, useState } from "react";

function computeRelative(dateStr: string): string {
  const now = new Date();
  let date: Date;

  // If it's a full datetime (filed_at), parse directly; otherwise treat as date-only
  if (dateStr.includes(" ") || dateStr.includes("T")) {
    // filed_at is UTC from SEC (e.g. "2026-03-13 17:25:29")
    date = new Date(dateStr.replace(" ", "T") + "Z");
  } else {
    // date-only: use noon to avoid timezone edge cases
    date = new Date(dateStr + "T12:00:00");
  }

  const diffMs = now.getTime() - date.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDays = Math.floor(diffHr / 24);
  if (diffDays <= 7) return `${diffDays}d ago`;
  const month = date.toLocaleString("en-US", { month: "short" });
  const day = date.getDate();
  const year = date.getFullYear();
  if (year === now.getFullYear()) return `${month} ${day}`;
  return `${month} ${day}, ${year}`;
}

export function RelativeTime({ date }: { date: string | null | undefined }) {
  const [text, setText] = useState(date ?? "—");

  useEffect(() => {
    if (date) setText(computeRelative(date));
  }, [date]);

  if (!date) return <>—</>;
  return <>{text}</>;
}
