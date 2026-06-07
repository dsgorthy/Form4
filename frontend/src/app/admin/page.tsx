"use client";

import Link from "next/link";
import { useAuth, useUser } from "@clerk/nextjs";

const SECTIONS = [
  {
    href: "/admin/strategies",
    label: "Strategies",
    description: "Per-strategy filter pass/fail, freshness contracts, recent rejections, Alpaca reconciliation.",
  },
  {
    href: "/admin/pipelines",
    label: "Pipelines",
    description: "Structured run history (start/end/status/duration/rows) for every batch job that adopts framework.observability.pipeline_run().",
  },
  {
    href: "/admin/jobs",
    label: "System Jobs",
    description: "Real-time launchd job health (log-mtime based). Pre-pipeline_runs source of truth for services not yet instrumented.",
  },
];

export default function AdminIndexPage() {
  const { isSignedIn } = useAuth();
  const { user } = useUser();

  if (!isSignedIn) {
    return (
      <div className="text-[#E8E8ED] py-10">
        <h1 className="text-2xl font-bold">Admin</h1>
        <p className="text-[#8888A0] mt-2">Sign in to view.</p>
      </div>
    );
  }

  return (
    <div className="text-[#E8E8ED] py-6">
      <div className="mb-6">
        <h1 className="text-2xl font-bold">Admin</h1>
        <p className="text-sm text-[#55556A] mt-1">
          Private operational dashboards. Visible only to accounts in{" "}
          <code className="text-[#8888A0]">ADMIN_USER_IDS</code>. Signed in as{" "}
          <code className="text-[#8888A0]">{user?.id}</code>.
        </p>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        {SECTIONS.map((s) => (
          <Link
            key={s.href}
            href={s.href}
            className="block rounded-lg border border-[#2A2A3A] bg-[#12121A] p-5 hover:bg-[#1A1A26] transition"
          >
            <div className="flex items-baseline justify-between mb-2">
              <h2 className="text-lg font-semibold">{s.label}</h2>
              <code className="text-xs text-[#55556A]">{s.href}</code>
            </div>
            <p className="text-sm text-[#8888A0]">{s.description}</p>
          </Link>
        ))}
      </div>
    </div>
  );
}
