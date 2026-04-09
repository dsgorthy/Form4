import Link from "next/link";
import { SignUpButton, SignInButton } from "@clerk/nextjs";
import { auth } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";

export const metadata = {
  title: "Form4 — Real-Time Insider Trading Intelligence",
  description:
    "Track SEC Form 4 insider buys and sells. AI-powered insider grades, validated trading strategies, and real-time cluster detection. 1.6M+ trades analyzed.",
};

async function getPreviewData() {
  const apiUrl = process.env.API_URL_INTERNAL || "http://localhost:8000/api/v1";
  try {
    const res = await fetch(`${apiUrl}/filings?limit=5&min_grade=B&trade_type=buy`, {
      next: { revalidate: 300 },
    });
    if (!res.ok) return [];
    const data = await res.json();
    return data.items || [];
  } catch {
    return [];
  }
}

function formatValue(v: number) {
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

export default async function LandingPage() {
  const { userId } = await auth();
  if (userId) redirect("/dashboard");

  const recentTrades = await getPreviewData();

  return (
    <div className="min-h-screen">
      {/* Hero */}
      <section className="px-4 pt-16 pb-20 text-center max-w-4xl mx-auto">
        <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold tracking-tight text-[#E8E8ED] leading-tight">
          Know what insiders know.
          <br />
          <span className="text-[#3B82F6]">Before the market does.</span>
        </h1>
        <p className="mt-6 text-lg sm:text-xl text-[#8888A0] max-w-2xl mx-auto">
          Real-time SEC Form 4 filings with AI-powered insider grades, cluster detection,
          and validated trading strategies. 1.6M+ trades analyzed since 2016.
        </p>
        <div className="mt-10 flex items-center justify-center gap-4">
          <SignUpButton mode="modal">
            <button className="rounded-lg bg-[#3B82F6] px-8 py-3 text-base font-semibold text-white hover:bg-[#2563EB] transition-colors">
              Start Free Trial
            </button>
          </SignUpButton>
          <SignInButton mode="modal">
            <button className="rounded-lg border border-[#2A2A3A] bg-[#12121A] px-8 py-3 text-base font-semibold text-[#E8E8ED] hover:border-[#3B82F6]/50 transition-colors">
              Sign In
            </button>
          </SignInButton>
        </div>
        <p className="mt-4 text-sm text-[#55556A]">7-day free trial. No credit card required.</p>
      </section>

      {/* Live Trades Preview */}
      {recentTrades.length > 0 && (
        <section className="px-4 pb-16 max-w-5xl mx-auto">
          <h2 className="text-center text-sm font-medium text-[#8888A0] uppercase tracking-wider mb-6">
            Recent Notable Insider Buys
          </h2>
          <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[#2A2A3A] text-[#55556A] text-xs uppercase">
                    <th className="text-left px-4 py-3 font-medium">Insider</th>
                    <th className="text-left px-4 py-3 font-medium">Ticker</th>
                    <th className="text-left px-4 py-3 font-medium">Grade</th>
                    <th className="text-right px-4 py-3 font-medium">Value</th>
                    <th className="text-left px-4 py-3 font-medium">Filed</th>
                  </tr>
                </thead>
                <tbody>
                  {recentTrades.map((t: any, i: number) => (
                    <tr key={i} className="border-b border-[#2A2A3A]/50 hover:bg-[#1A1A26]">
                      <td className="px-4 py-3 text-[#E8E8ED]">{t.insider_name || "—"}</td>
                      <td className="px-4 py-3">
                        <span className="font-mono font-semibold text-[#22C55E]">{t.ticker}</span>
                      </td>
                      <td className="px-4 py-3">
                        {t.trade_grade?.label ? (
                          <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                            t.trade_grade.label === "Exceptional" || t.trade_grade.label === "Strong"
                              ? "bg-[#22C55E]/10 text-[#22C55E]"
                              : "bg-[#8888A0]/10 text-[#8888A0]"
                          }`}>
                            {"★".repeat(t.trade_grade.stars || 0)} {t.trade_grade.label}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="px-4 py-3 text-right font-mono text-[#E8E8ED]">
                        {formatValue(t.value || 0)}
                      </td>
                      <td className="px-4 py-3 text-[#8888A0]">{t.filing_date || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="px-4 py-3 text-center border-t border-[#2A2A3A]">
              <SignUpButton mode="modal">
                <button className="text-sm text-[#3B82F6] hover:text-[#60A5FA] font-medium">
                  Sign up to see all filings in real time →
                </button>
              </SignUpButton>
            </div>
          </div>
        </section>
      )}

      {/* Features */}
      <section className="px-4 pb-20 max-w-5xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {[
            {
              title: "Insider Grades",
              desc: "Every insider scored A+ to F based on their historical track record. Know which insiders consistently beat the market.",
              icon: "🎯",
            },
            {
              title: "Cluster Detection",
              desc: "Real-time alerts when multiple insiders buy the same stock. Clusters historically outperform solo trades.",
              icon: "🔗",
            },
            {
              title: "Validated Strategies",
              desc: "3 backtested strategies running live paper portfolios. Quality Momentum: Sharpe 1.20, 68.7% win rate.",
              icon: "📊",
            },
          ].map((f) => (
            <div
              key={f.title}
              className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-6 hover:border-[#3B82F6]/30 transition-colors"
            >
              <div className="text-2xl mb-3">{f.icon}</div>
              <h3 className="text-lg font-semibold text-[#E8E8ED] mb-2">{f.title}</h3>
              <p className="text-sm text-[#8888A0] leading-relaxed">{f.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Stats Bar */}
      <section className="border-y border-[#2A2A3A] bg-[#12121A] py-12 px-4">
        <div className="max-w-4xl mx-auto grid grid-cols-2 md:grid-cols-4 gap-8 text-center">
          {[
            { value: "1.6M+", label: "Insider trades" },
            { value: "125K+", label: "Insiders tracked" },
            { value: "2016–now", label: "Data coverage" },
            { value: "< 5 min", label: "Filing delay" },
          ].map((s) => (
            <div key={s.label}>
              <div className="text-2xl sm:text-3xl font-bold text-[#E8E8ED]">{s.value}</div>
              <div className="text-sm text-[#8888A0] mt-1">{s.label}</div>
            </div>
          ))}
        </div>
      </section>

      {/* Pricing Preview */}
      <section className="px-4 py-20 max-w-4xl mx-auto text-center">
        <h2 className="text-2xl sm:text-3xl font-bold text-[#E8E8ED] mb-4">
          Start with a free trial
        </h2>
        <p className="text-[#8888A0] mb-10 max-w-xl mx-auto">
          Full access for 7 days. Then choose the plan that fits.
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-6 max-w-2xl mx-auto">
          <div className="rounded-xl border border-[#2A2A3A] bg-[#12121A] p-6 text-left">
            <div className="text-sm font-medium text-[#8888A0] mb-1">Free</div>
            <div className="text-3xl font-bold text-[#E8E8ED] mb-4">$0</div>
            <ul className="space-y-2 text-sm text-[#8888A0]">
              <li>Recent filings (90 days)</li>
              <li>Basic trade grades</li>
              <li>Company & insider pages</li>
            </ul>
          </div>
          <div className="rounded-xl border border-[#3B82F6]/50 bg-[#12121A] p-6 text-left ring-1 ring-[#3B82F6]/20">
            <div className="text-sm font-medium text-[#3B82F6] mb-1">Pro</div>
            <div className="text-3xl font-bold text-[#E8E8ED] mb-4">
              $25<span className="text-lg text-[#8888A0] font-normal">/mo</span>
            </div>
            <ul className="space-y-2 text-sm text-[#8888A0]">
              <li>Full history (2016+)</li>
              <li>Real-time filings (&lt; 5 min)</li>
              <li>Insider grades & track records</li>
              <li>Cluster detection & alerts</li>
              <li>Portfolio strategies</li>
            </ul>
            <SignUpButton mode="modal">
              <button className="mt-6 w-full rounded-lg bg-[#3B82F6] px-4 py-2.5 text-sm font-semibold text-white hover:bg-[#2563EB] transition-colors">
                Start Free Trial
              </button>
            </SignUpButton>
          </div>
        </div>
        <Link
          href="/pricing"
          className="inline-block mt-6 text-sm text-[#8888A0] hover:text-[#E8E8ED] transition-colors"
        >
          View full pricing & API plans →
        </Link>
      </section>
    </div>
  );
}
