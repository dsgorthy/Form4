import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  async redirects() {
    return [
      {
        // Renamed 2026-08-13: the page never screened a universe, it showed
        // one entity's detail. "Explore" matches what it does; the old nav
        // dropdown that owned that word became "More". Query strings are
        // preserved by Next, so /screener?ticker=AAPL still lands correctly.
        source: "/screener",
        destination: "/explore",
        permanent: true,
      },
      {
        source: "/research/market-overview",
        destination: "/research",
        permanent: true,
      },
      {
        source: "/scoring",
        destination: "/research/methodology",
        permanent: true,
      },
      {
        source: "/dashboard",
        destination: "/portfolio",
        permanent: true,
      },
      {
        source: "/paper-trading",
        destination: "/portfolio",
        permanent: true,
      },
      {
        source: "/sells",
        destination: "/feed?trade_type=sell",
        permanent: true,
      },
      {
        source: "/signals",
        destination: "/feed",
        permanent: true,
      },
    ];
  },
};

export default nextConfig;
