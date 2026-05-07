import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  async redirects() {
    return [
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
    ];
  },
};

export default nextConfig;
