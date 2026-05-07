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
    ];
  },
};

export default nextConfig;
