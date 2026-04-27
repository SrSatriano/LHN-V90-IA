import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/b3/:path*",
        destination: "http://127.0.0.1:9002/api/:path*",
      },
      {
        source: "/api/:path*",
        destination: "http://127.0.0.1:9002/api/:path*",
      },
    ];
  },
};

export default nextConfig;
