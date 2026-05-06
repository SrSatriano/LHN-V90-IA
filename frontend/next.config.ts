import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/b3/:path*",
        destination: "http://127.0.0.1:9002/api/:path*",
      },
      // Inclui /api/chat: não adicione app/api/chat/route.ts — uma rota App Router
      // tem precedência e contornaria LHN_API_KEY e o enriquecimento em server.py.
      {
        source: "/api/:path*",
        destination: "http://127.0.0.1:9002/api/:path*",
      },
    ];
  },
};

export default nextConfig;
