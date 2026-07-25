import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  allowedDevOrigins: ["192.168.56.1", "localhost", "*"],
  output: "standalone",
  turbopack: {
    root: path.resolve(__dirname),
  },
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          {
            key: "Content-Security-Policy",
            value: "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob: ws: wss: http: https:; script-src 'self' 'unsafe-inline' 'unsafe-eval' http: https:; style-src 'self' 'unsafe-inline' https:; img-src 'self' data: blob: https:; connect-src 'self' ws: wss: http: https:;",
          },
        ],
      },
    ];
  },
};

export default nextConfig;