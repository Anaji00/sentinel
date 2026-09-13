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
            // `http:` and `https:` as scheme sources match every host, so the
            // previous policy permitted loading and evaluating script from
            // anywhere and connecting anywhere -- not a weak CSP but the
            // absence of one expressed as a header, which a reviewer counting
            // headers would find present. It was also what made the browser's
            // direct calls to Coinbase, Polymarket, CISA and Yahoo-via-CORS-
            // proxy possible; those are gone, so the policy can say so.
            //
            // 'unsafe-inline' and 'unsafe-eval' remain on script-src: Next.js
            // inlines its bootstrap and the app has no nonce pipeline yet.
            // Narrowing the origins is the part that is available today.
            value: [
              "default-src 'self'",
              "script-src 'self' 'unsafe-inline' 'unsafe-eval'",
              "style-src 'self' 'unsafe-inline'",
              "img-src 'self' data: blob:",
              "font-src 'self' data:",
              // Same-origin XHR plus the WebSocket the live feed opens back
              // through the ingress.
              "connect-src 'self' ws: wss:",
              "frame-ancestors 'self'",
              "base-uri 'self'",
              "form-action 'self'",
              "object-src 'none'",
            ].join("; "),
          },
        ],
      },
    ];
  },
};

export default nextConfig;