import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  allowedDevOrigins: ["192.168.56.1", "localhost", "*"],
  output: "standalone",
  turbopack: {
    root: path.resolve(__dirname),
  },
  // Pages folded into others keep their old addresses working. Eleven pages
  // became four (Calls, Markets, Performance, Intel); each old page is now a
  // view of the one that absorbed it. A rule with `has: view` passes the old
  // ?view= through (redirects keep the query) and must come before the bare
  // rule for the same source, because the first match wins.
  async redirects() {
    return [
      { source: "/predictions", destination: "/calls?view=record", permanent: false },
      { source: "/board", destination: "/calls", permanent: false },
      { source: "/signals", destination: "/calls?view=signals", permanent: false },
      { source: "/charts", destination: "/markets", permanent: false },
      { source: "/crypto", destination: "/markets?view=crypto", permanent: false },
      { source: "/options", has: [{ type: "query", key: "view" }], destination: "/markets", permanent: false },
      { source: "/options", destination: "/markets?view=flow", permanent: false },
      { source: "/book", destination: "/performance", permanent: false },
      { source: "/strategies", destination: "/performance#years", permanent: false },
      { source: "/intelligence", has: [{ type: "query", key: "view", value: "osint" }], destination: "/intel", permanent: false },
      { source: "/intelligence", destination: "/intel?view=console", permanent: false },
      { source: "/map", destination: "/intel", permanent: false },
      { source: "/cases", has: [{ type: "query", key: "view" }], destination: "/intel", permanent: false },
      { source: "/cases", destination: "/intel?view=cases", permanent: false },
      { source: "/flow", destination: "/markets?view=flow", permanent: false },
      { source: "/ticker", has: [{ type: "query", key: "symbol", value: "(?<symbol>[A-Za-z0-9.-]{1,10})" }], destination: "/calls?t=:symbol", permanent: false },
      { source: "/ticker", destination: "/calls", permanent: false },
      { source: "/osint", destination: "/intel?view=osint", permanent: false },
      { source: "/macro", destination: "/markets?view=economy", permanent: false },
      { source: "/reports", destination: "/intel?view=briefs", permanent: false },
      { source: "/attribution", destination: "/performance#years", permanent: false },
    ];
  },
  async headers() {
    return [
      {
        // Browsers already revalidate a worker script, but an intermediate
        // cache holding an old sw.js would pin phones to old caching rules.
        source: "/sw.js",
        headers: [{ key: "Cache-Control", value: "no-cache, no-store, must-revalidate" }],
      },
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