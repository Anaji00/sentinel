import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  allowedDevOrigins: ["192.168.56.1", "localhost", "*"],
  output: "standalone",
};

export default nextConfig;