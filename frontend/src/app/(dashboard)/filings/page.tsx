import React from "react";
import Institutional13FPanel from "@/components/Institutional13FPanel";

export const metadata = {
  title: "Corporate Filings & 13F Intelligence | Sentinel",
  description: "SEC EDGAR 8-K material event tracking, 13F institutional hedge fund portfolios, and regulatory disclosures.",
};

export default function FilingsPage() {
  return (
    <div className="flex flex-col gap-6 p-6 max-w-7xl mx-auto">
      {/* Top Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2.5">
          <span>📄</span>
          <span>Corporate Filings & 13F Institutional Intelligence</span>
        </h1>
        <p className="text-sm text-slate-400">
          Real-time primary source SEC EDGAR 8-K material events, quarterly 13F-HR institutional holdings, and regulatory disclosures.
        </p>
      </div>

      {/* Main 13F Institutional Intelligence Panel */}
      <Institutional13FPanel />
    </div>
  );
}
