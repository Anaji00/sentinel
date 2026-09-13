"use client";

import React, { useState, useEffect } from "react";
import { ProvenanceBadge, type ProvenanceType } from "./ProvenanceBadge";
import { ProvenanceValue } from "./ProvenanceValue";
import { formatPercent } from '../lib/format';

interface Position {
  ticker?: string;
  cusip?: string;
  issuer_name: string;
  class_title: string;
  market_value_usd: number;
  shares: number;
  weight_pct: number;
  change_type: "NEW" | "INCREASED" | "DECREASED" | "EXITED" | "MAINTAINED";
  change_pct: number;
  prev_shares?: number;
}

interface FilerSummary {
  filer_id: string;
  filer_name: string;
  manager_name: string;
  cik: string;
  style: string;
  total_value_usd: number;
  holdings_count: number;
  top_10_concentration_pct: number;
  report_period: string;
  is_synthetic?: boolean;
  source_type?: string;
}

interface PortfolioReport extends FilerSummary {
  new_positions: Position[];
  exited_positions: Position[];
  increased_positions: Position[];
  decreased_positions: Position[];
  top_holdings: Position[];
  filing_url?: string;
  is_synthetic?: boolean;
  source_type?: string;
}

// No hardcoded roster.
//
// This held eight funds with invented aggregates -- Berkshire at
// $247,100,000,000 across 41 positions, 88.4% concentration -- and offered
// Scion Asset Management, which the gateway's prominent-filer list does not
// contain, so the dropdown named a fund the platform cannot serve. The roster
// now comes from /13f/prominent, which returns ten real filers with real CIKs.
const NO_FILERS: FilerSummary[] = [];

/** The provenance label for a report, defaulting to the weakest.
 *
 * Each badge previously read
 *   source_type || (is_synthetic ? "disclosed_placeholder" : "live_measurement")
 * and both fields are optional. On the fallback path -- which ran on every
 * render, because the fetch went to a URL with no route -- neither was set, so
 * the chain evaluated to "live_measurement" and certified invented holdings as
 * a live measurement, five times on one panel. A `||` chain that treats absent
 * provenance as proven provenance is the exact inversion of what a provenance
 * badge is for.
 */
function provenanceOf(report: PortfolioReport | null): ProvenanceType {
  if (!report) return "disclosed_placeholder";
  if (report.source_type) return report.source_type as any;
  if (report.is_synthetic === false) return "live_measurement";
  // Unknown provenance is not proven provenance.
  return "disclosed_placeholder";
}

export const Institutional13FPanel: React.FC = () => {
  const [filers, setFilers] = useState<FilerSummary[]>(NO_FILERS);
  const [selectedFilerId, setSelectedFilerId] = useState<string>("");
  const [report, setReport] = useState<PortfolioReport | null>(null);
  const [activeTab, setActiveTab] = useState<"top" | "new" | "increased" | "decreased" | "exited">("top");
  const [loading, setLoading] = useState<boolean>(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  // The roster the gateway actually serves.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch("/api/proxy/api/v1/filings/13f/prominent");
        if (!res.ok) return;
        const rows = await res.json();
        if (cancelled || !Array.isArray(rows) || rows.length === 0) return;
        setFilers(rows);
        setSelectedFilerId((current) => current || rows[0].filer_id);
      } catch {
        /* leave the roster empty rather than inventing one */
      }
    })();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    async function fetchFilerData() {
      if (!selectedFilerId) { setLoading(false); return; }
      setLoading(true);
      setLoadError(null);
      try {
        const res = await fetch(`/api/proxy/api/v1/filings/13f/${selectedFilerId}`);
        if (res.ok) {
          const data = await res.json();
          setReport(data);
        } else {
          // No invented holdings.
          //
          // This rendered DEFAULT_FILERS plus a hardcoded top-ten -- AAPL at
          // 300,000,000 shares, "DECREASED -25.0%" -- attributed to Warren
          // Buffett under a subtitle reading "Quarterly SEC Form 13F-HR". It
          // ran on every render, because the fetch went to /api/v1/... on the
          // Next.js origin where no route exists, so the 404 was the normal
          // path. The request now goes through the proxy; a genuine failure
          // shows nothing rather than something made up.
          setReport(null);
          setLoadError(`Could not load 13F data (HTTP ${res.status}).`);
        }
      } catch (err) {
        console.error("Error fetching 13F:", err);
      } finally {
        setLoading(false);
      }
    }

    fetchFilerData();
  }, [selectedFilerId, filers]);

  const renderBadge = (changeType: string, changePct: number) => {
    switch (changeType) {
      case "NEW":
        return <span className="px-2 py-0.5 text-xs font-semibold rounded bg-emerald-500/20 text-emerald-400 border border-emerald-500/40">NEW</span>;
      case "INCREASED":
        return <span className="px-2 py-0.5 text-xs font-semibold rounded bg-cyan-500/20 text-cyan-400 border border-cyan-500/40">+{changePct.toFixed(1)}%</span>;
      case "DECREASED":
        return <span className="px-2 py-0.5 text-xs font-semibold rounded bg-amber-500/20 text-amber-400 border border-amber-500/40">{changePct.toFixed(1)}%</span>;
      case "EXITED":
        return <span className="px-2 py-0.5 text-xs font-semibold rounded bg-rose-500/20 text-rose-400 border border-rose-500/40">EXITED</span>;
      default:
        return <span className="px-2 py-0.5 text-xs font-medium rounded bg-slate-800 text-slate-400 border border-slate-700">HOLD</span>;
    }
  };

  const getActivePositions = (): Position[] => {
    if (!report) return [];
    if (activeTab === "top") return report.top_holdings || [];
    if (activeTab === "new") return report.new_positions || [];
    if (activeTab === "increased") return report.increased_positions || [];
    if (activeTab === "decreased") return report.decreased_positions || [];
    if (activeTab === "exited") return report.exited_positions || [];
    return report.top_holdings || [];
  };

  const activePositions = getActivePositions();

  return (
    <div className="flex flex-col gap-4 bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-2xl text-slate-100">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 pb-4">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xl">🏛️</span>
            <h2 className="text-lg font-bold tracking-wide text-white">13F Institutional Intelligence & Ownership Flow</h2>
            <ProvenanceBadge
              sourceType={provenanceOf(report)}
            />
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Quarterly SEC Form 13F-HR portfolio holdings & QoQ changes from premier asset managers
          </p>
          {loadError && (
            <p className="text-xs text-amber-400 mt-1">{loadError} Nothing is shown rather than a placeholder.</p>
          )}
        </div>
        {report?.filing_url && (
          <a
            href={report.filing_url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold rounded-lg bg-sky-500/10 text-sky-400 border border-sky-500/30 hover:bg-sky-500/20 transition-colors"
          >
            <span>Primary SEC EDGAR</span>
            <span>↗</span>
          </a>
        )}
      </div>

      {/* Filer Selector Carousel */}
      <div className="flex items-center gap-2 overflow-x-auto pb-2 scrollbar-thin scrollbar-thumb-slate-700">
        {filers.map((f) => {
          const isSelected = f.filer_id === selectedFilerId;
          return (
            <button
              key={f.filer_id}
              onClick={() => setSelectedFilerId(f.filer_id)}
              className={`flex flex-col items-start px-3.5 py-2 rounded-lg border text-left whitespace-nowrap transition-all ${
                isSelected
                  ? "bg-sky-600/20 border-sky-500 text-sky-200 shadow-md shadow-sky-500/10"
                  : "bg-slate-950/60 border-slate-800 text-slate-400 hover:border-slate-700 hover:text-slate-200"
              }`}
            >
              <span className="text-xs font-bold text-white">{f.manager_name}</span>
              <span className="text-[11px] text-slate-400">{f.filer_name.slice(0, 22)}</span>
            </button>
          );
        })}
      </div>

      {/* Manager Summary Metrics */}
      {report && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div className="p-3 bg-slate-950/70 border border-slate-800 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-[11px] text-slate-400 font-medium">Total Portfolio Value</span>
              <ProvenanceBadge sourceType={provenanceOf(report)} />
            </div>
            <div className="text-base font-bold text-emerald-400 mt-0.5">
              ${(report.total_value_usd / 1e9).toFixed(2)}B
            </div>
          </div>
          <div className="p-3 bg-slate-950/70 border border-slate-800 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-[11px] text-slate-400 font-medium">Report Period</span>
              <ProvenanceBadge sourceType={provenanceOf(report)} />
            </div>
            <div className="text-base font-bold text-sky-400 mt-0.5">{report.report_period}</div>
          </div>
          <div className="p-3 bg-slate-950/70 border border-slate-800 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-[11px] text-slate-400 font-medium">Holdings Count</span>
              <ProvenanceBadge sourceType={provenanceOf(report)} />
            </div>
            <div className="text-base font-bold text-slate-200 mt-0.5">{report.holdings_count} positions</div>
          </div>
          <div className="p-3 bg-slate-950/70 border border-slate-800 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-[11px] text-slate-400 font-medium">Top 10 Concentration</span>
              <ProvenanceBadge sourceType={provenanceOf(report)} />
            </div>
            <div className="text-base font-bold text-purple-400 mt-0.5">{formatPercent(report.top_10_concentration_pct, { decimals: 1 })}</div>
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="flex items-center gap-2 border-b border-slate-800 pt-1">
        <button
          onClick={() => setActiveTab("top")}
          className={`px-3 py-1.5 text-xs font-semibold border-b-2 transition-colors ${
            activeTab === "top"
              ? "border-sky-400 text-sky-300"
              : "border-transparent text-slate-400 hover:text-slate-200"
          }`}
        >
          Top Holdings ({report?.top_holdings?.length || 0})
        </button>
        <button
          onClick={() => setActiveTab("new")}
          className={`px-3 py-1.5 text-xs font-semibold border-b-2 transition-colors ${
            activeTab === "new"
              ? "border-emerald-400 text-emerald-300"
              : "border-transparent text-slate-400 hover:text-slate-200"
          }`}
        >
          New Stakes ({report?.new_positions?.length || 0})
        </button>
        <button
          onClick={() => setActiveTab("increased")}
          className={`px-3 py-1.5 text-xs font-semibold border-b-2 transition-colors ${
            activeTab === "increased"
              ? "border-cyan-400 text-cyan-300"
              : "border-transparent text-slate-400 hover:text-slate-200"
          }`}
        >
          Increased ({report?.increased_positions?.length || 0})
        </button>
        <button
          onClick={() => setActiveTab("decreased")}
          className={`px-3 py-1.5 text-xs font-semibold border-b-2 transition-colors ${
            activeTab === "decreased"
              ? "border-amber-400 text-amber-300"
              : "border-transparent text-slate-400 hover:text-slate-200"
          }`}
        >
          Trimmed ({report?.decreased_positions?.length || 0})
        </button>
        <button
          onClick={() => setActiveTab("exited")}
          className={`px-3 py-1.5 text-xs font-semibold border-b-2 transition-colors ${
            activeTab === "exited"
              ? "border-rose-400 text-rose-300"
              : "border-transparent text-slate-400 hover:text-slate-200"
          }`}
        >
          Exited ({report?.exited_positions?.length || 0})
        </button>
      </div>

      {/* Holdings Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-left text-xs border-collapse">
          <thead>
            <tr className="border-b border-slate-800 text-slate-400 font-semibold bg-slate-950/40">
              <th className="py-2.5 px-3">Ticker / Company</th>
              <th className="py-2.5 px-3">Market Value ($M)</th>
              <th className="py-2.5 px-3">Shares</th>
              <th className="py-2.5 px-3">Weight</th>
              <th className="py-2.5 px-3">QoQ Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60">
            {activePositions.length === 0 ? (
              <tr>
                <td colSpan={5} className="text-center py-6 text-slate-500">
                  No positions found in this category for {report?.report_period}.
                </td>
              </tr>
            ) : (
              activePositions.map((pos, idx) => (
                <tr key={`${pos.ticker || pos.cusip || idx}`} className="hover:bg-slate-800/30 transition-colors">
                  <td className="py-2.5 px-3">
                    <div className="flex items-center gap-2">
                      <span className="font-bold text-sky-400 bg-sky-500/10 px-1.5 py-0.5 rounded border border-sky-500/30">
                        {pos.ticker || "N/A"}
                      </span>
                      <span className="text-slate-200 font-medium">{pos.issuer_name}</span>
                    </div>
                  </td>
                  <td className="py-2.5 px-3 font-semibold text-emerald-400">
                    ${(pos.market_value_usd / 1e6).toFixed(2)}M
                  </td>
                  <td className="py-2.5 px-3 text-slate-300">
                    {pos.shares.toLocaleString()}
                  </td>
                  <td className="py-2.5 px-3 text-slate-300 font-medium">
                    {formatPercent(pos.weight_pct, { decimals: 2 })}
                  </td>
                  <td className="py-2.5 px-3">
                    {renderBadge(pos.change_type, pos.change_pct)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default Institutional13FPanel;
