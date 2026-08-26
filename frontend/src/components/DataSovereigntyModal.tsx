"use client";

import React, { useState } from "react";

export const DataSovereigntyModal: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <>
      {/* Trigger Button */}
      <button
        onClick={() => setIsOpen(true)}
        className="inline-flex items-center gap-1.5 px-3 py-1 text-xs font-semibold rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 hover:bg-emerald-500/20 transition-colors shadow-sm"
        title="View Sentinel Data Sovereignty & Anti-Surveillance Manifest"
      >
        <span className="text-xs">🛡️</span>
        <span>Data Sovereignty: 100% Local</span>
      </button>

      {/* Modal Dialog */}
      {isOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80">
          <div className="flex flex-col gap-4 bg-slate-900 border border-slate-700 rounded-2xl max-w-2xl w-full p-6 shadow-2xl text-slate-100 max-h-[90vh] overflow-y-auto">
            {/* Header */}
            <div className="flex items-center justify-between border-b border-slate-800 pb-4">
              <div className="flex items-center gap-2.5">
                <span className="text-2xl">🔒</span>
                <div>
                  <h2 className="text-lg font-bold text-white">Sentinel Data Sovereignty & Anti-Surveillance Posture</h2>
                  <p className="text-xs text-slate-400">Verifiable local-first architecture breakdown</p>
                </div>
              </div>
              <button
                onClick={() => setIsOpen(false)}
                className="text-slate-400 hover:text-white text-lg font-bold p-1"
              >
                ✕
              </button>
            </div>

            {/* Architecture Highlights */}
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <div className="p-3.5 bg-emerald-950/20 border border-emerald-500/30 rounded-xl">
                <div className="flex items-center gap-1.5 text-xs font-bold text-emerald-400">
                  <span>🧠</span>
                  <span>100% Local LLM Inference</span>
                </div>
                <p className="text-xs text-slate-300 mt-1">
                  All hypothesis formulation and event synthesis runs via local Ollama instances (Llama 3 / Qwen 2.5). Zero prompts leave your machine.
                </p>
              </div>

              <div className="p-3.5 bg-sky-950/20 border border-sky-500/30 rounded-xl">
                <div className="flex items-center gap-1.5 text-xs font-bold text-sky-400">
                  <span>💾</span>
                  <span>Local Relational & Graph Storage</span>
                </div>
                <p className="text-xs text-slate-300 mt-1">
                  TimescaleDB hypertables, Neo4j knowledge graphs, and Qdrant embeddings reside strictly in local Docker/on-prem containers.
                </p>
              </div>

              <div className="p-3.5 bg-purple-950/20 border border-purple-500/30 rounded-xl">
                <div className="flex items-center gap-1.5 text-xs font-bold text-purple-400">
                  <span>🛰️</span>
                  <span>Outbound Read-Only Ingestion</span>
                </div>
                <p className="text-xs text-slate-300 mt-1">
                  SEC EDGAR, OpenSky ADS-B, and AIS feeds use anonymous read-only requests. No user portfolio balances or identity are ever transmitted.
                </p>
              </div>

              <div className="p-3.5 bg-amber-950/20 border border-amber-500/30 rounded-xl">
                <div className="flex items-center gap-1.5 text-xs font-bold text-amber-400">
                  <span>📜</span>
                  <span>Cryptographic Audit Trail</span>
                </div>
                <p className="text-xs text-slate-300 mt-1">
                  Every order submission, flag toggle, and watchlist mutation is immutably recorded in a SHA-256 hash-chained ledger.
                </p>
              </div>
            </div>

            {/* Prohibited Categories Box */}
            <div className="p-4 bg-slate-950 border border-rose-500/30 rounded-xl flex flex-col gap-2">
              <span className="text-xs font-bold text-rose-400 flex items-center gap-1.5">
                <span>🚫</span>
                <span>Permanent Anti-Surveillance Boundary</span>
              </span>
              <p className="text-xs text-slate-300 leading-relaxed">
                Sentinel strictly prohibits and rejects: mobile device carrier tracking, consumer location telemetry, facial recognition databases, and mass private-citizen communication interception.
              </p>
            </div>

            {/* Footer */}
            <div className="flex items-center justify-end pt-2 border-t border-slate-800">
              <button
                onClick={() => setIsOpen(false)}
                className="px-4 py-2 text-xs font-semibold rounded-lg bg-sky-600 hover:bg-sky-500 text-white transition-colors"
              >
                Understood & Verified
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
};

export default DataSovereigntyModal;
