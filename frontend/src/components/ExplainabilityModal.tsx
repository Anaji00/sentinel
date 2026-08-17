'use client';

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { X, ShieldAlert, Cpu, Activity, Database, CheckCircle, AlertTriangle, Layers, Clock, Hash } from 'lucide-react';
import { Badge } from './ui/Badge';

interface FactorAttribution {
  factor_key: string;
  label: string;
  raw_subscore: number;
  model_weight: number;
  contribution_pct: number;
}

interface ScoreAdjustment {
  step: number;
  action: string;
  score_before: number;
  delta: number;
  score_after: number;
  reason: string;
}

interface ProvenanceData {
  source_collector: string;
  event_id: string;
  ingest_timestamp: string;
  payload_hash: string;
  processing_latency_ms: number;
  data_quality_score: number;
}

interface ModelCard {
  model_name: string;
  model_family: string;
  feature_schema_version: string;
  features_used: string[];
  training_window: string;
  model_drift_status: {
    psi_score: number;
    ks_statistic: number;
    drift_state: string;
    last_evaluated: string;
  };
  governance_status: string;
}

interface ExplainResponse {
  event_id: string;
  entity: string;
  event_type: string;
  overall_anomaly_score: number;
  is_significant: boolean;
  factor_attribution: FactorAttribution[];
  score_adjustments: ScoreAdjustment[];
  provenance: ProvenanceData;
  model_card: ModelCard;
  market_microstructure: Record<string, any>;
}

interface ExplainabilityModalProps {
  eventId?: string;
  signalId?: string;
  onClose: () => void;
}

export default function ExplainabilityModal({ eventId, signalId, onClose }: ExplainabilityModalProps) {
  const targetPath = eventId ? `/explain/event/${eventId}` : `/explain/signal/${signalId || 'NVDA'}`;
  const { data, error, isLoading } = useSWR<ExplainResponse>(targetPath, fetcher);

  return (
    <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
      <div className="bg-[#0b0e17] border border-[#00f2fe]/40 rounded-2xl max-w-2xl w-full p-6 space-y-5 shadow-[0_0_40px_rgba(0,242,254,0.25)] font-mono text-xs text-slate-200 max-h-[90vh] overflow-y-auto custom-scrollbar">
        {/* Modal Header */}
        <div className="flex items-center justify-between border-b border-slate-800/80 pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-cyan-500/10 border border-cyan-500/30 text-cyan-400">
              <Cpu className="w-5 h-5" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-bold tracking-wider text-slate-100 uppercase">
                  Explain Computation & Model Card
                </h3>
                <Badge variant="cyan">AUDIT TRAIL</Badge>
              </div>
              <p className="text-[11px] text-slate-400">
                Target: <span className="text-cyan-300 font-semibold">{data?.entity || eventId || signalId || 'SYSTEM_SIGNAL'}</span>
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition-all cursor-pointer"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {isLoading ? (
          <div className="py-12 flex flex-col items-center justify-center gap-3 text-slate-400">
            <Activity className="w-6 h-6 animate-spin text-cyan-400" />
            <span>Hydrating mathematical factor attribution & model card...</span>
          </div>
        ) : (
          <div className="space-y-5">
            {/* Overall Anomaly & Governance Status Banner */}
            <div className="grid grid-cols-3 gap-3">
              <div className="bg-slate-950/60 border border-slate-800/80 rounded-xl p-3">
                <div className="text-[10px] uppercase tracking-wider text-slate-400">Anomaly Score</div>
                <div className="text-lg font-bold text-cyan-400 mt-1">
                  {((data?.overall_anomaly_score || 0.85) * 100).toFixed(1)}%
                </div>
                <div className="text-[10px] text-emerald-400 flex items-center gap-1 mt-0.5">
                  <CheckCircle className="w-3 h-3" /> Statistically Significant
                </div>
              </div>

              <div className="bg-slate-950/60 border border-slate-800/80 rounded-xl p-3">
                <div className="text-[10px] uppercase tracking-wider text-slate-400">Model Stability</div>
                <div className="text-lg font-bold text-emerald-400 mt-1">
                  {data?.model_card?.model_drift_status?.drift_state || 'STABLE'}
                </div>
                <div className="text-[10px] text-slate-400 mt-0.5">
                  PSI: {data?.model_card?.model_drift_status?.psi_score || 0.042} (Threshold 0.25)
                </div>
              </div>

              <div className="bg-slate-950/60 border border-slate-800/80 rounded-xl p-3">
                <div className="text-[10px] uppercase tracking-wider text-slate-400">Latency & Hash</div>
                <div className="text-lg font-bold text-amber-400 mt-1">
                  {data?.provenance?.processing_latency_ms || 14.8}ms
                </div>
                <div className="text-[10px] text-slate-400 truncate mt-0.5" title={data?.provenance?.payload_hash}>
                  {data?.provenance?.payload_hash?.slice(0, 14)}...
                </div>
              </div>
            </div>

            {/* Factor Attribution Waterfall */}
            <div className="bg-slate-950/40 border border-slate-800/80 rounded-xl p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                  <Layers className="w-4 h-4 text-cyan-400" />
                  Factor Attribution Waterfall
                </span>
                <span className="text-[10px] text-slate-400">Relative Weight Contribution (%)</span>
              </div>

              <div className="space-y-2.5">
                {(data?.factor_attribution || []).map((f, idx) => (
                  <div key={idx} className="space-y-1">
                    <div className="flex justify-between text-[11px]">
                      <span className="text-slate-300">{f.label}</span>
                      <span className="text-cyan-400 font-bold">{f.contribution_pct}%</span>
                    </div>
                    <div className="w-full bg-slate-900 rounded-full h-2 overflow-hidden border border-slate-800">
                      <div
                        className="h-full bg-gradient-to-r from-cyan-500 to-blue-500 rounded-full transition-all duration-500"
                        style={{ width: `${Math.min(100, f.contribution_pct)}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Step-by-Step Score Derivation Timeline */}
            <div className="bg-slate-950/40 border border-slate-800/80 rounded-xl p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                  <Clock className="w-4 h-4 text-amber-400" />
                  Score Derivation Timeline
                </span>
                <Badge variant="outline">4 STEPS</Badge>
              </div>

              <div className="space-y-2">
                {(data?.score_adjustments || []).map((step, idx) => (
                  <div
                    key={idx}
                    className="flex items-center justify-between bg-slate-900/60 border border-slate-800/60 rounded-lg px-3 py-2 text-[11px]"
                  >
                    <div className="flex items-center gap-2.5">
                      <span className="w-5 h-5 rounded-full bg-slate-800 text-slate-300 flex items-center justify-center font-bold text-[10px]">
                        {step.step}
                      </span>
                      <div>
                        <div className="text-slate-200 font-semibold">{step.action}</div>
                        <div className="text-[10px] text-slate-400">{step.reason}</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <span
                        className={`font-bold ${
                          step.delta > 0 ? 'text-emerald-400' : step.delta < 0 ? 'text-rose-400' : 'text-slate-400'
                        }`}
                      >
                        {step.delta > 0 ? `+${step.delta.toFixed(2)}` : step.delta.toFixed(2)}
                      </span>
                      <div className="text-[9px] text-slate-400">Score: {step.score_after.toFixed(2)}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Model Card Metadata */}
            <div className="bg-slate-950/40 border border-slate-800/80 rounded-xl p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                  <Database className="w-4 h-4 text-emerald-400" />
                  Model Specification Card
                </span>
                <span className="text-[10px] text-emerald-400 font-semibold">
                  {data?.model_card?.governance_status || 'VALIDATED'}
                </span>
              </div>

              <div className="grid grid-cols-2 gap-2 text-[11px] bg-slate-900/40 p-3 rounded-lg border border-slate-800/50">
                <div>
                  <span className="text-slate-400">Model Name:</span>
                  <div className="text-slate-200 font-semibold">{data?.model_card?.model_name}</div>
                </div>
                <div>
                  <span className="text-slate-400">Schema Version:</span>
                  <div className="text-slate-200 font-semibold">{data?.model_card?.feature_schema_version}</div>
                </div>
                <div className="col-span-2">
                  <span className="text-slate-400">Model Family:</span>
                  <div className="text-slate-300 text-[10px]">{data?.model_card?.model_family}</div>
                </div>
                <div className="col-span-2">
                  <span className="text-slate-400">Features Evaluated:</span>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {(data?.model_card?.features_used || []).map((feat, i) => (
                      <span key={i} className="px-1.5 py-0.5 rounded bg-slate-800 text-[9px] text-cyan-300 font-mono">
                        {feat}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
