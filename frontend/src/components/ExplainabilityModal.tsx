'use client';

import { ABSENT, formatNumber, formatPercent } from '../lib/format';
import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import {
  X,
  ShieldAlert,
  Cpu,
  Activity,
  Database,
  CheckCircle,
  AlertTriangle,
  Layers,
  Clock,
  Hash,
} from 'lucide-react';
import { Badge } from './ui/Badge';
import { useDialog } from './ui/useDialog';

/** One dimension the scorer measured, and its share of what it measured.
 *
 *  `model_weight` is gone because there was no model to weight: the server
 *  published a fixed 0.40 / 0.30 / 0.20 / 0.10 against a streaming RRCF scorer
 *  that has no such linear composite, and substituted plausible constants --
 *  2.4, 0.35, 0.5, 35.0 -- for whichever inputs the event did not carry.
 *
 *  `contribution_pct` is null when every measured dimension came back zero: a
 *  share of nothing is undefined, not 0%. */
interface FactorAttribution {
  factor_key: string;
  label: string;
  raw_subscore: number;
  contribution_pct: number | null;
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
  source_collector: string | null;
  event_id: string;
  ingest_timestamp: string | null;
  payload_hash: string;
  processing_latency_ms: number;
  /** Null until a quality signal is actually measured. */
  data_quality_score: number | null;
}

/** Mirrors the drift status published by the telemetry worker. */
type DriftState = 'STABLE' | 'SLIGHT_DRIFT' | 'SIGNIFICANT_DRIFT' | 'INITIALIZING' | 'UNKNOWN';

interface ModelCard {
  model_name: string;
  model_family: string;
  feature_schema_version: string;
  features_used: string[];
  training_window: string;
  model_drift_status: {
    /** Null when drift has not been evaluated -- absence, not stability. */
    psi_score: number | null;
    drift_state: DriftState;
    psi_threshold?: number;
    baseline_count?: number | null;
    current_count?: number | null;
    last_evaluated: string | null;
    detail?: string;
  };
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
  /** Which estimator produced the score, and how much of the history it
   *  wanted was available. A 0.4 from a warm-up curve and a 0.4 from a full
   *  percentile window are different claims. Null on events whose scorer
   *  records no breakdown. */
  score_basis?: string | null;
  score_coverage_fraction?: number | null;
}

interface ExplainabilityModalProps {
  eventId?: string;
  signalId?: string;
  onClose: () => void;
}

export default function ExplainabilityModal({
  eventId,
  signalId,
  onClose,
}: ExplainabilityModalProps) {
  const targetPath = eventId
    ? `/explain/event/${eventId}`
    : `/explain/signal/${signalId || 'NVDA'}`;
  const { data, error, isLoading } = useSWR<ExplainResponse>(targetPath, fetcher);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(true, onClose, 'Signal explanation');

  return (
    <div
      className="fixed inset-0 z-50 bg-black/80 flex items-center justify-center p-4"
      {...dialog.overlayProps}
    >
      <div
        className="bg-raised border border-accent/40 rounded-2xl max-w-2xl w-full p-6 space-y-5 text-xs text-ink max-h-[90vh] overflow-y-auto custom-scrollbar"
        {...dialog.panelProps}
      >
        {/* Modal Header */}
        <div className="flex items-center justify-between border-b border-line/80 pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-cyan-500/10 border border-cyan-500/30 text-cyan-400">
              <Cpu className="w-5 h-5" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-bold text-ink">Explain Computation & Model Card</h3>
                <Badge variant="live">AUDIT TRAIL</Badge>
              </div>
              <p className="text-micro text-ink-dim">
                Target:{' '}
                <span className="text-cyan-300 font-semibold">
                  {data?.entity || eventId || signalId || 'SYSTEM_SIGNAL'}
                </span>
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg bg-raised border border-line text-ink-dim hover:text-ink hover:bg-overlay transition-all cursor-pointer"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {isLoading ? (
          <div className="py-12 flex flex-col items-center justify-center gap-3 text-ink-dim">
            <Activity className="w-6 h-6 animate-spin text-cyan-400" />
            <span>Hydrating mathematical factor attribution & model card...</span>
          </div>
        ) : !data ? (
          /* No explanation is a state of its own, and it had none.
           *
           * `error` is declared and can never be set: the shared fetcher
           * catches every failure and returns null, so a 404, a 503 and a
           * working response with nothing in it all arrived here identically.
           * The panel then rendered its full frame -- score, waterfall,
           * timeline, model card -- against `data?.` optional chaining, which
           * reads as an event that was examined and found unremarkable rather
           * than as one that was never read. On this panel above all others,
           * those two must not look the same. */
          <div className="py-12 flex flex-col items-center justify-center gap-2 text-ink-dim">
            <span className="text-ink-dim font-bold">NO EXPLANATION AVAILABLE</span>
            <span className="text-micro text-center max-w-sm">
              The server returned no derivation for this {eventId ? 'event' : 'signal'}. Nothing
              below would be a description of it.
            </span>
          </div>
        ) : (
          <div className="space-y-5">
            {/* Overall Anomaly & Governance Status Banner */}
            <div className="grid grid-cols-3 gap-3">
              <div className="bg-page/60 border border-line/80 rounded-xl p-3">
                <div className="text-micro uppercase tracking-wider text-ink-dim">
                  Anomaly Score
                </div>
                <div className="text-lg font-bold text-cyan-400 mt-1">
                  {formatPercent(data?.overall_anomaly_score, { from: 'ratio', decimals: 1 })}
                </div>
                {/* Significance comes from the backend, which applies the
                    threshold. It was previously asserted unconditionally. */}
                {data?.is_significant === true ? (
                  <div className="text-micro text-emerald-400 flex items-center gap-1 mt-0.5">
                    <CheckCircle className="w-3 h-3" /> Statistically Significant
                  </div>
                ) : data?.is_significant === false ? (
                  <div className="text-micro text-ink-mute mt-0.5">
                    Below significance threshold
                  </div>
                ) : (
                  <div className="text-micro text-ink-mute mt-0.5">Significance not reported</div>
                )}
              </div>

              <div className="bg-page/60 border border-line/80 rounded-xl p-3">
                <div className="text-micro uppercase tracking-wider text-ink-dim">
                  Model Stability
                </div>
                <div
                  className={`text-lg font-bold mt-1 ${
                    data?.model_card?.model_drift_status?.drift_state === 'STABLE'
                      ? 'text-emerald-400'
                      : data?.model_card?.model_drift_status?.drift_state === 'SIGNIFICANT_DRIFT'
                        ? 'text-rose-400'
                        : data?.model_card?.model_drift_status?.drift_state === 'SLIGHT_DRIFT'
                          ? 'text-amber-400'
                          : 'text-ink-dim'
                  }`}
                >
                  {data?.model_card?.model_drift_status?.drift_state ?? ABSENT}
                </div>
                <div className="text-micro text-ink-dim mt-0.5">
                  PSI:{' '}
                  {formatNumber(data?.model_card?.model_drift_status?.psi_score, { decimals: 4 })}
                  {''}(Threshold{' '}
                  {formatNumber(data?.model_card?.model_drift_status?.psi_threshold ?? 0.25, {
                    decimals: 2,
                  })}
                  )
                </div>
              </div>

              <div className="bg-page/60 border border-line/80 rounded-xl p-3">
                <div className="text-micro uppercase tracking-wider text-ink-dim">
                  Latency & Hash
                </div>
                <div className="text-lg font-bold text-amber-400 mt-1">
                  {formatNumber(data?.provenance?.processing_latency_ms, { decimals: 2 })}
                  <span className="text-xs text-ink-mute ml-0.5">ms</span>
                </div>
                <div
                  className="text-micro text-ink-dim truncate mt-0.5"
                  title={data?.provenance?.payload_hash}
                >
                  {data?.provenance?.payload_hash?.slice(0, 14)}...
                </div>
              </div>
            </div>

            {/* Factor Attribution Waterfall */}
            <div className="bg-page/40 border border-line/80 rounded-xl p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-ink uppercase tracking-wider flex items-center gap-2">
                  <Layers className="w-4 h-4 text-cyan-400" />
                  Factor Attribution Waterfall
                </span>
                <span className="text-micro text-ink-dim">Relative Weight Contribution (%)</span>
              </div>

              <div className="space-y-2.5">
                {(data?.factor_attribution || []).map((f, idx) => (
                  <div key={idx} className="space-y-1">
                    <div className="flex justify-between text-micro">
                      <span className="text-ink-dim">{f.label}</span>
                      <span className="text-cyan-400 font-bold">
                        {f.contribution_pct === null ? '--' : `${f.contribution_pct}%`}
                      </span>
                    </div>
                    <div className="w-full bg-raised rounded-full h-2 overflow-hidden border border-line">
                      <div
                        className="h-full bg-gradient-to-r from-cyan-500 to-blue-500 rounded-full transition-all duration-500"
                        style={{ width: `${Math.min(100, f.contribution_pct ?? 0)}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Step-by-Step Score Derivation Timeline */}
            <div className="bg-page/40 border border-line/80 rounded-xl p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-ink uppercase tracking-wider flex items-center gap-2">
                  <Clock className="w-4 h-4 text-amber-400" />
                  Score Derivation Timeline
                </span>
                {/* However many steps there were. The server used to emit a
                    fixed four, and this badge was written to match. */}
                <Badge variant="neutral">{(data?.score_adjustments || []).length} STEPS</Badge>
              </div>

              <div className="space-y-2">
                {(data?.score_adjustments || []).map((step, idx) => (
                  <div
                    key={idx}
                    className="flex items-center justify-between bg-raised/60 border border-line/60 rounded-lg px-3 py-2 text-micro"
                  >
                    <div className="flex items-center gap-2.5">
                      <span className="w-5 h-5 rounded-full bg-overlay text-ink-dim flex items-center justify-center font-bold text-micro">
                        {step.step}
                      </span>
                      <div>
                        <div className="text-ink font-semibold">{step.action}</div>
                        <div className="text-micro text-ink-dim">{step.reason}</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <span
                        className={`font-bold ${
                          step.delta > 0
                            ? 'text-emerald-400'
                            : step.delta < 0
                              ? 'text-rose-400'
                              : 'text-ink-dim'
                        }`}
                      >
                        {step.delta > 0 ? `+${step.delta.toFixed(2)}` : step.delta.toFixed(2)}
                      </span>
                      <div className="text-micro text-ink-dim">
                        Score: {step.score_after.toFixed(2)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Model Card Metadata */}
            <div className="bg-page/40 border border-line/80 rounded-xl p-4 space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-ink uppercase tracking-wider flex items-center gap-2">
                  <Database className="w-4 h-4 text-emerald-400" />
                  Model Specification Card
                </span>
                {/* Schema version, not a governance verdict. The card
                    previously showed a hardcoded "VALIDATED" badge, which
                    asserted an approval that no system in the platform grants. */}
                <span className="text-micro text-ink-dim font-semibold">
                  SCHEMA {data?.model_card?.feature_schema_version ?? ABSENT}
                </span>
              </div>

              <div className="grid grid-cols-2 gap-2 text-micro bg-raised/40 p-3 rounded-lg border border-line/50">
                <div>
                  <span className="text-ink-dim">Model Name:</span>
                  <div className="text-ink font-semibold">{data?.model_card?.model_name}</div>
                </div>
                <div>
                  <span className="text-ink-dim">Schema Version:</span>
                  <div className="text-ink font-semibold">
                    {data?.model_card?.feature_schema_version}
                  </div>
                </div>
                <div className="col-span-2">
                  <span className="text-ink-dim">Model Family:</span>
                  <div className="text-ink-dim text-micro">{data?.model_card?.model_family}</div>
                </div>
                <div className="col-span-2">
                  <span className="text-ink-dim">Features Evaluated:</span>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {(data?.model_card?.features_used || []).map((feat, i) => (
                      <span
                        key={i}
                        className="px-1.5 py-0.5 rounded bg-overlay text-micro text-cyan-300 font-mono"
                      >
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
