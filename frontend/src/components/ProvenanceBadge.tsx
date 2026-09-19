'use client';

import React from 'react';
import { formatPercent } from '../lib/format';
import {
  IconMethodology,
  IconModel,
  IconPending,
  IconRadar,
  IconSimulated,
  type IconProps,
} from './ui/icons';

export type ProvenanceType =
  'live_measurement' | 'computed_deterministic' | 'llm_inference' | 'disclosed_placeholder';

interface ProvenanceBadgeProps {
  sourceType: ProvenanceType;
  methodology?: string;
  modelName?: string;
  confidence?: number;
  dataInputs?: string[];
  isSynthetic?: boolean;
  className?: string;
}

export const ProvenanceBadge: React.FC<ProvenanceBadgeProps> = ({
  sourceType,
  methodology,
  modelName,
  confidence,
  dataInputs = [],
  isSynthetic = false,
  className = '',
}) => {
  let badgeStyle = 'bg-overlay text-ink-dim border-line-strong';
  let label = 'Computed';
  // The mark, not a glyph: it inherits the badge's colour and its own
  // accessible name, so the provenance is readable without relying on hue.
  let Mark: React.FC<IconProps> = IconMethodology;

  switch (sourceType) {
    case 'live_measurement':
      badgeStyle = 'bg-emerald-500/15 text-emerald-400 border-emerald-500/40';
      label = 'Live Telemetry';
      Mark = IconRadar;
      break;
    case 'computed_deterministic':
      badgeStyle = 'bg-sky-500/15 text-sky-400 border-sky-500/40';
      label = isSynthetic ? 'Simulated' : 'Deterministic Math';
      Mark = isSynthetic ? IconSimulated : IconMethodology;
      break;
    case 'llm_inference':
      badgeStyle = 'bg-purple-500/15 text-purple-400 border-purple-500/40';
      label = confidence
        ? `Local AI (${formatPercent(confidence, { from: 'ratio', decimals: 0 })})`
        : 'Local AI Inference';
      Mark = IconModel;
      break;
    case 'disclosed_placeholder':
      badgeStyle = 'bg-amber-500/15 text-amber-400 border-amber-500/40';
      label = 'Pending History';
      Mark = IconPending;
      break;
  }

  const tooltipContent = [
    methodology ? `Methodology: ${methodology}` : null,
    modelName ? `Engine: ${modelName}` : null,
    confidence !== undefined && confidence !== null
      ? `Confidence: ${formatPercent(confidence, { from: 'ratio', decimals: 1 })}`
      : null,
    dataInputs.length > 0 ? `Inputs: ${dataInputs.join(', ')}` : null,
  ]
    .filter(Boolean)
    .join(' | ');

  return (
    <span
      title={tooltipContent || label}
      className={`inline-flex items-center gap-1.5 px-2 py-0.5 text-micro font-semibold rounded-md border tracking-tight transition-all cursor-help select-none ${badgeStyle} ${className}`}
    >
      <Mark label={label} className="shrink-0" />
      <span>{label}</span>
    </span>
  );
};

export default ProvenanceBadge;
