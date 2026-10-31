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
      badgeStyle = 'bg-positive/10 text-positive border-positive/30';
      label = 'Live';
      Mark = IconRadar;
      break;
    case 'computed_deterministic':
      badgeStyle = 'bg-accent/15 text-accent border-accent/40';
      label = isSynthetic ? 'Simulated' : 'Calculated';
      Mark = isSynthetic ? IconSimulated : IconMethodology;
      break;
    case 'llm_inference':
      badgeStyle = 'bg-accent-dim text-accent border-line-accent';
      label = confidence
        ? `AI estimate (${formatPercent(confidence, { from: 'ratio', decimals: 0 })})`
        : 'AI estimate';
      Mark = IconModel;
      break;
    case 'disclosed_placeholder':
      badgeStyle = 'bg-caution/10 text-caution border-caution/30';
      label = 'Needs more history';
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
      className={`inline-flex max-w-full items-center gap-1 px-1.5 py-0.5 text-micro font-medium rounded-md border whitespace-nowrap overflow-hidden transition-all cursor-help select-none ${badgeStyle} ${className}`}
    >
      <Mark label={label} className="shrink-0" />
      <span className="truncate">{label}</span>
    </span>
  );
};

export default ProvenanceBadge;
