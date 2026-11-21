'use client';

/**
 * The ways a situation could play out, as the model returned them: each with how likely it was judged, why, who would
 * gain, what would confirm it and what would rule it out. The number of paths is whatever came back, so this is a list,
 * not a fixed set of slots.
 */

import React from 'react';
import type { Scenario, ScenarioHypothesis } from '../lib/types';
import { formatNumber } from '../lib/format';

function Signals({ title, items, tone }: { title: string; items?: string[]; tone: string }) {
  if (!items || items.length === 0) return null;
  return (
    <div>
      <span className={`text-micro font-semibold ${tone}`}>{title}</span>
      <ul className="mt-0.5 list-disc space-y-0.5 pl-4 text-micro text-ink-dim">
        {items.map((s) => (
          <li key={s}>{s}</li>
        ))}
      </ul>
    </div>
  );
}

function Path({ h }: { h: ScenarioHypothesis }) {
  const p = typeof h.probability === 'number' ? Math.max(0, Math.min(100, h.probability)) : null;
  return (
    <li className="space-y-1.5 rounded-sm border border-line bg-page p-2.5">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-xs font-semibold text-ink">{h.label ?? 'Possible outcome'}</span>
        {p !== null && <span className="shrink-0 font-mono text-micro text-ink-dim">{formatNumber(p, { decimals: 0 })}% weight</span>}
      </div>
      {p !== null && (
        <div className="h-1 rounded-sm bg-line" role="presentation">
          <div className="h-1 rounded-sm bg-accent" style={{ width: `${p}%` }} />
        </div>
      )}
      {h.mechanism && <p className="text-micro text-ink-dim">{h.mechanism}</p>}
      {h.time_horizon && <p className="text-micro text-ink-mute">Over: {h.time_horizon}</p>}
      {h.beneficiaries && h.beneficiaries.length > 0 && <p className="text-micro text-ink-dim">Who would gain: {h.beneficiaries.join(', ')}</p>}
      <Signals title="What would confirm it" items={h.watch_signals} tone="text-positive" />
      <Signals title="What would rule it out" items={h.deny_signals} tone="text-negative" />
    </li>
  );
}

export default function ScenarioPaths({ scenario }: { scenario: Scenario }) {
  const paths = scenario.hypotheses ?? [];
  if (paths.length === 0 && !scenario.recommended_monitoring?.length) return null;
  return (
    <section aria-label="How this could play out" className="space-y-2">
      <span className="block font-bold text-accent">How this could play out</span>
      <p className="text-micro text-ink-mute">The weights are the model&apos;s own estimate. They have not been checked against what happened, so they are not odds.</p>
      {paths.length > 0 && (
        <ul className="space-y-2">
          {paths.map((h, i) => (
            <Path key={`${h.label ?? 'path'}-${i}`} h={h} />
          ))}
        </ul>
      )}
      <Signals title="What to keep an eye on" items={scenario.recommended_monitoring} tone="text-ink" />
    </section>
  );
}
