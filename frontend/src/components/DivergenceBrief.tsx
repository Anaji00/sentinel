'use client';

/**
 * The stage badge and the seven-section brief of a divergence alert (lib/divergence.ts). Sections the platform could not
 * measure say "not measured" and are shown, in a quieter tone, rather than hidden: that text is the point.
 */

import React from 'react';
import { briefBlocks, horizonWords, isNotMeasured, measuresLines, situationLine, stageView, type Brief, type Measures, type Situation } from '../lib/divergence';
import { Disclosure } from './ui/Disclosure';

const TONE: Record<string, string> = {
  caution: 'border-caution/30 bg-caution/10 text-caution',
  accent: 'border-line-accent bg-accent-dim text-accent',
  negative: 'border-negative/30 bg-negative/10 text-negative',
  muted: 'border-line bg-overlay text-ink-mute',
};

/** One badge: the stage, with the measurement window beside it ("Confirmed · over 5 sessions"). */
export function StageBadge({ situation, horizon }: { situation?: Situation | null; horizon?: string | null }) {
  const v = stageView(situation?.stage);
  if (!v) return null;
  const window = horizonWords(horizon);
  return (
    <span title={`${v.meaning}${window ? ` Measured ${window}.` : ''}`} className={`whitespace-nowrap rounded border px-2 py-0.5 text-micro font-bold uppercase tracking-wide ${TONE[v.tone]}`}>
      {v.word}
      {window ? ` · ${window}` : ''}
    </span>
  );
}

export function DivergenceBrief({ situation, brief, id, measures }: { situation?: Situation | null; brief?: Brief | null; id: string; measures?: Measures | null }) {
  const blocks = briefBlocks(brief);
  if (blocks.length === 0) return null;
  const line = situationLine(situation);
  return (
    <Disclosure variant="row" label="Intelligence brief" storageKey={`brief:${id}`} hint={line ?? undefined}>
      <div className="space-y-2.5 pt-1">
        {measuresLines(measures).length > 0 && (
          <ul className="list-disc space-y-0.5 pl-4 text-xs text-ink-dim" aria-label="Measurements">
            {measuresLines(measures).map((l) => (
              <li key={l}>{l}</li>
            ))}
          </ul>
        )}
        {blocks.map((b) => (
          <section key={b.key} aria-label={b.label} className="space-y-0.5">
            <h4 className="text-micro font-semibold uppercase tracking-[0.08em] text-ink-mute">{b.label}</h4>
            {b.lines.length > 1 ? (
              <ul className="list-disc space-y-0.5 pl-4 text-xs text-ink-dim">
                {b.lines.map((l, i) => (
                  <li key={i} className={isNotMeasured(l) ? 'text-ink-mute' : ''}>
                    {l}
                  </li>
                ))}
              </ul>
            ) : (
              <p className={`text-xs ${isNotMeasured(b.lines[0]) ? 'text-ink-mute' : 'text-ink-dim'}`}>{b.lines[0]}</p>
            )}
          </section>
        ))}
      </div>
    </Disclosure>
  );
}
