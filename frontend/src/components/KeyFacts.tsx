'use client';

/**
 * Under the price: how big the company is, its price to earnings, when it next reports, where today's price sits in
 * its 52-week range, and (folded away) its business figures for the last 12 months from SEC filings. Every figure is
 * dated or labelled, and one that cannot be worked out says "not meaningful" instead of showing zero.
 */

import React, { useState } from 'react';
import { okData, gradeLine, optionsWords, shortWords } from '../lib/evidence';
import { regimeView } from '../lib/divergence';
import { useEvidence } from '../lib/useEvidence';
import { barLabel } from '../lib/candles';
import { formatNumber } from '../lib/format';
import { businessFacts, factsRow, factsSentence, rangePosition } from '../lib/snapshot';

const day = (iso: string) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d');

function RangeBar({ price, low, high, from }: { price: number; low: number; high: number; from: string | null }) {
  const pos = rangePosition(price, low, high);
  if (pos == null) return null;
  const x = 4 + 192 * pos;
  const words = `Price ${formatNumber(price, { decimals: 2 })} is ${Math.round(pos * 100)}% of the way up its 52-week range, ${formatNumber(low, { decimals: 2 })} to ${formatNumber(high, { decimals: 2 })}`;
  return (
    <div className="w-full min-w-[15rem] max-w-xs flex-1 space-y-1" title={from ? `52-week range from ${day(from)}` : undefined}>
      <svg viewBox="0 0 200 18" role="img" aria-label={words} className="h-[18px] w-full">
        <line x1="4" x2="196" y1="9" y2="9" stroke="var(--border-strong)" strokeWidth="2" strokeLinecap="round" />
        <line x1="4" x2={x} y1="9" y2="9" stroke="var(--text-secondary)" strokeWidth="2" strokeLinecap="round" />
        <path d={`M${x} 2 l6 7 -6 7 -6 -7z`} fill="var(--accent)" stroke="var(--bg-raised)" strokeWidth="1.5" />
      </svg>
      <div className="flex items-baseline justify-between gap-2 whitespace-nowrap font-mono text-micro text-ink-mute">
        <span>{formatNumber(low, { decimals: 2 })}</span>
        <span className="font-sans">{Math.round(pos * 100)}% up its 52-week range</span>
        <span>{formatNumber(high, { decimals: 2 })}</span>
      </div>
    </div>
  );
}

export default function KeyFacts({ ticker, price }: { ticker: string; price: number | null }) {
  const evidence = useEvidence(ticker);
  const [open, setOpen] = useState<'business' | 'options' | 'seen' | null>(null);
  const data = okData(evidence.data?.sections.snapshot);
  const regime = regimeView(okData(evidence.data?.sections.regime));
  const grades = okData(evidence.data?.sections.producer_grade) ?? [];
  const positioning = okData(evidence.data?.sections.positioning);
  const options = [...optionsWords(positioning), ...shortWords(positioning)];
  if (!data) return null;
  const facts = factsRow(data, day);
  const sentence = factsSentence(facts);
  const business = businessFacts(data);
  const r = data.range_52w;
  const tabs: { id: 'business' | 'options' | 'seen'; label: string; hint?: string }[] = [
    ...(business.length > 0 ? [{ id: 'business' as const, label: 'The business' }] : []),
    ...(options.length > 0 ? [{ id: 'options' as const, label: 'Options and shorts', hint: options[1] ? options[1].split(' (')[0] : undefined }] : []),
    ...(regime || grades.length > 0 ? [{ id: 'seen' as const, label: 'Alert history', hint: regime ? regime.heading : undefined }] : []),
  ];
  if (facts.length === 0 && business.length === 0 && !r && !regime && grades.length === 0) return null;
  return (
    <section aria-label={`${ticker} key facts`} className="space-y-2 rounded-md border border-line bg-raised p-3">
      <div className="flex flex-wrap items-center gap-x-6 gap-y-2">
        {sentence.length > 0 && (
          <p className="min-w-0 flex-1 basis-60 text-sm leading-relaxed text-ink-dim">
            {sentence.map((part, i) =>
              part.fig ? (
                <span key={i} title={part.title} className="font-mono font-semibold tabular-nums text-ink">
                  {part.text}
                </span>
              ) : (
                <span key={i} title={part.title}>
                  {part.text}
                </span>
              ),
            )}
          </p>
        )}
        {r && price != null && r.low != null && r.high != null && <RangeBar price={price} low={r.low} high={r.high} from={r.from_date} />}
      </div>
      {tabs.length > 0 && (
        <div className="space-y-2">
          <div role="tablist" aria-label={`More on ${ticker}`} className="flex flex-wrap gap-1.5">
            {tabs.map((t) => (
              <button
                key={t.id}
                type="button"
                role="tab"
                aria-selected={open === t.id}
                onClick={() => setOpen(open === t.id ? null : t.id)}
                className={`rounded-full border px-3 py-1 text-xs transition-colors ${open === t.id ? 'border-accent bg-accent-dim text-ink' : 'border-line text-ink-dim hover:border-line-strong hover:text-ink'}`}
              >
                {t.label}
                {t.hint && <span className="ml-1.5 text-ink-mute">{t.hint}</span>}
              </button>
            ))}
          </div>
          {open === 'business' && (
            <div className="space-y-1">
              <ul className="flex flex-wrap gap-x-5 gap-y-1.5 text-sm">
                {business.map((f) => (
                  <li key={f.label} title={f.title} className="text-ink-dim">
                    {f.label} <span className={`font-mono font-semibold tabular-nums ${f.value === 'not meaningful' ? 'text-ink-mute' : 'text-ink'}`}>{f.value}</span>
                  </li>
                ))}
              </ul>
              <p className="text-micro text-ink-mute">{data.metrics?.as_of ? `From SEC filings, as of ${day(data.metrics.as_of)}` : 'From SEC filings'}</p>
            </div>
          )}
          {open === 'options' && (
            <ul className="list-disc space-y-1 pl-4 text-xs text-ink-dim">
              {options.map((l) => (
                <li key={l}>{l}</li>
              ))}
            </ul>
          )}
          {open === 'seen' && (
            <div className="space-y-2 text-xs text-ink-dim">
              {regime && (
                <p>
                  <span className="font-semibold text-ink">{regime.heading}.</span> {regime.action ?? ''}
                  {regime.reasons.length > 0 ? ` ${regime.reasons.join(' ')}` : ''}
                </p>
              )}
              {grades.length > 0 && (
                <ul className="list-disc space-y-1 pl-4">
                  {grades.map((g) => (
                    <li key={`${g.rule}-${g.graded}`}>
                      <span className="text-ink">{g.rule.replace(/_/g, ' ')}.</span> {gradeLine(g)}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </div>
      )}
    </section>
  );
}
