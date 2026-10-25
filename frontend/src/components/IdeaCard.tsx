'use client';

/**
 * One trade idea as a card: what to do, how far it should go, how sure the
 * model is, how much to put in, and the options trade if one is worth it,
 * readable at a glance; the full reasoning one click away.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import { ABSENT, formatNumber } from '../lib/format';
import {
  expectedMove,
  ideaDetails,
  plainIdea,
  plainOption,
  rankWords,
  rightWayOdds,
  STRUCTURE,
  usd,
  type Idea,
  type OptionIdea,
} from '../lib/ideas';
import { ConvictionMeter } from './ui/ConvictionMeter';
import { Sparkline } from './ui/Sparkline';

const pct = (v: number | null | undefined, digits = 1) =>
  v == null ? ABSENT : `${v >= 0 ? '+' : ''}${formatNumber(v, { decimals: digits })}%`;

/** The options trade in one line: structure, strikes, expiry. */
export function optionHeadline(o: OptionIdea): string {
  const strikes = o.legs
    .map((l) => `$${formatNumber(l.strike, { decimals: l.strike % 1 ? 2 : 0 })}`)
    .join('/');
  const kind = STRUCTURE[o.structure] ?? o.structure;
  return `${kind} ${strikes} · ${o.expiry}`;
}

function Stat({ label, value, tone }: { label: string; value: string; tone?: 'warn' }) {
  return (
    <div className="min-w-0">
      <div className="text-micro text-ink-mute">{label}</div>
      <div
        className={`font-mono text-sm truncate ${tone === 'warn' ? 'text-caution' : 'text-ink'}`}
      >
        {value}
      </div>
    </div>
  );
}

export function IdeaCard({
  idea: i,
  days,
  validated,
  horizonLabel,
  fallbackBps = null,
  compact = false,
}: {
  idea: Idea;
  days: number;
  validated: boolean;
  horizonLabel?: string;
  fallbackBps?: number | null;
  compact?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const long = i.direction > 0;
  const move = expectedMove(i, fallbackBps);
  const odds = rightWayOdds(i);
  const f = i.forecast;
  const sized = (f?.stock_weight ?? 0) > 0;
  const o = i.option;
  return (
    <article
      className={`group relative flex flex-col gap-3 rounded-2xl border bg-raised p-4 transition-colors hover:border-line-accent ${
        long ? 'border-positive/20' : 'border-negative/20'
      }`}
    >
      <div
        className={`absolute inset-x-0 top-0 h-0.5 rounded-t-2xl ${long ? 'bg-positive/60' : 'bg-negative/60'}`}
        aria-hidden
      />
      <header className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <Link
              href={`/ticker?symbol=${encodeURIComponent(i.ticker)}`}
              className="font-mono text-head font-semibold text-ink hover:text-accent"
            >
              {i.ticker}
            </Link>
            <span
              className={`rounded-full px-2 py-0.5 text-micro font-semibold tracking-wide ${
                long ? 'bg-positive/15 text-positive' : 'bg-negative/15 text-negative'
              }`}
            >
              {long ? 'BUY' : 'SHORT'}
            </span>
          </div>
          <div className="text-micro text-ink-mute">
            {i.price != null ? `$${formatNumber(i.price, { decimals: 2 })}` : ABSENT}{' '}
            {i.change_pct != null && (
              <span className={i.change_pct >= 0 ? 'text-positive' : 'text-negative'}>
                {pct(i.change_pct, 2)} today
              </span>
            )}
          </div>
        </div>
        <div className="text-right">
          <span className="rounded-md border border-line px-1.5 py-0.5 text-micro text-ink-dim">
            {horizonLabel ?? `${days} day${days === 1 ? '' : 's'}`}
          </span>
          <div className="mt-1 text-micro text-ink-mute">
            {i.industry_percentile != null
              ? `${rankWords(i.industry_percentile)} of its industry`
              : i.percentile != null
                ? `${rankWords(i.percentile)} of all stocks`
                : ABSENT}
          </div>
        </div>
      </header>

      <div className="flex items-end justify-between gap-3">
        <div>
          <div
            className={`text-display font-semibold leading-none font-mono ${
              move == null ? 'text-ink-mute' : move >= 0 ? 'text-positive' : 'text-negative'
            }`}
          >
            {pct(move)}
          </div>
          <div className="mt-1 text-micro text-ink-mute">
            {`${move == null || move >= 0 ? 'better' : 'worse'} than the S&P 500 over the next ${
              days === 1 ? 'day' : `${days} trading days`
            }`}
          </div>
        </div>
        <Sparkline
          values={i.spark}
          width={compact ? 96 : 120}
          height={compact ? 32 : 40}
          label={`${i.ticker}, last quarter`}
        />
      </div>

      <ConvictionMeter p={odds} />

      {!compact && (
        <div className="grid grid-cols-3 gap-2 rounded-xl bg-inset px-3 py-2">
          <Stat
            label="Most to put in"
            value={
              sized ? `${formatNumber((f?.stock_weight ?? 0) * 100, { decimals: 1 })}%` : 'skip'
            }
            tone={sized ? undefined : 'warn'}
          />
          <Stat
            label="Trading cost"
            value={
              f?.cost_bps != null ? `${formatNumber(f.cost_bps / 100, { decimals: 2 })}%` : ABSENT
            }
          />
          <Stat
            label="Price target"
            value={f?.target != null ? `$${formatNumber(f.target, { decimals: 2 })}` : ABSENT}
          />
        </div>
      )}

      {o ? (
        <div className="rounded-xl border border-line px-3 py-2 text-xs">
          <div className="flex items-center justify-between gap-2">
            <span className="text-ink">{optionHeadline(o)}</span>
            {o.prob_profit != null && (
              <span className="shrink-0 font-mono text-micro text-ink-dim">
                {formatNumber(o.prob_profit * 100, { decimals: 0 })}% chance of profit
              </span>
            )}
          </div>
          <div className="mt-0.5 font-mono text-micro text-ink-mute">
            {o.net < 0 ? 'costs' : 'pays you'} {usd(o.net)} · can lose up to {usd(o.max_loss)}
            {o.ev != null && ` · expected ${o.ev >= 0 ? 'profit' : 'loss'} ${usd(o.ev)}`}
          </div>
        </div>
      ) : (
        !compact && (
          <div className="text-micro text-ink-mute">No options trade is worth it at this forecast.</div>
        )
      )}

      <div className="flex flex-wrap gap-1.5">
        {f?.news_bps != null && (
          <span
            className={`rounded-full px-2 py-0.5 text-micro ${f.news_bps >= 0 ? 'bg-positive/10 text-positive' : 'bg-negative/10 text-negative'}`}
          >
            {f.news_bps >= 0 ? 'Good' : 'Bad'} news today
          </span>
        )}
        {i.earnings?.inside_hold && (
          <span className="rounded-full bg-caution/10 px-2 py-0.5 text-micro text-caution">
            Earnings ~{i.earnings.next_estimated ?? 'soon'}
          </span>
        )}
        {f && !f.edge_after_costs && (
          <span className="rounded-full bg-caution/10 px-2 py-0.5 text-micro text-caution">
            Gain smaller than costs
          </span>
        )}
        <span
          className={`rounded-full px-2 py-0.5 text-micro ${validated ? 'bg-accent-dim text-accent' : 'bg-inset text-ink-mute'}`}
          title={
            validated
              ? 'The model passed every test we set it, including the strictest'
              : 'The model did well on years of data it never saw, but has not passed every test yet'
          }
        >
          {validated ? 'Fully tested' : 'Still being tested'}
        </span>
      </div>

      {!compact && (
        <>
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            aria-expanded={open}
            className="self-start text-xs text-accent hover:underline"
          >
            {open ? 'Hide the reasoning' : 'Why this call'} {open ? '▴' : '▾'}
          </button>
          {open && (
            <div className="space-y-1.5 border-t border-line pt-2 text-micro leading-relaxed text-ink-dim">
              <p>{plainIdea(i, days, i.forecast?.expected_excess_bps ?? fallbackBps, validated)}</p>
              {o && <p>{plainOption(i.ticker, o)}</p>}
              {ideaDetails(i).map((d) => (
                <p key={d.label} className={d.tone === 'warn' ? 'text-caution' : ''}>
                  <span className="text-ink">{d.label}: </span>
                  {d.text}
                </p>
              ))}
            </div>
          )}
        </>
      )}
    </article>
  );
}

export default IdeaCard;
