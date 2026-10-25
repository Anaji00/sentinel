'use client';

/**
 * Is the model still working? (/predictions/model-health; shared/utils/ic_monitor.py)
 *
 * Each night's scores, graded once their horizon passes by how well they ranked
 * what the stocks then did against SPY, beside what the walk-forward promised.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { formatPercent } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface HorizonHealth {
  chosen_model: string | null;
  supported: boolean;
  periods: number;
  mean_ic: number | null;
  t: number | null;
  expected_ic: number | null;
  share_positive?: number;
  status: 'warming_up' | 'ok' | 'weak' | 'broken';
}

const HORIZON_LABEL: Record<string, string> = {
  daily: 'Next day',
  weekly: 'Next week',
  monthly: 'Next month',
  weekly_trial_f: 'Next week, trial F (shadow)',
};

/** One horizon's state in a sentence. */
export function healthLine(h: HorizonHealth): string {
  const ic = (x: number | null) => (x == null ? '?' : x.toFixed(3));
  if (h.status === 'warming_up')
    return `Still collecting results (${h.periods} so far). In testing its ranking score was ${ic(h.expected_ic)}.`;
  const verdict = { ok: 'Working as promised', weak: 'Weaker than promised', broken: 'Pointing the wrong way' }[h.status];
  return `${verdict}: ranking score ${ic(h.mean_ic)} live against ${ic(h.expected_ic)} in testing (above zero helps).`;
}

interface EquityRegime {
  level: 'calm' | 'stressed' | 'panic' | null;
  multiplier?: number;
  momentum_drawdown?: number | null;
  momentum_vol_ratio?: number | null;
  bear_market?: boolean;
  as_of?: string;
}

interface NewsModel {
  samples?: number;
  days?: number;
  mean_ic?: number | null;
  t?: number | null;
  validated?: boolean;
  note?: string;
}

/** The equity-regime gauge in a sentence. */
export function regimeLine(r: EquityRegime): string {
  if (!r.level) return 'The market gauge is computed with the nightly model.';
  const dd = formatPercent(r.momentum_drawdown, { decimals: 1, from: 'ratio', fallback: '?' });
  const what = { calm: 'Calm', stressed: 'Stressed', panic: 'Panic' }[r.level];
  return `${what}: last year's best stocks are ${dd} below their recent high compared with last year's worst; the portfolio's stock bets run at ${formatPercent(r.multiplier ?? 1, { decimals: 0, from: 'ratio' })} of their normal size.`;
}

/** The news model's walk-forward in a sentence. */
export function newsLine(n: NewsModel): string {
  if (n.mean_ic == null) return n.note ?? 'Not enough headlines yet.';
  return `${n.validated ? 'In use' : 'Not in use yet'}: ranking score ${n.mean_ic.toFixed(3)} over ${n.days} test days and ${n.samples} headlines (above zero helps).`;
}

const TONE: Record<HorizonHealth['status'], string> = {
  warming_up: 'text-ink-mute',
  ok: 'text-positive',
  weak: 'text-caution',
  broken: 'text-negative',
};

export function ModelHealth() {
  const { data } = useSWR<{ horizons: Record<string, HorizonHealth> }>('/predictions/model-health', fetcher, {
    refreshInterval: POLL.slow,
  });
  const { data: regime } = useSWR<EquityRegime>('/predictions/equity-regime', fetcher, { refreshInterval: POLL.slow });
  const { data: news } = useSWR<NewsModel>('/predictions/news-model', fetcher, { refreshInterval: POLL.slow });
  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-2 text-xs">
      <h2 className="text-sm font-semibold text-ink">Is the model still working?</h2>
      <p className="text-ink-dim">
        Each night&apos;s rankings are checked once the holding period has passed: did the stocks it ranked highest
        actually do better? The &ldquo;ranking score&rdquo; measures that (zero is no better than chance), next to what the
        model managed in testing.
      </p>
      {!data ? (
        <LoadingLines />
      ) : (
        <ul className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2">
          {Object.entries(data.horizons).map(([k, h]) => (
            <li key={k} className="rounded-lg border border-line bg-inset p-2.5 space-y-1">
              <div className="flex justify-between">
                <span className="font-semibold text-ink">{HORIZON_LABEL[k] ?? k}</span>
                <span className="text-micro text-ink-mute">{h.chosen_model ?? '—'}</span>
              </div>
              <p className={TONE[h.status]}>{healthLine(h)}</p>
            </li>
          ))}
        </ul>
      )}
      <dl className="grid grid-cols-1 md:grid-cols-2 gap-2">
        <div className="rounded-lg border border-line bg-inset p-2.5">
          <dt className="font-semibold text-ink">How calm is the market?</dt>
          <dd className={regime?.level === 'calm' ? 'text-positive' : regime?.level ? 'text-caution' : 'text-ink-mute'}>
            {regime ? regimeLine(regime) : 'Loading…'}
          </dd>
        </div>
        <div className="rounded-lg border border-line bg-inset p-2.5">
          <dt className="font-semibold text-ink">Do headlines predict the next session?</dt>
          <dd className={news?.validated ? 'text-positive' : 'text-ink-dim'}>{news ? newsLine(news) : 'Loading…'}</dd>
        </div>
      </dl>
    </section>
  );
}

export default ModelHealth;
