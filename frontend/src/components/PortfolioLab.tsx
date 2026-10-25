'use client';

/**
 * The portfolio constructions judged on history, not on paper trades still to
 * come (/predictions/portfolio-lab; shared/utils/portfolio_lab.py).
 *
 * The monthly model's own out-of-sample predictions, re-run weekly: the live
 * book, the same scores ranked within each industry, and the live book sized by
 * the probability each pick pays its costs. Plus the model's IC in each market
 * regime, so an edge that only shows up in calm rising markets is visible.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Construction {
  periods: number;
  gross_bps: number | null;
  net_bps: number | null;
  sharpe: number | null;
  t: number | null;
  industry_share_of_variance: number | null;
  industry_bps: number | null;
  max_industry_tilt: number | null;
  net_beta: number | null;
}

interface SizedStats {
  mean_bps: number | null;
  sharpe: number | null;
}

interface RegimeCell {
  dates: number;
  mean_ic: number;
  t: number | null;
}

export interface LabReport {
  live?: Construction;
  industry_neutral?: Construction;
  rank_weighted?: Partial<Construction>;
  meta_sized?: { periods: number; base: SizedStats; meta: SizedStats; mean_size: number | null };
  ic_by_regime?: Record<string, RegimeCell>;
  verdict?: string;
  model?: string;
  as_of?: string;
  generated_at?: string;
  note?: string;
}

const num = (x: number | null | undefined, d = 2): string => (x == null ? '—' : x.toFixed(d));
const pct = (x: number | null | undefined): string => (x == null ? '—' : `${Math.round(x * 100)}%`);
const tone = (x: number | null | undefined): string =>
  x == null ? 'text-ink-mute' : x > 0 ? 'text-positive' : x < 0 ? 'text-negative' : 'text-ink';

/** The rows of the comparison table, in reading order. */
export function labRows(
  r: LabReport,
): { name: string; hint: string; c: Partial<Construction> | undefined }[] {
  return [
    { name: 'Universe-wide', hint: 'top and bottom fifth of the whole universe', c: r.live },
    {
      name: 'Industry-neutral',
      hint: 'top and bottom fifth of every industry',
      c: r.industry_neutral,
    },
    ...(r.rank_weighted
      ? [
          {
            name: 'Rank-weighted',
            hint: 'every ranked stock, weighted by its rank',
            c: r.rank_weighted,
          },
        ]
      : []),
  ];
}

export function PortfolioLab() {
  const { data } = useSWR<LabReport>('/predictions/portfolio-lab', fetcher, {
    refreshInterval: POLL.slow,
  });
  const meta = data?.meta_sized;
  const regimes = Object.entries(data?.ic_by_regime ?? {});
  return (
    // Research detail, folded: most readers need the one-line answer, and the
    // table is there for anyone who wants to check it.
    <details className="group bg-raised border border-line rounded-xl p-4 text-xs">
      <summary className="cursor-pointer list-none space-y-1">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h2 className="text-sm font-semibold text-ink">
            Behind the scenes: how we test changes before using them{' '}
            <span className="text-micro font-normal text-accent group-open:hidden">Show</span>
            <span className="text-micro font-normal text-accent hidden group-open:inline">
              Hide
            </span>
          </h2>
          {data?.as_of && (
            <span className="text-micro text-ink-mute">history to {data.as_of.slice(0, 10)}</span>
          )}
        </div>
        <p className="text-ink-dim">
          Every proposed change to how the portfolio is built is first replayed on years of the
          model&apos;s past predictions, after costs. Only changes that clearly beat the current
          way, without a worse bad month, go live.
        </p>
      </summary>
      <div className="mt-3 space-y-3">
        <p className="text-ink-dim">
          Each row is one way of building the portfolio, replayed on the monthly model&apos;s
          predictions for every month it was tested on. &ldquo;From sector bets&rdquo; is how much
          of the ups and downs came from leaning into whole industries rather than picking stocks.
        </p>
        {!data ? (
          <LoadingLines />
        ) : data.note ? (
          <p className="text-ink-mute">{data.note}</p>
        ) : (
          <>
            {data.verdict && <p className="text-ink font-medium">{data.verdict}</p>}
            <div className="overflow-x-auto">
              <table className="w-full text-left tabular-nums">
                <thead className="text-ink-mute">
                  <tr className="border-b border-line">
                    <th className="py-1.5 pr-3 font-medium">Way of building it</th>
                    <th className="py-1.5 pr-3 font-medium">Months</th>
                    <th className="py-1.5 pr-3 font-medium">Gain a month</th>
                    <th
                      className="py-1.5 pr-3 font-medium"
                      title="Sharpe ratio: return for each unit of ups and downs; above 1 is good"
                    >
                      Steadiness
                    </th>
                    <th
                      className="py-1.5 pr-3 font-medium"
                      title="t-statistic: above 2 means unlikely to be luck"
                    >
                      Not luck?
                    </th>
                    <th className="py-1.5 pr-3 font-medium">From sector bets</th>
                    <th className="py-1.5 pr-3 font-medium">Biggest sector lean</th>
                    <th
                      className="py-1.5 pr-3 font-medium"
                      title="Net beta: 1 moves with the market, 0 ignores it"
                    >
                      Moves with market
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {labRows(data).map(({ name, hint, c }) => (
                    <tr key={name} className="border-b border-line/60">
                      <td className="py-1.5 pr-3">
                        <span className="text-ink font-semibold">{name}</span>
                        <span className="block text-micro text-ink-mute">{hint}</span>
                      </td>
                      <td className="py-1.5 pr-3">{c?.periods ?? '—'}</td>
                      <td className={`py-1.5 pr-3 ${tone(c?.net_bps)}`}>
                        {c?.net_bps == null ? '—' : `${num(c.net_bps / 100, 2)}%`}
                      </td>
                      <td className={`py-1.5 pr-3 ${tone(c?.sharpe)}`}>{num(c?.sharpe)}</td>
                      <td className="py-1.5 pr-3">{num(c?.t)}</td>
                      <td className="py-1.5 pr-3">{pct(c?.industry_share_of_variance)}</td>
                      <td className="py-1.5 pr-3">{pct(c?.max_industry_tilt)}</td>
                      <td className="py-1.5 pr-3">{num(c?.net_beta)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {meta && (
              <p className="text-ink-dim">
                Betting more on picks likelier to pay off (learned only from earlier picks,{' '}
                {meta.periods} months): steadiness {num(meta.base.sharpe)} with equal bets,{' '}
                {num(meta.meta.sharpe)} with sized bets
                {meta.mean_size != null && (
                  <>, with the average pick at {pct(meta.mean_size)} of full size</>
                )}
                .
              </p>
            )}
            {regimes.length > 0 && (
              <div className="space-y-1">
                <h3 className="font-semibold text-ink">Does the edge hold in every market?</h3>
                <p className="text-ink-mute">
                  How well the model&apos;s rankings matched what stocks then did, split by whether
                  the S&amp;P 500 had been rising or falling and whether markets were calm or
                  stormy. Above zero means it helped.
                </p>
                <ul className="grid grid-cols-2 md:grid-cols-4 gap-2">
                  {regimes.map(([label, cell]) => (
                    <li key={label} className="rounded-lg border border-line bg-inset p-2.5">
                      <span className="block text-ink font-semibold capitalize">{label}</span>
                      <span
                        className={tone(cell.mean_ic)}
                        title="Information coefficient (rank correlation)"
                      >
                        match {num(cell.mean_ic, 3)}
                      </span>
                      <span className="text-ink-mute"> · {cell.dates} months</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </>
        )}
      </div>
    </details>
  );
}

export default PortfolioLab;
