'use client';

/**
 * The nightly cross-sectional model: which stocks it expects to beat which over
 * the next week, and whether that expectation has held out of sample.
 *
 * Read from /predictions/model (services/agents/factor_engine.py,
 * shared/utils/factor_model.py). Every candidate model is walked forward with a
 * purge and graded on rank IC and a net-of-cost long-short portfolio; ranks from
 * a model that fails the gate are shown, and marked, but never vote on a trade.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';

interface ModelReport {
  model: string;
  folds: number;
  dates: number;
  mean_ic: number | null;
  ic_t: number | null;
  positive_fold_share: number | null;
  ls_gross_bps: number | null;
  ls_net_bps: number | null;
  net_sharpe: number | null;
  turnover: number | null;
  deflated_sharpe: number | null;
  vol_managed_sharpe: number | null;
  validated: boolean;
  reasons: string[];
}

interface Ranked {
  ticker: string;
  score: number;
  percentile: number;
  quintile: number;
  features: Record<string, number>;
}

interface ModelResponse {
  report: {
    generated_at: string;
    as_of: string;
    universe: number;
    trading_days: number;
    horizon_days: number;
    chosen: string | null;
    validated: boolean;
    models: ModelReport[];
    caveats: string[];
    gate: { min_ic: number; min_ic_t: number; min_positive_folds: number; min_net_sharpe: number; cost_bps_per_side: number };
    primary_horizon?: string | null;
    selected_horizon?: string | null;
    trials?: number;
    tradable?: number;
    horizons?: Record<string, { horizon_days: number; validated: boolean; chosen: string | null; net_sharpe: number | null }>;
  } | null;
  top: Ranked[];
  bottom: Ranked[];
  note?: string;
}

const LABELS: Record<string, string> = {
  mom_12_1: '12-1m momentum',
  mom_6_1: '6-1m momentum',
  rev_5: '1-week return',
  rev_21: '1-month return',
  hi52: 'near 52w high',
  max_21: 'max daily jump',
  vol_63: 'volatility',
  ivol_63: 'idiosyncratic vol',
  beta_126: 'beta',
  dvol_21: 'dollar volume',
  vtrend: 'volume trend',
  amihud_21: 'illiquidity',
  resmom_12_1: 'residual momentum',
  up_share_21: 'share of up days',
  rsv_21: 'place in 1m range',
  skew_63: 'return skew',
  pv_corr_21: 'price-volume corr',
  dtc: 'days to cover',
  ear: 'earnings reaction',
  ind_mom: 'industry momentum',
  ind_rev_21: '1m return vs industry',
};

/** The two features this name sits furthest from the middle of the pack on. */
function standouts(features: Record<string, number>): string {
  return Object.entries(features)
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
    .slice(0, 2)
    .map(([k, v]) => `${v > 0 ? 'high' : 'low'} ${LABELS[k] ?? k}`)
    .join(', ');
}

function RankList({ title, rows, tone }: { title: string; rows: Ranked[]; tone: string }) {
  return (
    <div className="space-y-1">
      <h4 className={`font-bold ${tone}`}>{title}</h4>
      <ul className="space-y-0.5">
        {rows.map((r) => (
          <li key={r.ticker} className="flex items-center justify-between gap-2 border-b border-line/60 pb-0.5">
            <Link href={`/ticker?symbol=${encodeURIComponent(r.ticker)}`} className="font-mono font-bold text-white hover:text-accent hover:underline">
              {r.ticker}
            </Link>
            <span className="text-micro text-ink-mute truncate">{standouts(r.features)}</span>
            <span className="font-mono text-micro text-ink-dim">p{formatNumber(r.percentile * 100, { decimals: 0 })}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

const HORIZON_LABEL: Record<string, string> = { daily: 'Daily', weekly: 'Weekly', monthly: 'Monthly' };

export function ModelRanking() {
  const [horizon, setHorizon] = useState<string | null>(null);
  const { data, error } = useSWR<ModelResponse>(
    `/predictions/model?limit=15${horizon ? `&horizon=${horizon}` : ''}`,
    fetcher,
    { refreshInterval: POLL.slow },
  );
  const rep = data?.report;
  const horizonKeys = ['daily', 'weekly', 'monthly'].filter((k) => rep?.horizons?.[k]);

  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <h3 className="font-bold text-white">Model ranking: next {rep?.horizon_days ?? 5} trading days</h3>
        {rep && (
          <span
            className={`px-2 py-0.5 rounded border text-micro font-bold uppercase ${
              rep.validated ? 'border-emerald-500/50 text-emerald-300' : 'border-amber-500/50 text-amber-300'
            }`}
          >
            {rep.validated ? `Validated · ${rep.chosen}` : 'Not validated'}
          </span>
        )}
      </div>
      {horizonKeys.length > 0 && (
        <div className="flex gap-1 flex-wrap" role="tablist" aria-label="Horizon">
          {horizonKeys.map((k) => {
            const sel = (horizon ?? rep?.primary_horizon) === k;
            const hv = rep?.horizons?.[k];
            return (
              <button
                key={k}
                type="button"
                role="tab"
                aria-selected={sel}
                onClick={() => setHorizon(k)}
                className={`px-2 py-0.5 rounded border text-micro font-bold ${
                  sel ? 'border-accent text-accent' : 'border-line text-ink-dim hover:text-white'
                }`}
              >
                {HORIZON_LABEL[k]} · Sharpe {hv?.net_sharpe != null ? formatNumber(hv.net_sharpe, { decimals: 2 }) : ABSENT}
                {rep?.primary_horizon === k ? ' · primary' : ''}
              </button>
            );
          })}
        </div>
      )}
      <p className="text-ink-dim">
        Every stock ranked against every other on momentum, reversal, 52-week-high distance, volatility, beta and
        liquidity, combined by a model chosen walk-forward: trained only on the past, tested on the next quarter, graded
        on rank correlation and a long-short portfolio net of {rep?.gate.cost_bps_per_side ?? 10} bps per side. Only a
        validated model votes on trades.
      </p>
      {error ? (
        <p className="text-ink-mute">The model report could not be read.</p>
      ) : !rep ? (
        <p className="text-ink-mute">{data?.note ?? 'Reading…'}</p>
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead className="text-ink-mute">
                <tr className="border-b border-line">
                  <th className="py-1">Model</th>
                  <th className="text-right">Mean IC</th>
                  <th className="text-right">IC t</th>
                  <th className="text-right">Folds +</th>
                  <th className="text-right">L/S net bps</th>
                  <th className="text-right">Net Sharpe</th>
                  <th className="text-right" title="Probability the Sharpe is real after correcting for the number of models tried (Bailey and Lopez de Prado). The gate needs 0.95.">
                    Deflated
                  </th>
                  <th className="text-right">Turnover</th>
                  <th className="pl-3">Verdict</th>
                </tr>
              </thead>
              <tbody>
                {rep.models.map((m) => (
                  <tr key={m.model} className="border-b border-line/60 align-top">
                    <td className="py-1 font-mono">{m.model}</td>
                    <td className="text-right font-mono">{m.mean_ic != null ? formatNumber(m.mean_ic, { decimals: 3 }) : ABSENT}</td>
                    <td className="text-right font-mono">{m.ic_t != null ? formatNumber(m.ic_t, { decimals: 2 }) : ABSENT}</td>
                    <td className="text-right font-mono">
                      {m.positive_fold_share != null ? `${formatNumber(m.positive_fold_share * 100, { decimals: 0 })}%` : ABSENT}
                    </td>
                    <td className="text-right font-mono">{m.ls_net_bps != null ? formatNumber(m.ls_net_bps, { decimals: 1 }) : ABSENT}</td>
                    <td className="text-right font-mono">{m.net_sharpe != null ? formatNumber(m.net_sharpe, { decimals: 2 }) : ABSENT}</td>
                    <td className="text-right font-mono">
                      {m.deflated_sharpe != null ? formatNumber(m.deflated_sharpe, { decimals: 2 }) : ABSENT}
                    </td>
                    <td className="text-right font-mono">{m.turnover != null ? `${formatNumber(m.turnover * 100, { decimals: 0 })}%` : ABSENT}</td>
                    <td className="pl-3">
                      <span className={m.validated ? 'text-emerald-300 font-bold' : 'text-ink-mute'}>
                        {m.validated ? 'passes' : m.reasons.join('; ') || 'fails'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <RankList title="Strongest ranks" rows={data?.top ?? []} tone="text-emerald-300" />
            <RankList title="Weakest ranks" rows={data?.bottom ?? []} tone="text-rose-300" />
          </div>
          <p className="text-micro text-ink-mute">
            {formatNumber(rep.universe, { decimals: 0 })} stocks
            {rep.tradable != null ? ` (${formatNumber(rep.tradable, { decimals: 0 })} tradable today)` : ''} ·{' '}
            {formatNumber(rep.trading_days, { decimals: 0 })} trading days
            {rep.trials != null ? ` · deflated across ${rep.trials} trials` : ''} · as of {rep.as_of} · computed {formatAge(rep.generated_at)} ago. {rep.caveats.join('; ')}.
          </p>
        </>
      )}
    </section>
  );
}

interface TickerModel {
  ticker: string;
  entry: Ranked | null;
  validated: boolean;
  model: string | null;
  as_of: string | null;
  horizon_days: number | null;
}

export function TickerModelCard({ ticker }: { ticker: string }) {
  const { data } = useSWR<TickerModel>(ticker ? `/predictions/model/${encodeURIComponent(ticker)}` : null, fetcher, {
    refreshInterval: POLL.slow,
  });
  const e = data?.entry;
  return (
    <div className="rounded-lg border border-line p-3 space-y-1">
      <div className="flex items-center justify-between gap-2">
        <span className="stat-label">Model rank, next {data?.horizon_days ?? 5} days</span>
        {data && (
          <span className={`text-micro font-bold ${data.validated ? 'text-emerald-300' : 'text-amber-300'}`}>
            {data.validated ? 'validated' : 'not validated'}
          </span>
        )}
      </div>
      {!data ? (
        <p className="text-micro text-ink-mute">Reading…</p>
      ) : !e ? (
        <p className="text-micro text-ink-mute">{ticker} is not in the ranked universe yet (it needs a year of daily history).</p>
      ) : (
        <>
          <p className="text-sm text-white font-mono">
            Quintile {e.quintile} of 5 · percentile {formatNumber(e.percentile * 100, { decimals: 0 })}
          </p>
          <ul className="grid grid-cols-2 gap-x-3 text-micro font-mono">
            {Object.entries(e.features).map(([k, v]) => (
              <li key={k} className="flex justify-between">
                <span className="text-ink-mute">{LABELS[k] ?? k}</span>
                <span className={v > 0.25 ? 'text-emerald-300' : v < -0.25 ? 'text-rose-300' : 'text-ink-dim'}>
                  {v >= 0 ? '+' : ''}
                  {formatNumber(v, { decimals: 2 })}
                </span>
              </li>
            ))}
          </ul>
          <p className="text-micro text-ink-mute">Feature values are cross-sectional ranks from -0.5 to +0.5. As of {data.as_of}.</p>
        </>
      )}
    </div>
  );
}

export default ModelRanking;
