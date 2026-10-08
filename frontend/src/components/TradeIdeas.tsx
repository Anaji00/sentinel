'use client';

/**
 * Trade ideas by horizon, from /predictions/recommendations
 * (services/agents/recommendation_engine.py, shared/utils/recommendations.py).
 *
 * Stock ideas are the cross-sectional model's strongest and weakest ranks at
 * each horizon; each carries an option structure chosen from the live chain by
 * implied against realised volatility. Every idea says whether its horizon's
 * model passed walk-forward validation: ideas from one that has not are
 * research, shown so they can be graded, not evidence.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { IconCheck } from './ui/icons';

interface OptionLeg {
  symbol: string;
  right: 'C' | 'P';
  side: 'buy' | 'sell';
  strike: number;
  delta: number;
  iv: number | null;
  mid: number;
}

interface OptionIdea {
  structure: string;
  expiry: string;
  dte: number;
  legs: OptionLeg[];
  net: number;
  max_loss: number;
  max_gain: number | null;
  breakeven: number;
  prob_profit_by_delta: number;
  iv_regime: string;
  atm_iv: number | null;
  realized_vol: number | null;
}

interface Idea {
  ticker: string;
  direction: 1 | -1;
  percentile: number | null;
  action: string;
  entry: number;
  stop: number | null;
  exit: string;
  expected_excess_bps: number | null;
  realized_vol: number | null;
  cp_iv_spread: number | null;
  option: OptionIdea | null;
}

interface HorizonIdeas {
  withheld?: string | null;
  horizon_days: number;
  validated: boolean;
  model: string | null;
  net_sharpe: number | null;
  deflated_sharpe: number | null;
  expected_excess_bps: number | null;
  ideas: Idea[];
}

interface IntradaySignal {
  id: string;
  instrument: string;
  horizon: string;
  direction: string;
  probability: number;
  expected_bps: number | null;
  entry_price: number | null;
  families?: string[];
}

interface Recommendations {
  generated_at?: string;
  as_of?: string;
  primary_horizon?: string;
  horizons: Record<string, HorizonIdeas>;
  intraday: IntradaySignal[];
  note?: string;
}

const STRUCTURE: Record<string, string> = {
  long_call: 'Buy call',
  long_put: 'Buy put',
  bull_call_debit: 'Bull call spread (debit)',
  bear_put_debit: 'Bear put spread (debit)',
  bull_put_credit: 'Bull put spread (credit)',
  bear_call_credit: 'Bear call spread (credit)',
};

const ORDER = ['daily', 'weekly', 'monthly'];
const LABEL: Record<string, string> = { daily: 'Daily', weekly: 'Weekly', monthly: 'Monthly' };

const money = (v: number | null | undefined) => (v == null ? ABSENT : `$${formatNumber(Math.abs(v), { decimals: 0 })}`);

function OptionCell({ o }: { o: OptionIdea | null }) {
  if (!o) return <span className="text-ink-mute">no tradable chain in window</span>;
  return (
    <div className="space-y-0.5">
      <div className="text-white">
        {STRUCTURE[o.structure] ?? o.structure} · {o.expiry} ({o.dte}d)
      </div>
      <div className="font-mono text-ink-dim">
        {o.legs.map((l) => `${l.side === 'buy' ? '+' : '-'}${l.strike}${l.right} @${formatNumber(l.mid, { decimals: 2 })}`).join('  ')}
      </div>
      <div className="font-mono text-micro text-ink-mute">
        {o.net < 0 ? 'debit' : 'credit'} {money(o.net)} · max loss {money(o.max_loss)} · max gain{' '}
        {o.max_gain == null ? 'open' : money(o.max_gain)} · BE {formatNumber(o.breakeven, { decimals: 2 })} · P(profit)~
        {formatNumber(o.prob_profit_by_delta * 100, { decimals: 0 })}%
      </div>
      <div className="text-micro text-ink-mute">
        IV {o.atm_iv != null ? `${formatNumber(o.atm_iv * 100, { decimals: 0 })}%` : ABSENT} vs realised{' '}
        {o.realized_vol != null ? `${formatNumber(o.realized_vol * 100, { decimals: 0 })}%` : ABSENT}: {o.iv_regime}
      </div>
    </div>
  );
}

export function TradeIdeas() {
  const { data, error } = useSWR<Recommendations>('/predictions/recommendations', fetcher, {
    refreshInterval: POLL.standard,
  });
  const available = ORDER.filter((h) => data?.horizons?.[h]);
  const [tab, setTab] = useState<string | null>(null);
  const active = tab && data?.horizons?.[tab] ? tab : data?.primary_horizon && data.horizons?.[data.primary_horizon] ? data.primary_horizon : available[0];
  const h = active ? data?.horizons?.[active] : undefined;

  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <h3 className="font-bold text-white">Trade ideas</h3>
        <div className="flex gap-1" role="tablist" aria-label="Horizon">
          {available.map((k) => (
            <button
              key={k}
              type="button"
              role="tab"
              aria-selected={k === active}
              onClick={() => setTab(k)}
              className={`px-2 py-0.5 rounded border text-micro font-bold ${
                k === active ? 'border-accent text-accent' : 'border-line text-ink-dim hover:text-white'
              }`}
            >
              {LABEL[k]}
              {data?.horizons?.[k]?.validated ? <IconCheck className="inline w-3 h-3 ml-1" label="validated" /> : null}
            </button>
          ))}
        </div>
      </div>
      <p className="text-ink-dim">
        Strongest and weakest names from the model at each horizon, each with an option structure picked from the live
        chain: options cheap against the stock&apos;s realised movement are bought, fairly priced ones spread, rich ones
        sold as defined-risk credit. Every idea is logged and graded. Not financial advice.
      </p>
      {error ? (
        <p className="text-ink-mute">Ideas could not be read.</p>
      ) : !data ? (
        <p className="text-ink-mute">Reading…</p>
      ) : !h ? (
        <p className="text-ink-mute">{data.note ?? 'No ideas yet.'}</p>
      ) : (
        <>
          <div
            className={`rounded border px-3 py-2 text-micro ${
              h.validated ? 'border-emerald-500/40 text-emerald-200' : 'border-amber-500/40 text-amber-200'
            }`}
          >
            {h.validated ? 'Validated' : 'Not validated'} · {h.model ?? ABSENT} · net Sharpe{' '}
            {h.net_sharpe != null ? formatNumber(h.net_sharpe, { decimals: 2 }) : ABSENT} · deflated{' '}
            {h.deflated_sharpe != null ? formatNumber(h.deflated_sharpe, { decimals: 2 }) : ABSENT} · expected excess per side{' '}
            {h.expected_excess_bps != null ? `${formatNumber(h.expected_excess_bps, { decimals: 0 })} bps` : ABSENT} over{' '}
            {h.horizon_days} day{h.horizon_days === 1 ? '' : 's'}
            {h.validated ? '' : ' — research only until the model passes its gate'}
          </div>
          {h.withheld && <p className="text-ink-mute">No ideas at this horizon. {h.withheld}.</p>}
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead className="text-ink-mute">
                <tr className="border-b border-line">
                  <th className="py-1">Ticker</th>
                  <th>Stock</th>
                  <th className="text-right">Entry</th>
                  <th className="text-right">Stop</th>
                  <th className="pl-3">Option structure</th>
                </tr>
              </thead>
              <tbody>
                {h.ideas.map((i) => (
                  <tr key={`${i.ticker}-${i.direction}`} className="border-b border-line/60 align-top">
                    <td className="py-1">
                      <Link
                        href={`/ticker?symbol=${encodeURIComponent(i.ticker)}`}
                        className="font-mono font-bold text-white hover:text-accent hover:underline"
                      >
                        {i.ticker}
                      </Link>
                      <div className="text-micro text-ink-mute">
                        p{i.percentile != null ? formatNumber(i.percentile * 100, { decimals: 0 }) : ABSENT}
                      </div>
                    </td>
                    <td className={i.direction > 0 ? 'text-emerald-300' : 'text-rose-300'}>
                      {i.action}
                      <div className="text-micro text-ink-mute">{i.exit}</div>
                    </td>
                    <td className="text-right font-mono">{formatNumber(i.entry, { decimals: 2 })}</td>
                    <td className="text-right font-mono">{i.stop != null ? formatNumber(i.stop, { decimals: 2 }) : ABSENT}</td>
                    <td className="pl-3">
                      <OptionCell o={i.option} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
      <div className="space-y-1">
        <h4 className="font-bold text-white">Intraday signals</h4>
        {(data?.intraday ?? []).length === 0 ? (
          <p className="text-micro text-ink-mute">
            None live. Intraday signals fire only from event families the reaction study has proven.
          </p>
        ) : (
          <ul className="space-y-0.5">
            {data!.intraday.map((s) => (
              <li key={s.id} className="flex justify-between gap-2 font-mono text-micro">
                <span className="text-white">{s.instrument}</span>
                <span className={s.direction === 'long' ? 'text-emerald-300' : 'text-rose-300'}>
                  {s.direction} {s.horizon}
                </span>
                <span>p {formatNumber(s.probability * 100, { decimals: 0 })}%</span>
                <span className="text-ink-mute">{(s.families ?? []).join(', ')}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
      {data?.generated_at && (
        <p className="text-micro text-ink-mute">
          Built {formatAge(data.generated_at)} ago from the model as of {data.as_of}.
        </p>
      )}
    </section>
  );
}

export default TradeIdeas;
