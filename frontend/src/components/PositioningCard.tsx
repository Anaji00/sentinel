'use client';

/**
 * Option-implied and short-side positioning for one name, from
 * /predictions/positioning/{ticker}: today's put/call volume, IV skew and
 * call-minus-put IV spread (shared/utils/options_positioning.py), their daily
 * history (option_daily), and FINRA short interest with days-to-cover.
 *
 * Each has a published record for the stock's next returns; none is yet
 * measured on this system, and the card says so.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Live {
  call_volume?: number;
  put_volume?: number;
  put_call_ratio?: number | null;
  iv_skew?: number;
  atm_iv?: number;
  skew_expiry_days?: number;
  cp_iv_spread?: number;
  cp_pairs?: number;
  as_of?: string;
}

interface Daily {
  day: string;
  put_call_ratio: number | null;
  iv_skew: number | null;
  atm_iv: number | null;
  cp_iv_spread: number | null;
}

interface ShortRow {
  settlement_date: string;
  short_qty: number;
  days_to_cover: number | null;
  change_pct: number | null;
}

interface Positioning {
  ticker: string;
  live: Live | null;
  daily: Daily[];
  short_interest: ShortRow[];
}

const pts = (v: number | null | undefined) => (v != null ? formatNumber(v * 100, { decimals: 1 }) : ABSENT);

export function PositioningCard({ ticker }: { ticker: string }) {
  const { data } = useSWR<Positioning>(ticker ? `/predictions/positioning/${encodeURIComponent(ticker)}` : null, fetcher, {
    refreshInterval: POLL.slow,
  });
  const live = data?.live;
  const si = data?.short_interest ?? [];
  const latest = si[0];

  return (
    <div className="rounded-lg border border-line p-3 space-y-2">
      <span className="stat-label">What options traders and short sellers are doing</span>
      {!data ? (
        <LoadingLines />
      ) : (
        <>
          <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-micro">
            <dt className="text-ink-mute" title="Put/call volume ratio: puts traded for every call today. Above 1 means more bets on a fall.">
              Bets on a fall for every bet on a rise
            </dt>
            <dd className="text-right font-mono">{live?.put_call_ratio != null ? formatNumber(live.put_call_ratio, { decimals: 2 }) : ABSENT}</dd>
            <dt
              className="text-ink-mute"
              title="IV of a 25-delta put minus a 50-delta call, about a month out. Steep skew preceded underperformance (Xing, Zhang and Zhao 2010)."
            >
              Price of crash protection{live?.skew_expiry_days != null ? ` (${live.skew_expiry_days} days out)` : ''}
            </dt>
            <dd className={`text-right font-mono ${(live?.iv_skew ?? 0) > 0.06 ? 'text-negative' : ''}`}>{pts(live?.iv_skew)}</dd>
            <dt className="text-ink-mute" title="At-the-money implied volatility: the yearly swing option prices expect">
              How much options expect it to swing (a year)
            </dt>
            <dd className="text-right font-mono">{pts(live?.atm_iv)}</dd>
            <dt
              className="text-ink-mute"
              title="Mean call IV minus put IV at matched strikes. Calls dearer than puts preceded outperformance (Cremers and Weinbaum 2010)."
            >
              Which way option traders lean{live?.cp_pairs != null ? ` (${live.cp_pairs} price pairs)` : ''}
            </dt>
            <dd className={`text-right font-mono ${(live?.cp_iv_spread ?? 0) > 0 ? 'text-positive' : (live?.cp_iv_spread ?? 0) < -0.02 ? 'text-negative' : ''}`}>
              {pts(live?.cp_iv_spread)}
            </dd>
            <dt
              className="text-ink-mute"
              title="Shares short over average daily volume (FINRA). High days-to-cover preceded underperformance (Hong, Li, Ni, Scheinkman and Yan 2015)."
            >
              Days for short sellers to buy back{latest ? ` (${latest.settlement_date})` : ''}
            </dt>
            <dd className="text-right font-mono">{latest?.days_to_cover != null ? formatNumber(latest.days_to_cover, { decimals: 2 }) : ABSENT}</dd>
            <dt className="text-ink-mute">Change in short selling</dt>
            <dd className="text-right font-mono">{latest?.change_pct != null ? `${formatNumber(latest.change_pct, { decimals: 1 })}%` : ABSENT}</dd>
          </dl>
          {data.daily.length > 1 && (
            <table className="w-full text-micro font-mono">
              <thead className="text-ink-mute">
                <tr>
                  <th className="text-left font-normal">Day</th>
                  <th className="text-right font-normal">Falls per rise</th>
                  <th className="text-right font-normal">Crash protection</th>
                  <th className="text-right font-normal">Lean</th>
                </tr>
              </thead>
              <tbody>
                {data.daily.slice(0, 8).map((d) => (
                  <tr key={d.day}>
                    <td>{d.day}</td>
                    <td className="text-right">{d.put_call_ratio != null ? formatNumber(d.put_call_ratio, { decimals: 2 }) : ABSENT}</td>
                    <td className="text-right">{pts(d.iv_skew)}</td>
                    <td className="text-right">{pts(d.cp_iv_spread)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <p className="text-micro text-ink-mute">
            Published predictors, recorded daily here since 7 Oct 2026; not yet measured on this system.
          </p>
        </>
      )}
    </div>
  );
}

export default PositioningCard;
