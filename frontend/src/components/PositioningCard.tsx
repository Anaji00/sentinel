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
      <span className="stat-label">Options and short positioning</span>
      {!data ? (
        <p className="text-micro text-ink-mute">Reading…</p>
      ) : (
        <>
          <dl className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-micro font-mono">
            <dt className="text-ink-mute" title="Put volume over call volume, today's session">Put/call volume</dt>
            <dd className="text-right">{live?.put_call_ratio != null ? formatNumber(live.put_call_ratio, { decimals: 2 }) : ABSENT}</dd>
            <dt
              className="text-ink-mute"
              title="IV of a 25-delta put minus a 50-delta call, about a month out. Steep skew preceded underperformance (Xing, Zhang and Zhao 2010)."
            >
              IV skew, vol pts{live?.skew_expiry_days != null ? ` (${live.skew_expiry_days}d)` : ''}
            </dt>
            <dd className={`text-right ${(live?.iv_skew ?? 0) > 0.06 ? 'text-rose-300' : ''}`}>{pts(live?.iv_skew)}</dd>
            <dt className="text-ink-mute">ATM IV</dt>
            <dd className="text-right">{pts(live?.atm_iv)}</dd>
            <dt
              className="text-ink-mute"
              title="Mean call IV minus put IV at matched strikes. Calls dearer than puts preceded outperformance (Cremers and Weinbaum 2010)."
            >
              Call-put IV spread{live?.cp_pairs != null ? ` (${live.cp_pairs} pairs)` : ''}
            </dt>
            <dd className={`text-right ${(live?.cp_iv_spread ?? 0) > 0 ? 'text-emerald-300' : (live?.cp_iv_spread ?? 0) < -0.02 ? 'text-rose-300' : ''}`}>
              {pts(live?.cp_iv_spread)}
            </dd>
            <dt
              className="text-ink-mute"
              title="Shares short over average daily volume (FINRA). High days-to-cover preceded underperformance (Hong, Li, Ni, Scheinkman and Yan 2015)."
            >
              Days to cover{latest ? ` (${latest.settlement_date})` : ''}
            </dt>
            <dd className="text-right">{latest?.days_to_cover != null ? formatNumber(latest.days_to_cover, { decimals: 2 }) : ABSENT}</dd>
            <dt className="text-ink-mute">Short interest change</dt>
            <dd className="text-right">{latest?.change_pct != null ? `${formatNumber(latest.change_pct, { decimals: 1 })}%` : ABSENT}</dd>
          </dl>
          {data.daily.length > 1 && (
            <table className="w-full text-micro font-mono">
              <thead className="text-ink-mute">
                <tr>
                  <th className="text-left font-normal">Day</th>
                  <th className="text-right font-normal">P/C</th>
                  <th className="text-right font-normal">Skew</th>
                  <th className="text-right font-normal">C-P IV</th>
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
