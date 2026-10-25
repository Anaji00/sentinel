'use client';

/**
 * The model portfolio's positions as actually taken in the paper brokerage
 * account (/predictions/auto-trade; services/agents/auto_trader.py): whether
 * the auto-trader is on, what the account holds, and the orders it sent.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface AutoOrder {
  day: string;
  symbol: string;
  side: string;
  qty: number;
  reason: string;
  status: string;
  error: string | null;
  submitted_at: string;
}

interface AutoDay {
  day: string;
  sent: number;
  rejected: number;
}

interface AutoPosition {
  symbol: string;
  qty: number;
  market_value: number;
  unrealized_pl: number;
}

export interface AutoTrade {
  enabled: boolean;
  venue: string;
  account: { portfolio_value: number; cash: number; is_live: boolean } | null;
  positions_count?: number;
  long_value?: number;
  short_value?: number;
  unrealized_pl?: number;
  positions: AutoPosition[];
  orders: AutoOrder[];
  days: AutoDay[];
}

const usd = (x: number | null | undefined): string =>
  x != null ? `${x < 0 ? '−' : ''}$${formatNumber(Math.abs(x), { decimals: 0 })}` : ABSENT;

/** The state of the auto-trader in one sentence. */
export function autoTradeLine(d: AutoTrade): string {
  if (!d.enabled) return 'Off: the model portfolio is valued on paper only, and no orders are sent.';
  const last = d.days[0];
  if (!last) return 'On: it places the first orders a few minutes after the next market open.';
  return `On: last traded ${last.day}, ${last.sent} order${last.sent === 1 ? '' : 's'} sent${
    last.rejected ? `, ${last.rejected} refused by the broker` : ''
  }.`;
}

export default function AutoTradePanel() {
  const { data, error } = useSWR<AutoTrade>('/predictions/auto-trade', fetcher, { refreshInterval: POLL.slow });
  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3 text-xs">
      <div className="flex flex-wrap items-baseline gap-2">
        <h2 className="font-bold text-ink text-sm">Positions taken automatically</h2>
        {data && (
          <span
            className={`px-2 py-0.5 rounded-full border text-micro font-semibold ${
              data.enabled ? 'border-positive/40 text-positive' : 'border-line text-ink-mute'
            }`}
          >
            {data.enabled ? 'On' : 'Off'} · {data.venue}
            {data.account?.is_live ? ' · REAL MONEY' : ''}
          </span>
        )}
      </div>
      <p className="text-ink-dim">
        Only the model&apos;s strongest calls, traded in a paper brokerage account: half the money in the stocks it ranks
        in its 90th to 99th percentile, half betting against its bottom tenth, equal amounts each, rebuilt every month
        and no index fund. This beat holding the top and bottom fifth in eleven years of testing. Up and down days for
        the market roughly cancel out, so the result is the stock picking alone. Paper money only.
      </p>
      {error ? (
        <p className="text-ink-mute">The auto-trader could not be read.</p>
      ) : !data ? (
        <LoadingLines />
      ) : (
        <>
          <p className="text-ink">{autoTradeLine(data)}</p>
          {data.account && (
            <div className="grid grid-cols-2 sm:grid-cols-5 gap-2">
              {[
                ['Account value', usd(data.account.portfolio_value)],
                ['Cash', usd(data.account.cash)],
                ['Owned', usd(data.long_value)],
                ['Bet against', usd(data.short_value)],
                ['Gain or loss, open', usd(data.unrealized_pl)],
              ].map(([k, v]) => (
                <div key={k} className="rounded-lg border border-line p-2">
                  <div className="text-micro text-ink-mute">{k}</div>
                  <div className="font-mono text-ink">{v}</div>
                </div>
              ))}
            </div>
          )}
          {data.positions.length > 0 && (
            <table className="w-full">
              <thead className="text-ink-mute">
                <tr className="border-b border-line">
                  <th className="py-1 text-left">
                    Largest positions ({data.positions_count ?? data.positions.length} held)
                  </th>
                  <th className="text-right">Shares</th>
                  <th className="text-right">Worth</th>
                  <th className="text-right">Gain or loss</th>
                </tr>
              </thead>
              <tbody>
                {data.positions.slice(0, 10).map((p) => (
                  <tr key={p.symbol} className="border-b border-line/60">
                    <td className="py-0.5 text-ink">{p.symbol}</td>
                    <td className="text-right font-mono">{formatNumber(p.qty, { decimals: p.qty % 1 ? 3 : 0 })}</td>
                    <td className="text-right font-mono">{usd(p.market_value)}</td>
                    <td className={`text-right font-mono ${p.unrealized_pl >= 0 ? 'text-positive' : 'text-negative'}`}>
                      {usd(p.unrealized_pl)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {data.orders.some((o) => o.status === 'rejected') && (
            <details className="text-micro text-ink-mute">
              <summary className="cursor-pointer">Orders the broker refused</summary>
              <ul className="mt-1 space-y-0.5">
                {data.orders
                  .filter((o) => o.status === 'rejected')
                  .slice(0, 15)
                  .map((o) => (
                    <li key={`${o.symbol}-${o.submitted_at}`}>
                      {o.symbol} {o.side} {formatNumber(o.qty, { decimals: 2 })}, {formatAge(o.submitted_at)} ago:{' '}
                      {o.error ?? 'no reason given'}
                    </li>
                  ))}
              </ul>
            </details>
          )}
        </>
      )}
    </section>
  );
}
