'use client';

/**
 * The actively managed model portfolio on paper (/predictions/model-portfolio;
 * shared/utils/model_portfolio.py, services/agents/portfolio_engine.py): SPY
 * plus the monthly model's long/short overlay, its buy/sell/short/cover orders
 * with their reasons, its holdings, and its value against SPY's.
 */

import React from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Order {
  decided_on: string;
  ticker: string;
  action: 'BUY' | 'SELL' | 'SHORT' | 'COVER';
  kind: 'rebalance' | 'maintenance';
  reason: string;
  percentile: number | null;
  fill_day: string | null;
  fill_price: number | null;
}

interface Holding {
  ticker: string;
  percentile: number | null;
}

interface PortfolioSummary {
  sessions: number;
  return_pct?: number;
  spy_return_pct?: number;
  excess_pct?: number;
  tracking_error_pct?: number | null;
  info_ratio?: number | null;
  ahead_share?: number;
  worst_vs_spy_pct?: number;
}

interface PortfolioData {
  design: { core: string; horizon: string; overlay_gross: number; rebalance_sessions: number; borrow_fee_annual: number };
  summary: PortfolioSummary;
  nav: { day: string; nav: number; spy_nav: number }[];
  orders: Order[];
  holdings: { long: Holding[]; short: Holding[] };
  state?: {
    started_on: string | null;
    marked_through: string | null;
    rebalanced_on: string | null;
    next_rebalance_in_sessions: number | null;
    pending: { decided_on: string; kind: string } | null;
  };
  shadows?: Record<string, { started_on: string | null; summary: PortfolioSummary; nav: { day: string; nav: number; spy_nav: number }[] }>;
  sizing?: {
    decided_on: string;
    gross: number;
    core: number;
    vol_scale: number;
    regime_multiplier: number;
    regime?: string | null;
    ex_ante_vol: number | null;
    beta_long: number | null;
    beta_short: number | null;
  } | null;
}

/** How big today's stock bets are, in a sentence. */
export function sizingLine(s: NonNullable<PortfolioData['sizing']>): string {
  const pct = (x: number) => `${Math.round(x * 100)}%`;
  const why: string[] = [];
  if (s.vol_scale !== 1)
    why.push(`how much its stocks swing (about ${s.ex_ante_vol != null ? pct(s.ex_ante_vol) : '?'} a year, against a 16% limit)`);
  if (s.regime_multiplier !== 1) why.push(`a ${s.regime ?? 'stressed'} market`);
  return `Since ${s.decided_on}: ${pct(s.gross)} of the portfolio in stocks to buy and ${pct(s.gross)} in bets against${
    why.length ? `, cut back for ${why.join(' and ')}` : ''
  }. The rest sits in an S&P 500 fund (${pct(s.core)}), so overall it rises and falls with the market.`;
}

/** Plain names for the shadow books (shared/utils/model_portfolio.py). */
export const SHADOW_LABEL: Record<string, string> = {
  industry_neutral: 'Alternative tracked alongside: one industry at a time',
  universe_wide: 'Alternative tracked alongside: the whole market at once',
};

/** How each shadow book differs from the headline one. */
export const SHADOW_TEXT: Record<string, string> = {
  industry_neutral: 'the same model, picking the best and worst within each industry so no sector dominates.',
  universe_wide:
    'the original version, picking the best and worst fifth of the whole market. It was replaced on 16 October: over 32 months of history it earned more but swung more, much of that from betting on whole sectors.',
};

/** What each order means for someone following along. */
export const ACTION_TEXT: Record<Order['action'], string> = {
  BUY: 'Buy',
  SELL: 'Sell',
  SHORT: 'Bet against (or sell if owned)',
  COVER: 'Close the bet against',
};

const ACTION_TONE: Record<Order['action'], string> = {
  BUY: 'border-positive/30 text-positive',
  SELL: 'border-caution/30 text-caution',
  SHORT: 'border-negative/30 text-negative',
  COVER: 'border-accent/50 text-accent',
};

const pct = (v: number | null | undefined, signed = false) =>
  v == null ? ABSENT : `${formatNumber(v, { decimals: 2, signed })}%`;

/** The portfolio's value relative to SPY's, as a small line (1.0 = level with SPY). */
function RelativeLine({ nav }: { nav: PortfolioData['nav'] }) {
  if (nav.length < 2) return null;
  const rel = nav.map((r) => r.nav / r.spy_nav);
  const lo = Math.min(1, ...rel);
  const hi = Math.max(1, ...rel);
  const span = hi - lo || 1e-6;
  const w = 320;
  const h = 60;
  const x = (k: number) => (k / (rel.length - 1)) * w;
  const y = (v: number) => h - ((v - lo) / span) * h;
  const last = rel[rel.length - 1];
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full max-w-md h-16" role="img" aria-label="Portfolio value relative to SPY">
      <line x1={0} x2={w} y1={y(1)} y2={y(1)} className="stroke-line" strokeDasharray="3 3" />
      <polyline
        fill="none"
        strokeWidth={1.5}
        className={last >= 1 ? 'stroke-positive' : 'stroke-negative'}
        points={rel.map((v, k) => `${x(k)},${y(v)}`).join(' ')}
      />
    </svg>
  );
}

function Holdings({ title, rows, tone }: { title: string; rows: Holding[]; tone: string }) {
  const shown = rows.slice(0, 15);
  return (
    <div className="space-y-1">
      <div className="text-ink-dim">
        {title} <span className="text-ink-mute">({rows.length})</span>
      </div>
      <div className="flex flex-wrap gap-1">
        {shown.map((r) => (
          <Link
            key={r.ticker}
            href={`/ticker?symbol=${encodeURIComponent(r.ticker)}`}
            title={r.percentile != null ? `ranks above ${formatNumber(r.percentile * 100, { decimals: 0 })}% of stocks` : 'no longer ranked'}
            className={`px-1.5 py-0.5 rounded border font-mono ${tone} hover:underline`}
          >
            {r.ticker}
          </Link>
        ))}
        {rows.length > shown.length && <span className="text-ink-mute">and {rows.length - shown.length} more</span>}
      </div>
    </div>
  );
}

export function ModelPortfolio() {
  const { data, error } = useSWR<PortfolioData>('/predictions/model-portfolio', fetcher, { refreshInterval: POLL.slow });
  const s = data?.summary;
  const latest = data?.orders.length ? data.orders[0].decided_on : null;
  const latestOrders = (data?.orders ?? []).filter((o) => o.decided_on === latest);
  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3 text-xs">
      <h2 className="font-bold text-ink text-sm">Model portfolio: can it beat the S&amp;P 500?</h2>
      <p className="text-ink-dim">
        A pretend portfolio, run as if it were real. Most of it sits in an S&amp;P 500 fund. On top of that it buys
        the stocks our model likes best in each industry and bets against the ones it likes least (only stocks that
        are easy to bet against), each side about{' '}
        {data ? formatNumber(data.design.overlay_gross * 100, { decimals: 0 }) : '30'}% of the portfolio. Because the
        buys and the bets against roughly cancel out the market&apos;s ups and downs, anything it earns beyond the
        S&amp;P 500 comes from picking the right stocks. It reshuffles every{' '}
        {data?.design.rebalance_sessions ?? 21}{' '}trading days and only drops a stock once it falls well out of favour.
        Every trade is filled at the next morning&apos;s opening price, after trading costs and fees.
      </p>
      <p className="text-micro text-ink-mute">
        Why it works this way: tested on 2023–26 data the model had never seen, its buys on their own only beat the
        S&amp;P 500 by holding riskier stocks. Its picks to avoid were the reliable part: they trailed the market by
        about 2.5% a month. Together they earned about 1.9% a month more than the S&amp;P 500 while barely moving with
        it, but that is only 32 months of evidence, so this portfolio is the live test. Not financial advice.
      </p>
      {error ? (
        <p className="text-ink-mute">The portfolio could not be read.</p>
      ) : !data ? (
        <LoadingLines />
      ) : !data.state ? (
        <p className="text-ink-mute">The first orders are placed after the next model run.</p>
      ) : (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-2">
            {[
              ['Portfolio', pct(s?.return_pct, true), ''],
              ['S&P 500', pct(s?.spy_return_pct, true), ''],
              ['Ahead of the S&P 500', pct(s?.excess_pct, true), ''],
              ['How far it strays', pct(s?.tracking_error_pct), 'Tracking error: how much its daily results differ from the S&P 500, a year'],
              ['Reward for that risk', s?.info_ratio != null ? formatNumber(s.info_ratio, { decimals: 2 }) : ABSENT, 'Information ratio: gain over the S&P 500 per unit of straying; above 0.5 is good'],
              ['Days ahead', s?.ahead_share != null ? `${formatNumber(s.ahead_share * 100, { decimals: 0 })}%` : ABSENT, 'Share of trading days it finished ahead of the S&P 500'],
              ['Worst stretch behind', pct(s?.worst_vs_spy_pct), 'The furthest it has fallen behind the S&P 500 from a high point'],
            ].map(([k, v, hint]) => (
              <div key={k} className="rounded border border-line px-2 py-1" title={hint || undefined}>
                <div className="text-micro text-ink-mute">{k}</div>
                <div className="font-mono text-ink">{v}</div>
              </div>
            ))}
          </div>
          {data.sizing && (
            <div className="rounded border border-line px-3 py-2 text-micro text-ink-dim">
              <span className="text-ink">How big the stock bets are</span>: {sizingLine(data.sizing)}
            </div>
          )}
          {Object.entries(data.shadows ?? {}).map(([k, sh]) => (
            <div key={k} className="rounded border border-dashed border-line px-3 py-2 text-micro text-ink-dim">
              <span className="text-ink">{SHADOW_LABEL[k] ?? k}</span>: {SHADOW_TEXT[k] ?? ''} Tracked beside the
              main portfolio, using the same model.{' '}
              {sh.summary.sessions > 0
                ? `Since ${sh.started_on}: ${pct(sh.summary.return_pct, true)} against SPY ${pct(sh.summary.spy_return_pct, true)} (${pct(sh.summary.excess_pct, true)} ahead) over ${sh.summary.sessions} trading day${sh.summary.sessions === 1 ? '' : 's'}.`
                : 'Its first orders fill at the next open.'}
            </div>
          ))}
          <div className="flex flex-wrap items-center gap-3">
            <RelativeLine nav={data.nav} />
            <div className="text-micro text-ink-mute space-y-0.5">
              <div>
                {s && s.sessions > 0
                  ? `${s.sessions} trading day${s.sessions === 1 ? '' : 's'} since ${data.state.started_on}; marked through ${data.state.marked_through}.`
                  : 'Not started: the first orders fill at the next open.'}
              </div>
              <div>
                Last rebalance {data.state.rebalanced_on ?? ABSENT}
                {data.state.next_rebalance_in_sessions != null
                  ? `; next in ${data.state.next_rebalance_in_sessions} trading day${data.state.next_rebalance_in_sessions === 1 ? '' : 's'}`
                  : ''}
                .
              </div>
              {data.state.pending && (
                <div className="text-caution">
                  Orders from {data.state.pending.decided_on} ({data.state.pending.kind}) fill at the next open.
                </div>
              )}
            </div>
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            <Holdings title="Bought" rows={data.holdings.long} tone="border-positive/40 text-positive" />
            <Holdings title="Bet against (avoid, or sell if you own them)" rows={data.holdings.short} tone="border-negative/40 text-negative" />
          </div>
          {latest && (
            <div className="space-y-1">
              <div className="text-ink-dim">
                Latest orders, decided {latest} ({latestOrders[0]?.kind}): {latestOrders.length}
              </div>
              <ul className="space-y-0.5 max-h-72 overflow-y-auto">
                {latestOrders.map((o) => (
                  <li key={`${o.ticker}-${o.action}`} className="flex gap-2 items-baseline">
                    <span className={`shrink-0 px-1.5 rounded border text-micro font-bold ${ACTION_TONE[o.action]}`}>
                      {ACTION_TEXT[o.action]}
                    </span>
                    <Link
                      href={`/ticker?symbol=${encodeURIComponent(o.ticker)}`}
                      className="shrink-0 font-mono font-bold text-ink hover:text-accent hover:underline"
                    >
                      {o.ticker}
                    </Link>
                    <span className="text-ink-mute">
                      {o.reason}
                      {o.fill_day && o.fill_price != null
                        ? ` Filled ${o.fill_day} at $${formatNumber(o.fill_price, { decimals: 2 })}.`
                        : ' Fills at the next open.'}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </section>
  );
}

export default ModelPortfolio;
