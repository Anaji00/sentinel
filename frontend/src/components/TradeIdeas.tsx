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
import {
  ideaDetails,
  LABEL,
  ORDER,
  plainIdea,
  plainOption,
  type Idea,
  type Recommendations,
} from '../lib/ideas';
import { POLL } from './ui/DataProvider';
import { IconCheck } from './ui/icons';
import { IdeaCard } from './IdeaCard';

export { ideaDetails, plainIdea, plainOption };

/** Event chips shown before "more": the strongest first, as the engine orders them. */
const EVENT_CHIPS = 12;

const HORIZON_HINT: Record<string, string> = {
  daily: 'Hold for one trading day',
  weekly: 'Hold for a week',
  monthly: 'Hold for about a month',
  events: 'Driven by insider buying, large trades and company filings',
};

/** A Sharpe ratio in words: how steady the model's record has been on data it
 * never saw. The number itself goes in the tooltip, for those who want it. */
export function recordWords(sharpe: number | null | undefined): string {
  if (sharpe == null) return 'not yet measured';
  if (sharpe >= 1.5) return 'strong';
  if (sharpe >= 0.8) return 'good';
  if (sharpe > 0) return 'modest';
  return 'weak';
}

function BetsMeter({ bets, of }: { bets: number | null | undefined; of: number }) {
  if (!of) return null;
  const share = bets != null ? Math.min(1, bets / of) : 0;
  return (
    <span
      className="inline-flex items-center gap-1.5 text-micro text-ink-mute"
      title="Stocks that tend to move together count as one bet"
    >
      <span className="inline-block h-1.5 w-16 rounded-full bg-inset overflow-hidden">
        <span
          className="block h-full rounded-full bg-accent"
          style={{ width: `${share * 100}%` }}
        />
      </span>
      really {bets != null ? formatNumber(bets, { decimals: 1 }) : ABSENT} separate bets of {of}
    </span>
  );
}

interface Column {
  title: string;
  ideas: Idea[];
  bets: number | null | undefined;
  of: number;
  tone: string;
}

export function TradeIdeas() {
  const { data, error } = useSWR<Recommendations>('/predictions/recommendations', fetcher, {
    refreshInterval: POLL.standard,
  });
  const available = ORDER.filter((h) => data?.horizons?.[h]);
  const [tab, setTab] = useState<string | null>(null);
  const [showAllEvents, setShowAllEvents] = useState(false);
  const active =
    tab && data?.horizons?.[tab]
      ? tab
      : data?.primary_horizon && data.horizons?.[data.primary_horizon]
        ? data.primary_horizon
        : available[0];
  const h = active ? data?.horizons?.[active] : undefined;
  const label = (k: string) => LABEL[k]?.split(' (')[0] ?? k;
  const columns: Column[] = h
    ? [
        {
          title: 'Buy',
          ideas: h.ideas.filter((i) => i.direction > 0),
          bets: h.effective_bets?.long,
          of: h.effective_bets?.long_ideas ?? 0,
          tone: 'text-positive',
        },
        {
          title: 'Bet against (short)',
          ideas: h.ideas.filter((i) => i.direction < 0),
          bets: h.effective_bets?.short,
          of: h.effective_bets?.short_ideas ?? 0,
          tone: 'text-negative',
        },
      ]
    : [];

  return (
    <section className="rounded-2xl border border-line bg-raised p-4 sm:p-5 space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 className="text-head font-semibold text-ink">Today&apos;s calls</h2>
          <p className="text-xs text-ink-mute">
            Stocks to buy and stocks to bet against, how much each should beat or trail the S&amp;P
            500, how likely that is, and the best options trade if one is worth making. Not financial
            advice.
          </p>
        </div>
        <div
          className="inline-flex rounded-xl border border-line bg-inset p-1"
          role="tablist"
          aria-label="How long to hold"
        >
          {available.map((k) => (
            <button
              key={k}
              type="button"
              role="tab"
              aria-selected={k === active}
              title={HORIZON_HINT[k]}
              onClick={() => setTab(k)}
              className={`rounded-lg px-3 py-1.5 text-xs font-medium transition-colors ${
                k === active ? 'bg-overlay text-ink shadow-panel' : 'text-ink-dim hover:text-ink'
              }`}
            >
              {label(k)}
              {data?.horizons?.[k]?.validated ? (
                <IconCheck className="inline w-3 h-3 ml-1 text-accent" label="fully tested" />
              ) : null}
            </button>
          ))}
        </div>
      </div>
      {error ? (
        <p className="text-ink-mute">Ideas could not be read.</p>
      ) : !data ? (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {[0, 1, 2, 3].map((n) => (
            <div key={n} className="h-64 rounded-2xl border border-line bg-inset animate-pulse" />
          ))}
        </div>
      ) : !h ? (
        <p className="text-ink-mute">{data.note ?? 'No ideas yet.'}</p>
      ) : (
        <>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-micro">
            <span
              className={`rounded-full px-2 py-0.5 ${
                h.validated ? 'bg-accent-dim text-accent' : 'bg-caution/10 text-caution'
              }`}
            >
              {h.validated ? 'Fully tested model' : 'Model still being tested'}
            </span>
            <span
              className="text-ink-mute"
              title={`Sharpe ratio after costs on years of data the model never saw: ${
                h.net_sharpe != null ? formatNumber(h.net_sharpe, { decimals: 2 }) : ABSENT
              } (${h.model ?? ABSENT})`}
            >
              Past record: {recordWords(h.net_sharpe)} · {HORIZON_HINT[active ?? ''] ?? ''}
            </span>
            {data.pulse && data.pulse.sizing_scale < 0.95 && (
              <span className="text-caution">
                Suggested sizes cut to {formatNumber(data.pulse.sizing_scale * 100, { decimals: 0 })}%
                of normal: fast-rising stocks are swinging harder than usual
              </span>
            )}
          </div>
          {h.withheld && (
            <p className="text-xs text-ink-mute">No calls for this holding period. {h.withheld}.</p>
          )}
          <div className="grid gap-5 lg:grid-cols-2">
            {columns.map((c) => (
              <div key={c.title} className="space-y-3">
                <div className="flex items-baseline justify-between gap-2">
                  <h3 className={`text-sm font-semibold ${c.tone}`}>
                    {c.title} <span className="text-ink-mute font-normal">({c.ideas.length})</span>
                  </h3>
                  <BetsMeter bets={c.bets} of={c.of} />
                </div>
                {c.ideas.length === 0 ? (
                  <p className="rounded-2xl border border-dashed border-line p-4 text-xs text-ink-mute">
                    None for this holding period.
                  </p>
                ) : (
                  <div className="grid gap-3 sm:grid-cols-2">
                    {c.ideas.map((i) => (
                      <IdeaCard
                        key={`${i.ticker}-${i.direction}`}
                        idea={i}
                        days={h.horizon_days}
                        validated={h.validated}
                        horizonLabel={label(active ?? '')}
                        fallbackBps={h.expected_excess_bps}
                      />
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
          {(data.consensus ?? []).some((c) => c.horizons.length > 1) && (
            <div className="rounded-xl border border-line px-3 py-2 text-micro space-y-1">
              <div className="text-ink font-semibold">Called for more than one holding period</div>
              <div className="flex flex-wrap gap-1.5">
                {(data.consensus ?? [])
                  .filter((c) => c.horizons.length > 1)
                  .slice(0, 10)
                  .map((c) => (
                    <Link
                      key={`${c.ticker}-${c.direction}`}
                      href={`/ticker?symbol=${encodeURIComponent(c.ticker)}`}
                      title={c.reasons.slice(0, 2).join('; ')}
                      className={`rounded-full px-2 py-0.5 font-mono ${
                        c.direction > 0
                          ? 'bg-positive/10 text-positive'
                          : 'bg-negative/10 text-negative'
                      }`}
                    >
                      {c.direction > 0 ? 'Buy' : 'Sell'} {c.ticker} ·{' '}
                      {c.horizons.map((k) => label(k)).join(', ')}
                    </Link>
                  ))}
              </div>
            </div>
          )}
          <details className="rounded-xl border border-line px-3 py-2 text-micro text-ink-dim">
            <summary className="cursor-pointer text-ink">How these are built</summary>
            <div className="mt-2 space-y-1.5 leading-relaxed">
              <p>
                The model ranks every stock against others in its own industry and picks the
                strongest to buy and the weakest to bet against, so no single sector fills a side.
                Stocks that tend to move together count as one bet. Each call&apos;s forecast is
                checked against how the model did on years of data it never saw. Every option
                available is priced at what you would actually pay, and only the best one is shown.
                Suggested sizes are kept deliberately small and cut further when markets get jumpy.
                Every call, options included, is recorded and scored later against what really
                happened.
              </p>
              {h.long_note && <p>{h.long_note}</p>}
              {h.book && (
                <p>
                  Bought together as a ${formatNumber(h.book.book_usd / 1e6, { decimals: 0 })}M
                  portfolio: it moves {formatNumber(Math.abs(h.book.net_beta), { decimals: 2 })}×
                  as much as the market ({h.book.net_beta >= 0 ? 'with' : 'against'} it); to cancel
                  that out you would {h.book.spy_hedge_usd >= 0 ? 'buy' : 'sell'} $
                  {formatNumber(Math.abs(h.book.spy_hedge_usd) / 1e6, { decimals: 2 })}M of an S&amp;P
                  500 fund. It could typically swing about{' '}
                  {h.book.ex_ante_vol != null
                    ? `${formatNumber(h.book.ex_ante_vol * 100, { decimals: 1 })}%`
                    : ABSENT}{' '}
                  in a year. Its biggest industry is {h.book.top_industry} at{' '}
                  {formatNumber(h.book.top_industry_share * 100, { decimals: 0 })}%, and getting in
                  costs about ${formatNumber(h.book.cost_to_enter_usd, { decimals: 0 })}.
                </p>
              )}
              {data.book_effective_bets && (
                <p>
                  The model portfolio holds {data.book_effective_bets.long_names} stocks to buy, but
                  they behave like only{' '}
                  {formatNumber(data.book_effective_bets.long ?? 0, { decimals: 1 })} separate bets;
                  its {data.book_effective_bets.short_names} bets against behave like{' '}
                  {formatNumber(data.book_effective_bets.short ?? 0, { decimals: 1 })}.
                </p>
              )}
            </div>
          </details>
        </>
      )}
      <div className="space-y-2">
        <h3 className="text-sm font-semibold text-ink">Quick signals from news and filings</h3>
        {(data?.intraday ?? []).length === 0 ? (
          <p className="text-micro text-ink-mute">
            None right now. These only fire on kinds of events that have moved prices reliably for years.
          </p>
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {(showAllEvents ? data!.intraday : data!.intraday.slice(0, EVENT_CHIPS)).map((s) => (
              <Link
                key={s.id}
                href={`/ticker?symbol=${encodeURIComponent(s.instrument)}`}
                title={(s.families ?? []).join(', ')}
                className={`rounded-full border px-2.5 py-1 text-micro font-mono ${
                  s.direction === 'long'
                    ? 'border-positive/30 text-positive'
                    : 'border-negative/30 text-negative'
                }`}
              >
                {s.instrument} {s.direction === 'long' ? '▲' : '▼'} {s.horizon} ·{' '}
                {formatNumber(s.probability * 100, { decimals: 0 })}%
              </Link>
            ))}
            {data!.intraday.length > EVENT_CHIPS && (
              <button
                type="button"
                onClick={() => setShowAllEvents((v) => !v)}
                className="rounded-full border border-line px-2.5 py-1 text-micro text-accent hover:bg-inset"
              >
                {showAllEvents ? 'Show fewer' : `+${data!.intraday.length - EVENT_CHIPS} more`}
              </button>
            )}
          </div>
        )}
      </div>
      {data?.generated_at && (
        <p className="text-micro text-ink-mute">
          Updated {formatAge(data.generated_at)} ago from the model as of {data.as_of}.
        </p>
      )}
    </section>
  );
}

export default TradeIdeas;
