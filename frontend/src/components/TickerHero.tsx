'use client';

/**
 * The stock page's first screen: the price and its quarter, what the model and
 * the evidence say, and today's call on it if there is one, above the full
 * detail. The detail answers "show me everything"; this answers "so what?".
 */

import React from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { useSearchParams } from 'next/navigation';
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { LABEL, forecastIsPlateau, rankWords, sharedForecastKeys, type Idea, type Recommendations } from '../lib/ideas';
import { IdeaCard } from './IdeaCard';
import { POLL } from './ui/DataProvider';
import CandleChart from './CandleChart';
import KeyFacts from './KeyFacts';
import StockRecord from './StockRecord';
import PriceAlerts from './PriceAlerts';
import { barLabel } from '../lib/candles';
import { quoteCaption } from '../lib/liveFigures';
import { okData } from '../lib/evidence';
import { useEvidence } from '../lib/useEvidence';
import { SCORES_STALE_DAYS, headlinePrice, scoreAgeDays } from '../lib/heroPrice';
import { marketSession } from '../lib/marketSession';
import { IconFollow } from './ui/icons';
import { toggleTicker, usePreferences } from '../lib/preferences';

interface ClosesResponse {
  closes: { day: string; close: number }[];
}

interface TickerModel {
  entry: { percentile: number; quintile: number } | null;
  validated: boolean;
  horizon_days: number | null;
  /** The date the model's scores were written. */
  as_of?: string | null;
}

interface BoardRow {
  ticker: string;
  name: string | null;
  verdict?: string;
  confidence?: 'higher' | 'moderate' | 'low' | 'none';
}

/** Today's idea on this stock, if any horizon has one: the longest first. */
export function ideaFor(
  rec: Recommendations | undefined,
  ticker: string,
): { idea: Idea; horizon: string; days: number; validated: boolean } | null {
  for (const h of ['monthly', 'weekly', 'daily', 'events']) {
    const hz = rec?.horizons?.[h];
    const idea = hz?.ideas.find((i) => i.ticker === ticker);
    if (hz && idea) return { idea, horizon: h, days: hz.horizon_days, validated: hz.validated };
  }
  return null;
}

function RankBar({ percentile }: { percentile: number }) {
  const pctile = Math.round(percentile * 100);
  const tone = pctile >= 80 ? 'bg-positive' : pctile <= 20 ? 'bg-negative' : 'bg-accent';
  return (
    <div className="space-y-1">
      <div className="relative h-2 rounded-full bg-inset">
        <div className="absolute inset-y-0 left-1/5 right-1/5 border-x border-line" aria-hidden />
        <div
          className={`absolute top-1/2 h-3.5 w-3.5 -translate-x-1/2 -translate-y-1/2 rounded-full ring-2 ring-raised ${tone}`}
          style={{ left: `${pctile}%` }}
        />
      </div>
      <div className="flex justify-between text-micro text-ink-mute">
        <span>Weakest</span>
        <span>Strongest</span>
      </div>
    </div>
  );
}

/** `symbol` overrides the page's ?symbol=, so the stock drawer can show the same header. */
export function TickerHero({ symbol }: { symbol?: string } = {}) {
  const params = useSearchParams();
  const t = (symbol || params?.get('symbol') || '').trim().toUpperCase().slice(0, 10);
  // Daily closes from the same history the model reads, oldest first, so the
  // chart, the price and the call on the card all agree.
  const history = useSWR<ClosesResponse>(
    t ? `/predictions/closes/${encodeURIComponent(t)}?days=90` : null,
    fetcher,
    {
      refreshInterval: POLL.slow,
    },
  );
  const evidence = useEvidence(t || null);
  const model = useSWR<TickerModel>(
    t ? `/predictions/model/${encodeURIComponent(t)}` : null,
    fetcher,
    {
      refreshInterval: POLL.slow,
    },
  );
  const board = useSWR<{ board: BoardRow[] }>(t ? '/signals/matrix' : null, fetcher, {
    refreshInterval: POLL.standard,
  });
  const recs = useSWR<Recommendations>(t ? '/predictions/recommendations' : null, fetcher, {
    refreshInterval: POLL.standard,
  });
  const { prefs, save } = usePreferences();
  // The clock is read after mount and ticked each minute, so the server render and the first paint agree.
  const [session, setSession] = React.useState<string>('');
  const [nowMs, setNowMs] = React.useState(0);
  React.useEffect(() => {
    const tick = () => {
      setSession(marketSession(new Date()).label);
      setNowMs(Date.now());
    };
    tick();
    const id = window.setInterval(tick, 60_000);
    return () => window.clearInterval(id);
  }, []);
  const following = Boolean(prefs?.tickers.includes(t));
  if (!t) return null;

  const closes = (history.data?.closes ?? [])
    .map((c) => c.close)
    .filter((c) => Number.isFinite(c));
  const lastDay = history.data?.closes?.length ? history.data.closes[history.data.closes.length - 1].day : null;
  const call = ideaFor(recs.data, t);
  // The price and its change are worked out from the same daily closes the chart draws, so the header and the last
  // candle cannot disagree. A newer trade, when there is one, is shown as that and measured from the last close.
  const fallback = headlinePrice(closes, call?.idea.price ?? null);
  const quote = okData(evidence.data?.sections.quote);
  const fromQuote = quote && quote.price != null;
  const last = fromQuote ? quote.price : (fallback?.price ?? null);
  const change = fromQuote ? quote.change_pct : (fallback?.changePct ?? null);
  const closeLabel = lastDay ? barLabel(`${lastDay}T00:00:00Z`, '1d') : null;
  const quarter = closes.length > 1 ? (closes[closes.length - 1] / closes[0] - 1) * 100 : null;
  const row = board.data?.board.find((r) => r.ticker === t);
  const e = model.data?.entry;
  const scoreAge = scoreAgeDays(model.data?.as_of, nowMs);
  const scoresStale = scoreAge != null && scoreAge >= SCORES_STALE_DAYS;

  return (
    // A container query, not a window breakpoint: the same header shows full
    // width on /ticker and in the 560px stock drawer.
    <div className="@container space-y-3">
    <section className="rounded-md border border-line bg-raised p-4 sm:p-5">
      <div className="space-y-2 min-w-0">
        <div className="flex items-baseline gap-2">
          <h2 className="font-mono text-lead font-semibold text-ink">{t}</h2>
          {row?.name && <span className="truncate text-xs text-ink-mute">{row.name}</span>}
          {prefs && (
            <button
              type="button"
              aria-pressed={following}
              onClick={() => void save({ tickers: toggleTicker(prefs.tickers, t) })}
              className={`ml-auto shrink-0 rounded-full border px-2.5 py-0.5 text-micro ${
                following ? 'border-accent/50 bg-accent-dim text-accent' : 'border-line text-ink-dim hover:text-ink'
              }`}
            >
              <IconFollow on={following} className="mr-1 inline h-3 w-3 align-[-1px]" />
              {following ? 'Following' : 'Follow'}
            </button>
          )}
        </div>
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <span className="font-mono text-display font-semibold leading-none text-ink">
            {last != null ? `$${formatNumber(last, { decimals: 2 })}` : ABSENT}
          </span>
          {change != null && (
            <span
              className={`font-mono text-sm ${change >= 0 ? 'text-positive' : 'text-negative'}`}
            >
              {change >= 0 ? '+' : ''}
              {formatNumber(change, { decimals: 2 })}%
            </span>
          )}
          {(fromQuote || fallback) && (
            <span className="min-w-0 basis-full text-micro text-ink-mute @md:basis-auto">
              {fromQuote
                ? quoteCaption(quote, quote.change_base_date ? barLabel(`${quote.change_base_date}T00:00:00Z`, '1d') : null)
                : fallback?.kind === 'trade'
                  ? `latest trade, from the ${closeLabel ?? 'last'} close`
                  : `close ${closeLabel ?? ''}`.trim()}
              {!fromQuote && session ? ` · ${session}` : ''}
            </span>
          )}
        </div>
        <p className="text-micro text-ink-mute">
          Last 3 months
          {quarter != null && (
            <>
              {' '}
              ·{' '}
              <span className={quarter >= 0 ? 'text-positive' : 'text-negative'}>
                {quarter >= 0 ? '+' : ''}
                {formatNumber(quarter, { decimals: 1 })}%
              </span>
            </>
          )}
        </p>
      </div>
    </section>
    <CandleChart ticker={t} />
    <KeyFacts ticker={t} price={last} />
    <StockRecord ticker={t} />
    <PriceAlerts ticker={t} price={last} />
    <section className="grid gap-4 rounded-md border border-line bg-raised p-4 sm:p-5 @3xl:grid-cols-[1fr_1.2fr]">
      <div className="space-y-4">
        <div className="space-y-2">
          <div className="flex items-baseline justify-between text-micro">
            <span className="text-ink-mute">
              Our model&apos;s view, next {model.data?.horizon_days ?? 21} trading days
            </span>
            {model.data && (
              <span className={model.data.validated ? 'text-accent' : 'text-caution'}>
                {model.data.validated ? 'fully tested' : 'still being tested'}
              </span>
            )}
          </div>
          {e ? (
            <>
              <div className="text-lead font-semibold text-ink" title={`Percentile ${formatNumber(e.percentile * 100, { decimals: 0 })}`}>
                {rankWords(e.percentile)} of stocks
              </div>
              <RankBar percentile={e.percentile} />
              {model.data?.as_of && (
                <p className={`text-micro ${scoresStale ? 'text-caution' : 'text-ink-mute'}`}>
                  {scoresStale
                    ? `Out of date: these scores are from ${barLabel(`${model.data.as_of.slice(0, 10)}T00:00:00Z`, '1d')}, ${scoreAge} days ago.`
                    : `Scores from ${barLabel(`${model.data.as_of.slice(0, 10)}T00:00:00Z`, '1d')}.`}
                </p>
              )}
            </>
          ) : (
            <p className="text-xs text-ink-mute">
              {model.data ? 'Not ranked: the model only ranks stocks with enough price history and trading volume.' : 'Loading the model view'}
            </p>
          )}
        </div>
        <div className="space-y-1">
          <div className="text-micro text-ink-mute">What the evidence says</div>
          {row?.verdict ? (
            <span
              className={`inline-block rounded-full px-2.5 py-1 text-xs ${
                /bear/i.test(row.verdict)
                  ? 'bg-negative/10 text-negative'
                  : /bull/i.test(row.verdict)
                    ? 'bg-positive/10 text-positive'
                    : 'bg-inset text-ink-dim'
              }`}
            >
              {row.verdict}
            </span>
          ) : (
            <p className="text-xs text-ink-mute">No unusual activity in the last three days.</p>
          )}
        </div>
      </div>

      <div className="min-w-0">
        {call ? (
          <IdeaCard
            idea={call.idea}
            days={call.days}
            validated={call.validated}
            horizonLabel={LABEL[call.horizon]?.split(' (')[0] ?? call.horizon}
            plateau={forecastIsPlateau(call.idea, sharedForecastKeys(recs.data?.horizons?.[call.horizon]?.ideas))}
          />
        ) : (
          <div className="flex h-full flex-col justify-center gap-2 rounded-md border border-dashed border-line p-4">
            <p className="text-sm text-ink">Not on today&apos;s calls</p>
            <p className="text-xs text-ink-mute">
              The model only calls the strongest and weakest names in each industry. Everything it
              knows about {t} is below.
            </p>
            <Link href="/calls" className="text-xs text-accent hover:underline">
              See today&apos;s calls &rarr;
            </Link>
          </div>
        )}
      </div>
    </section>
    </div>
  );
}

export default TickerHero;
