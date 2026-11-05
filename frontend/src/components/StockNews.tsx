'use client';

/**
 * Daily company news (/predictions/stock-news, /predictions/news-movers;
 * shared/utils/stock_news.py): Alpaca's feed, each headline's tone read by
 * FinBERT. With a ticker, that stock's recent articles; without, the stocks
 * whose news read most positive and most negative today, then the latest.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { formatDateTime } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Article {
  id: number;
  at: string;
  symbols: string[];
  headline: string;
  source: string | null;
  url: string | null;
  tone: number | null;
}

interface Mover {
  ticker: string;
  articles: number;
  tone: number;
  headline: string;
}

/** A tone in words, for the chip beside a headline. */
export function toneLabel(tone: number | null): { text: string; className: string } {
  if (tone == null) return { text: 'reading', className: 'text-ink-mute border-line' };
  if (tone >= 0.6) return { text: 'positive', className: 'text-positive border-positive/40' };
  if (tone <= -0.6) return { text: 'negative', className: 'text-negative border-negative/40' };
  return { text: 'neutral', className: 'text-ink-dim border-line' };
}

function Articles({ articles }: { articles: Article[] }) {
  if (articles.length === 0) return <p className="text-ink-mute">No articles in this window.</p>;
  return (
    <ul className="divide-y divide-line/60">
      {articles.map((a) => {
        const t = toneLabel(a.tone);
        return (
          <li key={a.id} className="py-1.5 flex gap-2 items-start">
            <span className={`shrink-0 rounded border px-1.5 text-micro ${t.className}`}>{t.text}</span>
            <span className="min-w-0">
              {a.url ? (
                <a href={a.url} target="_blank" rel="noopener noreferrer" className="text-ink hover:underline">
                  {a.headline}
                </a>
              ) : (
                <span className="text-ink">{a.headline}</span>
              )}
              <span className="block text-micro text-ink-mute">
                {formatDateTime(a.at, 'America/New_York')} · {a.symbols.slice(0, 4).join(', ')}
                {a.source ? ` · ${a.source}` : ''}
              </span>
            </span>
          </li>
        );
      })}
    </ul>
  );
}

function MoverList({ title, rows, tone }: { title: string; rows: Mover[]; tone: string }) {
  return (
    <div className="space-y-1 min-w-0">
      <h3 className={`font-semibold ${tone}`}>{title}</h3>
      {rows.length === 0 ? (
        <p className="text-ink-mute">None yet today.</p>
      ) : (
        <ul className="space-y-1">
          {rows.map((m) => (
            <li key={m.ticker} className="min-w-0">
              <a href={`?t=${encodeURIComponent(m.ticker)}`} className="font-semibold text-ink hover:underline">
                {m.ticker}
              </a>
              <span className="text-micro text-ink-mute">
                {' '}
                · {m.articles} articles · tone {m.tone.toFixed(2)}
              </span>
              <span className="block text-micro text-ink-dim truncate">{m.headline}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export function StockNews({ ticker }: { ticker?: string }) {
  const q = ticker ? `ticker=${encodeURIComponent(ticker)}&days=7&limit=40` : 'days=1&limit=25';
  const { data } = useSWR<{ articles: Article[] }>(`/predictions/stock-news?${q}`, fetcher, {
    refreshInterval: POLL.standard,
  });
  const movers = useSWR<{ positive: Mover[]; negative: Mover[] }>(ticker ? null : '/predictions/news-movers?hours=24', fetcher, {
    refreshInterval: POLL.slow,
  });
  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3 text-xs">
      <h2 className="text-sm font-semibold text-ink">{ticker ? `News on ${ticker}` : "Today's company news"}</h2>
      <p className="text-ink-dim">
        The latest company news, each headline marked positive, negative or neutral by an AI trained on financial news.
        Strongly worded stories about one or two companies are tested to see whether they move prices, and how much a
        stock is in the news feeds our model.
      </p>
      {!ticker && movers.data && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <MoverList title="Best news today" rows={movers.data.positive} tone="text-positive" />
          <MoverList title="Worst news today" rows={movers.data.negative} tone="text-negative" />
        </div>
      )}
      {!data ? <LoadingLines /> : <Articles articles={data.articles} />}
    </section>
  );
}

export default StockNews;
