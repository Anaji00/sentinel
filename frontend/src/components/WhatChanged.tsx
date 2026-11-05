'use client';

/**
 * At the top of the stock drawer: what changed on this stock in the last week.
 * Each part carries its date and how it is known, and a part with nothing to show
 * says why: "no data stored" and "could not read" are different from "nothing changed".
 * One request, kept five minutes by the gateway, made when the drawer opens.
 */

import React from 'react';
import { Button } from './ui/Button';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import {
  changesKey,
  dayLabel,
  eventKindLabel,
  flowView,
  macroLine,
  optionsLine,
  priceLine,
  rankLine,
  safeUrl,
  sectionNote,
  shortLine,
  type StockChanges,
} from '../lib/whatChanged';
import { ClockTime } from './ui/ClockTime';
import { Disclosure } from './ui/Disclosure';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

function Part({ label, meta, children }: { label: string; meta?: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="space-y-0.5">
      <div className="flex items-baseline justify-between gap-3">
        <h4 className="text-micro font-semibold text-ink-dim">{label}</h4>
        {meta && <span className="shrink-0 text-micro text-ink-mute">{meta}</span>}
      </div>
      {children}
    </div>
  );
}

function Missing({ label, status }: { label: string; status: string | undefined }) {
  const note = sectionNote(label, status as never);
  if (!note) return null;
  return <p className={`text-sm ${note.tone === 'caution' ? 'text-caution' : 'text-ink-mute'}`}>{note.text}</p>;
}

export default function WhatChanged({ ticker }: { ticker: string }) {
  const { data, error, isLoading, mutate } = useSWR<StockChanges>(changesKey(ticker), fetcher, {
    refreshInterval: POLL.slow,
  });
  const s = data?.sections ?? {};
  const [allItems, setAllItems] = React.useState(false);

  return (
    <section aria-label={`What changed for ${ticker}`} className="space-y-3.5 rounded-xl border border-line bg-inset p-4">
      <h3 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">
        What changed{data ? ` in the last ${data.window_days} days` : ''}
      </h3>

      {isLoading ? (
        <LoadingLines lines={4} label="Finding what changed" />
      ) : error || !data ? (
        <p role="status" className="text-sm text-caution">
          Could not read what changed for {ticker}.{' '}
          <Button variant="link" type="button" onClick={() => void mutate()} className="underline hover:text-ink">
            Try again
          </Button>
        </p>
      ) : (
        <>
          <Part label="Price" meta={data.price ? `${dayLabel(data.price.as_of)} · measured` : undefined}>
            {data.price ? <p className="text-sm text-ink">{priceLine(data.price)}</p> : <Missing label="prices" status={s.price} />}
          </Part>

          <Part label="Model rank" meta={data.rank ? `${dayLabel(data.rank.as_of)} · model estimate` : undefined}>
            {data.rank && rankLine(data.rank) ? (
              <>
                <p className="text-sm text-ink">{rankLine(data.rank)}</p>
                <p className="text-micro text-ink-mute">{data.rank.note}</p>
              </>
            ) : (
              <Missing label="a model rank" status={s.rank} />
            )}
          </Part>

          <Part label="Large trades">
            {data.flow.length > 0 ? (
              <div className="space-y-2.5">
                {data.flow.map((f) => {
                  const v = flowView(f);
                  return (
                    <div key={f.kind} className="space-y-1">
                      <p className="text-sm text-ink">{v.title}</p>
                      <dl className="grid grid-cols-3 gap-2 text-sm">
                        {v.parts.map((p) => (
                          <div key={p.label}>
                            <dt className="text-micro text-ink-mute">{p.label}</dt>
                            <dd className="font-mono text-ink">{p.usd}</dd>
                            <dd className="text-micro text-ink-mute">
                              {p.trades} {p.trades === 1 ? 'trade' : 'trades'}
                            </dd>
                          </div>
                        ))}
                      </dl>
                      {v.caution && <p className="text-micro text-caution">{v.caution}</p>}
                      <p className="text-micro text-ink-mute">{v.note}</p>
                    </div>
                  );
                })}
              </div>
            ) : (
              <Missing label="large trades" status={s.flow} />
            )}
          </Part>

          <Disclosure label="more measures" storageKey="changes:more">
          <Part label="Options" meta={data.options ? `${dayLabel(data.options.as_of)} · measured` : undefined}>
            {data.options && optionsLine(data.options) ? (
              <p className="text-sm text-ink">{optionsLine(data.options)}</p>
            ) : (
              <Missing label="options data" status={s.options} />
            )}
          </Part>

          <Part
            label="Short sellers"
            meta={data.short_interest ? `${dayLabel(data.short_interest.as_of)} report · twice a month` : undefined}
          >
            {data.short_interest && shortLine(data.short_interest) ? (
              <p className="text-sm text-ink">{shortLine(data.short_interest)}</p>
            ) : (
              <Missing label="short-selling reports" status={s.short_interest} />
            )}
          </Part>

          <Part label="Moves with">
            {data.macro_links.length > 0 ? (
              <ul className="space-y-0.5 text-sm text-ink">
                {data.macro_links.map((m) => (
                  <li key={m.symbol}>{macroLine(m)}</li>
                ))}
                <li className="text-micro text-ink-mute">
                  Measured over {data.macro_links[0].sessions ?? '?'} trading days
                  {data.macro_links[0].as_of ? `, as of ${dayLabel(data.macro_links[0].as_of)}` : ''}.
                </li>
              </ul>
            ) : (
              <Missing label="links to oil, rates or other markets" status={s.macro_links} />
            )}
          </Part>

          </Disclosure>

          <Part label="Recent items">
            {data.events.length > 0 ? (
              <>
              <ul className="space-y-2">
                {(allItems ? data.events : data.events.slice(0, 4)).map((e) => {
                  const href = safeUrl(e.url);
                  const text = e.headline || eventKindLabel(e.kind);
                  return (
                    <li key={`${e.at}:${text}`} className="grid grid-cols-[4.5rem_minmax(0,1fr)] gap-x-3 text-sm">
                      <span className="pt-0.5 text-micro text-ink-mute">
                        <ClockTime value={e.at} withDate seconds={false} />
                      </span>
                      <span className="min-w-0">
                        {href ? (
                          <a href={href} target="_blank" rel="noopener noreferrer" className="text-ink hover:text-accent hover:underline">
                            {text}
                          </a>
                        ) : (
                          <span className="text-ink">{text}</span>
                        )}
                        <span className="block text-micro text-ink-mute">{eventKindLabel(e.kind)}</span>
                      </span>
                    </li>
                  );
                })}
              </ul>
            {data.events.length > 4 && (
              <Button variant="link" type="button" onClick={() => setAllItems((v) => !v)} className="mt-2 text-xs">
                {allItems ? 'Show fewer' : `Show all ${data.events.length}`}
              </Button>
            )}
              </>
            ) : (
              <Missing label="recent filings or news" status={s.events} />
            )}
          </Part>
        </>
      )}
    </section>
  );
}
