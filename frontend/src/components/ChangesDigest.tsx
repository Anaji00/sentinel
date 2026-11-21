'use client';

/**
 * What changed since you last looked: new ideas, filings, stocks moving against their peers, price alerts that fired and
 * a new model run, counted from a time that is named in the heading. The time is kept in this browser (the server keeps
 * no per-person last-visit), starts at 24 hours ago, and moves forward only when you press the button. A source that
 * could not be read says so; it is never counted as zero changes.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { barLabel } from '../lib/candles';
import { feedLines, gutterParts, hoursAgoIso, type Feed } from '../lib/digest';
import { clockNy } from '../lib/liveFigures';
import { usePreferences } from '../lib/preferences';
import { StockLink } from './StockDrawer';
import { Button } from './ui/Button';
import { POLL } from './ui/DataProvider';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

const KEY = 'sentinel:last-seen';
const SHOWN = 8;
const day = (iso: string) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d');

function readSeen(): string {
  try {
    const v = window.localStorage.getItem(KEY);
    if (v && !Number.isNaN(Date.parse(v))) return v;
  } catch {
    // Storage can be blocked; the default below is used.
  }
  return hoursAgoIso(Date.now(), 24);
}

/** The sentence with its ticker as a link where it already appears; if it does not appear, the link comes first. */
function Linked({ text, ticker }: { text: string; ticker?: string }) {
  if (!ticker) return <>{text}</>;
  const at = text.search(new RegExp(`\\b${ticker.replace(/[^A-Z0-9]/gi, '')}\\b`));
  const link = <StockLink ticker={ticker} className="font-mono font-semibold text-ink hover:text-accent" />;
  if (at < 0) {
    return (
      <>
        {link}
        {' '}
        {text}
      </>
    );
  }
  return (
    <>
      {text.slice(0, at)}
      {link}
      {text.slice(at + ticker.length)}
    </>
  );
}

export default function ChangesDigest() {
  const [since, setSince] = React.useState<string | null>(null);
  const [all, setAll] = React.useState(false);
  const { prefs, save } = usePreferences();
  const followed = prefs?.tickers ?? [];
  // The account's own last-seen time wins, so it follows you between devices; this browser's copy is the fallback.
  const accountSeen = prefs?.last_seen_at ?? null;
  React.useEffect(() => {
    if (!prefs) return;
    setSince(accountSeen && !Number.isNaN(Date.parse(accountSeen)) ? accountSeen : readSeen());
  }, [prefs, accountSeen]);
  const tickers = followed.slice(0, 30).join(',');
  const key = since ? `/feed?since=${encodeURIComponent(since)}&limit=50${tickers ? `&tickers=${encodeURIComponent(tickers)}` : ''}` : null;
  const { data, error } = useSWR<Feed>(key, fetcher, { refreshInterval: POLL.slow, revalidateOnFocus: false });

  const lines = feedLines(data);
  const failed = data?.failed?.length ?? 0;
  const shown = all ? lines : lines.slice(0, SHOWN);

  const markSeen = () => {
    const now = new Date().toISOString();
    try {
      window.localStorage.setItem(KEY, now);
    } catch {
      // Without storage the cut-off simply resets on the next visit.
    }
    setSince(now);
    setAll(false);
    // Kept on the account when signed in; if that fails, this browser still remembers it.
    void save({ last_seen_at: now }).catch(() => undefined);
  };

  return (
    <section aria-label="What changed since you last looked" className="space-y-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">
          What changed since {since ? `${day(since)}, ${clockNy(since)}` : 'you last looked'}
        </h2>
        <Button variant="outline" size="sm" onClick={markSeen}>
          Mark all as seen
        </Button>
      </div>
      {failedWithout(error, data) ? (
        <PanelError what="the list of changes" inline />
      ) : !data ? (
        <LoadingLines lines={3} label="Looking for changes" />
      ) : lines.length === 0 ? (
        <p role="status" className="text-xs text-ink-mute">
          Nothing important since then{tickers ? ' on the stocks you follow' : ''}.
        </p>
      ) : (
        <>
          <ol className="grid gap-0" aria-label="Changes, newest first">
            {shown.map((l, i) => {
              const g = gutterParts(l.at, day, (iso) => clockNy(iso).replace(' New York time', ''));
              return (
                <li key={l.key} className={`grid grid-cols-[4.25rem_minmax(0,1fr)] gap-x-3 border-t border-line py-2.5 first:border-t-0 first:pt-0 ${l.failed ? 'text-caution' : ''}`}>
                  <div className="pt-0.5 font-mono text-micro leading-tight tabular-nums text-ink-mute">
                    {g ? (
                      <>
                        <time dateTime={l.at ?? undefined}>{g.date}</time>
                        {g.time ? <div>{g.time}</div> : null}
                      </>
                    ) : null}
                  </div>
                  <p className={`min-w-0 text-balance ${i === 0 && !l.failed ? 'text-head leading-snug text-ink' : 'text-xs leading-snug text-ink-dim'}`}>
                    <Linked text={l.text} ticker={l.ticker} />
                  </p>
                </li>
              );
            })}
          </ol>
          <p className="text-micro text-ink-mute">
            {lines.length > SHOWN && !all ? (
              <Button variant="link" size="sm" onClick={() => setAll(true)}>
                Show all {lines.length}
              </Button>
            ) : null}{' '}
            {tickers ? 'Filtered to the stocks you follow. ' : 'All stocks. Follow stocks to narrow this. '}
            {failed > 0 ? `${failed} source${failed === 1 ? '' : 's'} could not be read. ` : ''}Newest 20 of each kind; the model&apos;s run is judged against its own date.
          </p>
        </>
      )}
    </section>
  );
}
