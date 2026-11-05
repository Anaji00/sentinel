'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { failedWithout } from '../lib/dataHealth';
import { PanelError } from './ui/PanelError';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { decodeEntities, formatPercent } from '../lib/format';
import { IconShield } from '@/components/ui/icons';
import { ClockTime } from './ui/ClockTime';
import { POLL } from './ui/DataProvider';

interface OsintItem {
  id: string;
  headline: string;
  source: string;
  occurred_at: string;
  anomaly_score: number;
  sentiment: number;
  reliability: number;
  is_threat: boolean;
  is_ofac_hit: boolean;
  region: string;
  tags: string[];
}

/** Paperwork, not intelligence: prospectus supplements for structured notes
 * (424B2 and kin), routine ownership forms and fund holdings reports arrive by
 * the hundred a day and drowned the stories this view exists to surface. They
 * stay on the Filings page. */
export function isRoutineFiling(headline: string): boolean {
  return /\bfiled Form (424B\d|FWP|13F-HR|N-PORT|497K?|S-8|D)(?![\w-])/i.test(headline);
}

export default function OsintThreatMatrix() {
  const [selectedTheater, setSelectedTheater] = useState<string>('ALL');
  const [onlySanctions, setOnlySanctions] = useState<boolean>(false);

  const { data, error: newsError, mutate: retryNews } = useSWR<OsintItem[]>('/api/v1/events/news?limit=30', fetcher, {
    refreshInterval: POLL.live,
  });

  // The endpoint returns a bare JSON array, so `data?.events` was
  // undefined on every successful fetch and the `||` fired on the
  // normal path rather than on an outage -- permanently, on a refresh
  // timer. What it fell back to was not placeholder text but invented
  // records stamped `occurred_at: new Date()`, which is what turns a
  // fixture into a claim: a row dated now, with a source attribution
  // and an anomaly score, is indistinguishable from a live detection.
  //
  // An empty list is the honest answer when there is nothing to show.
  //
  // The endpoint serves NormalizedEvent rows, not OsintItem rows: there is no
  // `id`, and `tags` is absent on many of them -- `item.tags.map` on such a row
  // took the whole page down. Mapped explicitly, with the flags derived from
  // what the row actually carries.
  const events: OsintItem[] = (Array.isArray(data) ? (data as unknown[]) : []).map((raw) => {
    const e = raw as Record<string, unknown>;
    const tags = Array.isArray(e.tags) ? (e.tags as unknown[]).map(String) : [];
    const score = typeof e.anomaly_score === 'number' ? e.anomaly_score : 0;
    return {
      id: String(e.event_id ?? e.id ?? ''),
      headline: decodeEntities(String(e.headline ?? e.title ?? '')),
      source: String(e.source ?? ''),
      occurred_at: String(e.occurred_at ?? ''),
      anomaly_score: score,
      sentiment: typeof e.sentiment === 'number' ? e.sentiment : 0,
      reliability: typeof e.reliability === 'number' ? e.reliability : 0,
      is_threat: Boolean(e.is_threat) || score >= 0.7,
      is_ofac_hit:
        Boolean(e.is_ofac_hit) || tags.some((t) => /ofac|sanction/i.test(t)),
      region: String(e.region ?? 'GLOBAL'),
      tags,
    };
  });

  const filteredEvents = events.filter((e) => {
    if (isRoutineFiling(e.headline)) return false;
    if (onlySanctions && !e.is_ofac_hit) return false;
    if (selectedTheater !== 'ALL' && e.region !== selectedTheater) return false;
    return true;
  });

  return (
    <Card className="h-full flex flex-col overflow-hidden">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-4 border-b border-line bg-inset/80 shrink-0">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-accent-dim border border-accent/60 flex items-center justify-center">
            <span className="text-accent text-lg">
              <IconShield className="inline-block shrink-0" />
            </span>
          </div>
          <div>
            <h2 className="panel-title flex items-center gap-2">
              Open-source watch
              <span className="px-2 py-0.5 rounded text-micro bg-negative/10 text-negative border border-negative/30">
                Live radar
              </span>
            </h2>
            <p className="text-micro text-ink-dim">Sanctions hits, ships going dark, and how unusual each story is</p>
          </div>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-2 text-xs flex-wrap">
          <button
            onClick={() => setOnlySanctions(!onlySanctions)}
            className={`px-3 py-1 rounded-lg font-bold border transition-all cursor-pointer text-micro ${
              onlySanctions
                ? 'bg-negative/10 text-negative border-negative/30 shadow-panel'
                : 'bg-raised text-ink-dim border-line hover:text-ink'
            }`}
          >
            OFAC sanctions only
          </button>

          <select
            value={selectedTheater}
            onChange={(e) => setSelectedTheater(e.target.value)}
            className="bg-raised text-accent border border-line rounded-lg px-3 py-1 text-micro font-bold outline-none cursor-pointer"
          >
            <option value="ALL">All theaters</option>
            <option value="MIDDLE_EAST">Middle East / Hormuz</option>
            <option value="RED_SEA">Red Sea / Bab-el-Mandeb</option>
            <option value="EUROPE">Eastern Europe</option>
            <option value="TAIWAN_STRAIT">Taiwan Strait</option>
            <option value="GLOBAL">Global airspace</option>
          </select>
        </div>
      </div>

      {/* Stream List */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3">
        {failedWithout(newsError, data) && <PanelError what="the news feed" onRetry={() => void retryNews()} />}
        {filteredEvents.map((item) => (
          <div
            key={item.id}
            className="p-3.5 rounded-xl bg-raised/60 border border-line hover:border-accent/50 transition-all hover:bg-raised/90 glass-panel-hover"
          >
            <div className="flex items-center justify-between gap-2 mb-2">
              <div className="flex items-center gap-2 flex-wrap text-micro">
                <span className="px-2 py-0.5 rounded bg-accent-dim text-accent border border-line font-bold uppercase">
                  {item.region}
                </span>
                {item.is_ofac_hit && (
                  <span className="px-2 py-0.5 rounded bg-negative/10 text-negative border border-negative/30 font-extrabold animate-pulse">
                    OFAC match
                  </span>
                )}
                {item.is_threat && (
                  <span className="px-2 py-0.5 rounded bg-caution/10 text-caution border border-caution/30 font-bold">
                    High threat
                  </span>
                )}
                <span className="text-ink-dim">
                  via <strong className="text-ink">{item.source}</strong>
                </span>
              </div>

              <span
                className={`px-2 py-0.5 rounded text-micro font-bold ${
                  item.anomaly_score >= 0.8
                    ? 'bg-negative/10 text-negative border border-negative/30'
                    : 'bg-caution/10 text-caution border border-caution/30'
                }`}
              >
                {formatPercent(item.anomaly_score, { from: 'ratio', decimals: 0 })} unusual
              </span>
            </div>

            <h3 className="text-xs sm:text-sm font-sans font-bold text-ink leading-snug mb-2">
              {item.headline}
            </h3>

            <div className="flex items-center justify-between text-micro text-ink-dim pt-2 border-t border-line/80">
              <div className="flex items-center gap-1.5 flex-wrap">
                {item.tags.map((t) => (
                  <span
                    key={t}
                    className="text-accent/80 bg-accent-dim px-1.5 py-0.5 rounded text-micro"
                  >
                    #{t}
                  </span>
                ))}
              </div>
              <span>
                <ClockTime value={item.occurred_at} />
              </span>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}
