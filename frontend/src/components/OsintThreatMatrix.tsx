'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { formatPercent } from '../lib/format';
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

export default function OsintThreatMatrix() {
  const [selectedTheater, setSelectedTheater] = useState<string>('ALL');
  const [onlySanctions, setOnlySanctions] = useState<boolean>(false);

  const { data } = useSWR<OsintItem[]>('/api/v1/events/news?limit=30', fetcher, {
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
  const events = Array.isArray(data) ? data : [];

  const filteredEvents = events.filter((e) => {
    if (onlySanctions && !e.is_ofac_hit) return false;
    if (selectedTheater !== 'ALL' && e.region !== selectedTheater) return false;
    return true;
  });

  return (
    <Card className="h-full flex flex-col bg-[#080b13]/90 border border-cyan-500/20 shadow-2xl overflow-hidden font-mono">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-4 border-b border-cyan-500/20 bg-[#0c101d]/80 shrink-0">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-cyan-950/80 border border-accent/60 flex items-center justify-center">
            <span className="text-accent text-lg">
              <IconShield className="inline-block shrink-0" />
            </span>
          </div>
          <div>
            <h2 className="text-sm font-semibold text-white flex items-center gap-2">
              OSINT matrix
              <span className="px-2 py-0.5 rounded text-micro bg-rose-500/20 text-rose-300 border border-rose-500/40">
                LIVE RADAR
              </span>
            </h2>
            <p className="text-micro text-ink-dim">OFAC sanctions, AIS gaps and event novelty</p>
          </div>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-2 text-xs flex-wrap">
          <button
            onClick={() => setOnlySanctions(!onlySanctions)}
            className={`px-3 py-1 rounded-lg font-bold border transition-all cursor-pointer text-micro ${
              onlySanctions
                ? 'bg-rose-500/20 text-rose-300 border-rose-500/50 shadow-panel'
                : 'bg-raised text-ink-dim border-line hover:text-white'
            }`}
          >
            OFAC sanctions only
          </button>

          <select
            value={selectedTheater}
            onChange={(e) => setSelectedTheater(e.target.value)}
            className="bg-raised text-cyan-300 border border-cyan-500/30 rounded-lg px-3 py-1 text-micro font-bold outline-none cursor-pointer"
          >
            <option value="ALL">ALL THEATERS</option>
            <option value="MIDDLE_EAST">MIDDLE EAST / HORMUZ</option>
            <option value="RED_SEA">RED SEA / BAB-EL-MANDEB</option>
            <option value="EUROPE">EASTERN EUROPE</option>
            <option value="TAIWAN_STRAIT">TAIWAN STRAIT</option>
            <option value="GLOBAL">GLOBAL AIRSPACE</option>
          </select>
        </div>
      </div>

      {/* Stream List */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3">
        {filteredEvents.map((item) => (
          <div
            key={item.id}
            className="p-3.5 rounded-xl bg-raised/60 border border-cyan-500/15 hover:border-accent/50 transition-all hover:bg-raised/90 glass-panel-hover"
          >
            <div className="flex items-center justify-between gap-2 mb-2">
              <div className="flex items-center gap-2 flex-wrap text-micro">
                <span className="px-2 py-0.5 rounded bg-cyan-500/10 text-cyan-300 border border-cyan-500/30 font-bold uppercase">
                  {item.region}
                </span>
                {item.is_ofac_hit && (
                  <span className="px-2 py-0.5 rounded bg-rose-500/20 text-rose-300 border border-rose-500/40 font-extrabold animate-pulse">
                    OFAC match
                  </span>
                )}
                {item.is_threat && (
                  <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 border border-amber-500/40 font-bold">
                    HIGH THREAT
                  </span>
                )}
                <span className="text-ink-dim">
                  via <strong className="text-white">{item.source}</strong>
                </span>
              </div>

              <span
                className={`px-2 py-0.5 rounded text-micro font-bold ${
                  item.anomaly_score >= 0.8
                    ? 'bg-rose-500/20 text-rose-300 border border-rose-500/40'
                    : 'bg-amber-500/20 text-amber-300 border border-amber-500/40'
                }`}
              >
                FSD NOVELTY: {formatPercent(item.anomaly_score, { from: 'ratio', decimals: 0 })}
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
                    className="text-cyan-400/80 bg-cyan-950/40 px-1.5 py-0.5 rounded text-micro"
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
