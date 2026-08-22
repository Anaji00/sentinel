'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { formatPercent } from '../lib/format';

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

  const { data } = useSWR<{ events: OsintItem[] }>(
    '/api/v1/events/news?limit=30',
    fetcher,
    { refreshInterval: 5000 }
  );

  const events = data?.events || [
    {
      id: 'osint-1',
      headline: 'OFAC Sanctions Update: Special Designated Nationals Fleet Alert in Strait of Hormuz',
      source: 'OFAC / Reuters OSINT',
      occurred_at: new Date().toISOString(),
      anomaly_score: 0.92,
      sentiment: -0.65,
      reliability: 0.95,
      is_threat: true,
      is_ofac_hit: true,
      region: 'MIDDLE_EAST',
      tags: ['SANCTIONS', 'MARITIME', 'OFAC', 'HORMUZ'],
    },
    {
      id: 'osint-2',
      headline: 'Unusual Naval Auxiliary Vessel Movement Detected Near Bab-el-Mandeb Bottleneck',
      source: 'AISStream OSINT',
      occurred_at: new Date(Date.now() - 120000).toISOString(),
      anomaly_score: 0.84,
      sentiment: -0.40,
      reliability: 0.88,
      is_threat: true,
      is_ofac_hit: false,
      region: 'RED_SEA',
      tags: ['NAVAL', 'RED_SEA', 'AIS_GAP'],
    },
    {
      id: 'osint-3',
      headline: 'BGP Routing Hijack Anomalies Detected Impacting Financial Exchange Data Center',
      source: 'BGP Telemetry Stream',
      occurred_at: new Date(Date.now() - 360000).toISOString(),
      anomaly_score: 0.78,
      sentiment: -0.30,
      reliability: 0.90,
      is_threat: true,
      is_ofac_hit: false,
      region: 'GLOBAL',
      tags: ['CYBER', 'BGP', 'NETWORK'],
    },
    {
      id: 'osint-4',
      headline: 'Emergency Squawk 7700 Declared by Transport Aircraft in Eastern European Airspace',
      source: 'ADSB Network Feed',
      occurred_at: new Date(Date.now() - 720000).toISOString(),
      anomaly_score: 0.81,
      sentiment: -0.50,
      reliability: 0.92,
      is_threat: true,
      is_ofac_hit: false,
      region: 'EUROPE',
      tags: ['AVIATION', 'EMERGENCY', 'SQUAWK_7700'],
    },
  ];

  const filteredEvents = events.filter((e) => {
    if (onlySanctions && !e.is_ofac_hit) return false;
    if (selectedTheater !== 'ALL' && e.region !== selectedTheater) return false;
    return true;
  });

  return (
    <Card className="h-full flex flex-col bg-[#080b13]/90 border border-cyan-500/20 backdrop-blur-xl shadow-2xl overflow-hidden font-mono select-none">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-4 border-b border-cyan-500/20 bg-[#0c101d]/80 shrink-0">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-cyan-950/80 border border-[#00f2fe]/60 flex items-center justify-center shadow-[0_0_12px_rgba(0,242,254,0.3)]">
            <span className="text-[#00f2fe] text-lg">🛡️</span>
          </div>
          <div>
            <h2 className="text-sm font-black text-white tracking-widest uppercase flex items-center gap-2">
              OSINT & GEOPOLITICAL THREAT MATRIX
              <span className="px-2 py-0.5 rounded text-[9px] bg-rose-500/20 text-rose-300 border border-rose-500/40">LIVE RADAR</span>
            </h2>
            <p className="text-[10px] text-slate-400">REAL-TIME OFAC SANCTIONS, AIS GAPS & TACTICAL EVENT NOVELTY</p>
          </div>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-2 text-xs flex-wrap">
          <button
            onClick={() => setOnlySanctions(!onlySanctions)}
            className={`px-3 py-1 rounded-lg font-bold border transition-all cursor-pointer text-[10px] ${
              onlySanctions
                ? 'bg-rose-500/20 text-rose-300 border-rose-500/50 shadow-[0_0_12px_rgba(239,68,68,0.3)]'
                : 'bg-slate-900 text-slate-400 border-slate-800 hover:text-white'
            }`}
          >
            🚨 OFAC SANCTIONS ONLY
          </button>

          <select
            value={selectedTheater}
            onChange={(e) => setSelectedTheater(e.target.value)}
            className="bg-slate-900 text-cyan-300 border border-cyan-500/30 rounded-lg px-3 py-1 text-[10px] font-bold outline-none cursor-pointer"
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
            className="p-3.5 rounded-xl bg-slate-900/60 border border-cyan-500/15 hover:border-[#00f2fe]/50 transition-all hover:bg-slate-900/90 glass-panel-hover"
          >
            <div className="flex items-center justify-between gap-2 mb-2">
              <div className="flex items-center gap-2 flex-wrap text-[10px]">
                <span className="px-2 py-0.5 rounded bg-cyan-500/10 text-cyan-300 border border-cyan-500/30 font-bold uppercase">
                  {item.region}
                </span>
                {item.is_ofac_hit && (
                  <span className="px-2 py-0.5 rounded bg-rose-500/20 text-rose-300 border border-rose-500/40 font-extrabold animate-pulse">
                    ⚠️ OFAC TARGET MATCH
                  </span>
                )}
                {item.is_threat && (
                  <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 border border-amber-500/40 font-bold">
                    HIGH THREAT
                  </span>
                )}
                <span className="text-slate-400">via <strong className="text-white">{item.source}</strong></span>
              </div>

              <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                item.anomaly_score >= 0.8
                  ? 'bg-rose-500/20 text-rose-300 border border-rose-500/40'
                  : 'bg-amber-500/20 text-amber-300 border border-amber-500/40'
              }`}>
                FSD NOVELTY: {formatPercent(item.anomaly_score, { from: 'ratio', decimals: 0 })}
              </span>
            </div>

            <h3 className="text-xs sm:text-sm font-sans font-bold text-slate-100 leading-snug mb-2">
              {item.headline}
            </h3>

            <div className="flex items-center justify-between text-[10px] text-slate-400 pt-2 border-t border-slate-800/80">
              <div className="flex items-center gap-1.5 flex-wrap">
                {item.tags.map((t) => (
                  <span key={t} className="text-cyan-400/80 bg-cyan-950/40 px-1.5 py-0.5 rounded text-[9px]">
                    #{t}
                  </span>
                ))}
              </div>
              <span>{new Date(item.occurred_at).toLocaleTimeString()}</span>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}
