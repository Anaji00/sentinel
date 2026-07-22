'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Scenario, NormalizedEvent } from '../lib/types';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { Skeleton } from './ui/Skeleton';

export default function IntelligenceFeed() {
  const [activeTab, setActiveTab] = useState<'events' | 'scenarios'>('events');
  const [selectedDomain, setSelectedDomain] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');

  // Fetch AI Scenarios
  const { data: scenarios, isLoading: scenariosLoading } = useSWR<Scenario[]>(
    '/scenarios?limit=20',
    fetcher,
    { refreshInterval: 15000 }
  );

  // Dynamic Event domain fetches
  const { data: tradfiEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'tradfi' ? '/events/tradfi?limit=25' : null,
    fetcher,
    { refreshInterval: 10000 }
  );
  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'crypto' ? '/events/crypto?limit=25' : null,
    fetcher,
    { refreshInterval: 10000 }
  );
  const { data: predictionEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'prediction' ? '/events/prediction?limit=25' : null,
    fetcher,
    { refreshInterval: 10000 }
  );
  const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'cyber' ? '/events/cyber?limit=25' : null,
    fetcher,
    { refreshInterval: 10000 }
  );
  const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'maritime' ? '/events/maritime?limit=25' : null,
    fetcher,
    { refreshInterval: 10000 }
  );

  // Merge and sort events
  const rawEvents: NormalizedEvent[] = [];
  if (selectedDomain === 'all' || selectedDomain === 'tradfi') rawEvents.push(...(tradfiEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'crypto') rawEvents.push(...(cryptoEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'prediction') rawEvents.push(...(predictionEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'cyber') rawEvents.push(...(cyberEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'maritime') rawEvents.push(...(maritimeEvents || []));

  const sortedEvents = rawEvents
    .sort((a, b) => new Date(b.occurred_at).getTime() - new Date(a.occurred_at).getTime())
    .filter((e) =>
      searchQuery
        ? e.type.toLowerCase().includes(searchQuery.toLowerCase()) ||
          e.source.toLowerCase().includes(searchQuery.toLowerCase())
        : true
    )
    .slice(0, 40);

  const mainTabs = [
    { id: 'events', label: 'LIVE STREAM', count: sortedEvents.length },
    { id: 'scenarios', label: 'AI SCENARIOS', count: scenarios?.length || 0 },
  ];

  const domainTabs = [
    { id: 'all', label: 'ALL' },
    { id: 'tradfi', label: 'TRADFI' },
    { id: 'crypto', label: 'CRYPTO' },
    { id: 'prediction', label: 'PRED' },
    { id: 'cyber', label: 'CYBER' },
    { id: 'maritime', label: 'AIS' },
  ];

  return (
    <Card
      title="INTELLIGENCE FEED"
      badge={<Badge variant="live" pulse>REALTIME</Badge>}
      headerAction={
        <Tabs
          tabs={mainTabs}
          activeTab={activeTab}
          onChange={(id) => setActiveTab(id as 'events' | 'scenarios')}
        />
      }
      noPadding
    >
      {/* Sub-Header Controls */}
      <div className="p-3 border-b border-cyan-500/10 bg-slate-950/60 flex flex-col gap-2.5">
        <div className="flex items-center justify-between">
          <Tabs
            tabs={domainTabs}
            activeTab={selectedDomain}
            onChange={setSelectedDomain}
          />
        </div>
        <div className="relative">
          <input
            type="text"
            placeholder="Filter stream by ticker, domain or event type..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-slate-900/80 border border-cyan-500/20 rounded-md px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 font-mono focus:outline-none focus:border-[#66fcf1]"
          />
        </div>
      </div>

      {/* Main Stream Area */}
      <div className="p-3 flex-1 overflow-y-auto space-y-2.5">
        {activeTab === 'events' ? (
          sortedEvents.length === 0 ? (
            <div className="text-slate-400 text-xs font-mono text-center py-12">
              Awaiting live intelligence telemetry stream...
            </div>
          ) : (
            sortedEvents.map((evt, idx) => (
              <div
                key={idx}
                className="p-3 rounded-lg bg-slate-900/50 border border-cyan-500/10 hover:border-cyan-500/30 transition-all backdrop-blur-md flex flex-col gap-2"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Badge variant={evt.anomaly_score > 0.7 ? 'anomaly' : 'live'}>
                      {evt.source.toUpperCase()}
                    </Badge>
                    <span className="text-xs font-mono font-bold text-slate-200">
                      {evt.type}
                    </span>
                  </div>
                  <span className="text-[10px] font-mono text-slate-400">
                    {new Date(evt.occurred_at).toLocaleTimeString()}
                  </span>
                </div>
                <div className="text-xs text-slate-300 font-sans leading-relaxed">
                  {JSON.stringify(evt.financial_data || evt.raw_payload || {}).substring(0, 140)}...
                </div>
                {evt.anomaly_score > 0 && (
                  <div className="flex items-center gap-2 text-[10px] font-mono text-amber-400">
                    <span>ANOMALY SCORE: {(evt.anomaly_score * 100).toFixed(0)}%</span>
                  </div>
                )}
              </div>
            ))
          )
        ) : (
          scenariosLoading ? (
            <div className="space-y-3">
              <Skeleton height="80px" width="100%" />
              <Skeleton height="80px" width="100%" />
            </div>
          ) : (
            scenarios?.map((sc, idx) => (
              <div
                key={idx}
                className="p-3.5 rounded-lg bg-slate-900/60 border border-cyan-500/15 flex flex-col gap-2"
              >
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold font-mono text-[#66fcf1]">
                    {sc.title || sc.headline}
                  </span>
                  <Badge variant="warning">
                    PROB: {((sc.probability ?? sc.confidence_overall ?? 0.5) * 100).toFixed(0)}%
                  </Badge>
                </div>
                <p className="text-xs text-slate-300 leading-relaxed">
                  {sc.summary || sc.description}
                </p>
              </div>
            ))
          )
        )}
      </div>
    </Card>
  );
}