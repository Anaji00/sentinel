'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatCurrency, formatPercent } from '../lib/format';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { NormalizedEvent } from '../lib/types';
import { IconTarget } from '@/components/ui/icons';
import { ClockTime } from './ui/ClockTime';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';

export default function PredictionMarketPanel() {
  const [activeCategory, setActiveCategory] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(Boolean(selectedEvent), () => setSelectedEvent(null), 'Contract detail');

  const { data: predEvents } = useSWR<NormalizedEvent[]>('/events/prediction?limit=100', fetcher, {
    refreshInterval: POLL.live,
  });

  const categoryTabs = [
    { id: 'all', label: 'ALL MARKETS' },
    { id: 'macro', label: 'MACRO & FED RATES' },
    { id: 'geopolitics', label: 'GEOPOLITICS' },
    { id: 'tech', label: 'TECH & AI' },
  ];

  const filteredEvents = useMemo(() => {
    return (predEvents || []).filter((e) => {
      const headlineStr = (e.headline || e.summary || '').toLowerCase();
      const entityStr = (e.primary_entity_name || e.entity_name || '').toLowerCase();
      const q = searchQuery.toLowerCase();

      const matchesSearch = !q || headlineStr.includes(q) || entityStr.includes(q);
      if (!matchesSearch) return false;
      if (activeCategory === 'all') return true;

      const fullText = `${headlineStr} ${entityStr} ${(e.tags || []).join(' ')}`;
      if (activeCategory === 'macro')
        return (
          fullText.includes('rate') ||
          fullText.includes('fed') ||
          fullText.includes('cpi') ||
          fullText.includes('recession')
        );
      if (activeCategory === 'geopolitics')
        return (
          fullText.includes('election') ||
          fullText.includes('war') ||
          fullText.includes('sanction') ||
          fullText.includes('china') ||
          fullText.includes('taiwan')
        );
      if (activeCategory === 'tech')
        return (
          fullText.includes('ai') ||
          fullText.includes('gpt') ||
          fullText.includes('nvidia') ||
          fullText.includes('model')
        );
      return true;
    });
  }, [predEvents, activeCategory, searchQuery]);

  const highVolumeCount = useMemo(() => {
    return (predEvents || []).filter((e) => (e.prediction_market_data?.total_volume || 0) >= 500000)
      .length;
  }, [predEvents]);

  return (
    <Card
      title="POLYMARKET & KALSHI PROBABILITY RADAR"
      badge={
        <Badge variant="live" pulse>
          Live odds
        </Badge>
      }
      headerAction={
        <Tabs tabs={categoryTabs} activeTab={activeCategory} onChange={setActiveCategory} />
      }
      noPadding
    >
      <div className="p-3.5 space-y-3">
        {/* Metric Summary HUD */}
        <div className="grid grid-cols-4 gap-2.5">
          <div className="p-2.5 bg-page border border-line rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">
              Tracked Prediction Contracts
            </div>
            <div className="text-base font-bold text-purple-400 mt-0.5">
              {predEvents?.length || 0}
            </div>
          </div>
          <div className="p-2.5 bg-page border border-purple-500/30 rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">
              High Volume (&gt; $500k)
            </div>
            <div className="text-base font-bold text-purple-400 mt-0.5">{highVolumeCount}</div>
          </div>
          <div className="p-2.5 bg-page border border-cyan-500/30 rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">
              Probability Swing Signal
            </div>
            <div className="text-base font-bold text-cyan-300 mt-0.5">&ge; 10% Shift</div>
          </div>
          <div className="p-2.5 bg-page border border-emerald-500/30 rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">Cross-Domain Engine</div>
            <div className="text-base font-bold text-emerald-400 mt-0.5">EXCITATION ACTIVE</div>
          </div>
        </div>

        {/* Search Bar */}
        <div className="relative">
          <input
            type="text"
            placeholder="Search prediction contract, ticker, or catalyst..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-inset border border-purple-500/20 rounded-xl px-3.5 py-2 text-xs text-ink placeholder-slate-500 focus:outline-none focus:border-purple-400/60 font-mono transition-colors"
          />
        </div>

        {/* Contract List */}
        <div className="space-y-2 max-h-[520px] overflow-y-auto pr-1">
          {filteredEvents.length > 0 ? (
            filteredEvents.map((e, i) => {
              const pd: any = e.prediction_market_data || {};
              // Probabilities and volume are reported only when quoted. A
              // defaulted 50/50 book or a "$120k Vol" placeholder is a market
              // claim the platform has not observed.
              const yesPct =
                typeof pd.yes_probability === 'number' ? pd.yes_probability * 100 : null;
              const noPct =
                typeof pd.no_probability === 'number'
                  ? pd.no_probability * 100
                  : yesPct !== null
                    ? 100 - yesPct
                    : null;
              const yesProb = formatPercent(yesPct, { decimals: 0 });
              const noProb = formatPercent(noPct, { decimals: 0 });
              const volumeStr =
                typeof pd.total_volume === 'number'
                  ? `${formatCurrency(pd.total_volume)} Vol`
                  : `${ABSENT} Vol`;

              return (
                <div
                  key={e.event_id || i}
                  onClick={() => setSelectedEvent(e)}
                  className="p-3 bg-raised/70 rounded-xl border border-line hover:border-purple-500/50 transition-all cursor-pointer hover:bg-raised/95 shadow-panel"
                >
                  <div className="flex justify-between items-center mb-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-purple-300 font-bold text-xs uppercase flex items-center gap-1">
                        {pd.ticker || e.primary_entity_name || 'Prediction Contract'}
                      </span>
                      <span className="text-micro text-ink-dim bg-page px-1.5 py-0.5 rounded border border-line">
                        {e.source || 'PolyMarket API'}
                      </span>
                    </div>
                    <span className="text-micro font-bold px-2 py-0.5 rounded border bg-purple-500/20 text-purple-300 border-purple-500/40">
                      {volumeStr}
                    </span>
                  </div>

                  <p className="text-ink text-xs font-sans font-semibold leading-snug">
                    {e.headline || e.summary}
                  </p>

                  {/* Probability Bar */}
                  <div className="mt-2.5 space-y-1">
                    <div className="flex justify-between text-micro font-bold">
                      <span className="text-emerald-400">YES: {yesProb}</span>
                      <span className="text-rose-400">NO: {noProb}</span>
                    </div>
                    <div className="w-full h-1.5 bg-page rounded-full overflow-hidden flex border border-line">
                      <div className="bg-emerald-400 h-full" style={{ width: `${yesPct ?? 0}%` }} />
                      <div className="bg-rose-500 h-full" style={{ width: `${noPct ?? 0}%` }} />
                    </div>
                  </div>

                  <div className="mt-2 text-micro text-ink-dim flex justify-between items-center pt-1 border-t border-line/80">
                    <span>
                      Category: <span className="text-ink">{pd.category || 'Geopolitics'}</span>
                    </span>
                    <span>
                      <ClockTime value={e.occurred_at} />
                    </span>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="p-8 text-center border border-dashed border-purple-500/20 rounded-xl text-ink-dim text-xs">
              No matching prediction market contracts found.
            </div>
          )}
        </div>
      </div>

      {/* Contract Inspector Modal */}
      {selectedEvent && (
        <div
          className="fixed inset-0 z-50 bg-black/80 flex items-center justify-center p-4"
          {...dialog.overlayProps}
        >
          <div
            className="bg-raised border border-purple-500/50 rounded-2xl max-w-xl w-full p-5 space-y-4 shadow-panel text-xs text-ink"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-purple-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-lg">
                  <IconTarget className="inline-block shrink-0" />
                </span>
                <span className="font-bold text-purple-300 uppercase tracking-wider">
                  Contract detail
                </span>
              </div>
              <button
                onClick={() => setSelectedEvent(null)}
                className="text-ink-dim hover:text-white text-xs font-bold px-2 py-0.5 rounded bg-overlay cursor-pointer"
              >
                CLOSE
              </button>
            </div>

            <div className="space-y-3">
              <div>
                <span className="text-ink-dim block mb-0.5 text-micro uppercase font-bold">
                  CONTRACT TITLE:
                </span>
                <p className="text-white font-bold font-sans text-sm">
                  {selectedEvent.headline || selectedEvent.summary}
                </p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-page p-3 rounded-xl border border-line">
                <div>
                  <span className="text-ink-dim">EVENT ID:</span>{' '}
                  <span className="text-purple-300 font-bold block truncate">
                    {selectedEvent.event_id}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">SOURCE:</span>{' '}
                  <span className="text-purple-300 font-bold block">{selectedEvent.source}</span>
                </div>
                <div>
                  <span className="text-ink-dim">ANOMALY SCORE:</span>{' '}
                  <span className="text-purple-400 font-bold block">
                    {selectedEvent.anomaly_score.toFixed(2)}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">TIMESTAMP:</span>{' '}
                  <span className="text-ink block">
                    {new Date(selectedEvent.occurred_at).toUTCString()}
                  </span>
                </div>
              </div>

              {selectedEvent.prediction_market_data && (
                <div className="p-3 bg-page rounded-xl border border-purple-500/30 space-y-2">
                  <span className="text-purple-300 font-bold block text-micro">
                    Odds and volume
                  </span>
                  <div className="grid grid-cols-2 gap-2 text-micro">
                    <div>
                      <span className="text-ink-dim block">YES Bid Probability:</span>{' '}
                      <span className="text-emerald-400 font-bold">
                        {formatPercent(selectedEvent.prediction_market_data.yes_probability, {
                          from: 'ratio',
                          decimals: 1,
                        })}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">NO Bid Probability:</span>{' '}
                      <span className="text-rose-400 font-bold">
                        {formatPercent(selectedEvent.prediction_market_data.no_probability, {
                          from: 'ratio',
                          decimals: 1,
                        })}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Total Volume:</span>{' '}
                      <span className="text-cyan-300 font-bold">
                        {selectedEvent.prediction_market_data.total_volume
                          ? formatCurrency(selectedEvent.prediction_market_data.total_volume, {
                              decimals: 0,
                            })
                          : 'N/A'}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Resolution Date:</span>{' '}
                      <span className="text-ink font-bold">
                        {selectedEvent.prediction_market_data.resolution_date || 'N/A'}
                      </span>
                    </div>
                  </div>
                </div>
              )}
            </div>

            <button
              onClick={() => setSelectedEvent(null)}
              className="w-full py-2 bg-raised text-purple-300 border border-purple-500/40 rounded-xl text-xs font-bold hover:bg-overlay transition-colors cursor-pointer"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
