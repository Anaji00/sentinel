'use client';

import React, { useMemo, useState } from 'react';
import { Input } from './ui/Input';
import useSWR from 'swr';
import { failedWithout } from '../lib/dataHealth';
import { PanelError } from './ui/PanelError';
import { fetcher } from '../lib/api';
import { ABSENT, formatCurrency, formatNumber, formatPercent } from '../lib/format';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { NormalizedEvent, PredictionMarketData } from '../lib/types';
import { IconTarget } from '@/components/ui/icons';
import { ClockTime } from './ui/ClockTime';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';
import { NoteTile, StatTile } from './ui/StatTile';

// Scores arrive null on some rows; one null took the whole Macro page down.
function fixed(value: number | null | undefined, digits: number): string {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '—';
}


export default function PredictionMarketPanel() {
  const [activeCategory, setActiveCategory] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(Boolean(selectedEvent), () => setSelectedEvent(null), 'Contract detail');

  const { data: predEvents, error: predError, mutate: retryPred } = useSWR<NormalizedEvent[]>('/events/prediction?limit=100', fetcher, {
    refreshInterval: POLL.live,
  });

  const categoryTabs = [
    { id: 'all', label: 'All' },
    { id: 'macro', label: 'Economy & the Fed' },
    { id: 'geopolitics', label: 'World events' },
    { id: 'tech', label: 'Tech & AI' },
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
      title="What prediction markets expect"
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
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
          <StatTile value={formatNumber(predEvents?.length ?? 0, { decimals: 0 })} label="Betting markets followed" />
          <StatTile
            value={formatNumber(highVolumeCount, { decimals: 0 })}
            label="With over $500K bet on them"
          />
          <NoteTile title="What this shows">
            Real-money betting markets on elections, the Fed and world events. The odds are what bettors collectively
            think will happen; a jump of 10 points or more is flagged.
          </NoteTile>
        </div>

        {/* Search Bar */}
        <div className="relative">
          <Input
            label="Search prediction markets"
            hideLabel
            size="sm"
            containerClassName="w-full"
            className="font-mono"
            type="text"
            placeholder="Search prediction contract, ticker, or catalyst..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>

        {/* Contract List */}
        <div className="space-y-2 max-h-[520px] overflow-y-auto pr-1">
          {filteredEvents.length > 0 ? (
            filteredEvents.map((e, i) => {
              const pd: Partial<PredictionMarketData> = e.prediction_market_data || {};
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
                  className="p-3 bg-raised/70 rounded-xl border border-line hover:border-line-accent transition-all cursor-pointer hover:bg-raised/95 shadow-panel"
                >
                  <div className="flex justify-between items-center mb-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-accent font-bold text-xs flex items-center gap-1">
                        {pd.ticker || e.primary_entity_name || 'Prediction Contract'}
                      </span>
                      <span className="text-micro text-ink-dim bg-page px-1.5 py-0.5 rounded border border-line">
                        {e.source || 'PolyMarket API'}
                      </span>
                    </div>
                    <span className="text-micro font-bold px-2 py-0.5 rounded border bg-accent-dim text-accent border-line-accent">
                      {volumeStr}
                    </span>
                  </div>

                  <p className="text-ink text-xs font-sans font-semibold leading-snug">
                    {e.headline || e.summary}
                  </p>

                  {/* Probability Bar */}
                  <div className="mt-2.5 space-y-1">
                    <div className="flex justify-between text-micro font-bold">
                      <span className="text-positive">YES: {yesProb}</span>
                      <span className="text-negative">NO: {noProb}</span>
                    </div>
                    <div className="w-full h-1.5 bg-page rounded-full overflow-hidden flex border border-line">
                      <div className="bg-positive h-full" style={{ width: `${yesPct ?? 0}%` }} />
                      <div className="bg-negative h-full" style={{ width: `${noPct ?? 0}%` }} />
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
          ) : failedWithout(predError, predEvents) ? (
            <PanelError what="prediction markets" onRetry={() => void retryPred()} />
          ) : (
            <div className="p-8 text-center border border-dashed border-line rounded-xl text-ink-dim text-xs">
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
            className="bg-raised border border-line-accent rounded-2xl max-w-xl w-full p-5 space-y-4 shadow-panel text-xs text-ink"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-line pb-3">
              <div className="flex items-center gap-2">
                <span className="text-lg">
                  <IconTarget className="inline-block shrink-0" />
                </span>
                <span className="font-bold text-accent">
                  Contract detail
                </span>
              </div>
              <button
                onClick={() => setSelectedEvent(null)}
                className="text-ink-dim hover:text-ink text-xs font-bold px-2 py-0.5 rounded bg-overlay cursor-pointer"
              >
                CLOSE
              </button>
            </div>

            <div className="space-y-3">
              <div>
                <span className="text-ink-dim block mb-0.5 text-micro uppercase font-bold">
                  Contract title:
                </span>
                <p className="text-ink font-bold font-sans text-sm">
                  {selectedEvent.headline || selectedEvent.summary}
                </p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-page p-3 rounded-xl border border-line">
                <div>
                  <span className="text-ink-dim">Event ID:</span>{' '}
                  <span className="text-accent font-bold block truncate">
                    {selectedEvent.event_id}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">Source:</span>{' '}
                  <span className="text-accent font-bold block">{selectedEvent.source}</span>
                </div>
                <div>
                  <span className="text-ink-dim">Anomaly score:</span>{' '}
                  <span className="text-accent font-bold block">
                    {fixed(selectedEvent.anomaly_score, 2)}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">Time:</span>{' '}
                  <span className="text-ink block">
                    {new Date(selectedEvent.occurred_at).toUTCString()}
                  </span>
                </div>
              </div>

              {selectedEvent.prediction_market_data && (
                <div className="p-3 bg-page rounded-xl border border-line space-y-2">
                  <span className="text-accent font-bold block text-micro">
                    Odds and volume
                  </span>
                  <div className="grid grid-cols-2 gap-2 text-micro">
                    <div>
                      <span className="text-ink-dim block">YES Bid Probability:</span>{' '}
                      <span className="text-positive font-bold">
                        {formatPercent(selectedEvent.prediction_market_data.yes_probability, {
                          from: 'ratio',
                          decimals: 1,
                        })}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">NO Bid Probability:</span>{' '}
                      <span className="text-negative font-bold">
                        {formatPercent(selectedEvent.prediction_market_data.no_probability, {
                          from: 'ratio',
                          decimals: 1,
                        })}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Total Volume:</span>{' '}
                      <span className="text-accent font-bold">
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
              className="w-full py-2 bg-raised text-accent border border-line-accent rounded-xl text-xs font-bold hover:bg-overlay transition-colors cursor-pointer"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
