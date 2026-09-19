'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { NormalizedEvent } from '../lib/types';
import { DataGrid } from './ui/DataGrid';
import { formatCurrency, formatNumber, formatPercent } from '../lib/format';
import { IconSignal } from '@/components/ui/icons';
import { ClockTime } from './ui/ClockTime';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';

export default function CryptoAnalytics() {
  const [activeChain, setActiveChain] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(
    Boolean(selectedEvent),
    () => setSelectedEvent(null),
    'Derivatives detail',
  );

  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>('/events/crypto?limit=100', fetcher, {
    refreshInterval: POLL.live,
  });

  const chainTabs = [
    { id: 'all', label: 'ALL DOMAINS' },
    { id: 'funding', label: 'PERP FUNDING' },
    { id: 'oi', label: 'OPEN INTEREST' },
    { id: 'micro', label: 'MICROSTRUCTURE' },
    { id: 'whales', label: 'WHALE TRANSFERS' },
  ];

  const filteredEvents = useMemo(() => {
    return (cryptoEvents || []).filter((e) => {
      const entityStr = (e.primary_entity_name || e.entity_name || '').toLowerCase();
      const headlineStr = (e.headline || e.summary || '').toLowerCase();
      const sourceStr = (e.source || '').toLowerCase();
      const typeStr = (e.type || '').toLowerCase();
      const q = searchQuery.toLowerCase();

      const matchesSearch =
        !q ||
        entityStr.includes(q) ||
        headlineStr.includes(q) ||
        sourceStr.includes(q) ||
        typeStr.includes(q);
      if (!matchesSearch) return false;
      if (activeChain === 'all') return true;

      if (activeChain === 'funding')
        return typeStr.includes('funding') || headlineStr.includes('funding');
      if (activeChain === 'oi')
        return typeStr.includes('interest') || headlineStr.includes('open interest');
      if (activeChain === 'micro')
        return !!(e.crypto_data?.market_microstructure || e.market_microstructure);
      if (activeChain === 'whales')
        return (
          typeStr.includes('transfer') ||
          headlineStr.includes('transfer') ||
          headlineStr.includes('whale')
        );
      return true;
    });
  }, [cryptoEvents, activeChain, searchQuery]);

  const highSeverityCount = useMemo(() => {
    return (cryptoEvents || []).filter((e) => e.anomaly_score >= 0.75).length;
  }, [cryptoEvents]);

  const fundingExtremesCount = useMemo(() => {
    return (cryptoEvents || []).filter((e) => (e.type || '').includes('funding')).length;
  }, [cryptoEvents]);

  return (
    <Card
      title="CRYPTO DERIVATIVES & MICROSTRUCTURE HUD"
      badge={
        <Badge variant="live" pulse>
          Binance &amp; Coinbase, live
        </Badge>
      }
      headerAction={<Tabs tabs={chainTabs} activeTab={activeChain} onChange={setActiveChain} />}
      noPadding
    >
      <div className="p-3.5 space-y-3">
        {/* Metric Summary HUD */}
        <div className="grid grid-cols-4 gap-2.5">
          <div className="p-2.5 bg-page border border-line rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">
              Total Monitored Streams
            </div>
            <div className="text-base font-bold text-amber-400 mt-0.5">
              {cryptoEvents?.length || 0}
            </div>
          </div>
          <div className="p-2.5 bg-page border border-purple-500/30 rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">
              Perp Funding Anomalies
            </div>
            <div className="text-base font-bold text-purple-400 mt-0.5">{fundingExtremesCount}</div>
          </div>
          <div className="p-2.5 bg-page border border-rose-500/30 rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">
              High Anomaly (Score &ge; 0.75)
            </div>
            <div className="text-base font-bold text-rose-400 mt-0.5">{highSeverityCount}</div>
          </div>
          <div className="p-2.5 bg-page border border-cyan-500/30 rounded-xl">
            <div className="text-ink-dim text-micro uppercase font-bold">Microstructure Engine</div>
            <div className="text-base font-bold text-accent mt-0.5">OFI / KYLE / AMIHUD</div>
          </div>
        </div>

        {/* Search Bar */}
        <div className="relative">
          <input
            type="text"
            placeholder="Filter by pair (BTCUSDT), funding rate, OFI, or exchange..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-inset border border-amber-500/20 rounded-xl px-3.5 py-2 text-xs text-ink placeholder-slate-500 focus:outline-none focus:border-amber-400/60 font-mono transition-colors"
          />
        </div>

        {/* Stream Items List */}
        <div className="space-y-2 max-h-[520px] overflow-y-auto pr-1">
          {filteredEvents.length > 0 ? (
            filteredEvents.map((e, i) => {
              const isHighSeverity = e.anomaly_score >= 0.75;
              const cd = e.crypto_data || {};
              const micro = cd.market_microstructure || e.market_microstructure;

              let typeBadge = 'CRYPTO';
              let badgeColor = 'bg-amber-500/20 text-amber-300 border-amber-500/40';
              if ((e.type || '').includes('funding')) {
                typeBadge = 'PERP FUNDING';
                badgeColor = 'bg-purple-500/20 text-purple-300 border-purple-500/40';
              } else if ((e.type || '').includes('interest')) {
                typeBadge = 'OPEN INTEREST';
                badgeColor = 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40';
              } else if ((e.type || '').includes('liquidation')) {
                typeBadge = 'LIQUIDATION';
                badgeColor = 'bg-rose-500/20 text-rose-300 border-rose-500/40';
              } else if (micro) {
                typeBadge = 'MICROSTRUCTURE';
                badgeColor = 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40';
              }

              return (
                <div
                  key={e.event_id || i}
                  onClick={() => setSelectedEvent(e)}
                  className={`p-3 bg-raised/70 rounded-xl border transition-all cursor-pointer hover:bg-raised/95 ${
                    isHighSeverity
                      ? 'border-rose-500/40 hover:border-rose-400 shadow-panel'
                      : 'border-line hover:border-amber-500/50'
                  }`}
                >
                  <div className="flex justify-between items-center mb-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-amber-300 font-bold text-xs uppercase flex items-center gap-1">
                        {cd.pair || e.primary_entity_name || e.entity_name || 'Crypto Asset'}
                      </span>
                      <span
                        className={`text-micro font-extrabold px-1.5 py-0.5 rounded border ${badgeColor}`}
                      >
                        {typeBadge}
                      </span>
                      <span className="text-micro text-ink-dim bg-page px-1.5 py-0.5 rounded border border-line">
                        {e.source || 'Binance Futures'}
                      </span>
                    </div>
                    <span
                      className={`text-micro font-bold px-2 py-0.5 rounded border ${
                        isHighSeverity
                          ? 'bg-rose-500/20 text-rose-400 border-rose-500/40'
                          : 'bg-amber-500/20 text-amber-300 border-amber-500/40'
                      }`}
                    >
                      SCORE: {e.anomaly_score.toFixed(2)}
                    </span>
                  </div>

                  <p className="text-ink text-xs font-sans font-semibold leading-snug">
                    {e.headline || e.summary}
                  </p>

                  {/* Rows the browser fetched directly, because the backend
                      returned nothing for this domain. They carry raw exchange
                      fields but none of the platform's enrichment, anomaly
                      scoring or correlation, so they are labelled rather than
                      presented as Sentinel's own analysis. */}
                  {e.data_provenance && (
                    <span
                      className="mt-1 inline-block rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 text-micro font-bold uppercase tracking-wide text-amber-300"
                      title="Fetched directly from the exchange by your browser. Not enriched, scored or correlated by Sentinel."
                    >
                      Unenriched · direct feed
                    </span>
                  )}

                  {/* Contextual Metric Cards for Funding, Basis, & Microstructure */}
                  <div className="mt-2 grid grid-cols-3 gap-2 pt-2 border-t border-line/80 text-micro">
                    {cd.funding_rate !== undefined && (
                      <div className="bg-page p-1.5 rounded border border-purple-500/20">
                        <span className="text-ink-dim block text-micro">FUNDING RATE:</span>
                        <span
                          className={`font-bold ${cd.funding_rate > 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                        >
                          {formatPercent(cd.funding_rate, { from: 'ratio', decimals: 4 })} (
                          {cd.basis_bps
                            ? `${cd.basis_bps > 0 ? '+' : ''}${cd.basis_bps.toFixed(1)} bps basis`
                            : 'N/A'}
                          )
                        </span>
                      </div>
                    )}

                    {cd.open_interest !== undefined && (
                      <div className="bg-page p-1.5 rounded border border-cyan-500/20">
                        <span className="text-ink-dim block text-micro">OPEN INTEREST:</span>
                        <span className="text-cyan-300 font-bold">
                          {formatNumber(cd.open_interest, { decimals: 0 })} tokens
                        </span>
                      </div>
                    )}

                    {micro && (
                      <div className="bg-page p-1.5 rounded border border-emerald-500/20 col-span-2">
                        <span className="text-ink-dim block text-micro">
                          ORDER FLOW IMBALANCE (OFI) / KYLE &lambda;:
                        </span>
                        <span className="text-emerald-400 font-bold">
                          OFI: {micro.order_flow_imbalance?.toFixed(3) || 'N/A'} | &lambda;:{' '}
                          {micro.kyle_lambda?.toExponential(2) || 'N/A'}
                        </span>
                      </div>
                    )}
                  </div>

                  <div className="mt-1.5 text-micro text-ink-dim flex justify-between items-center">
                    <span>
                      Mark:{' '}
                      <span className="text-ink">
                        $
                        {cd.mark_price
                          ? formatNumber(cd.mark_price)
                          : cd.price
                            ? formatNumber(cd.price)
                            : 'N/A'}
                      </span>
                    </span>
                    <span>
                      <ClockTime value={e.occurred_at} />
                    </span>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="p-8 text-center border border-dashed border-amber-500/20 rounded-xl text-ink-dim text-xs">
              No matching crypto events found.
            </div>
          )}
        </div>
      </div>

      {/* Inspector Modal */}
      {selectedEvent && (
        <div
          className="fixed inset-0 z-50 bg-black/80 flex items-center justify-center p-4"
          {...dialog.overlayProps}
        >
          <div
            className="bg-raised border border-amber-500/50 rounded-2xl max-w-xl w-full p-5 space-y-4 shadow-panel text-xs text-ink"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-amber-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-lg">
                  <IconSignal className="inline-block shrink-0" />
                </span>
                <span className="font-bold text-amber-300 uppercase tracking-wider">
                  Derivatives and order flow
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
                  HEADLINE / SUMMARY:
                </span>
                <p className="text-white font-bold font-sans text-sm">
                  {selectedEvent.headline || selectedEvent.summary}
                </p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-page p-3 rounded-xl border border-line">
                <div>
                  <span className="text-ink-dim">EVENT ID:</span>{' '}
                  <span className="text-amber-300 font-bold block truncate">
                    {selectedEvent.event_id}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">SOURCE:</span>{' '}
                  <span className="text-amber-300 font-bold block">{selectedEvent.source}</span>
                </div>
                <div>
                  <span className="text-ink-dim">ANOMALY SCORE:</span>{' '}
                  <span className="text-rose-400 font-bold block">
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

              {selectedEvent.crypto_data && (
                <div className="p-3 bg-page rounded-xl border border-purple-500/30 space-y-2">
                  <span className="text-purple-300 font-bold block text-micro">
                    Crypto derivatives
                  </span>
                  <div className="grid grid-cols-3 gap-2 text-micro">
                    <div>
                      <span className="text-ink-dim block">Funding Rate:</span>{' '}
                      <span className="text-white font-bold">
                        {formatPercent(selectedEvent.crypto_data.funding_rate, {
                          from: 'ratio',
                          decimals: 4,
                        })}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Mark Price:</span>{' '}
                      <span className="text-white font-bold">
                        {selectedEvent.crypto_data.mark_price
                          ? formatCurrency(selectedEvent.crypto_data.mark_price)
                          : 'N/A'}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Index Price:</span>{' '}
                      <span className="text-white font-bold">
                        {selectedEvent.crypto_data.index_price
                          ? formatCurrency(selectedEvent.crypto_data.index_price)
                          : 'N/A'}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Perp-Spot Basis:</span>{' '}
                      <span className="text-cyan-300 font-bold">
                        {selectedEvent.crypto_data.basis_bps
                          ? `${selectedEvent.crypto_data.basis_bps.toFixed(2)} bps`
                          : 'N/A'}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Open Interest:</span>{' '}
                      <span className="text-cyan-300 font-bold">
                        {selectedEvent.crypto_data.open_interest
                          ? formatNumber(selectedEvent.crypto_data.open_interest, { decimals: 0 })
                          : 'N/A'}
                      </span>
                    </div>
                  </div>
                </div>
              )}

              <div>
                <span className="text-amber-400 font-bold block mb-1 text-micro">
                  EVENT DETAIL:
                </span>
                <div className="max-h-40 overflow-y-auto">
                  <DataGrid
                    data={selectedEvent.crypto_data || selectedEvent.domain_data || selectedEvent}
                    omit={['raw_payload']}
                  />
                </div>
              </div>
            </div>

            <button
              onClick={() => setSelectedEvent(null)}
              className="w-full py-2 bg-raised text-amber-300 border border-amber-500/40 rounded-xl text-xs font-bold hover:bg-overlay transition-colors cursor-pointer"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
