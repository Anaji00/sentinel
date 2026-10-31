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
import { NoteTile, StatTile } from './ui/StatTile';

// Scores arrive null on some rows; a null here crashed the whole view.
function fixed(value: number | null | undefined, digits: number): string {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '—';
}


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
    { id: 'all', label: 'Everything' },
    { id: 'funding', label: 'Futures fees' },
    { id: 'oi', label: 'Open bets' },
    { id: 'micro', label: 'Price moves' },
    { id: 'whales', label: 'Big transfers' },
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
      title="Crypto futures and order flow"
      badge={
        <Badge variant="live" pulse>
          Binance &amp; Coinbase, live
        </Badge>
      }
      headerAction={<Tabs tabs={chainTabs} activeTab={activeChain} onChange={setActiveChain} />}
      noPadding
    >
      <div className="p-3.5 space-y-3">
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
          <StatTile value={formatNumber(cryptoEvents?.length ?? 0, { decimals: 0 })} label="Recent events" />
          <StatTile value={formatNumber(fundingExtremesCount, { decimals: 0 })} label="Unusual futures fees" />
          <StatTile
            value={formatNumber(highSeverityCount, { decimals: 0 })}
            label="Very unusual events"
            tone={highSeverityCount ? 'text-negative' : 'text-ink'}
          />
          <NoteTile title="What a futures fee means">
            Futures traders pay each other a small fee to keep bets open. A high positive fee means most are betting on a
            rise.
          </NoteTile>
        </div>

        {/* Search Bar */}
        <div className="relative">
          <input
            type="text"
            placeholder="Filter by pair (BTCUSDT), funding rate, OFI, or exchange..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-inset border border-caution/30 rounded-xl px-3.5 py-2 text-xs text-ink placeholder:text-ink-mute focus:outline-none focus:border-caution/30 font-mono transition-colors"
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
              let badgeColor = 'bg-caution/10 text-caution border-caution/30';
              if ((e.type || '').includes('funding')) {
                typeBadge = 'FUTURES FEE';
                badgeColor = 'bg-accent-dim text-accent border-line-accent';
              } else if ((e.type || '').includes('interest')) {
                typeBadge = 'OPEN BETS';
                badgeColor = 'bg-accent-dim text-accent border-line-accent';
              } else if ((e.type || '').includes('liquidation')) {
                typeBadge = 'FORCED SALE';
                badgeColor = 'bg-negative/10 text-negative border-negative/30';
              } else if (micro) {
                typeBadge = 'PRICE MOVE';
                badgeColor = 'bg-positive/10 text-positive border-positive/30';
              }

              return (
                <div
                  key={e.event_id || i}
                  onClick={() => setSelectedEvent(e)}
                  className={`p-3 bg-raised/70 rounded-xl border transition-all cursor-pointer hover:bg-raised/95 ${
                    isHighSeverity
                      ? 'border-negative/30 hover:border-negative/30 shadow-panel'
                      : 'border-line hover:border-caution/30'
                  }`}
                >
                  <div className="flex justify-between items-center mb-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-caution font-bold text-xs flex items-center gap-1">
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
                          ? 'bg-negative/10 text-negative border-negative/30'
                          : 'bg-caution/10 text-caution border-caution/30'
                      }`}
                    >
                      SCORE: {fixed(e.anomaly_score, 2)}
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
                      className="mt-1 inline-block rounded border border-caution/30 bg-caution/10 px-1.5 py-0.5 text-micro font-bold uppercase tracking-wide text-caution"
                      title="Fetched directly from the exchange by your browser. Not enriched, scored or correlated by Sentinel."
                    >
                      Unenriched · direct feed
                    </span>
                  )}

                  {/* Contextual Metric Cards for Funding, Basis, & Microstructure */}
                  <div className="mt-2 grid grid-cols-3 gap-2 pt-2 border-t border-line/80 text-micro">
                    {cd.funding_rate !== undefined && (
                      <div className="bg-page p-1.5 rounded border border-line">
                        <span className="text-ink-dim block text-micro">Futures fee:</span>
                        <span
                          className={`font-bold ${cd.funding_rate > 0 ? 'text-positive' : 'text-negative'}`}
                        >
                          {formatPercent(cd.funding_rate, { from: 'ratio', decimals: 4 })} (
                          {cd.basis_bps
                            ? `futures ${Math.abs(cd.basis_bps / 100).toFixed(3)}% ${cd.basis_bps >= 0 ? 'above' : 'below'} the spot price`
                            : 'N/A'}
                          )
                        </span>
                      </div>
                    )}

                    {cd.open_interest !== undefined && (
                      <div className="bg-page p-1.5 rounded border border-line">
                        <span className="text-ink-dim block text-micro">Open bets:</span>
                        <span className="text-accent font-bold">
                          {formatNumber(cd.open_interest, { decimals: 0 })} tokens
                        </span>
                      </div>
                    )}

                    {micro && (
                      <div className="bg-page p-1.5 rounded border border-positive/30 col-span-2">
                        <span className="text-ink-dim block text-micro">
                          ORDER FLOW IMBALANCE (OFI) / KYLE &lambda;:
                        </span>
                        <span className="text-positive font-bold">
                          OFI: {micro.order_flow_imbalance?.toFixed(3) || 'N/A'}{' '}| &lambda;:{' '}
                          {micro.kyle_lambda?.toExponential(2) || 'N/A'}
                        </span>
                      </div>
                    )}
                  </div>

                  <div className="mt-1.5 text-micro text-ink-dim flex justify-between items-center">
                    <span>
                      Price:{' '}
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
            <div className="p-8 text-center border border-dashed border-caution/30 rounded-xl text-ink-dim text-xs">
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
            className="bg-raised border border-caution/30 rounded-2xl max-w-xl w-full p-5 space-y-4 shadow-panel text-xs text-ink"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-caution/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-lg">
                  <IconSignal className="inline-block shrink-0" />
                </span>
                <span className="font-bold text-caution">
                  Derivatives and order flow
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
                  Headline / summary:
                </span>
                <p className="text-ink font-bold font-sans text-sm">
                  {selectedEvent.headline || selectedEvent.summary}
                </p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-page p-3 rounded-xl border border-line">
                <div>
                  <span className="text-ink-dim">Event ID:</span>{' '}
                  <span className="text-caution font-bold block truncate">
                    {selectedEvent.event_id}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">SOURCE:</span>{' '}
                  <span className="text-caution font-bold block">{selectedEvent.source}</span>
                </div>
                <div>
                  <span className="text-ink-dim">Anomaly score:</span>{' '}
                  <span className="text-negative font-bold block">
                    {fixed(selectedEvent.anomaly_score, 2)}
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
                <div className="p-3 bg-page rounded-xl border border-line space-y-2">
                  <span className="text-accent font-bold block text-micro">
                    Crypto derivatives
                  </span>
                  <div className="grid grid-cols-3 gap-2 text-micro">
                    <div>
                      <span className="text-ink-dim block">Funding Rate:</span>{' '}
                      <span className="text-ink font-bold">
                        {formatPercent(selectedEvent.crypto_data.funding_rate, {
                          from: 'ratio',
                          decimals: 4,
                        })}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Mark Price:</span>{' '}
                      <span className="text-ink font-bold">
                        {selectedEvent.crypto_data.mark_price
                          ? formatCurrency(selectedEvent.crypto_data.mark_price)
                          : 'N/A'}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Index Price:</span>{' '}
                      <span className="text-ink font-bold">
                        {selectedEvent.crypto_data.index_price
                          ? formatCurrency(selectedEvent.crypto_data.index_price)
                          : 'N/A'}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Perp-Spot Basis:</span>{' '}
                      <span className="text-accent font-bold">
                        {selectedEvent.crypto_data.basis_bps
                          ? `${selectedEvent.crypto_data.basis_bps.toFixed(2)} bps`
                          : 'N/A'}
                      </span>
                    </div>
                    <div>
                      <span className="text-ink-dim block">Open Interest:</span>{' '}
                      <span className="text-accent font-bold">
                        {selectedEvent.crypto_data.open_interest
                          ? formatNumber(selectedEvent.crypto_data.open_interest, { decimals: 0 })
                          : 'N/A'}
                      </span>
                    </div>
                  </div>
                </div>
              )}

              <div>
                <span className="text-caution font-bold block mb-1 text-micro">
                  Event detail:
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
              className="w-full py-2 bg-raised text-caution border border-caution/30 rounded-xl text-xs font-bold hover:bg-overlay transition-colors cursor-pointer"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
