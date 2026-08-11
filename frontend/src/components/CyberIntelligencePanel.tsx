'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { NormalizedEvent } from '../lib/types';

export default function CyberIntelligencePanel() {
  const [activeFilter, setActiveFilter] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
    '/events/cyber?limit=100',
    fetcher,
    { refreshInterval: 5000 }
  );

  const filterTabs = [
    { id: 'all', label: 'ALL THREATS' },
    { id: 'cve', label: 'CVE & VULNERABILITIES' },
    { id: 'bgp', label: 'BGP ROUTE LEAKS' },
    { id: 'ransomware', label: 'RANSOMWARE GROUPS' },
  ];

  const filteredEvents = useMemo(() => {
    return (cyberEvents || []).filter(e => {
      const headlineStr = (e.headline || e.summary || '').toLowerCase();
      const entityStr = (e.primary_entity_name || e.entity_name || '').toLowerCase();
      const typeStr = (e.type || '').toLowerCase();
      const q = searchQuery.toLowerCase();

      const matchesSearch = !q || headlineStr.includes(q) || entityStr.includes(q) || typeStr.includes(q);
      if (!matchesSearch) return false;
      if (activeFilter === 'all') return true;

      if (activeFilter === 'cve') return typeStr.includes('vulnerability') || headlineStr.includes('cve');
      if (activeFilter === 'bgp') return typeStr.includes('bgp') || headlineStr.includes('bgp') || headlineStr.includes('route');
      if (activeFilter === 'ransomware') return typeStr.includes('ransomware') || headlineStr.includes('ransomware') || headlineStr.includes('breach');
      return true;
    });
  }, [cyberEvents, activeFilter, searchQuery]);

  const criticalCount = useMemo(() => {
    return (cyberEvents || []).filter(e => e.anomaly_score >= 0.75 || (e.security_data?.cvss_score || 0) >= 8.0).length;
  }, [cyberEvents]);

  const cisaKevCount = useMemo(() => {
    return (cyberEvents || []).filter(e => e.security_data?.cisa_kev).length;
  }, [cyberEvents]);

  return (
    <Card
      title="CYBER INTELLIGENCE & INFRASTRUCTURE HUD"
      badge={<Badge variant="live" pulse>CISA & BGP REAL-TIME</Badge>}
      headerAction={
        <Tabs
          tabs={filterTabs}
          activeTab={activeFilter}
          onChange={setActiveFilter}
        />
      }
      noPadding
    >
      <div className="p-3.5 space-y-3 font-mono">
        {/* Metric Summary HUD */}
        <div className="grid grid-cols-4 gap-2.5">
          <div className="p-2.5 bg-slate-950 border border-slate-800 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Threat Events Monitored</div>
            <div className="text-base font-bold text-rose-400 mt-0.5">{cyberEvents?.length || 0}</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-rose-500/30 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Critical Vulnerabilities</div>
            <div className="text-base font-bold text-rose-400 mt-0.5">{criticalCount}</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-amber-500/30 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">CISA KEV Exploits Active</div>
            <div className="text-base font-bold text-amber-300 mt-0.5">{cisaKevCount}</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-purple-500/30 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Routing Stream</div>
            <div className="text-base font-bold text-purple-400 mt-0.5">BGP HIJACK DETECTOR</div>
          </div>
        </div>

        {/* Search Bar */}
        <div className="relative">
          <input
            type="text"
            placeholder="Search by CVE-ID, target organisation, ASN, or ransomware group..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-[#080a10] border border-rose-500/20 rounded-xl px-3.5 py-2 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-rose-400/60 font-mono transition-colors"
          />
        </div>

        {/* Threat Events Stream */}
        <div className="space-y-2 max-h-[520px] overflow-y-auto pr-1">
          {filteredEvents.length > 0 ? (
            filteredEvents.map((e, i) => {
              const sd: any = e.security_data || {};
              const isCritical = e.anomaly_score >= 0.75 || (sd.cvss_score || 0) >= 8.0;

              return (
                <div
                  key={e.event_id || i}
                  onClick={() => setSelectedEvent(e)}
                  className={`p-3 bg-slate-900/70 rounded-xl border transition-all cursor-pointer hover:bg-slate-900/95 ${
                    isCritical
                      ? 'border-rose-500/40 hover:border-rose-400 shadow-[0_0_15px_rgba(244,63,94,0.15)]'
                      : 'border-slate-800 hover:border-purple-500/50'
                  }`}
                >
                  <div className="flex justify-between items-center mb-1.5">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-rose-400 font-bold text-xs uppercase flex items-center gap-1">
                        🔐 {sd.cve_id || sd.affected_org || e.primary_entity_name || 'Cyber Target'}
                      </span>
                      {sd.cvss_score !== undefined && (
                        <span className={`text-[9px] font-extrabold px-1.5 py-0.5 rounded border ${
                          sd.cvss_score >= 9.0 ? 'bg-rose-500/20 text-rose-300 border-rose-500/40' : 'bg-amber-500/20 text-amber-300 border-amber-500/40'
                        }`}>
                          CVSS {sd.cvss_score.toFixed(1)}
                        </span>
                      )}
                      {sd.cisa_kev && (
                        <span className="px-1.5 py-0.5 rounded border border-rose-500/40 bg-rose-500/20 text-rose-300 font-extrabold text-[9px]">
                          CISA KEV ACTIVE
                        </span>
                      )}
                      {sd.asn && (
                        <span className="text-[10px] text-slate-400 bg-slate-950 px-1.5 py-0.5 rounded border border-slate-800">
                          {sd.asn}
                        </span>
                      )}
                    </div>
                    <span className={`text-[10px] font-bold px-2 py-0.5 rounded border ${
                      isCritical
                        ? 'bg-rose-500/20 text-rose-400 border-rose-500/40'
                        : 'bg-purple-500/20 text-purple-300 border-purple-500/40'
                    }`}>
                      SCORE: {e.anomaly_score.toFixed(2)}
                    </span>
                  </div>

                  <p className="text-slate-200 text-xs font-sans font-semibold leading-snug">
                    {e.headline || e.summary}
                  </p>

                  <div className="mt-2 text-[10px] text-slate-400 flex justify-between items-center pt-1 border-t border-slate-800/80">
                    <span>Source: <span className="text-cyan-400 font-bold">{e.source || 'CISA Feed'}</span></span>
                    <span>{new Date(e.occurred_at).toLocaleTimeString()}</span>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="p-8 text-center border border-dashed border-rose-500/20 rounded-xl text-slate-400 text-xs">
              No matching cyber threat events found.
            </div>
          )}
        </div>
      </div>

      {/* Cyber Event Inspector Modal */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4 font-mono">
          <div className="bg-[#0b0e17] border border-rose-500/50 rounded-2xl max-w-xl w-full p-5 space-y-4 shadow-[0_0_40px_rgba(244,63,94,0.25)] text-xs text-slate-200">
            <div className="flex items-center justify-between border-b border-rose-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-lg">🔐</span>
                <span className="font-bold text-rose-400 uppercase tracking-wider">CYBER THREAT TELEMETRY INSPECTOR</span>
              </div>
              <button
                onClick={() => setSelectedEvent(null)}
                className="text-slate-400 hover:text-white text-xs font-bold px-2 py-0.5 rounded bg-slate-800 cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-3">
              <div>
                <span className="text-slate-400 block mb-0.5 text-[10px] uppercase font-bold">THREAT HEADLINE:</span>
                <p className="text-white font-bold font-sans text-sm">{selectedEvent.headline || selectedEvent.summary}</p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-slate-950 p-3 rounded-xl border border-slate-800">
                <div><span className="text-slate-400">EVENT ID:</span> <span className="text-rose-300 font-bold block truncate">{selectedEvent.event_id}</span></div>
                <div><span className="text-slate-400">SOURCE:</span> <span className="text-rose-300 font-bold block">{selectedEvent.source}</span></div>
                <div><span className="text-slate-400">ANOMALY SCORE:</span> <span className="text-rose-400 font-bold block">{selectedEvent.anomaly_score.toFixed(2)}</span></div>
                <div><span className="text-slate-400">TIMESTAMP:</span> <span className="text-slate-200 block">{new Date(selectedEvent.occurred_at).toUTCString()}</span></div>
              </div>

              {selectedEvent.security_data && (
                <div className="p-3 bg-slate-950 rounded-xl border border-rose-500/30 space-y-2">
                  <span className="text-rose-400 font-bold block text-[11px]">VULNERABILITY & ROUTING DETAILS</span>
                  <div className="grid grid-cols-2 gap-2 text-[10px]">
                    <div><span className="text-slate-400 block">CVE ID:</span> <span className="text-white font-bold">{selectedEvent.security_data.cve_id || 'N/A'}</span></div>
                    <div><span className="text-slate-400 block">CVSS Score:</span> <span className="text-white font-bold">{selectedEvent.security_data.cvss_score ? selectedEvent.security_data.cvss_score.toFixed(1) : 'N/A'}</span></div>
                    <div><span className="text-slate-400 block">Affected Org:</span> <span className="text-cyan-300 font-bold">{selectedEvent.security_data.affected_org || 'N/A'}</span></div>
                    <div><span className="text-slate-400 block">IP / ASN:</span> <span className="text-purple-300 font-bold">{selectedEvent.security_data.ip_address || selectedEvent.security_data.asn || 'N/A'}</span></div>
                  </div>
                </div>
              )}

              <div>
                <span className="text-rose-400 font-bold block mb-1 text-[10px]">RAW THREAT PAYLOAD:</span>
                <pre className="p-3 bg-slate-950 rounded-xl border border-slate-800 text-[10px] text-rose-200 overflow-x-auto max-h-40">
                  {JSON.stringify(selectedEvent.security_data || selectedEvent.domain_data || selectedEvent.raw_payload || selectedEvent, null, 2)}
                </pre>
              </div>
            </div>

            <button
              onClick={() => setSelectedEvent(null)}
              className="w-full py-2 bg-slate-900 text-rose-400 border border-rose-500/40 rounded-xl text-xs font-bold hover:bg-slate-800 transition-colors cursor-pointer"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
