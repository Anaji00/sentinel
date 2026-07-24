'use client';

import React, { useEffect, useState } from "react";
import { ComposableMap, Geographies, Geography, Marker, Line } from "react-simple-maps";
import useSWR from "swr";
import { fetcher } from "../lib/api";
import { NormalizedEvent } from "../lib/types";
import { useLiveEvents } from "../lib/useLiveEvents";

// Polyfill d3 selection.prototype.interrupt to prevent 'r.interrupt is not a function' errors during SVG map transforms
if (typeof window !== "undefined") {
  try {
    const d3Select = require("d3-selection");
    if (d3Select && d3Select.selection && typeof d3Select.selection.prototype.interrupt !== "function") {
      d3Select.selection.prototype.interrupt = function () {
        return this;
      };
    }
  } catch (e) {
    // fallback
  }
}

const geoUrl = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

const GLOBAL_CHOKEPOINTS = [
    { name: "Strait of Hormuz", lon: 56.5, lat: 26.5, risk: "CRITICAL", theater: "Middle East" },
    { name: "Strait of Malacca", lon: 101.4, lat: 2.5, risk: "HIGH", theater: "Indo-Pacific" },
    { name: "Bab-el-Mandeb", lon: 43.3, lat: 12.6, risk: "CRITICAL", theater: "Red Sea" },
    { name: "Suez Canal", lon: 32.3, lat: 30.6, risk: "HIGH", theater: "MENA" },
    { name: "Taiwan Strait", lon: 119.5, lat: 24.0, risk: "CRITICAL", theater: "East Asia" },
    { name: "Panama Canal", lon: -79.6, lat: 9.1, risk: "ELEVATED", theater: "Americas" },
    { name: "Bosphorus Strait", lon: 29.0, lat: 41.1, risk: "ELEVATED", theater: "Black Sea" },
    { name: "Dardanelles", lon: 26.4, lat: 40.2, risk: "ELEVATED", theater: "Mediterranean" },
    { name: "Cape of Good Hope", lon: 18.5, lat: -34.4, risk: "WATCH", theater: "Africa" },
    { name: "South China Sea", lon: 114.0, lat: 14.0, risk: "HIGH", theater: "Indo-Pacific" },
    { name: "Red Sea Corridor", lon: 38.0, lat: 20.0, risk: "CRITICAL", theater: "Red Sea" },
    { name: "Black Sea Theater", lon: 34.0, lat: 43.0, risk: "HIGH", theater: "Eastern Europe" },
];

export default function GlobalMap() {
    // Real-time WebSocket Live Feed connection for maritime
    const wsLiveEvents = useLiveEvents('maritime');

    const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
        "/events/maritime?limit=60",
        fetcher,
        { refreshInterval: 4000 }
    );

    const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
        "/events/cyber?limit=20",
        fetcher,
        { refreshInterval: 4000 }
    );

    // Combine REST data with WebSocket live events
    const rawMaritime: NormalizedEvent[] = [...wsLiveEvents, ...(maritimeEvents || [])];

    // Deduplicate vessels by MMSI / Entity ID
    const vesselMap = new Map<string, any>();

    rawMaritime.forEach((e: any) => {
        const d = e.vessel_data || e.domain_data || e.raw_payload || {};
        const mmsi = d.mmsi || e.primary_entity?.id || e.primary_entity_name || 'UNKNOWN';
        const lat = Number(d.latitude || d.lat || e.latitude);
        const lon = Number(d.longitude || d.lon || e.longitude);

        if (!isNaN(lat) && !isNaN(lon) && lat !== 0 && lon !== 0 && !vesselMap.has(mmsi)) {
            const vtype = String(d.vessel_type || d.type || e.vessel_data?.vessel_type || '').toLowerCase();
            const name = e.primary_entity_name || e.entity_name || d.name || e.primary_entity?.name || `VESSEL_${mmsi}`;
            const isTanker = vtype.includes('tanker') || vtype.includes('oil') || vtype.includes('lng') || vtype.includes('crude');

            vesselMap.set(mmsi, {
                mmsi,
                name,
                lat,
                lon,
                isTanker,
                vessel_type: d.vessel_type || (isTanker ? 'Tanker' : 'Cargo Vessel'),
                anomaly: e.anomaly_score || 0.0,
                speed: d.speed_knots || d.speed || 0.0,
                heading: d.heading || 0,
                region: e.region || 'International Waters',
                nav_status: d.nav_status || 'Underway',
            });
        }
    });

    const vessels = Array.from(vesselMap.values());
    const tankersCount = vessels.filter(v => v.isTanker).length;

    const bgpLinks = (cyberEvents || []).map((e, idx) => {
        const d = e.domain_data || {};
        if (d.from_coords && d.to_coords) {
            return {
                from: d.from_coords as [number, number],
                to: d.to_coords as [number, number],
                id: e.event_id,
                anomaly: e.anomaly_score
            };
        }
        return null;
    }).filter(Boolean) as Array<{ from: [number, number]; to: [number, number]; id: string; anomaly: number }>;

    const [showVessels, setShowVessels] = useState(true);
    const [tankersOnly, setTankersOnly] = useState(false);
    const [showCyber, setShowCyber] = useState(true);
    const [showChokepoints, setShowChokepoints] = useState(true);
    const [showRadar, setShowRadar] = useState(true);

    const [selectedObject, setSelectedObject] = useState<{ type: 'vessel' | 'chokepoint' | 'cyber'; data: any } | null>(null);

    const filteredVessels = tankersOnly ? vessels.filter(v => v.isTanker) : vessels;

    return (
        <div className="w-full h-full bg-[#06080d] relative overflow-hidden font-mono text-white rounded-xl border border-cyan-500/20 shadow-[0_0_25px_rgba(0,0,0,0.6)]">
            {/* Map Top Header Overlay */}
            <div className="absolute top-3 left-3 z-10 bg-slate-950/90 px-3 py-2 rounded-lg border border-[#00f2fe]/30 text-xs backdrop-blur-md flex flex-wrap items-center gap-3 shadow-xl">
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-[#00f2fe] animate-ping" />
                    <span className="text-[#00f2fe] font-bold tracking-wider">GLOBAL SPATIAL TELEMETRY RADAR</span>
                </div>

                {/* Layer Toggles */}
                <div className="flex items-center gap-1.5 font-mono text-[10px]">
                    <button
                        onClick={() => { setShowVessels(true); setTankersOnly(false); }}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showVessels && !tankersOnly ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        ALL VESSELS ({vessels.length})
                    </button>
                    <button
                        onClick={() => { setShowVessels(true); setTankersOnly(!tankersOnly); }}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            tankersOnly ? 'bg-amber-500/30 text-amber-300 border-amber-500/60 glow-amber' : 'bg-slate-900 text-amber-400/70 border-slate-800'
                        }`}
                    >
                        🛢️ TANKERS ({tankersCount})
                    </button>
                    <button
                        onClick={() => setShowCyber(!showCyber)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showCyber ? 'bg-rose-500/20 text-rose-400 border-rose-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        CYBER ({bgpLinks.length})
                    </button>
                    <button
                        onClick={() => setShowChokepoints(!showChokepoints)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showChokepoints ? 'bg-amber-500/20 text-amber-300 border-amber-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        CHOKEPOINTS ({GLOBAL_CHOKEPOINTS.length})
                    </button>
                    <button
                        onClick={() => setShowRadar(!showRadar)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showRadar ? 'bg-[#00f2fe]/20 text-[#00f2fe] border-[#00f2fe]/50 glow-cyan' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        RADAR SWEEP
                    </button>
                </div>
            </div>

            {/* Selected Object Detail Card Overlay */}
            {selectedObject && (
                <div className="absolute top-14 left-3 z-20 bg-slate-950/95 border border-[#00f2fe]/40 p-3.5 rounded-lg max-w-xs text-xs backdrop-blur-md shadow-2xl space-y-2 font-mono">
                    <div className="flex items-center justify-between border-b border-cyan-500/20 pb-1.5">
                        <span className="font-bold text-[#00f2fe] uppercase">
                            {selectedObject.data.isTanker ? '🛢️ TANKER' : selectedObject.type.toUpperCase()}: {selectedObject.data.name || selectedObject.data.id}
                        </span>
                        <button onClick={() => setSelectedObject(null)} className="text-slate-400 hover:text-white font-bold text-[10px] bg-slate-800 px-1.5 py-0.5 rounded cursor-pointer">✕</button>
                    </div>
                    {selectedObject.type === 'vessel' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>TYPE: <span className="text-amber-300 font-bold">{selectedObject.data.vessel_type}</span></div>
                            <div>MMSI: <span className="text-cyan-400 font-mono">{selectedObject.data.mmsi}</span></div>
                            <div>POSITION: <span className="text-white font-bold">{selectedObject.data.lat.toFixed(4)}, {selectedObject.data.lon.toFixed(4)}</span></div>
                            <div>REGION: <span className="text-emerald-400">{selectedObject.data.region}</span></div>
                            <div>SPEED: <span className="text-emerald-400 font-bold">{selectedObject.data.speed} knots</span></div>
                            <div>NAV STATUS: <span className="text-slate-200">{selectedObject.data.nav_status}</span></div>
                            <div>ANOMALY SCORE: <span className="text-rose-400 font-bold">{(selectedObject.data.anomaly || 0).toFixed(2)}</span></div>
                        </div>
                    )}
                    {selectedObject.type === 'chokepoint' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>RISK TIER: <span className="text-amber-400 font-bold">{selectedObject.data.risk}</span></div>
                            <div>THEATER: <span className="text-cyan-400 font-bold">{selectedObject.data.theater}</span></div>
                            <div>COORDINATES: <span className="text-white">{selectedObject.data.lat}, {selectedObject.data.lon}</span></div>
                        </div>
                    )}
                    {selectedObject.type === 'cyber' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>PATH: <span className="text-rose-400 font-bold">{selectedObject.data.from.join(', ')} → {selectedObject.data.to.join(', ')}</span></div>
                            <div>BGP ANOMALY: <span className="text-rose-400 font-bold">{(selectedObject.data.anomaly || 0).toFixed(2)}</span></div>
                        </div>
                    )}
                </div>
            )}

            {/* Animated SVG Radar Reticle Overlay */}
            {showRadar && (
                <div className="absolute inset-0 pointer-events-none z-0 flex items-center justify-center opacity-30">
                    <svg className="w-[500px] h-[500px]" viewBox="0 0 500 500">
                        {/* Concentric Radar Rings */}
                        <circle cx="250" cy="250" r="220" fill="none" stroke="#00f2fe" strokeWidth="1" strokeDasharray="6 6" />
                        <circle cx="250" cy="250" r="160" fill="none" stroke="#00f2fe" strokeWidth="1" opacity="0.6" />
                        <circle cx="250" cy="250" r="100" fill="none" stroke="#00f2fe" strokeWidth="1" opacity="0.4" />
                        <circle cx="250" cy="250" r="40" fill="none" stroke="#00f2fe" strokeWidth="1" opacity="0.2" />

                        {/* Crosshairs Axis */}
                        <line x1="250" y1="30" x2="250" y2="470" stroke="#00f2fe" strokeWidth="1" opacity="0.3" />
                        <line x1="30" y1="250" x2="470" y2="250" stroke="#00f2fe" strokeWidth="1" opacity="0.3" />

                        {/* Rotating Radar Sweep Cone */}
                        <g className="radar-sweep">
                            <path
                                d="M 250 250 L 250 30 A 220 220 0 0 1 430 140 Z"
                                fill="url(#radarGradient)"
                                opacity="0.75"
                            />
                        </g>

                        <defs>
                            <radialGradient id="radarGradient" cx="50%" cy="50%" r="50%">
                                <stop offset="0%" stopColor="#00f2fe" stopOpacity="0.4" />
                                <stop offset="100%" stopColor="#00f2fe" stopOpacity="0" />
                            </radialGradient>
                        </defs>
                    </svg>
                </div>
            )}

            {/* Visual HUD overlays */}
            <div className="absolute bottom-3 right-3 z-10 bg-slate-950/80 px-3 py-2 rounded-lg border border-cyan-500/20 text-[10px] space-y-1.5 backdrop-blur-md shadow-lg font-mono">
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-[#f59e0b] animate-pulse" />
                    <span className="text-amber-300 font-bold">🛢️ Active Tankers ({tankersCount})</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
                    <span>Total Vessels ({vessels.length})</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 bg-amber-500/20 border border-amber-500/60 rounded" />
                    <span>Maritime Chokepoints ({GLOBAL_CHOKEPOINTS.length} Active)</span>
                </div>
            </div>

            <ComposableMap projection="geoMercator" projectionConfig={{ scale: 110 }} className="w-full h-full">
                <Geographies geography={geoUrl}>
                    {({ geographies }: { geographies: any[] }) =>
                        geographies.map((geo: any) => (
                            <Geography
                                key={geo.rsmKey}
                                geography={geo}
                                fill="#0f172a"
                                stroke="#1e293b"
                                strokeWidth={0.5}
                                style={{
                                    default: { outline: "none" },
                                    hover: { fill: "#1e293b", outline: "none" },
                                    pressed: { outline: "none" },
                                }}
                            />
                        ))
                    }
                </Geographies>

                {/* Chokepoint Geofence Markers */}
                {showChokepoints && GLOBAL_CHOKEPOINTS.map((m) => (
                    <Marker key={m.name} coordinates={[m.lon, m.lat]} onClick={() => setSelectedObject({ type: 'chokepoint', data: m })}>
                        <circle r={7} fill="rgba(245, 158, 11, 0.25)" stroke="#f59e0b" strokeWidth={1.5} className="animate-ping cursor-pointer" />
                        <circle r={3.5} fill="#f59e0b" className="cursor-pointer" />
                        <text textAnchor="middle" y={-10} style={{ fontFamily: "monospace", fill: "#fbbf24", fontSize: "7px", fontWeight: "bold" }} className="cursor-pointer">
                            ⚓ {m.name}
                        </text>
                    </Marker>
                ))}

                {/* Vessel Telemetry Markers (Tankers highlighted in Amber/Gold, Cargo in Emerald) */}
                {showVessels && filteredVessels.map((v, i) => (
                    <Marker key={i} coordinates={[v.lon, v.lat]} onClick={() => setSelectedObject({ type: 'vessel', data: v })}>
                        {v.isTanker ? (
                            <>
                                <circle r={7} fill="rgba(245, 158, 11, 0.35)" stroke="#f59e0b" strokeWidth={1.5} className="animate-ping cursor-pointer" />
                                <circle r={4} fill="#f59e0b" stroke="#ffffff" strokeWidth={1} className="cursor-pointer" />
                                <text textAnchor="middle" y={-9} style={{ fontFamily: "monospace", fill: "#fef08a", fontSize: "7.5px", fontWeight: "bold" }} className="cursor-pointer">
                                    🛢️ {v.name}
                                </text>
                            </>
                        ) : (
                            <>
                                <circle r={6} fill="rgba(16, 185, 129, 0.3)" stroke="#10b981" strokeWidth={1} className="animate-ping cursor-pointer" />
                                <circle r={3.5} fill="#10b981" stroke="#ffffff" strokeWidth={1} className="cursor-pointer" />
                                <text textAnchor="middle" y={-8} style={{ fontFamily: "monospace", fill: "#34d399", fontSize: "7px", fontWeight: "bold" }} className="cursor-pointer">
                                    {v.name}
                                </text>
                            </>
                        )}
                    </Marker>
                ))}

                {/* BGP Attack Vector Arcs */}
                {showCyber && bgpLinks.map((link, idx) => (
                    <Line
                        key={idx}
                        from={link.from}
                        to={link.to}
                        stroke="#ef4444"
                        strokeWidth={1.5}
                        strokeDasharray="4 4"
                        onClick={() => setSelectedObject({ type: 'cyber', data: link })}
                        style={{ cursor: 'pointer' }}
                    />
                ))}
            </ComposableMap>
        </div>
    );
}
