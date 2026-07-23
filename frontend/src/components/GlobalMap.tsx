'use client';

import React, { useEffect, useState } from "react";
import { ComposableMap, Geographies, Geography, Marker, Line } from "react-simple-maps";
import useSWR from "swr";
import { fetcher } from "../lib/api";
import { NormalizedEvent } from "../lib/types";

const geoUrl = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

export default function GlobalMap() {
    const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
        "/events/maritime?limit=30",
        fetcher,
        { refreshInterval: 4000 }
    );

    const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
        "/events/cyber?limit=20",
        fetcher,
        { refreshInterval: 4000 }
    );

    // Extract vessel telemetry markers strictly from API domain_data
    const vessels = (maritimeEvents || [])
        .map((e: any) => {
            const d = e.domain_data || e.vessel_data || e.raw_payload || {};
            const lat = Number(d.latitude || d.lat);
            const lon = Number(d.longitude || d.lon);
            if (!isNaN(lat) && !isNaN(lon) && lat !== 0 && lon !== 0) {
                return {
                    name: e.primary_entity_name || e.entity_name || d.name || d.mmsi || 'VESSEL_AIS',
                    lat,
                    lon,
                    anomaly: e.anomaly_score || 0.0,
                    speed: d.speed || 0.0,
                };
            }
            return null;
        })
        .filter(Boolean) as Array<{ name: string; lat: number; lon: number; anomaly: number; speed: number }>;

    const defaultMarkers: Array<{ name: string; lon: number; lat: number; risk: string }> = [];

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
    const [showCyber, setShowCyber] = useState(true);
    const [showChokepoints, setShowChokepoints] = useState(true);
    const [showRadar, setShowRadar] = useState(true);

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
                        onClick={() => setShowVessels(!showVessels)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showVessels ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        VESSELS ({vessels.length})
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
                        CHOKEPOINTS ({defaultMarkers.length})
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
            <div className="absolute bottom-3 right-3 z-10 bg-slate-950/80 px-3 py-2 rounded-lg border border-cyan-500/20 text-[10px] space-y-1.5 backdrop-blur-md shadow-lg">
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
                    <span>Active Telemetry ({vessels.length} Vessels)</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-rose-500 animate-pulse" />
                    <span>Cyber Routing Hijacks ({bgpLinks.length} Paths)</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 bg-amber-500/20 border border-amber-500/60 rounded" />
                    <span>Geofence Risk Corridors ({defaultMarkers.length} Active)</span>
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
                {showChokepoints && defaultMarkers.map((m) => (
                    <Marker key={m.name} coordinates={[m.lon, m.lat]}>
                        <circle r={6} fill="rgba(245, 158, 11, 0.25)" stroke="#f59e0b" strokeWidth={1.5} className="animate-ping" />
                        <circle r={3} fill="#f59e0b" />
                        <text textAnchor="middle" y={-10} style={{ fontFamily: "monospace", fill: "#fbbf24", fontSize: "7px", fontWeight: "bold" }}>
                            {m.name}
                        </text>
                    </Marker>
                ))}

                {/* Vessel Telemetry Markers */}
                {showVessels && vessels.map((v, i) => (
                    <Marker key={i} coordinates={[v.lon, v.lat]}>
                        <circle r={6} fill="rgba(16, 185, 129, 0.3)" stroke="#10b981" strokeWidth={1} className="animate-ping" />
                        <circle r={3.5} fill="#10b981" stroke="#ffffff" strokeWidth={1} />
                        <text textAnchor="middle" y={-8} style={{ fontFamily: "monospace", fill: "#34d399", fontSize: "7px", fontWeight: "bold" }}>
                            {v.name}
                        </text>
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
                    />
                ))}
            </ComposableMap>
        </div>
    );
}
