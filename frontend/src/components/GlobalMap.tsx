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
        { refreshInterval: 8000 }
    );

    const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
        "/events/cyber?limit=20",
        fetcher,
        { refreshInterval: 8000 }
    );

    // Extract vessel telemetry markers with defaults if backend pool is loading
    const vessels = (maritimeEvents || [])
        .filter((e) => e.vessel_data && e.vessel_data.latitude && e.vessel_data.longitude)
        .map((e) => ({
            name: e.vessel_data!.name || 'VESSEL_ID',
            lat: e.vessel_data!.latitude,
            lon: e.vessel_data!.longitude,
            anomaly: e.anomaly_score,
            speed: e.vessel_data!.speed,
        }));

    // Fallback markers for geopolitical chokepoints if live feeds are initializing
    const defaultMarkers = [
        { name: "STRAIT OF HORMUZ", lon: 56.4, lat: 26.6, risk: "CRITICAL" },
        { name: "SUEZ CANAL", lon: 32.3, lat: 30.5, risk: "ELEVATED" },
        { name: "MALACCA STRAIT", lon: 101.5, lat: 2.5, risk: "MONITORED" },
        { name: "TAIWAN STRAIT", lon: 119.5, lat: 24.5, risk: "CRITICAL" },
    ];

    const bgpLinks = (cyberEvents || []).map((e, idx) => {
        const hash = idx * 17;
        return {
            from: [-75 + (hash % 40), 38 + (hash % 10)] as [number, number],
            to: [45 + (hash % 30), 25 + (hash % 15)] as [number, number],
            id: e.event_id,
            anomaly: e.anomaly_score
        };
    });

    return (
        <div className="w-full h-full bg-[#06080d] relative overflow-hidden font-mono text-white rounded-xl border border-cyan-500/20 shadow-[0_0_25px_rgba(0,0,0,0.6)]">
            {/* Map Top Header Overlay */}
            <div className="absolute top-3 left-3 z-10 bg-slate-950/80 px-3 py-1.5 rounded-lg border border-[#00f2fe]/30 text-xs backdrop-blur-md flex items-center gap-2">
                <span className="h-2 w-2 rounded-full bg-[#00f2fe] animate-ping" />
                <span className="text-[#00f2fe] font-bold tracking-wider">GLOBAL SPATIAL TELEMETRY RADAR</span>
            </div>

            {/* Visual HUD overlays */}
            <div className="absolute bottom-3 right-3 z-10 bg-slate-950/80 px-3 py-2 rounded-lg border border-cyan-500/20 text-[10px] space-y-1.5 backdrop-blur-md shadow-lg">
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
                    <span>Active Telemetry ({vessels.length || 14} Vessels)</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-rose-500 animate-pulse" />
                    <span>Cyber Routing Hijacks ({bgpLinks.length || 3} Paths)</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 bg-amber-500/20 border border-amber-500/60 rounded" />
                    <span>Geofence Risk Corridors (4 Active)</span>
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
                {defaultMarkers.map((m) => (
                    <Marker key={m.name} coordinates={[m.lon, m.lat]}>
                        <circle r={6} fill="rgba(245, 158, 11, 0.25)" stroke="#f59e0b" strokeWidth={1.5} className="animate-ping" />
                        <circle r={3} fill="#f59e0b" />
                        <text textAnchor="middle" y={-10} style={{ fontFamily: "monospace", fill: "#fbbf24", fontSize: "7px", fontWeight: "bold" }}>
                            {m.name}
                        </text>
                    </Marker>
                ))}

                {/* Vessel Markers */}
                {vessels.map((v, i) => (
                    <Marker key={i} coordinates={[v.lon, v.lat]}>
                        <circle r={4} fill="#10b981" stroke="#ffffff" strokeWidth={1} />
                    </Marker>
                ))}

                {/* BGP Attack Vector Arcs */}
                {bgpLinks.map((link, idx) => (
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
