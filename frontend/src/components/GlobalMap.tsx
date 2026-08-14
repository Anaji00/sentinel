'use client';

import React, { useEffect, useMemo, useState } from "react";
import { ComposableMap, Geographies, Geography, Marker, Line } from "react-simple-maps";
import useSWR from "swr";
import { fetcher } from "../lib/api";
import { NormalizedEvent } from "../lib/types";
import { useLiveEvents } from "../lib/useLiveEvents";

// Max vessel & flight markers rendered to prevent SVG DOM overhead
const MAX_RENDERED_VESSELS = 60;
const MAX_RENDERED_FLIGHTS = 40;

const geoUrl = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

// Strategic Maritime Chokepoints
const GLOBAL_CHOKEPOINTS = [
    { name: "Strait of Hormuz", lon: 56.5, lat: 26.5, risk: "CRITICAL" },
    { name: "Strait of Malacca", lon: 101.4, lat: 2.5, risk: "HIGH" },
    { name: "Bab-el-Mandeb", lon: 43.3, lat: 12.6, risk: "CRITICAL" },
    { name: "Suez Canal", lon: 32.3, lat: 30.6, risk: "HIGH" },
    { name: "Taiwan Strait", lon: 119.5, lat: 24.0, risk: "CRITICAL" },
    { name: "Panama Canal", lon: -79.6, lat: 9.1, risk: "ELEVATED" },
    { name: "Bosphorus Strait", lon: 29.0, lat: 41.1, risk: "ELEVATED" },
    { name: "Dardanelles", lon: 26.4, lat: 40.2, risk: "ELEVATED" },
    { name: "Cape of Good Hope", lon: 18.5, lat: -34.4, risk: "WATCH" },
    { name: "South China Sea", lon: 114.0, lat: 14.0, risk: "HIGH" },
];

// Global Financial Exchange Hubs
const FINANCIAL_EXCHANGES = [
    { name: "NYSE / Wall St", symbol: "NYSE", lon: -74.006, lat: 40.7128, region: "US Equities & Derivatives", keyTickers: ["NVDA", "AAPL", "SPY"] },
    { name: "London Stock Exchange", symbol: "LSE", lon: -0.1278, lat: 51.5074, region: "Energy, Commodities & Forex", keyTickers: ["BRENT", "GOLD", "SHEL"] },
    { name: "Tokyo Stock Exchange", symbol: "TSE", lon: 139.6917, lat: 35.6895, region: "Nikkei & Tech Hardware", keyTickers: ["SONY", "8035.T", "6758.T"] },
    { name: "Hong Kong Exchange", symbol: "HKEX", lon: 114.1694, lat: 22.3193, region: "Hang Seng & China Tech", keyTickers: ["9988.HK", "700.HK", "BABA"] },
    { name: "Taiwan Stock Exchange", symbol: "TAIEX", lon: 121.5654, lat: 25.0330, region: "Semiconductor & TSMC Hub", keyTickers: ["TSM", "2330.TW", "2317.TW"] },
    { name: "Singapore Exchange", symbol: "SGX", lon: 103.8198, lat: 1.3521, region: "Maritime Freight Derivatives", keyTickers: ["DBS", "SGX", "WILMAR"] },
    { name: "Frankfurt Exchange", symbol: "FWB", lon: 8.6821, lat: 50.1109, region: "DAX Industrial Supply Chain", keyTickers: ["SAP", "SIE", "BAS"] },
    { name: "Riyadh Tadawul", symbol: "TADAWUL", lon: 46.7133, lat: 24.7136, region: "Crude Oil & Saudi Aramco", keyTickers: ["2222.SR", "1150.SR"] },
];

const REGION_COORDINATES_MAP: Record<string, [number, number]> = {
    // Strategic Regional Centroids matched against backend classify_region() strings
    "strait of hormuz": [26.5, 56.5],
    "persian gulf": [26.0, 52.0],
    "gulf of oman": [24.5, 58.5],
    "iranian territorial": [27.0, 52.5],
    "iran airspace": [32.4, 53.6],
    "strait of malacca": [2.5, 101.4],
    "singapore": [1.3, 103.8],
    "bab-el-mandeb": [12.6, 43.3],
    "red sea": [18.0, 40.0],
    "gulf of aden": [12.5, 48.0],
    "suez canal": [30.0, 32.5],
    "taiwan strait": [24.0, 119.5],
    "taiwan adiz": [24.0, 121.5],
    "south china sea": [14.0, 114.0],
    "east china sea": [29.0, 125.0],
    "black sea": [43.5, 34.2],
    "ukrainian waters": [46.0, 31.0],
    "ukraine airspace": [48.3, 31.1],
    "bosphorus strait": [41.1, 29.0],
    "north korean waters": [39.0, 128.0],
    "north korea adiz": [39.0, 127.5],
    "panama canal": [9.1, -79.6],
    "israeli territorial": [32.0, 34.5],
    "israeli airspace": [31.5, 35.0],
    "syrian territorial": [35.5, 35.5],
    "syrian airspace": [35.0, 38.0],
    "yemeni airspace": [15.5, 47.5],
    "russian airspace": [55.7, 37.6],
    "barents sea": [72.0, 35.0],
    "caspian sea": [41.8, 50.8],
    "somali territorial": [3.0, 47.0],
    "poland": [52.2, 21.0],
};

function resolveRegionFallback(regionName: string, defaultLat = 25.0, defaultLon = 55.0): [number, number] {
    if (!regionName) return [defaultLat, defaultLon];
    const rLower = regionName.toLowerCase().trim();
    for (const [key, coords] of Object.entries(REGION_COORDINATES_MAP)) {
        if (rLower.includes(key) || key.includes(rLower)) {
            return coords;
        }
    }
    return [defaultLat, defaultLon];
}

/**
 * Deterministic Golden-Angle Spiral Anti-Collision Helper
 * Prevents co-located vessels or flights from stacking directly on top of each other.
 */
function applySpatialAntiCollision<T extends { lat: number; lon: number }>(items: T[]): T[] {
    const positionCounts = new Map<string, number>();
    return items.map((item) => {
        // Quantize position key to ~10m grid (~0.0001 deg) to only deconflict exact pixel overlaps
        const gridKey = `${item.lat.toFixed(4)},${item.lon.toFixed(4)}`;
        const count = positionCounts.get(gridKey) || 0;
        positionCounts.set(gridKey, count + 1);

        if (count === 0) {
            return item;
        }

        // Apply a subtle micro golden-angle spiral offset (~200m) to preserve geodetic position accuracy
        const angle = count * 2.39996; // Golden angle in radians
        const radius = 0.002 * Math.sqrt(count); // Micro radial offset step in degrees (~200 meters)
        const offsetLat = Math.sin(angle) * radius;
        const offsetLon = Math.cos(angle) * radius * 1.15;

        return {
            ...item,
            lat: Math.max(-85, Math.min(85, item.lat + offsetLat)),
            lon: Math.max(-180, Math.min(180, item.lon + offsetLon)),
        };
    });
}

const StaticWorldBase = React.memo(function StaticWorldBase() {
    return (
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
    );
});

export default function GlobalMap() {
    // D3 selection polyfill
    useEffect(() => {
        import('d3-selection').then((d3Select) => {
            if (d3Select?.selection?.prototype && typeof d3Select.selection.prototype.interrupt !== 'function') {
                d3Select.selection.prototype.interrupt = function () { return this; };
            }
        }).catch((err) => console.debug('[d3-selection polyfill debug]:', err));
    }, []);

    // Real-time WebSocket Live Feed connection for all multi-domain events
    const wsLiveEvents = useLiveEvents('all');

    // Multi-Domain REST telemetry fetches
    const { data: maritimeEvents } = useSWR<NormalizedEvent[]>("/events/maritime?limit=250", fetcher, { refreshInterval: 6000 });
    const { data: aviationEvents } = useSWR<NormalizedEvent[]>("/events/aviation?limit=80", fetcher, { refreshInterval: 8000 });
    const { data: cyberEvents } = useSWR<NormalizedEvent[]>("/events/cyber?limit=30", fetcher, { refreshInterval: 10000 });
    const { data: tradfiEvents } = useSWR<NormalizedEvent[]>("/events/tradfi?limit=40", fetcher, { refreshInterval: 6000 });

    // Layer Toggles State
    const [showVessels, setShowVessels] = useState(true);
    const [tankersOnly, setTankersOnly] = useState(false);
    const [showFlights, setShowFlights] = useState(true);
    const [showExchanges, setShowExchanges] = useState(true);
    const [showCyber, setShowCyber] = useState(true);
    const [showChokepoints, setShowChokepoints] = useState(true);
    const [showRadar, setShowRadar] = useState(true);

    const [selectedObject, setSelectedObject] = useState<{
        type: 'vessel' | 'flight' | 'exchange' | 'chokepoint' | 'cyber';
        data: any;
    } | null>(null);

    // 1. MARITIME TELEMETRY COMPUTATION (WITH SPATIAL ANTI-COLLISION)
    const rawMaritime = useMemo(() => {
        const liveMaritime = wsLiveEvents.filter(e => {
            const t = (e.type || '').toLowerCase();
            const s = (e.source || '').toLowerCase();
            return t.includes('vessel') || t.includes('maritime') || t.includes('ais') || s.includes('ais');
        });
        const merged = [...liveMaritime, ...(maritimeEvents || [])];
        return Array.from(new Map(merged.map(e => [e.event_id, e])).values());
    }, [wsLiveEvents, maritimeEvents]);

    const { vessels, tankersCount } = useMemo(() => {
        const vesselMap = new Map<string, any>();

        rawMaritime.forEach((e: any) => {
            const d = e.vessel_data || e.domain_data || e.raw_payload || {};
            const meta = d.MetaData || d.meta || {};
            const pos = d.Message?.PositionReport || d.PositionReport || {};
            
            const mmsi = String(d.mmsi || meta.MMSI || e.primary_entity?.id || e.primary_entity_id || e.primary_entity_name || e.event_id || 'UNKNOWN');
            
            const parseValidCoord = (candidates: any[], maxBound: number): number | null => {
                for (const c of candidates) {
                    if (c !== null && c !== undefined && c !== '') {
                        const n = parseFloat(String(c));
                        if (!isNaN(n) && n !== 0 && Math.abs(n) <= maxBound) {
                            return n;
                        }
                    }
                }
                return null;
            };

            let lat = parseValidCoord([e.latitude, d.latitude, d.lat, pos.Latitude, meta.latitude, d.position?.latitude, e.lat], 90);
            let lon = parseValidCoord([e.longitude, d.longitude, d.lon, pos.Longitude, meta.longitude, d.position?.longitude, e.lon], 180);

            // Fallback realistic region coordinates ONLY if lat/lon are missing, using backend emitted region
            if (lat === null || lon === null) {
                const reg = String(e.region || d.region || meta.region || e.vessel_data?.region || '');
                const [fLat, fLon] = resolveRegionFallback(reg, 25.0, 55.0);
                lat = fLat;
                lon = fLon;
            }

            const vtype = String(d.vessel_type || d.type || meta.ShipType || e.vessel_data?.vessel_type || '').toLowerCase();
            const name = e.primary_entity_name || e.entity_name || meta.ShipName || d.name || e.primary_entity?.name || `VESSEL_${mmsi}`;
            const nameUpper = name.toUpperCase();
            
            const isTanker = vtype.includes('tanker') || vtype.includes('oil') || vtype.includes('lng') || vtype.includes('crude') || vtype.includes('petro') || vtype.includes('lpg') ||
                nameUpper.includes('TANKER') || nameUpper.includes('OIL') || nameUpper.includes('CRUDE') || nameUpper.includes('PETRO') || nameUpper.includes('LNG') || nameUpper.includes('LPG') || nameUpper.includes('CHEM') ||
                nameUpper.includes('VLCC') || nameUpper.includes('ULCC') || nameUpper.includes('AFRAMAX') || nameUpper.includes('SUEZMAX');

            vesselMap.set(mmsi, {
                mmsi,
                name,
                lat,
                lon,
                isTanker,
                vessel_type: d.vessel_type || (isTanker ? 'Oil / Gas Tanker' : 'Cargo Vessel'),
                anomaly: e.anomaly_score || 0.0,
                speed: d.speed_knots || d.speed || (pos.Sog ? parseFloat(String(pos.Sog)) : 12.4),
                heading: d.heading || pos.TrueHeading || 0,
                region: e.region || 'International Shipping Lane',
                nav_status: d.nav_status || 'Underway Using Engine',
            });
        });

        const rawList = Array.from(vesselMap.values());
        // Apply spatial anti-collision spiral to prevent marker stacking
        const deconflictList = applySpatialAntiCollision(rawList);

        return { vessels: deconflictList, tankersCount: deconflictList.filter(v => v.isTanker).length };
    }, [rawMaritime]);

    const filteredVessels = useMemo(() => {
        const base = tankersOnly ? vessels.filter(v => v.isTanker) : vessels;
        return base
            .sort((a, b) => (b.anomaly || 0) - (a.anomaly || 0))
            .slice(0, MAX_RENDERED_VESSELS);
    }, [vessels, tankersOnly]);

    // 2. AVIATION TELEMETRY COMPUTATION (WITH SPATIAL ANTI-COLLISION)
    const rawAviation = useMemo(() => {
        const liveAviation = wsLiveEvents.filter(e => {
            const t = (e.type || '').toLowerCase();
            const s = (e.source || '').toLowerCase();
            return t.includes('flight') || t.includes('aviation') || t.includes('adsb') || s.includes('opensky');
        });
        const merged = [...liveAviation, ...(aviationEvents || [])];
        return Array.from(new Map(merged.map(e => [e.event_id, e])).values());
    }, [wsLiveEvents, aviationEvents]);

    const flights = useMemo(() => {
        const flightMap = new Map<string, any>();

        rawAviation.forEach((e: any) => {
            const d = e.flight_data || e.domain_data || e.raw_payload || {};
            const icao24 = String(d.icao24 || e.primary_entity?.id || e.primary_entity_id || e.event_id || 'UNKNOWN').toUpperCase();
            const parseValidCoord = (candidates: any[], maxBound: number): number | null => {
                for (const c of candidates) {
                    if (c !== null && c !== undefined && c !== '') {
                        const n = parseFloat(String(c));
                        if (!isNaN(n) && n !== 0 && Math.abs(n) <= maxBound) {
                            return n;
                        }
                    }
                }
                return null;
            };

            let lat = parseValidCoord([e.latitude, d.latitude, d.lat], 90);
            let lon = parseValidCoord([e.longitude, d.longitude, d.lon], 180);

            // Fallback realistic region coordinates ONLY if lat/lon are missing, using backend emitted region
            if (lat === null || lon === null) {
                const reg = String(e.region || d.region || e.flight_data?.region || '');
                const [fLat, fLon] = resolveRegionFallback(reg, 38.8, -77.0);
                lat = fLat;
                lon = fLon;
            }

            const callsign = d.callsign || e.primary_entity_name || e.entity_name || `FLT_${icao24}`;
            const squawk = String(d.squawk || '');
            const isEmergency = ['7500', '7600', '7700'].includes(squawk) || e.anomaly_score >= 0.7;

            flightMap.set(icao24, {
                icao24,
                callsign,
                lat,
                lon,
                altitude_ft: d.baro_altitude_m ? Math.round(d.baro_altitude_m * 3.28084) : e.altitude_ft || 32000,
                speed_kts: d.velocity_ms ? Math.round(d.velocity_ms * 1.94384) : 450,
                squawk: squawk || '1200',
                isEmergency,
                origin_country: d.origin_country || e.country_code || 'US',
                anomaly: e.anomaly_score || 0.1,
                headline: e.headline,
            });
        });

        const rawList = Array.from(flightMap.values());
        // Apply spatial anti-collision spiral to prevent flight marker stacking
        const deconflictList = applySpatialAntiCollision(rawList);

        return deconflictList
            .sort((a, b) => (b.anomaly || 0) - (a.anomaly || 0))
            .slice(0, MAX_RENDERED_FLIGHTS);
    }, [rawAviation]);

    // 3. FINANCIAL ANOMALIES MAPPED TO EXCHANGES
    const exchangeAnomalies = useMemo(() => {
        const map = new Map<string, any[]>();
        (tradfiEvents || []).forEach((e: any) => {
            const d = e.financial_data || e.domain_data || {};
            const ticker = d.ticker || e.primary_entity_name || 'MARKET';
            const anomaly = e.anomaly_score || 0.5;

            // Map ticker to an exchange hub
            let matchedExchange = FINANCIAL_EXCHANGES.find(ex => ex.keyTickers.includes(ticker)) || FINANCIAL_EXCHANGES[0];
            const existing = map.get(matchedExchange.symbol) || [];
            existing.push({ ticker, anomaly, headline: e.headline });
            map.set(matchedExchange.symbol, existing);
        });
        return map;
    }, [tradfiEvents]);

    // 4. BGP CYBER LINKS
    const bgpLinks = useMemo(() => (cyberEvents || []).map((e) => {
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
    }).filter(Boolean) as Array<{ from: [number, number]; to: [number, number]; id: string; anomaly: number }>, [cyberEvents]);

    return (
        <div className="w-full h-full bg-[#06080d] relative overflow-hidden font-mono text-white rounded-xl border border-cyan-500/20 shadow-[0_0_25px_rgba(0,0,0,0.6)]">
            {/* Top Bar Navigation & Multi-Domain Layer Toggles */}
            <div className="absolute top-3 left-3 right-3 z-10 bg-slate-950/90 px-3.5 py-2 rounded-lg border border-[#00f2fe]/30 text-xs backdrop-blur-md flex flex-wrap items-center justify-between gap-2 shadow-2xl">
                <div className="flex items-center gap-2">
                    <span className="h-2 w-2 rounded-full bg-[#00f2fe] animate-ping" />
                    <span className="text-[#00f2fe] font-bold tracking-wider uppercase">
                        SENTINEL MULTI-DOMAIN SPATIAL RADAR
                    </span>
                </div>

                {/* Layer Control Buttons */}
                <div className="flex items-center gap-1 font-mono text-[10px] overflow-x-auto">
                    <button
                        onClick={() => { setShowVessels(true); setTankersOnly(false); }}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showVessels && !tankersOnly ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        VESSELS ({vessels.length})
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
                        onClick={() => setShowFlights(!showFlights)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showFlights ? 'bg-cyan-500/20 text-cyan-300 border-cyan-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        ✈️ FLIGHTS ({flights.length})
                    </button>
                    <button
                        onClick={() => setShowExchanges(!showExchanges)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showExchanges ? 'bg-purple-500/20 text-purple-300 border-purple-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        📈 EXCHANGES ({FINANCIAL_EXCHANGES.length})
                    </button>
                    <button
                        onClick={() => setShowCyber(!showCyber)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showCyber ? 'bg-rose-500/20 text-rose-400 border-rose-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        ⚡ CYBER ({bgpLinks.length})
                    </button>
                    <button
                        onClick={() => setShowChokepoints(!showChokepoints)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showChokepoints ? 'bg-amber-500/20 text-amber-300 border-amber-500/50' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        ⚓ CHOKEPOINTS ({GLOBAL_CHOKEPOINTS.length})
                    </button>
                    <button
                        onClick={() => setShowRadar(!showRadar)}
                        className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
                            showRadar ? 'bg-[#00f2fe]/20 text-[#00f2fe] border-[#00f2fe]/50 glow-cyan' : 'bg-slate-900 text-slate-500 border-slate-800'
                        }`}
                    >
                        SWEEP
                    </button>
                </div>
            </div>

            {/* Object Inspector Drawer Modal */}
            {selectedObject && (
                <div className="absolute top-14 left-3 z-20 bg-slate-950/95 border border-[#00f2fe]/50 p-4 rounded-xl max-w-sm text-xs backdrop-blur-md shadow-2xl space-y-2.5 font-mono">
                    <div className="flex items-center justify-between border-b border-cyan-500/30 pb-2">
                        <span className="font-bold text-[#00f2fe] uppercase tracking-wide">
                            {selectedObject.type === 'vessel' && (selectedObject.data.isTanker ? '🛢️ TANKER INSPECTOR' : '🚢 VESSEL INSPECTOR')}
                            {selectedObject.type === 'flight' && '✈️ AIRSPACE FLIGHT INSPECTOR'}
                            {selectedObject.type === 'exchange' && '📈 FINANCIAL EXCHANGE HUB'}
                            {selectedObject.type === 'chokepoint' && '⚓ MARITIME CHOKEPOINT'}
                            {selectedObject.type === 'cyber' && '⚡ CYBER BGP ATTACK VECTOR'}
                        </span>
                        <button onClick={() => setSelectedObject(null)} className="text-slate-400 hover:text-white font-bold text-xs bg-slate-800 px-2 py-0.5 rounded cursor-pointer">
                            ✕
                        </button>
                    </div>

                    {selectedObject.type === 'vessel' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>NAME: <span className="text-white font-bold">{selectedObject.data.name}</span></div>
                            <div>TYPE: <span className="text-amber-300 font-bold">{selectedObject.data.vessel_type}</span></div>
                            <div>MMSI: <span className="text-cyan-400 font-mono">{selectedObject.data.mmsi}</span></div>
                            <div>POSITION: <span className="text-white font-bold">{selectedObject.data.lat.toFixed(4)}, {selectedObject.data.lon.toFixed(4)}</span></div>
                            <div>REGION: <span className="text-emerald-400">{selectedObject.data.region}</span></div>
                            <div>SPEED: <span className="text-emerald-400 font-bold">{selectedObject.data.speed} knots</span></div>
                            <div>ANOMALY SCORE: <span className="text-rose-400 font-bold">{(selectedObject.data.anomaly || 0).toFixed(2)}</span></div>
                        </div>
                    )}

                    {selectedObject.type === 'flight' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>CALLSIGN: <span className="text-cyan-300 font-bold">{selectedObject.data.callsign}</span></div>
                            <div>ICAO24: <span className="text-slate-400 font-mono">{selectedObject.data.icao24}</span></div>
                            <div>ALTITUDE: <span className="text-emerald-400 font-bold">{selectedObject.data.altitude_ft.toLocaleString()} ft</span></div>
                            <div>SPEED: <span className="text-emerald-400 font-bold">{selectedObject.data.speed_kts} knots</span></div>
                            <div>SQUAWK CODE: <span className={selectedObject.data.isEmergency ? "text-rose-400 font-bold animate-pulse" : "text-slate-300"}>{selectedObject.data.squawk}</span></div>
                            <div>ORIGIN COUNTRY: <span className="text-amber-300">{selectedObject.data.origin_country}</span></div>
                            <div>ANOMALY SCORE: <span className="text-rose-400 font-bold">{(selectedObject.data.anomaly || 0).toFixed(2)}</span></div>
                        </div>
                    )}

                    {selectedObject.type === 'exchange' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>EXCHANGE: <span className="text-purple-300 font-bold">{selectedObject.data.name} ({selectedObject.data.symbol})</span></div>
                            <div>FOCUS: <span className="text-slate-200">{selectedObject.data.region}</span></div>
                            <div>COORDINATES: <span className="text-slate-400">{selectedObject.data.lat}, {selectedObject.data.lon}</span></div>
                            <div className="pt-1">
                                <span className="text-slate-400 uppercase text-[9px] font-bold">Key Tracked Instruments:</span>
                                <div className="flex flex-wrap gap-1 pt-1">
                                    {selectedObject.data.keyTickers.map((t: string) => (
                                        <span key={t} className="px-1.5 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/40 text-[9px] font-bold">
                                            {t}
                                        </span>
                                    ))}
                                </div>
                            </div>
                        </div>
                    )}

                    {selectedObject.type === 'chokepoint' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>LOCATION: <span className="text-white font-bold">{selectedObject.data.name}</span></div>
                            <div>RISK TIER: <span className="text-amber-400 font-bold">{selectedObject.data.risk}</span></div>
                            <div>COORDINATES: <span className="text-slate-400">{selectedObject.data.lat}, {selectedObject.data.lon}</span></div>
                        </div>
                    )}

                    {selectedObject.type === 'cyber' && (
                        <div className="space-y-1 text-[11px] text-slate-300">
                            <div>ATTACK PATH: <span className="text-rose-400 font-bold">{selectedObject.data.from.join(', ')} → {selectedObject.data.to.join(', ')}</span></div>
                            <div>BGP ANOMALY SCORE: <span className="text-rose-400 font-bold">{(selectedObject.data.anomaly || 0).toFixed(2)}</span></div>
                        </div>
                    )}
                </div>
            )}

            {/* SVG Radar Sweep Overlay */}
            {showRadar && (
                <div className="absolute inset-0 pointer-events-none z-0 flex items-center justify-center opacity-25">
                    <svg className="w-[500px] h-[500px]" viewBox="0 0 500 500">
                        <circle cx="250" cy="250" r="220" fill="none" stroke="#00f2fe" strokeWidth="1" strokeDasharray="6 6" />
                        <circle cx="250" cy="250" r="160" fill="none" stroke="#00f2fe" strokeWidth="1" opacity="0.6" />
                        <circle cx="250" cy="250" r="100" fill="none" stroke="#00f2fe" strokeWidth="1" opacity="0.4" />
                        <circle cx="250" cy="250" r="40" fill="none" stroke="#00f2fe" strokeWidth="1" opacity="0.2" />
                        <line x1="250" y1="30" x2="250" y2="470" stroke="#00f2fe" strokeWidth="1" opacity="0.3" />
                        <line x1="30" y1="250" x2="470" y2="250" stroke="#00f2fe" strokeWidth="1" opacity="0.3" />
                        <g className="radar-sweep">
                            <path d="M 250 250 L 250 30 A 220 220 0 0 1 430 140 Z" fill="url(#globalMapRadarGradient)" opacity="0.75" />
                        </g>
                        <defs>
                            <radialGradient id="globalMapRadarGradient" cx="50%" cy="50%" r="50%">
                                <stop offset="0%" stopColor="#00f2fe" stopOpacity="0.4" />
                                <stop offset="100%" stopColor="#00f2fe" stopOpacity="0" />
                            </radialGradient>
                        </defs>
                    </svg>
                </div>
            )}

            {/* Bottom-Right Multi-Domain Spatial HUD */}
            <div className="absolute bottom-3 right-3 z-10 bg-slate-950/85 px-3 py-2 rounded-lg border border-cyan-500/30 text-[10px] space-y-1 backdrop-blur-md shadow-lg font-mono">
                <div className="flex items-center gap-2">
                    <span className="h-1.5 w-1.5 rounded-full bg-[#f59e0b] animate-pulse" />
                    <span className="text-amber-300 font-bold">🛢️ Tankers ({tankersCount})</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
                    <span>Total Vessels ({vessels.length})</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-1.5 w-1.5 rounded-full bg-cyan-400 animate-pulse" />
                    <span className="text-cyan-300 font-bold">✈️ Flights ({flights.length})</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="h-1.5 w-1.5 rounded-full bg-purple-400 animate-pulse" />
                    <span className="text-purple-300 font-bold">📈 Financial Exchanges ({FINANCIAL_EXCHANGES.length})</span>
                </div>
            </div>

            {/* Interactive World Map Canvas */}
            <ComposableMap projection="geoMercator" projectionConfig={{ scale: 110 }} className="w-full h-full">
                <StaticWorldBase />

                {/* 1. Maritime Chokepoints Layer */}
                {showChokepoints && GLOBAL_CHOKEPOINTS.map((m) => (
                    <Marker key={m.name} coordinates={[m.lon, m.lat]} onClick={() => setSelectedObject({ type: 'chokepoint', data: m })}>
                        <circle r={7} fill="rgba(245, 158, 11, 0.25)" stroke="#f59e0b" strokeWidth={1.5} className="animate-ping cursor-pointer" />
                        <circle r={3.5} fill="#f59e0b" className="cursor-pointer" />
                        <text textAnchor="middle" y={-10} style={{ fontFamily: "monospace", fill: "#fbbf24", fontSize: "7px", fontWeight: "bold" }} className="cursor-pointer">
                            ⚓ {m.name}
                        </text>
                    </Marker>
                ))}

                {/* 2. Global Financial Exchange Hubs Layer */}
                {showExchanges && FINANCIAL_EXCHANGES.map((ex) => {
                    const anomalies = exchangeAnomalies.get(ex.symbol) || [];
                    const topAnomaly = anomalies.length > 0 ? anomalies[0] : null;

                    return (
                        <Marker key={ex.symbol} coordinates={[ex.lon, ex.lat]} onClick={() => setSelectedObject({ type: 'exchange', data: ex })}>
                            <rect x={-5} y={-5} width={10} height={10} fill="rgba(168, 85, 247, 0.4)" stroke="#c084fc" strokeWidth={1.5} className="cursor-pointer" />
                            <text textAnchor="middle" y={-9} style={{ fontFamily: "monospace", fill: "#e9d5ff", fontSize: "7.5px", fontWeight: "bold" }} className="cursor-pointer">
                                📈 {ex.symbol}
                            </text>
                            {topAnomaly && (
                                <text textAnchor="middle" y={15} style={{ fontFamily: "monospace", fill: "#f43f5e", fontSize: "7px", fontWeight: "bold" }} className="cursor-pointer">
                                    🔥 {topAnomaly.ticker}
                                </text>
                            )}
                        </Marker>
                    );
                })}

                {/* 3. Aviation ADS-B Flights Layer */}
                {showFlights && flights.map((flt) => (
                    <Marker key={flt.icao24} coordinates={[flt.lon, flt.lat]} onClick={() => setSelectedObject({ type: 'flight', data: flt })}>
                        {flt.isEmergency ? (
                            <>
                                <circle r={8} fill="rgba(239, 68, 68, 0.35)" stroke="#ef4444" strokeWidth={1.5} className="animate-ping cursor-pointer" />
                                <circle r={4} fill="#ef4444" stroke="#ffffff" strokeWidth={1} className="cursor-pointer" />
                                <text textAnchor="middle" y={-10} style={{ fontFamily: "monospace", fill: "#fca5a5", fontSize: "7.5px", fontWeight: "bold" }} className="cursor-pointer">
                                    ⚠️ ✈️ {flt.callsign}
                                </text>
                            </>
                        ) : (
                            <>
                                <circle r={4.5} fill="rgba(6, 182, 212, 0.4)" stroke="#06b6d4" strokeWidth={1} className="cursor-pointer" />
                                <text textAnchor="middle" y={-8} style={{ fontFamily: "monospace", fill: "#67e8f9", fontSize: "7px", fontWeight: "bold" }} className="cursor-pointer">
                                    ✈️ {flt.callsign}
                                </text>
                            </>
                        )}
                    </Marker>
                ))}

                {/* 4. Maritime AIS Vessels Layer */}
                {showVessels && filteredVessels.map((v) => (
                    <Marker key={v.mmsi} coordinates={[v.lon, v.lat]} onClick={() => setSelectedObject({ type: 'vessel', data: v })}>
                        {v.isTanker ? (
                            <>
                                <circle r={6} fill="rgba(245, 158, 11, 0.35)" stroke="#f59e0b" strokeWidth={1.5} opacity={0.7} className="cursor-pointer" />
                                <circle r={3.5} fill="#f59e0b" stroke="#ffffff" strokeWidth={1} className="cursor-pointer" />
                                <text textAnchor="middle" y={-9} style={{ fontFamily: "monospace", fill: "#fef08a", fontSize: "7.5px", fontWeight: "bold" }} className="cursor-pointer">
                                    🛢️ {v.name}
                                </text>
                            </>
                        ) : (
                            <>
                                <circle r={5} fill="rgba(16, 185, 129, 0.3)" stroke="#10b981" strokeWidth={1} opacity={0.6} className="cursor-pointer" />
                                <circle r={3} fill="#10b981" stroke="#ffffff" strokeWidth={1} className="cursor-pointer" />
                                <text textAnchor="middle" y={-8} style={{ fontFamily: "monospace", fill: "#34d399", fontSize: "7px", fontWeight: "bold" }} className="cursor-pointer">
                                    {v.name}
                                </text>
                            </>
                        )}
                    </Marker>
                ))}

                {/* 5. BGP Cyber Attack Vectors Layer */}
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
