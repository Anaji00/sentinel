'use client';

import { ABSENT, formatNumber } from '../lib/format';
import React, { useEffect, useMemo, useState } from 'react';
import { ComposableMap, Geographies, Geography, Marker } from 'react-simple-maps';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { NormalizedEvent } from '../lib/types';
import { useLiveEvents } from '../lib/useLiveEvents';
import { PALETTE } from '../lib/palette';
import { IconClose } from './ui/icons';
import { POLL } from './ui/DataProvider';

// Max vessel & flight markers rendered to prevent SVG DOM overhead
const MAX_RENDERED_VESSELS = 60;
const MAX_RENDERED_FLIGHTS = 40;

/**
 * The basemap, served from this origin.
 *
 * This was a jsdelivr URL fetched at runtime, and it had stopped working: the
 * CSP tightened earlier in this audit says `connect-src 'self'`, so the
 * browser blocked it and react-simple-maps rendered no landmasses at all --
 * silently, because a `<Geographies>` with no data draws nothing rather than
 * raising. The map has been a field of markers on an empty background.
 *
 * Fetching it from a CDN was also the wrong shape for this product regardless
 * of the CSP: `DataSovereigntyModal` tells the operator that ingestion is
 * read-only and anonymous and that nothing about them leaves the deployment,
 * while every page view announced itself to a third party.
 *
 * world-atlas v2 (Natural Earth, public domain), vendored at
 * `public/countries-110m.json`: 177 country geometries, 105 KB.
 */
const geoUrl = '/countries-110m.json';

// Strategic Maritime Chokepoints
const GLOBAL_CHOKEPOINTS = [
  { name: 'Strait of Hormuz', lon: 56.5, lat: 26.5, risk: 'CRITICAL' },
  { name: 'Strait of Malacca', lon: 101.4, lat: 2.5, risk: 'HIGH' },
  { name: 'Bab-el-Mandeb', lon: 43.3, lat: 12.6, risk: 'CRITICAL' },
  { name: 'Suez Canal', lon: 32.3, lat: 30.6, risk: 'HIGH' },
  { name: 'Taiwan Strait', lon: 119.5, lat: 24.0, risk: 'CRITICAL' },
  { name: 'Panama Canal', lon: -79.6, lat: 9.1, risk: 'ELEVATED' },
  { name: 'Bosphorus Strait', lon: 29.0, lat: 41.1, risk: 'ELEVATED' },
  { name: 'Dardanelles', lon: 26.4, lat: 40.2, risk: 'ELEVATED' },
  { name: 'Cape of Good Hope', lon: 18.5, lat: -34.4, risk: 'WATCH' },
  { name: 'South China Sea', lon: 114.0, lat: 14.0, risk: 'HIGH' },
];

// Global Financial Exchange Hubs
const FINANCIAL_EXCHANGES = [
  {
    name: 'NYSE / Wall St',
    symbol: 'NYSE',
    lon: -74.006,
    lat: 40.7128,
    region: 'US Equities & Derivatives',
    keyTickers: ['NVDA', 'AAPL', 'SPY'],
  },
  {
    name: 'London Stock Exchange',
    symbol: 'LSE',
    lon: -0.1278,
    lat: 51.5074,
    region: 'Energy, Commodities & Forex',
    keyTickers: ['BRENT', 'GOLD', 'SHEL'],
  },
  {
    name: 'Tokyo Stock Exchange',
    symbol: 'TSE',
    lon: 139.6917,
    lat: 35.6895,
    region: 'Nikkei & Tech Hardware',
    keyTickers: ['SONY', '8035.T', '6758.T'],
  },
  {
    name: 'Hong Kong Exchange',
    symbol: 'HKEX',
    lon: 114.1694,
    lat: 22.3193,
    region: 'Hang Seng & China Tech',
    keyTickers: ['9988.HK', '700.HK', 'BABA'],
  },
  {
    name: 'Taiwan Stock Exchange',
    symbol: 'TAIEX',
    lon: 121.5654,
    lat: 25.033,
    region: 'Semiconductor & TSMC Hub',
    keyTickers: ['TSM', '2330.TW', '2317.TW'],
  },
  {
    name: 'Singapore Exchange',
    symbol: 'SGX',
    lon: 103.8198,
    lat: 1.3521,
    region: 'Maritime Freight Derivatives',
    keyTickers: ['DBS', 'SGX', 'WILMAR'],
  },
  {
    name: 'Frankfurt Exchange',
    symbol: 'FWB',
    lon: 8.6821,
    lat: 50.1109,
    region: 'DAX Industrial Supply Chain',
    keyTickers: ['SAP', 'SIE', 'BAS'],
  },
  {
    name: 'Riyadh Tadawul',
    symbol: 'TADAWUL',
    lon: 46.7133,
    lat: 24.7136,
    region: 'Crude Oil & Saudi Aramco',
    keyTickers: ['2222.SR', '1150.SR'],
  },
];

const REGION_COORDINATES_MAP: Record<string, [number, number]> = {
  // Strategic Regional Centroids matched against backend classify_region() strings"strait of hormuz": [26.5, 56.5],"persian gulf": [26.0, 52.0],"gulf of oman": [24.5, 58.5],"iranian territorial": [27.0, 52.5],"iran airspace": [32.4, 53.6],"strait of malacca": [2.5, 101.4],"singapore": [1.3, 103.8],"bab-el-mandeb": [12.6, 43.3],"red sea": [18.0, 40.0],"gulf of aden": [12.5, 48.0],"suez canal": [30.0, 32.5],"taiwan strait": [24.0, 119.5],"taiwan adiz": [24.0, 121.5],"south china sea": [14.0, 114.0],"east china sea": [29.0, 125.0],"black sea": [43.5, 34.2],"ukrainian waters": [46.0, 31.0],"ukraine airspace": [48.3, 31.1],"bosphorus strait": [41.1, 29.0],"north korean waters": [39.0, 128.0],"north korea adiz": [39.0, 127.5],"panama canal": [9.1, -79.6],"israeli territorial": [32.0, 34.5],"israeli airspace": [31.5, 35.0],"syrian territorial": [35.5, 35.5],"syrian airspace": [35.0, 38.0],"yemeni airspace": [15.5, 47.5],"russian airspace": [55.7, 37.6],"barents sea": [72.0, 35.0],"caspian sea": [41.8, 50.8],"somali territorial": [3.0, 47.0],"poland": [52.2, 21.0],
};

/**
 * The centroid of a named region, or null when the name is not one we can place.
 *
 * This took `defaultLat = 25.0, defaultLon = 55.0` and returned it whenever the
 * region was empty or unrecognised. 25N 55E is the Persian Gulf, a few miles
 * off Dubai and just inside the approaches to the Strait of Hormuz -- so every
 * vessel that reported no position was drawn in one of the most closely watched
 * stretches of water on the map. Measured on the live feed: 34 of the 250
 * vessels the map holds at any moment, 13.6%, were stacked on that one point,
 * none of them there. The aviation caller did the same thing with 38.8 -77.0,
 * which is Washington DC.
 *
 * Returning null makes the absence visible to the caller, which drops the
 * marker rather than placing it somewhere plausible. A vessel we cannot locate
 * is not a vessel in the Gulf.
 */
function resolveRegionFallback(regionName: string): [number, number] | null {
  if (!regionName) return null;
  const rLower = regionName.toLowerCase().trim();
  for (const [key, coords] of Object.entries(REGION_COORDINATES_MAP)) {
    if (rLower.includes(key) || key.includes(rLower)) {
      return coords;
    }
  }
  return null;
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

interface ChokepointReading {
  occurred_at: string;
  headline: string | null;
  source: string | null;
  latitude: number | null;
  longitude: number | null;
  age_seconds: number | null;
}

interface ChokepointEntry {
  region: string;
  ais: ChokepointReading | null;
  sar: ChokepointReading | null;
  ais_silent: boolean;
  observed: boolean;
  radar_grid: RadarGrid | null;
}

interface RadarCell {
  latitude: number;
  longitude: number;
  target_pixels: number;
  water_pixels: number;
  target_density: number;
}

interface RadarGrid {
  observed_on: string | null;
  cells_imaged: number;
  cells: RadarCell[];
}

interface ChokepointStatus {
  window_hours: number;
  chokepoints: ChokepointEntry[];
  ais_silent: string[];
  unobserved_by_any_instrument: string[];
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
              default: { outline: 'none' },
              hover: { fill: '#1e293b', outline: 'none' },
              pressed: { outline: 'none' },
            }}
          />
        ))
      }
    </Geographies>
  );
});

export default function GlobalMap() {
  // The `d3-selection` polyfill that stood here patched nothing.
  //
  // It added `selection.prototype.interrupt` to whatever `import('d3-selection')`
  // resolved to, which was the project's direct dependency at v3. The map is
  // drawn by react-simple-maps@3, whose `d3-zoom@2` carries its own *nested*
  // `d3-selection@2` and `d3-transition@2` -- a different copy, with a
  // different prototype object, which already has `interrupt` because the
  // nested d3-transition installs it. So the patch was applied to a prototype
  // nothing in this component ever called into.
  //
  // It could not be noticed either: the whole thing was wrapped in a `.catch`
  // that wrote to `console.debug`. Removing the two unused direct dependencies
  // is what surfaced it, by making the import fail to resolve at build time.

  // Real-time WebSocket Live Feed connection for all multi-domain events
  const wsLiveEvents = useLiveEvents('all');

  // Multi-Domain REST telemetry fetches
  const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
    // region_spread reserves the newest few rows for every region before the
    // rest of the page is filled by recency. Without it these 250 rows span
    // 184 seconds of the busiest four regions, and a chokepoint reporting a
    // handful of vessels a day never appears on the map at all.
    '/events/maritime?limit=250&region_spread=3',
    fetcher,
    { refreshInterval: POLL.live },
  );
  const { data: aviationEvents } = useSWR<NormalizedEvent[]>('/events/aviation?limit=80&region_spread=2', fetcher, {
    refreshInterval: POLL.standard,
  });
  // Which straits are reporting, and from which instrument.
  //
  // AIS is terrestrial and volunteer-fed, so it is dense over Taiwan and
  // absent over the Gulf; Sentinel-1 is not. Without this the map could not
  // distinguish a quiet strait from one no instrument is watching, and
  // Bab-el-Mandeb -- observed hourly by SAR -- appeared on it nowhere at all.
  // POLL.slow: a chokepoint's coverage changes when an orbit comes round or a
  // receiver drops, not tick by tick.
  const { data: chokepointStatus } = useSWR<ChokepointStatus>('/chokepoints', fetcher, {
    refreshInterval: POLL.slow,
  });

  // Radar returns, flattened across chokepoints.
  //
  // These are not vessels and are not labelled as such: a Sentinel-1 cell says
  // how much of that water returned like metal, with no MMSI, no name and no
  // way to tell two adjacent hulls from one large one. What it does say is
  // where, in a strait where nothing is transmitting.
  const radarContacts = useMemo(() => {
    if (!chokepointStatus) return [];
    return chokepointStatus.chokepoints.flatMap((c) =>
      (c.radar_grid?.cells ?? []).map((cell) => ({
        region: c.region,
        observedOn: c.radar_grid?.observed_on ?? null,
        lat: cell.latitude,
        lon: cell.longitude,
        density: cell.target_density,
        targetPixels: cell.target_pixels,
      })),
    );
  }, [chokepointStatus]);

  const { data: tradfiEvents } = useSWR<NormalizedEvent[]>('/events/tradfi?limit=40', fetcher, {
    refreshInterval: POLL.live,
  });

  // Layer Toggles State
  const [showVessels, setShowVessels] = useState(true);
  const [tankersOnly, setTankersOnly] = useState(false);
  const [showFlights, setShowFlights] = useState(true);
  const [showExchanges, setShowExchanges] = useState(true);
  const [showChokepoints, setShowChokepoints] = useState(true);
  const [showRadar, setShowRadar] = useState(true);

  const [selectedObject, setSelectedObject] = useState<{
    type: 'vessel' | 'flight' | 'exchange' | 'chokepoint' | 'radar';
    data: any;
  } | null>(null);

  // 1. MARITIME TELEMETRY COMPUTATION (WITH SPATIAL ANTI-COLLISION)
  const rawMaritime = useMemo(() => {
    const liveMaritime = wsLiveEvents.filter((e) => {
      const t = (e.type || '').toLowerCase();
      const s = (e.source || '').toLowerCase();
      return (
        t.includes('vessel') || t.includes('maritime') || t.includes('ais') || s.includes('ais')
      );
    });
    const merged = [...liveMaritime, ...(maritimeEvents || [])];
    return Array.from(new Map(merged.map((e) => [e.event_id, e])).values());
  }, [wsLiveEvents, maritimeEvents]);

  const { vessels, tankersCount } = useMemo(() => {
    const vesselMap = new Map<string, any>();

    rawMaritime.forEach((e: any) => {
      const d = e.vessel_data || e.domain_data || e.raw_payload || {};
      const meta = d.MetaData || d.meta || {};
      const pos = d.Message?.PositionReport || d.PositionReport || {};

      const mmsi = String(
        d.mmsi ||
          meta.MMSI ||
          e.primary_entity?.id ||
          e.primary_entity_id ||
          e.primary_entity_name ||
          e.event_id ||
          'UNKNOWN',
      );

      const parseValidCoord = (candidates: any[], maxBound: number): number | null => {
        for (const c of candidates) {
          if (c !== null && c !== undefined && c !== '') {
            const n = parseFloat(String(c));
            // `n !== 0` was here, which discards the equator and the prime
            // meridian. Vessels in the Gulf of Guinea report latitudes of
            // exactly 0 and were being treated as having no position at all --
            // and then placed in the Persian Gulf by the fallback below. The
            // (0, 0) pair is handled by the caller, where both values are known.
            if (!isNaN(n) && Math.abs(n) <= maxBound) {
              return n;
            }
          }
        }
        return null;
      };

      let lat = parseValidCoord(
        [e.latitude, d.latitude, d.lat, pos.Latitude, meta.latitude, d.position?.latitude, e.lat],
        90,
      );
      let lon = parseValidCoord(
        [
          e.longitude,
          d.longitude,
          d.lon,
          pos.Longitude,
          meta.longitude,
          d.position?.longitude,
          e.lon,
        ],
        180,
      );

      // Null Island: both axes exactly zero is AIS shorthand for no fix.
      if (lat === 0 && lon === 0) {
        lat = null;
        lon = null;
      }

      // A region centroid stands in for a missing position, and is marked as
      // approximate so the panel can say so. A region we cannot place means the
      // vessel is not drawn at all.
      let approximatePosition = false;
      if (lat === null || lon === null) {
        const reg = String(e.region || d.region || meta.region || e.vessel_data?.region || '');
        const centroid = resolveRegionFallback(reg);
        if (!centroid) return;
        lat = centroid[0];
        lon = centroid[1];
        approximatePosition = true;
      }

      const vtype = String(
        d.vessel_type || d.type || meta.ShipType || e.vessel_data?.vessel_type || '',
      ).toLowerCase();
      const name =
        e.primary_entity_name ||
        e.entity_name ||
        meta.ShipName ||
        d.name ||
        e.primary_entity?.name ||
        `VESSEL_${mmsi}`;
      const nameUpper = name.toUpperCase();

      const isTanker =
        vtype.includes('tanker') ||
        vtype.includes('oil') ||
        vtype.includes('lng') ||
        vtype.includes('crude') ||
        vtype.includes('petro') ||
        vtype.includes('lpg') ||
        nameUpper.includes('TANKER') ||
        nameUpper.includes('OIL') ||
        nameUpper.includes('CRUDE') ||
        nameUpper.includes('PETRO') ||
        nameUpper.includes('LNG') ||
        nameUpper.includes('LPG') ||
        nameUpper.includes('CHEM') ||
        nameUpper.includes('VLCC') ||
        nameUpper.includes('ULCC') ||
        nameUpper.includes('AFRAMAX') ||
        nameUpper.includes('SUEZMAX');

      vesselMap.set(mmsi, {
        mmsi,
        name,
        lat,
        lon,
        isTanker,
        vessel_type: d.vessel_type || (isTanker ? 'Oil / Gas Tanker' : 'Cargo Vessel'),
        anomaly: e.anomaly_score ?? 0.0,
        approximatePosition,
        // 12.4 knots, 'Underway Using Engine' and 'International Shipping Lane'
        // were the defaults here. None of them came from the vessel: a ship
        // that reported no speed was shown making way at a specific, plausible
        // 12.4 knots, and one that reported no navigational status was asserted
        // to be under engine. The panel renders ABSENT for null, which is what
        // the aviation panel beside it already does for altitude and squawk.
        speed: d.speed_knots ?? d.speed ?? (pos.Sog != null ? parseFloat(String(pos.Sog)) : null),
        heading: d.heading ?? pos.TrueHeading ?? null,
        region: e.region || null,
        nav_status: d.nav_status || null,
      });
    });

    const rawList = Array.from(vesselMap.values());
    // Apply spatial anti-collision spiral to prevent marker stacking
    const deconflictList = applySpatialAntiCollision(rawList);

    return {
      vessels: deconflictList,
      tankersCount: deconflictList.filter((v) => v.isTanker).length,
    };
  }, [rawMaritime]);

  const filteredVessels = useMemo(() => {
    const base = tankersOnly ? vessels.filter((v) => v.isTanker) : vessels;
    return base.sort((a, b) => (b.anomaly || 0) - (a.anomaly || 0)).slice(0, MAX_RENDERED_VESSELS);
  }, [vessels, tankersOnly]);

  // 2. AVIATION TELEMETRY COMPUTATION (WITH SPATIAL ANTI-COLLISION)
  const rawAviation = useMemo(() => {
    const liveAviation = wsLiveEvents.filter((e) => {
      const t = (e.type || '').toLowerCase();
      const s = (e.source || '').toLowerCase();
      return (
        t.includes('flight') ||
        t.includes('aviation') ||
        t.includes('adsb') ||
        s.includes('opensky')
      );
    });
    const merged = [...liveAviation, ...(aviationEvents || [])];
    return Array.from(new Map(merged.map((e) => [e.event_id, e])).values());
  }, [wsLiveEvents, aviationEvents]);

  const flights = useMemo(() => {
    const flightMap = new Map<string, any>();

    rawAviation.forEach((e: any) => {
      const d = e.flight_data || e.domain_data || e.raw_payload || {};
      const icao24 = String(
        d.icao24 || e.primary_entity?.id || e.primary_entity_id || e.event_id || 'UNKNOWN',
      ).toUpperCase();
      const parseValidCoord = (candidates: any[], maxBound: number): number | null => {
        for (const c of candidates) {
          if (c !== null && c !== undefined && c !== '') {
            const n = parseFloat(String(c));
            // `n !== 0` was here, which discards the equator and the prime
            // meridian. Vessels in the Gulf of Guinea report latitudes of
            // exactly 0 and were being treated as having no position at all --
            // and then placed in the Persian Gulf by the fallback below. The
            // (0, 0) pair is handled by the caller, where both values are known.
            if (!isNaN(n) && Math.abs(n) <= maxBound) {
              return n;
            }
          }
        }
        return null;
      };

      let lat = parseValidCoord([e.latitude, d.latitude, d.lat], 90);
      let lon = parseValidCoord([e.longitude, d.longitude, d.lon], 180);

      // As above: a region centroid is an approximation, and an unplaceable
      // region is not a flight over Washington DC.
      let approximatePosition = false;
      if (lat === 0 && lon === 0) {
        lat = null;
        lon = null;
      }
      if (lat === null || lon === null) {
        const reg = String(e.region || d.region || e.flight_data?.region || '');
        const centroid = resolveRegionFallback(reg);
        if (!centroid) return;
        lat = centroid[0];
        lon = centroid[1];
        approximatePosition = true;
      }

      const callsign = d.callsign || e.primary_entity_name || e.entity_name || `FLT_${icao24}`;
      const squawk = String(d.squawk || '');
      const isEmergency = ['7500', '7600', '7700'].includes(squawk) || e.anomaly_score >= 0.7;

      flightMap.set(icao24, {
        icao24,
        callsign,
        lat,
        lon,
        // Absent, not invented.
        //
        // These four fields defaulted to 32,000 ft, 450 kts, squawk
        // 1200 and origin US, and the detail panel rendered them as
        // measurements. Over two days of live flight events: 36,178 of
        // 90,108 (40.1%) carry no squawk, so two fifths of the aircraft
        // on this map asserted 1200 -- which is not a placeholder but
        // the assigned code for VFR flight outside controlled airspace,
        // a specific operational claim. And `baro_altitude_m` of 0 is
        // falsy, so an aircraft on the ground was drawn at 32,000 feet.
        altitude_ft:
          d.baro_altitude_m != null
            ? Math.round(d.baro_altitude_m * 3.28084)
            : (e.altitude_ft ?? null),
        speed_kts: d.velocity_ms != null ? Math.round(d.velocity_ms * 1.94384) : null,
        squawk: squawk || null,
        isEmergency,
        origin_country: d.origin_country || e.country_code || null,
        anomaly: e.anomaly_score ?? 0.0,
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
      const anomaly = e.anomaly_score ?? 0.0;

      // Map ticker to an exchange hub
      let matchedExchange =
        FINANCIAL_EXCHANGES.find((ex) => ex.keyTickers.includes(ticker)) || FINANCIAL_EXCHANGES[0];
      const existing = map.get(matchedExchange.symbol) || [];
      existing.push({ ticker, anomaly, headline: e.headline });
      map.set(matchedExchange.symbol, existing);
    });
    return map;
  }, [tradfiEvents]);

  // 4. BGP CYBER LINKS -- removed, because they could never be drawn.
  //
  // The layer plotted an arc from `domain_data.from_coords` to
  // `domain_data.to_coords`. Neither name appears anywhere in the platform:
  // not in `SecurityData`, not in the cyber enricher, not in any collector.
  // The toggle beside it has therefore always read "CYBER (0)" and the layer
  // has never drawn a line.
  //
  // Nor is the arc derivable. A BGP event knows one endpoint -- the origin
  // AS and the country it is registered in -- and the other end of a hijack
  // is the announced prefix, which this deployment has no way to place on a
  // map. Restoring the layer means an ASN-to-location dataset the platform
  // does not have; drawing it from what is here would mean inventing the
  // half that is missing, which is the class of defect this audit exists to
  // remove. The cyber domain is served by CyberIntelligencePanel, which
  // renders what is actually known about these events.

  return (
    <div className="w-full h-full bg-inset relative overflow-hidden text-white rounded-xl border border-cyan-500/20 shadow-panel">
      {/* Top Bar Navigation & Multi-Domain Layer Toggles */}
      <div className="absolute top-3 left-3 right-3 z-10 bg-page/90 px-3.5 py-2 rounded-lg border border-accent/30 text-xs flex flex-wrap items-center justify-between gap-2 shadow-2xl">
        <div className="flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-accent animate-ping" />
          <span className="text-accent font-semibold">Global map</span>
        </div>

        {/* Layer Control Buttons */}
        <div className="flex items-center gap-1 text-micro overflow-x-auto">
          <button
            onClick={() => {
              setShowVessels(true);
              setTankersOnly(false);
            }}
            className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
              showVessels && !tankersOnly
                ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/50'
                : 'bg-raised text-ink-mute border-line'
            }`}
          >
            VESSELS ({vessels.length})
          </button>
          <button
            onClick={() => {
              setShowVessels(true);
              setTankersOnly(!tankersOnly);
            }}
            className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
              tankersOnly
                ? 'bg-amber-500/30 text-amber-300 border-amber-500/60 glow-amber'
                : 'bg-raised text-amber-400/70 border-line'
            }`}
          >
            TANKERS ({tankersCount})
          </button>
          <button
            onClick={() => setShowFlights(!showFlights)}
            className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
              showFlights
                ? 'bg-cyan-500/20 text-cyan-300 border-cyan-500/50'
                : 'bg-raised text-ink-mute border-line'
            }`}
          >
            FLIGHTS ({flights.length})
          </button>
          <button
            onClick={() => setShowExchanges(!showExchanges)}
            className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
              showExchanges
                ? 'bg-purple-500/20 text-purple-300 border-purple-500/50'
                : 'bg-raised text-ink-mute border-line'
            }`}
          >
            EXCHANGES ({FINANCIAL_EXCHANGES.length})
          </button>
          <button
            onClick={() => setShowChokepoints(!showChokepoints)}
            className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
              showChokepoints
                ? 'bg-amber-500/20 text-amber-300 border-amber-500/50'
                : 'bg-raised text-ink-mute border-line'
            }`}
          >
            CHOKEPOINTS ({GLOBAL_CHOKEPOINTS.length})
          </button>
          <button
            onClick={() => setShowRadar(!showRadar)}
            className={`px-2 py-0.5 rounded border transition-all cursor-pointer font-bold ${
              showRadar
                ? 'bg-accent/20 text-accent border-accent/50 glow-cyan'
                : 'bg-raised text-ink-mute border-line'
            }`}
          >
            SWEEP
          </button>
        </div>
      </div>

      {/* Object Inspector Drawer Modal */}
      {selectedObject && (
        <div className="absolute top-14 left-3 z-20 bg-page/95 border border-accent/50 p-4 rounded-xl max-w-sm text-xs shadow-2xl space-y-2.5">
          <div className="flex items-center justify-between border-b border-cyan-500/30 pb-2">
            <span className="font-bold text-accent uppercase tracking-wide">
              {selectedObject.type === 'vessel' &&
                (selectedObject.data.isTanker ? 'TANKER INSPECTOR' : 'VESSEL INSPECTOR')}
              {selectedObject.type === 'flight' && 'AIRSPACE FLIGHT INSPECTOR'}
              {selectedObject.type === 'exchange' && 'FINANCIAL EXCHANGE HUB'}
              {selectedObject.type === 'chokepoint' && 'MARITIME CHOKEPOINT'}
              {selectedObject.type === 'radar' && 'RADAR RETURN (NO TRANSPONDER)'}
            </span>
            <button
              onClick={() => setSelectedObject(null)}
              aria-label="Close"
              className="text-ink-dim hover:text-white font-bold text-xs bg-overlay px-2 py-0.5 rounded cursor-pointer"
            >
              <IconClose />
            </button>
          </div>

          {selectedObject.type === 'vessel' && (
            <div className="space-y-1 text-micro text-ink-dim">
              <div>
                NAME: <span className="text-white font-bold">{selectedObject.data.name}</span>
              </div>
              <div>
                TYPE:{' '}
                <span className="text-amber-300 font-bold">{selectedObject.data.vessel_type}</span>
              </div>
              <div>
                MMSI: <span className="text-cyan-400 font-mono">{selectedObject.data.mmsi}</span>
              </div>
              <div>
                POSITION:{' '}
                <span className="text-white font-bold">
                  {selectedObject.data.lat.toFixed(4)}, {selectedObject.data.lon.toFixed(4)}
                </span>
              </div>
              <div>
                REGION:{' '}
                <span className="text-emerald-400">{selectedObject.data.region ?? ABSENT}</span>
              </div>
              {selectedObject.data.approximatePosition && (
                <div className="text-amber-400">
                  POSITION IS THE REGION CENTROID &mdash; the vessel reported none.
                </div>
              )}
              <div>
                SPEED:{' '}
                <span className="text-emerald-400 font-bold">
                  {selectedObject.data.speed ?? ABSENT} knots
                </span>
              </div>
              <div>
                ANOMALY SCORE:{' '}
                <span className="text-rose-400 font-bold">
                  {(selectedObject.data.anomaly || 0).toFixed(2)}
                </span>
              </div>
            </div>
          )}

          {selectedObject.type === 'flight' && (
            <div className="space-y-1 text-micro text-ink-dim">
              <div>
                CALLSIGN:{' '}
                <span className="text-cyan-300 font-bold">{selectedObject.data.callsign}</span>
              </div>
              <div>
                ICAO24: <span className="text-ink-dim font-mono">{selectedObject.data.icao24}</span>
              </div>
              <div>
                ALTITUDE:{' '}
                <span className="text-emerald-400 font-bold">
                  {selectedObject.data.altitude_ft != null
                    ? `${formatNumber(selectedObject.data.altitude_ft, { decimals: 0 })} ft`
                    : ABSENT}
                </span>
              </div>
              <div>
                SPEED:{' '}
                <span className="text-emerald-400 font-bold">
                  {selectedObject.data.speed_kts ?? ABSENT} knots
                </span>
              </div>
              <div>
                SQUAWK CODE:{' '}
                <span
                  className={
                    selectedObject.data.isEmergency
                      ? 'text-rose-400 font-bold animate-pulse'
                      : 'text-ink-dim'
                  }
                >
                  {selectedObject.data.squawk ?? ABSENT}
                </span>
              </div>
              <div>
                ORIGIN COUNTRY:{' '}
                <span className="text-amber-300">
                  {selectedObject.data.origin_country ?? ABSENT}
                </span>
              </div>
              <div>
                ANOMALY SCORE:{' '}
                <span className="text-rose-400 font-bold">
                  {(selectedObject.data.anomaly || 0).toFixed(2)}
                </span>
              </div>
            </div>
          )}

          {selectedObject.type === 'exchange' && (
            <div className="space-y-1 text-micro text-ink-dim">
              <div>
                EXCHANGE:{' '}
                <span className="text-purple-300 font-bold">
                  {selectedObject.data.name} ({selectedObject.data.symbol})
                </span>
              </div>
              <div>
                FOCUS: <span className="text-ink">{selectedObject.data.region}</span>
              </div>
              <div>
                COORDINATES:{' '}
                <span className="text-ink-dim">
                  {selectedObject.data.lat}, {selectedObject.data.lon}
                </span>
              </div>
              <div className="pt-1">
                <span className="text-ink-dim uppercase text-micro font-bold">
                  Key Tracked Instruments:
                </span>
                <div className="flex flex-wrap gap-1 pt-1">
                  {selectedObject.data.keyTickers.map((t: string) => (
                    <span
                      key={t}
                      className="px-1.5 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/40 text-micro font-bold"
                    >
                      {t}
                    </span>
                  ))}
                </div>
              </div>
            </div>
          )}

          {selectedObject.type === 'radar' && (
            <div className="space-y-1 text-micro text-ink-dim">
              <div>
                REGION:{' '}
                <span className="text-white font-bold">{selectedObject.data.region}</span>
              </div>
              <div>
                CELL CENTRE:{' '}
                <span className="text-white font-bold">
                  {selectedObject.data.lat.toFixed(4)}, {selectedObject.data.lon.toFixed(4)}
                </span>
              </div>
              <div>
                TARGET DENSITY:{' '}
                <span className="text-violet-300 font-bold">
                  {selectedObject.data.density}
                </span>
              </div>
              <div>
                IMAGED:{' '}
                <span className="text-emerald-400">
                  {selectedObject.data.observedOn ?? ABSENT}
                </span>
              </div>
              {/* Said on every one of these, because a reader who takes a radar
                  cell for a vessel count will draw conclusions it cannot carry. */}
              <div className="text-amber-400 pt-1">
                Sentinel-1 SAR. This is water returning like metal, not a vessel
                count &mdash; no MMSI, no name, and two hulls close together read
                as one.
              </div>
            </div>
          )}

          {selectedObject.type === 'chokepoint' && (
            <div className="space-y-1 text-micro text-ink-dim">
              <div>
                LOCATION: <span className="text-white font-bold">{selectedObject.data.name}</span>
              </div>
              <div>
                RISK TIER:{' '}
                <span className="text-amber-400 font-bold">{selectedObject.data.risk}</span>
              </div>
              <div>
                COORDINATES:{' '}
                <span className="text-ink-dim">
                  {selectedObject.data.lat}, {selectedObject.data.lon}
                </span>
              </div>
            </div>
          )}
        </div>
      )}

      {/* SVG Radar Sweep Overlay */}
      {showRadar && (
        <div className="absolute inset-0 pointer-events-none z-0 flex items-center justify-center opacity-25">
          <svg className="w-[500px] h-[500px]" viewBox="0 0 500 500">
            <circle
              cx="250"
              cy="250"
              r="220"
              fill="none"
              stroke={PALETTE.accent}
              strokeWidth="1"
              strokeDasharray="6 6"
            />
            <circle
              cx="250"
              cy="250"
              r="160"
              fill="none"
              stroke={PALETTE.accent}
              strokeWidth="1"
              opacity="0.6"
            />
            <circle
              cx="250"
              cy="250"
              r="100"
              fill="none"
              stroke={PALETTE.accent}
              strokeWidth="1"
              opacity="0.4"
            />
            <circle
              cx="250"
              cy="250"
              r="40"
              fill="none"
              stroke={PALETTE.accent}
              strokeWidth="1"
              opacity="0.2"
            />
            <line
              x1="250"
              y1="30"
              x2="250"
              y2="470"
              stroke={PALETTE.accent}
              strokeWidth="1"
              opacity="0.3"
            />
            <line
              x1="30"
              y1="250"
              x2="470"
              y2="250"
              stroke={PALETTE.accent}
              strokeWidth="1"
              opacity="0.3"
            />
            <g className="radar-sweep">
              <path
                d="M 250 250 L 250 30 A 220 220 0 0 1 430 140 Z"
                fill="url(#globalMapRadarGradient)"
                opacity="0.75"
              />
            </g>
            <defs>
              <radialGradient id="globalMapRadarGradient" cx="50%" cy="50%" r="50%">
                <stop offset="0%" stopColor={PALETTE.accent} stopOpacity="0.4" />
                <stop offset="100%" stopColor={PALETTE.accent} stopOpacity="0" />
              </radialGradient>
            </defs>
          </svg>
        </div>
      )}

      {/* Chokepoint coverage: reporting, AIS-silent, or unobserved. */}
      {chokepointStatus && chokepointStatus.chokepoints.length > 0 && (
        <div className="absolute bottom-3 left-3 z-10 bg-page/85 px-3 py-2 rounded-lg border border-accent/30 text-micro space-y-1 shadow-lg max-w-xs">
          <div className="text-ink-dim font-bold uppercase tracking-wide">
            Chokepoints &middot; last {chokepointStatus.window_hours}h
          </div>
          {chokepointStatus.chokepoints.map((c) => {
            const instruments = [c.ais ? 'AIS' : null, c.sar ? 'SAR' : null].filter(Boolean);
            return (
              <div key={c.region} className="flex items-center justify-between gap-3">
                <span className="flex items-center gap-2">
                  <span
                    className={
                      'h-1.5 w-1.5 rounded-full ' +
                      (!c.observed
                        ? 'bg-rose-500'
                        : c.ais_silent
                          ? 'bg-amber-400'
                          : 'bg-emerald-400')
                    }
                  />
                  <span className={c.observed ? '' : 'text-ink-dim'}>{c.region}</span>
                </span>
                <span
                  className={
                    'font-mono ' + (c.observed ? 'text-emerald-300' : 'text-rose-400')
                  }
                >
                  {/* Not observed and quiet are different claims. */}
                  {instruments.length > 0 ? instruments.join('+') : 'no coverage'}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {/* Bottom-Right Multi-Domain Spatial HUD */}
      <div className="absolute bottom-3 right-3 z-10 bg-page/85 px-3 py-2 rounded-lg border border-cyan-500/30 text-micro space-y-1 shadow-lg">
        <div className="flex items-center gap-2">
          <span className="h-1.5 w-1.5 rounded-full bg-caution animate-pulse" />
          <span className="text-amber-300 font-bold"> Tankers ({tankersCount})</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
          <span>Total Vessels ({vessels.length})</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="h-1.5 w-1.5 rounded-full bg-cyan-400 animate-pulse" />
          <span className="text-cyan-300 font-bold"> Flights ({flights.length})</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="h-1.5 w-1.5 rounded-full bg-purple-400 animate-pulse" />
          <span className="text-purple-300 font-bold">
            {' '}
            Financial Exchanges ({FINANCIAL_EXCHANGES.length})
          </span>
        </div>
      </div>

      {/* Interactive World Map Canvas */}
      <ComposableMap
        projection="geoMercator"
        projectionConfig={{ scale: 110 }}
        className="w-full h-full"
      >
        <StaticWorldBase />

        {/* 1. Maritime Chokepoints Layer */}
        {showChokepoints &&
          GLOBAL_CHOKEPOINTS.map((m) => (
            <Marker
              key={m.name}
              coordinates={[m.lon, m.lat]}
              onClick={() => setSelectedObject({ type: 'chokepoint', data: m })}
            >
              <circle
                r={7}
                fill="rgba(245, 158, 11, 0.25)"
                stroke={PALETTE.caution}
                strokeWidth={1.5}
                className="animate-ping cursor-pointer"
              />
              <circle r={3.5} fill={PALETTE.caution} className="cursor-pointer" />
              <text
                textAnchor="middle"
                y={-10}
                style={{
                  fontFamily: 'monospace',
                  fill: PALETTE.caution,
                  fontSize: '7px',
                  fontWeight: 'bold',
                }}
                className="cursor-pointer"
              >
                {m.name}
              </text>
            </Marker>
          ))}

        {/* 1b. Radar returns, where no transponder is reporting.
             Drawn as a square rather than a vessel glyph on purpose: this is a
             cell of water that returned like metal, not a ship. It carries no
             identity and cannot separate two hulls from one. */}
        {showChokepoints &&
          radarContacts.map((rc, i) => (
            <Marker
              key={`radar-${rc.region}-${i}`}
              coordinates={[rc.lon, rc.lat]}
              onClick={() => setSelectedObject({ type: 'radar', data: rc })}
            >
              <rect
                x={-3}
                y={-3}
                width={6}
                height={6}
                fill="rgba(56, 189, 248, 0.18)"
                stroke={PALETTE.info}
                strokeWidth={1}
                className="cursor-pointer"
              />
            </Marker>
          ))}

        {/* 2. Global Financial Exchange Hubs Layer */}
        {showExchanges &&
          FINANCIAL_EXCHANGES.map((ex) => {
            const anomalies = exchangeAnomalies.get(ex.symbol) || [];
            const topAnomaly = anomalies.length > 0 ? anomalies[0] : null;

            return (
              <Marker
                key={ex.symbol}
                coordinates={[ex.lon, ex.lat]}
                onClick={() => setSelectedObject({ type: 'exchange', data: ex })}
              >
                <rect
                  x={-5}
                  y={-5}
                  width={10}
                  height={10}
                  fill="rgba(168, 85, 247, 0.4)"
                  stroke="#c084fc"
                  strokeWidth={1.5}
                  className="cursor-pointer"
                />
                <text
                  textAnchor="middle"
                  y={-9}
                  style={{
                    fontFamily: 'monospace',
                    fill: '#e9d5ff',
                    fontSize: '7.5px',
                    fontWeight: 'bold',
                  }}
                  className="cursor-pointer"
                >
                  {ex.symbol}
                </text>
                {topAnomaly && (
                  <text
                    textAnchor="middle"
                    y={15}
                    style={{
                      fontFamily: 'monospace',
                      fill: PALETTE.negative,
                      fontSize: '7px',
                      fontWeight: 'bold',
                    }}
                    className="cursor-pointer"
                  >
                    {topAnomaly.ticker}
                  </text>
                )}
              </Marker>
            );
          })}

        {/* 3. Aviation ADS-B Flights Layer */}
        {showFlights &&
          flights.map((flt) => (
            <Marker
              key={flt.icao24}
              coordinates={[flt.lon, flt.lat]}
              onClick={() => setSelectedObject({ type: 'flight', data: flt })}
            >
              {flt.isEmergency ? (
                <>
                  <circle
                    r={8}
                    fill="rgba(239, 68, 68, 0.35)"
                    stroke={PALETTE.negative}
                    strokeWidth={1.5}
                    className="animate-ping cursor-pointer"
                  />
                  <circle
                    r={4}
                    fill={PALETTE.negative}
                    stroke="#ffffff"
                    strokeWidth={1}
                    className="cursor-pointer"
                  />
                  <text
                    textAnchor="middle"
                    y={-10}
                    style={{
                      fontFamily: 'monospace',
                      fill: '#fca5a5',
                      fontSize: '7.5px',
                      fontWeight: 'bold',
                    }}
                    className="cursor-pointer"
                  >
                    {flt.callsign}
                  </text>
                </>
              ) : (
                <>
                  <circle
                    r={4.5}
                    fill="rgba(6, 182, 212, 0.4)"
                    stroke="#06b6d4"
                    strokeWidth={1}
                    className="cursor-pointer"
                  />
                  <text
                    textAnchor="middle"
                    y={-8}
                    style={{
                      fontFamily: 'monospace',
                      fill: '#67e8f9',
                      fontSize: '7px',
                      fontWeight: 'bold',
                    }}
                    className="cursor-pointer"
                  >
                    {flt.callsign}
                  </text>
                </>
              )}
            </Marker>
          ))}

        {/* 4. Maritime AIS Vessels Layer */}
        {showVessels &&
          filteredVessels.map((v) => (
            <Marker
              key={v.mmsi}
              coordinates={[v.lon, v.lat]}
              onClick={() => setSelectedObject({ type: 'vessel', data: v })}
            >
              {v.isTanker ? (
                <>
                  <circle
                    r={6}
                    fill="rgba(245, 158, 11, 0.35)"
                    stroke={PALETTE.caution}
                    strokeWidth={1.5}
                    opacity={0.7}
                    className="cursor-pointer"
                  />
                  <circle
                    r={3.5}
                    fill={PALETTE.caution}
                    stroke="#ffffff"
                    strokeWidth={1}
                    className="cursor-pointer"
                  />
                  <text
                    textAnchor="middle"
                    y={-9}
                    style={{
                      fontFamily: 'monospace',
                      fill: '#fef08a',
                      fontSize: '7.5px',
                      fontWeight: 'bold',
                    }}
                    className="cursor-pointer"
                  >
                    {v.name}
                  </text>
                </>
              ) : (
                <>
                  <circle
                    r={5}
                    fill="rgba(16, 185, 129, 0.3)"
                    stroke={PALETTE.positive}
                    strokeWidth={1}
                    opacity={0.6}
                    className="cursor-pointer"
                  />
                  <circle
                    r={3}
                    fill={PALETTE.positive}
                    stroke="#ffffff"
                    strokeWidth={1}
                    className="cursor-pointer"
                  />
                  <text
                    textAnchor="middle"
                    y={-8}
                    style={{
                      fontFamily: 'monospace',
                      fill: PALETTE.positive,
                      fontSize: '7px',
                      fontWeight: 'bold',
                    }}
                    className="cursor-pointer"
                  >
                    {v.name}
                  </text>
                </>
              )}
            </Marker>
          ))}
      </ComposableMap>
    </div>
  );
}
