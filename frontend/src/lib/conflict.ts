/**
 * Shapes returned by /conflict-zones and /vessels/live
 * (services/api_gateway/routes/conflict_zones.py).
 */

export type ZoneStatus = 'active-escalating' | 'escalating' | 'active' | 'watch';
export type ZoneTrend = 'rising' | 'steady' | 'falling' | 'unmeasured';

export interface InstrumentMove {
  symbol: string;
  occurred_at: string;
  headline: string | null;
  anomaly_score: number | null;
}

export interface RippleEffect {
  target: string;
  kind: 'instrument' | 'chokepoint' | 'sector';
  why: string;
  latest_move?: InstrumentMove | null;
}

export interface ZoneArticle {
  title: string;
  url: string;
  domain: string | null;
  seendate: string | null;
  sourcecountry: string | null;
}

export interface PlatformHeadline {
  occurred_at: string;
  source: string | null;
  headline: string | null;
  url: string | null;
  anomaly_score: number | null;
}

export interface NavWarning {
  id: string;
  nav_area: string | null;
  area: string;
  text: string;
  centroid: [number, number];
  points: [number, number][];
  zone_id: string | null;
  military: boolean;
  issued: string | null;
}

export interface ConflictZone {
  zone_id: string;
  name: string;
  prior: 'active' | 'flashpoint';
  bbox: [number, number, number, number];
  center: [number, number];
  regions: string[];
  parties: string[];
  drivers: string[];
  ripple: RippleEffect[];
  status: ZoneStatus;
  trend: ZoneTrend;
  news_volume_24h: number | null;
  news_baseline_daily: number | null;
  news_ratio: number | null;
  volume_series: number[];
  live_updated_at: string | null;
  news: ZoneArticle[];
  platform_headlines: PlatformHeadline[];
  activity_24h: Record<string, number>;
  nav_warnings: NavWarning[];
}

export interface ConflictZonesResponse {
  generated_at: string;
  zones: ConflictZone[];
  nav_warnings_updated_at: string | null;
  military_warnings_outside_zones: NavWarning[];
}

export interface LiveVessel {
  mmsi: string;
  name: string | null;
  lat: number;
  lon: number;
  speed: number | null;
  heading: number | null;
  region: string | null;
  vessel_type: string | null;
  tanker: boolean;
  sanctioned: boolean;
  age_seconds: number | null;
  dark: boolean;
}

export interface LightDetection {
  lat: number;
  lon: number;
  radiance: number | null;
  qf: number;
  observed_at: string | null;
  zone_id: string | null;
}

export interface LiveVesselsResponse {
  generated_at: string;
  light_detections: LightDetection[];
  light_detections_file: string | null;
  count: number;
  dark: number;
  tankers: number;
  vessels: LiveVessel[];
  note: string;
}

export interface ExposureZone {
  zone_id: string;
  name: string;
  status: ZoneStatus;
  weight: number;
  why: string;
}

export interface ExposedInstrument {
  symbol: string;
  score: number;
  zones: ExposureZone[];
  mentions: number;
  observations: number;
  latest_move: InstrumentMove | null;
  holders: string | null;
}

export interface ConflictExposureResponse {
  generated_at: string;
  window_hours: number;
  instruments: ExposedInstrument[];
}

/** Fill and stroke for a zone, by live status. */
export const ZONE_COLOURS: Record<ZoneStatus, { fill: string; stroke: string; label: string }> = {
  'active-escalating': { fill: 'rgba(239, 68, 68, 0.22)', stroke: '#ef4444', label: 'Active, escalating' },
  active: { fill: 'rgba(239, 68, 68, 0.12)', stroke: '#f87171', label: 'Active conflict' },
  escalating: { fill: 'rgba(249, 115, 22, 0.18)', stroke: '#fb923c', label: 'Flashpoint, escalating' },
  watch: { fill: 'rgba(245, 158, 11, 0.07)', stroke: '#fbbf24', label: 'Flashpoint, watch' },
};

/** Human labels for the observation types counted per zone. */
export const ACTIVITY_LABELS: Record<string, string> = {
  vessel_dark: 'Dark vessels',
  vessel_spoof: 'AIS spoofing',
  vessel_sts: 'Tanker STS transfers',
  flight_anomaly: 'Aviation emergencies',
  supply_chain_metric: 'Radar passes',
  viirs_lit_vessels: 'Lit vessels last night (VIIRS)',
};
