// frontend/src/lib/types.ts

export interface ScenarioHypothesis {
    label?: string;
    mechanism?: string;
    probability?: number;
}

/** One agent's contribution to a consensus evidence trail.
 *
 *  `reasoning` was declared here and produced by nothing: neither
 *  `EvidenceContributor` nor the route that builds these dicts has ever
 *  carried it, and no component rendered it. `weight` is the field the
 *  producers do carry beyond these four. */
export interface EvidenceItem {
    agent_name: string;
    direction?: string;
    conviction?: number;
    score?: number;
    weight?: number;
}

export interface Corroboration {
    independent_sources: number;
    total_reports: number;
    corroboration_score: number;
    is_single_sourced: boolean;
    is_syndicated: boolean;
    minutes_to_corroboration: number | null;
    contributing_sources: string[];
}

export interface Scenario {
    scenario_id?: string;
    correlation_id: string;
    status: string;
    headline: string;
    /** The synthesis body, under the name the `scenarios` table gives it.
     *
     *  The card rendered `s.narrative || s.description` and neither name has
     *  ever existed on either side of the wire: the reasoning service computes
     *  the prose, writes it to `narrative_summary`, and `/scenarios` returns
     *  the row as it stands. Every scenario card in the feed showed a headline
     *  above an empty paragraph. */
    narrative_summary?: string;
    significance: string;
    confidence_overall: number;
    confidence_rationale?: string;
    hypotheses?: ScenarioHypothesis[];
    recommended_monitoring?: string[];
    created_at: string;
    updated_at?: string;
    trace_id?: string;
    supporting_event_ids?: string[];
    primary_entity_id?: string;
    primary_entity_name?: string;
}

export interface Entity {
    id: string;
    type: string;
    name: string;
    country_code?: string;
    flags?: string[];
}

export interface MarketMicrostructure {
    order_flow_imbalance?: number;
    kyle_lambda?: number;
    amihud_illiquidity?: number;
    vwap?: number;
    twap?: number;
    bid_ask_spread?: number;
    realized_volatility?: number;
    ewma_volatility?: number;
}

export interface FinancialData {
    ticker?: string;
    instrument_type?: string;
    trade_type?: string;
    side?: string;
    option_type?: string; // 'CALL' | 'PUT'
    premium_usd?: number;
    underlying_price?: number;
    volume?: number;
    open_interest?: number;
    implied_volatility?: number;
    strike?: number;
    expiry?: string;
    open_price?: number;
    close_price?: number;
    high_price?: number;
    low_price?: number;
    volume_oi_ratio?: number;
    otm_percentage?: number;
    // Earnings calendar fields
    earnings_report_date?: string;
    earnings_session?: string;
    eps_estimate?: number;
    eps_actual?: number;
    eps_surprise_pct?: number;
    revenue_estimate?: number;
    revenue_actual?: number;
    // Reference data fields
    sector?: string;
    industry?: string;
    index_membership?: string[];
    market_cap_tier?: string;
    exchange?: string;
}

export interface ScoreAdjustment {
    reason: string;
    delta: number;
}

export interface CryptoData {
    pair?: string;
    trade_type?: string;
    side?: string;
    price?: number;
    size_tokens?: number;
    notional_usd?: number;
    leverage?: number;
    funding_rate?: number;
    mark_price?: number;
    index_price?: number;
    basis_bps?: number;
    open_interest?: number;
    open_price?: number;
    high_price?: number;
    low_price?: number;
    close_price?: number;
    market_microstructure?: MarketMicrostructure;
}

/**
 * The AIS payload as `VesselData` defines it, field for field.
 *
 * Eight of the sixteen names here were the client's own. `flag`, `course` and
 * `ship_type` are `flag_state`, `course_over_ground` and `vessel_type` on the
 * wire, so a vessel's flag state -- resolved from its MMSI on every position
 * report -- could not be read through this contract at all. `name` was
 * declared *required* and never sent: the ship's name travels as
 * `primary_entity.name`. `is_tanker`, `is_dark` and `is_sanctioned` were
 * likewise never sent, and each is already expressed: a tanker is
 * `vessel_type`, dark is the `vessel_dark` event type, and a sanctions hit is
 * a flag on the entity.
 *
 * `latitude` and `longitude` are optional because a `vessel_static` report
 * carries identity without a position.
 */
export interface VesselData {
    mmsi: string;
    imo?: string;
    flag_state?: string;
    speed_knots?: number;
    course_over_ground?: number;
    heading?: number;
    latitude?: number;
    longitude?: number;
    vessel_type?: string;
    cargo_type?: string;
    nav_status?: string;
    draught?: number;
    length_meters?: number;
    destination?: string;
    eta?: string;
    gap_hours?: number;
    last_seen_region?: string;
}

/**
 * OpenSky state vectors, in the units OpenSky reports them.
 *
 * The previous shape declared `altitude_feet` and `velocity_knots` against a
 * server that sends metres and metres per second -- a reader trusting the
 * names would have been wrong by 3.28x and 1.94x. It also declared required
 * `latitude`/`longitude`, which `FlightData` has never carried: an aircraft's
 * position is on the event, not in its payload. `GlobalMap` reads
 * `baro_altitude_m` and `velocity_ms` off an `any` and converts them, which is
 * why nothing broke and why nothing caught it either.
 *
 * `is_military` has no producer anywhere in the platform. `is_emergency` is
 * decided from the squawk and carried as the `aviation_emergency` tag.
 */
export interface FlightData {
    icao24: string;
    callsign?: string;
    origin_country?: string;
    baro_altitude_m?: number;
    geo_altitude_m?: number;
    velocity_ms?: number;
    true_track?: number;
    vertical_rate?: number;
    on_ground?: boolean;
    squawk?: string;
    aircraft_type?: string;
    operator?: string;
    registration?: string;
}

export interface SecurityData {
    breach_type?: string;
    affected_org?: string;
    ip_address?: string;
    cve_id?: string;
    cvss_score?: number;
    cisa_kev?: boolean;
    asn?: string;
    ransomware_group?: string;
    route_leak_prefix?: string;
    exposure_type?: string;
    port?: number;
    record_count?: number;
    data_types?: string[];
    source_url?: string;
}

export interface PredictionMarketData {
    ticker: string;
    market_id?: string;
    question?: string;
    outcome?: string;
    category?: string;
    total_volume?: number;
    shares_traded?: number;
    notional_usd?: number;
    price_usd?: number;
    liquidity_pool_size?: number;
    yes_bid?: number;
    no_bid?: number;
    yes_probability?: number;
    no_probability?: number;
    probability_delta_24h?: number;
    resolution_date?: string;
}

export interface NormalizedEvent {
    /** Set only on rows the browser fetched directly, because the backend
     * returned nothing for that domain. Absent means the row came through
     * the platform's own collection, enrichment and anomaly scoring. */
    data_provenance?: string;
    event_id: string;
    trace_id?: string;
    type: string;
    occurred_at: string;
    source: string;
    primary_entity?: Entity;
    /** The entity's identifier, as every /events endpoint returns it. It was
     *  absent from this interface while components read it through casts, so
     *  the contract did not describe what the server actually sends. */
    primary_entity_id?: string;
    primary_entity_name?: string;
    entity_name?: string;
    /** Which domain the row belongs to, decided server-side from the payload
     *  column it carries. Present because deriving it on the client meant
     *  substring-matching `type`, and "market_anomaly" contains "market" -- so
     *  Coinbase candle anomalies were labelled TRADFI. */
    domain?: 'crypto' | 'prediction' | 'maritime' | 'aviation' | 'cyber' | 'tradfi' | 'news';
    /** Present on geocoded rows; the map plots from these. */
    latitude?: number | null;
    longitude?: number | null;
    headline: string;
    summary?: string;
    tags?: string[];
    country_code?: string;
    region?: string;
    anomaly_score: number;
    /** Independent corroboration of the claim, for events that can have it
     *  (news, OSINT). Distinct from the source's own track record: a trusted
     *  outlet reporting alone and four outlets agreeing are different things. */
    corroboration?: Corroboration;
    financial_data?: FinancialData;
    crypto_data?: CryptoData;
    vessel_data?: VesselData;
    flight_data?: FlightData;
    security_data?: SecurityData;
    prediction_market_data?: PredictionMarketData;
    market_microstructure?: MarketMicrostructure;
    score_adjustments?: ScoreAdjustment[];
    domain_data?: Record<string, any>;
    raw_payload?: Record<string, any>;
}

/**
 * Authenticated identity as returned by /api/auth/session.
 *
 * Deliberately narrow: the platform has no subscription tier, no monthly API
 * quota, and no per-user API key. An earlier shape modelled all three, which
 * meant the account panel could only be populated by inventing them.
 */
export interface SessionIdentity {
    email: string | null;
    role: string | null;
}