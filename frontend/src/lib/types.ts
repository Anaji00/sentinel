// frontend/src/lib/types.ts

export interface ScenarioHypothesis {
    label?: string;
    mechanism?: string;
    probability?: number;
}

export interface EvidenceItem {
    agent_name: string;
    direction?: string;
    conviction?: number;
    score?: number;
    reasoning?: string;
}

export interface Scenario {
    scenario_id?: string;
    correlation_id: string;
    status: string;
    headline: string;
    title?: string;
    description?: string;
    summary?: string;
    significance: string;
    confidence_overall: number;
    confidence_rationale?: string;
    hypotheses?: ScenarioHypothesis[];
    recommended_monitoring?: string[];
    probability?: number;
    created_at: string;
    primary_entity_name?: string;
    evidence_trail?: EvidenceItem[];
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
    timestamp?: string;
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
    strike_price?: number;
    expiry?: string;
    expiration_date?: string;
    current_price?: number;
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
    symbol?: string;
    trade_type?: string;
    side?: string;
    price?: number;
    size_tokens?: number;
    volume?: number;
    market_cap?: number;
    change_24h?: number;
    leverage?: number;
    funding_rate?: number;
    mark_price?: number;
    index_price?: number;
    basis_bps?: number;
    open_interest?: number;
    market_microstructure?: MarketMicrostructure;
}

export interface VesselData {
    mmsi: string;
    name: string;
    flag?: string;
    speed?: number;
    speed_knots?: number;
    course?: number;
    heading?: number;
    latitude: number;
    longitude: number;
    ship_type?: string;
    draught?: number;
    destination?: string;
    eta?: string;
    is_tanker?: boolean;
    is_dark?: boolean;
    is_sanctioned?: boolean;
}

export interface FlightData {
    icao24: string;
    callsign?: string;
    origin_country?: string;
    latitude: number;
    longitude: number;
    altitude_feet?: number;
    velocity_knots?: number;
    heading?: number;
    vertical_rate?: number;
    squawk?: string;
    is_military?: boolean;
    is_emergency?: boolean;
}

export interface SecurityData {
    breach_type: string;
    affected_org?: string;
    severity?: string;
    ip_address?: string;
    cve_id?: string;
    cvss_score?: number;
    cisa_kev?: boolean;
    asn?: string;
    ransomware_group?: string;
    target_sector?: string;
    route_leak_prefix?: string;
}

export interface PredictionMarketData {
    ticker: string;
    market_id?: string;
    title?: string;
    question?: string;
    outcome?: string;
    category?: string;
    total_volume?: number;
    shares_traded?: number;
    notional_usd?: number;
    price_usd?: number;
    volume_24h?: number;
    yes_bid?: number;
    no_bid?: number;
    yes_probability?: number;
    no_probability?: number;
    price_change_24h?: number;
    resolution_date?: string;
}

export interface NormalizedEvent {
    event_id: string;
    trace_id?: string;
    type: string;
    occurred_at: string;
    source: string;
    primary_entity?: Entity;
    primary_entity_name?: string;
    entity_name?: string;
    headline: string;
    summary?: string;
    tags?: string[];
    country_code?: string;
    region?: string;
    anomaly_score: number;
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

export interface AccountProfile {
    id: string;
    name: string;
    email: string;
    role: string;
    tier: 'INSTITUTIONAL' | 'SENTINEL_PRO' | 'ENTERPRISE';
    apiKey: string;
    apiUsageMonth: number;
    apiQuotaMonth: number;
    activeAgents: number;
    watchlistCount: number;
    notificationChannels: string[];
    created_at: string;
}