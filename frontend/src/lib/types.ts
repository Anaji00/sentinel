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

export interface FinancialData {
    ticker: string;
    instrument_type?: string;
    trade_type?: string;
    premium_usd?: number;
    underlying_price?: number;
    volume?: number;
    strike_price?: number;
    expiration_date?: string;
    current_price?: number;
}

export interface CryptoData {
    symbol?: string;
    price?: number;
    volume?: number;
    market_cap?: number;
    change_24h?: number;
}

export interface VesselData {
    mmsi: string;
    name: string;
    flag?: string;
    speed?: number;
    course?: number;
    latitude: number;
    longitude: number;
}

export interface SecurityData {
    breach_type: string;
    affected_org?: string;
    severity?: string;
    ip_address?: string;
}

export interface PredictionMarketData {
    ticker: string;
    title?: string;
    total_volume?: number;
    yes_bid?: number;
    no_bid?: number;
    yes_probability?: number;
    no_probability?: number;
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
    security_data?: SecurityData;
    prediction_market_data?: PredictionMarketData;
    domain_data?: Record<string, any>;
    raw_payload?: Record<string, any>;
}