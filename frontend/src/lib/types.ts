// frontend/src/lib/types.ts

export interface Scenario {
    correlation_id: string;
    status: string;
    headline: string;
    significance: string;
    confidence_overall: number;
    created_at: string;
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
    headline: string;
    summary?: string;
    tags?: string[];
    country_code?: string;
    region?: string;
    anomaly_score: number;
    financial_data?: FinancialData;
    vessel_data?: VesselData;
    security_data?: SecurityData;
    prediction_market_data?: PredictionMarketData;
}