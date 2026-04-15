// These mirror backend Pydantic models exactly.
export interface Scenario {
    correlation_id: string;
    status: string;
    headline: string;
    significance: string;
    confidence_overall: number;
    created_at: string;
}

export interface MaritimeEvent {
    event_id: string;
    occurred_at: string;
    vessel_name: string;
    region: string;
    anomaly_score: number;
    domain_data: any; // The JSONB payload
}