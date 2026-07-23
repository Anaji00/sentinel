from __future__ import annotations
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import json
from psycopg2.extras import Json

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

class EventType(str, Enum):
    VESSEL_POSITION = "vessel_position"
    VESSEL_DARK = "vessel_dark"
    VESSEL_STS = "vessel_sts"
    VESSEL_SPOOF = "vessel_spoof"
    VESSEL_STATIC = "vessel_static"
    FLIGHT_POSITION = "flight_position"
    FLIGHT_DARK = "flight_dark"
    FLIGHT_ANOMALY = "flight_anomaly"
    OPTIONS_FLOW = "options_flow"
    DARK_POOL = "dark_pool"
    FUTURES_COT = "futures_cot"
    PRICE_ANOMALY = "price_anomaly"
    INSIDER_TRADE = "insider_trade"
    EQUITY_BLOCK = "equity_block"          
    CRYPTO_TRADE = "crypto_trade"          
    MARKET_CANDLE = "market_candle"        
    MARKET_ANOMALY = "market_anomaly"      
    HEADLINE = "headline"
    SOCIAL_SIGNAL = "social_signal"
    NARRATIVE_CLUSTER = "narrative_cluster"
    BREACH_DETECTED = "breach_detected"
    INFRA_EXPOSED = "infra_exposed"
    BGP_ANOMALY = "bgp_anomaly"
    RANSOMWARE = "ransomware"
    CLIMATE_STRESS = "climate_stress"
    INFASTRUCTURE = "infrastructure"
    SPORTS_LINE_MOVEMENT = "sports_line_movement"
    PREDICTION_MARKET_TRADE = "prediction_market_trade"
    CRYPTO_LIQUIDATION = "crypto_liquidation"
    CRYPTO_PERP_FUNDING = "crypto_perp_funding"
    CUSTOM = "custom"
    CRYPTO_TRANSFER = "crypto_transfer"
    PREDICTION_MARKET = "prediction_market"
    VULNERABILITY = "vulnerability"

class EntityType(str, Enum):
    VESSEL = "vessel"
    AIRCRAFT = "aircraft"
    COMPANY = "company"
    PERSON = "person"
    COUNTRY = "country"
    INSTRUMENT = "instrument"
    INFRASTRUCTURE = "infrastructure"
    MEDIA_SOURCE = "media_source"
    UNKNOWN = "unknown"

class AlertTier(str, Enum):
    WATCH = "WATCH"
    ALERT = "ALERT"
    ELEVATED = "ELEVATED"
    INTELLIGENCE = "INTELLIGENCE"
    CRITICAL = "CRITICAL"

class ScenarioStatus(str, Enum):
    HYPOTHESIS = "hypothesis"
    CONFIRMED = "confirmed"
    DENIED = "denied"
    DEVELOPING = "developing"

class ScenarioHypothesis(BaseModel):
    label: str
    probability: int
    mechanism: str
    beneficiaries: List[str]
    watch_signals: List[str]
    deny_signals: List[str]
    time_horizon: str

class Entity(BaseModel):
    id: str
    type: EntityType = EntityType.UNKNOWN
    name: Optional[str] = None
    flags: List[str] = Field(default_factory=list) 
    country_code: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
 
    def is_flagged(self) -> bool:
        return len(self.flags) > 0
    def has_flag(self, flag: str) -> bool:
        return flag in self.flags
    
class VesselData(BaseModel):
    mmsi: Optional[str] = None
    imo: Optional[str] = None
    speed_knots: Optional[float] = None
    heading: Optional[int] = None
    course_over_ground: Optional[float] = None
    nav_status: Optional[str] = None
    destination: Optional[str] = None
    eta: Optional[str] = None
    vessel_type: Optional[str] = None
    flag_state: Optional[str] = None
    cargo_type: Optional[int] = None
    length_meters: Optional[float] = None
    draught: Optional[float] = None
    gap_hours: Optional[float] = None
    last_seen_region: Optional[str] = None

class FlightData(BaseModel):
    icao24: Optional[str] = None
    callsign: Optional[str] = None
    origin_country: Optional[str] = None
    baro_altitude_m: Optional[float] = None
    geo_altitude_m: Optional[float] = None
    velocity_ms: Optional[float] = None
    true_track: Optional[float] = None
    vertical_rate: Optional[float] = None
    on_ground: Optional[bool] = None
    squawk: Optional[str] = None
    aircraft_type: Optional[str] = None
    operator: Optional[str] = None
    registration: Optional[str] = None

class BettingData(BaseModel):
    matchup: Optional[str] = None
    market_type: Optional[str] = None
    selection: Optional[str] = None
    implied_probablity: Optional[float] = None
    american_odds: Optional[int] = None
    sharp_book_deviation: Optional[float] = None

class PredictionMarketData(BaseModel):
   market_id: str
   question: str
   outcome: str
   shares_traded: float
   price_usd: float
   liquidity_pool_size: Optional[float] = None
   notional_usd: Optional[float] = None
   yes_bid: Optional[float] = None
   no_bid: Optional[float] = None
   yes_probability: Optional[float] = None
   no_probability: Optional[float] = None

class CryptoData(BaseModel):
    pair: str
    trade_type: str
    side: str
    price: float 
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    size_tokens: float
    leverage: Optional[float] = None

class FinancialData(BaseModel):
    ticker: Optional[str] = None
    instrument_type: Optional[str] = None
    side: Optional[str] = None
    trade_type: Optional[str] = None
    strike: Optional[float] = None
    expiry: Optional[str] = None
    premium_usd: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    implied_volatility: Optional[float] = None
    underlying_price: Optional[float] = None
    open_price: Optional[float] = None
    close_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    exchange: Optional[str] = None
    volume_oi_ratio: Optional[float] = None
    otm_percentage: Optional[float] = None

class SecurityData(BaseModel):
    breach_type: Optional[str] = None
    affected_org: Optional[str] = None
    record_count: Optional[int] = None
    data_types: List[str] = Field(default_factory=list)
    source_url: Optional[str] = None
    cve_id: Optional[str] = None
    cvss_score: Optional[float] = None
    exposure_type: Optional[str] = None
    ip_address: Optional[str] = None
    port: Optional[int] = None

class RawEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str
    type: Optional[str] = "custom"
    collected_at: datetime = Field(default_factory=_utcnow)
    occurred_at: Optional[datetime] = None
    financial_data: Optional[Dict[str, Any]] = None
    raw_payload: Dict[str, Any] = Field(default_factory=dict)

class NormalizedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4())) 
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: EventType
    occurred_at: datetime 
    collected_at: datetime = Field(default_factory=_utcnow)
    source: str
    source_reliability: float = 1.0
    primary_entity: Entity
    related_entities: List[Entity] = Field(default_factory=list)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_ft: Optional[float] = None
    region: Optional[str] = None
    country_code: Optional[str] = None
    headline: Optional[str] = None
    summary: Optional[str] = None
    url: Optional[str] = None
    language: Optional[str] = "en"
    vessel_data: Optional[VesselData] = None
    flight_data: Optional[FlightData] = None
    financial_data: Optional[FinancialData] = None
    security_data: Optional[SecurityData] = None
    betting_data: Optional[BettingData] = None
    prediction_market_data: Optional[PredictionMarketData] = None
    crypto_data: Optional[CryptoData] = None

    tags: List[str] = Field(default_factory=list) 
    named_entities: List[str] = Field(default_factory=list) 
    sentiment: Optional[float] = None
    anomaly_score: float = 0.0
    correlation_ids: List[str] = Field(default_factory=list)

    @field_validator("anomaly_score")
    @classmethod
    def validate_anomaly_score(cls, v):
        if not (0.0 <= v <= 1.0):
            raise ValueError("anomaly_score must be between 0.0 and 1.0")
        return v

    def to_tuple(self) -> tuple:
        pe = self.primary_entity
        # FIXED: Enforce Pydantic V2 compatability `.model_dump_json()` over `.json()`
        return (
            self.event_id, 
            self.type.value, 
            self.occurred_at, 
            self.collected_at,
            self.source, 
            float(self.source_reliability),
            pe.id if pe else None, 
            pe.type.value if pe else EntityType.UNKNOWN.value,
            pe.name if pe else None, 
            Json(pe.flags) if pe and pe.flags else Json([]),
            self.longitude, 
            self.latitude,
            self.region, 
            self.country_code, 
            self.headline, 
            self.summary, 
            self.url,
            Json(self.vessel_data.model_dump(mode='json')) if self.vessel_data else None,
            Json(self.flight_data.model_dump(mode='json')) if self.flight_data else None,
            Json(self.financial_data.model_dump(mode='json')) if self.financial_data else None,
            Json(self.security_data.model_dump(mode='json')) if self.security_data else None,
            Json(self.betting_data.model_dump(mode='json')) if self.betting_data else None,
            Json(self.prediction_market_data.model_dump(mode='json')) if self.prediction_market_data else None,
            Json(self.crypto_data.model_dump(mode='json')) if self.crypto_data else None,
            self.tags, 
            self.named_entities, 
            float(self.sentiment) if self.sentiment is not None else None, 
            float(self.anomaly_score),
            self.correlation_ids,
            self.trace_id
        )

    def is_physical(self) -> bool:
        return self.type in [
            EventType.VESSEL_POSITION, EventType.VESSEL_DARK, EventType.VESSEL_STS,
            EventType.FLIGHT_POSITION, EventType.FLIGHT_DARK, EventType.FLIGHT_ANOMALY,
        ]

    def is_financial(self) -> bool:
        return self.type in [
            EventType.OPTIONS_FLOW, EventType.DARK_POOL, EventType.FUTURES_COT,
            EventType.PRICE_ANOMALY, EventType.INSIDER_TRADE, EventType.EQUITY_BLOCK,             
            EventType.CRYPTO_TRADE, EventType.MARKET_CANDLE, EventType.MARKET_ANOMALY,           
            EventType.CRYPTO_LIQUIDATION, EventType.PREDICTION_MARKET_TRADE, EventType.CRYPTO_TRANSFER,
        ]
    
    def to_summary(self) -> str:
        parts = [f"[{self.type.value}]", f"src: {self.source}"]
        if self.primary_entity.name: parts.append(f"entity:{self.primary_entity.name}")
        if self.region: parts.append(f"region:{self.region}")
        if self.headline: parts.append(f"headline:{self.headline[:80]}")
        if self.anomaly_score > 0.5: parts.append(f"ANOMALY:{self.anomaly_score:.2f}")
        return " | ".join(parts)

class CorrelationCluster(BaseModel):
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rule_id: str
    rule_name: str
    alert_tier: AlertTier
    detected_at: datetime = Field(default_factory=_utcnow)
    trigger_event_id: str
    supporting_event_ids: List[str] = Field(default_factory=list)
    entity_ids: List[str] = Field(default_factory=list)
    entity_names: List[str] = Field(default_factory=list)
    description: str
    tags: List[str] = Field(default_factory=list)
    scenario: Optional[Dict[str, Any]] = None

# FIXED: Removed the shadowed class definition block. Combined the DB model with Hypotheses array.
class Scenario(BaseModel):
    scenario_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)
    status: ScenarioStatus = ScenarioStatus.HYPOTHESIS
    headline: str
    significance: str
    hypotheses: List[ScenarioHypothesis]
    recommended_monitoring: List[str]
    confidence_overall: int
    confidence_rationale: str
    confidence_history: List[Dict] = Field(default_factory=list)
    supporting_event_ids: List[str] = Field(default_factory=list)