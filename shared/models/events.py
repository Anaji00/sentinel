
from __future__ import annotations
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator

"""
shared/models/events.py — The lingua franca of SENTINEL.
 
Every service produces or consumes one of two models:
  RawEvent:        Collectors produce this. Source-native, untouched.
  NormalizedEvent: Everything downstream consumes this. Structured, enriched.
 
All datetime fields now use timezone.utc explicitly.

  DB SCHEMA MAPPING:
  This file strictly adheres to the schema defined in 'sentinel_schema.sql'.
  - NormalizedEvent  -> 'events' table (Hypertable)
  - CorrelationCluster -> 'correlations' table
  - Scenario         -> 'scenarios' table

  datetime.utcnow() returns a naive datetime — mixing naive and aware datetimes
  raises TypeError in comparisons (gap detector, correlation queries).
  All defaults now use datetime.now(timezone.utc) for tz-aware datetimes.
"""

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

class EventType(str, Enum):
    # Maritime
    VESSEL_POSITION = "vessel_position"
    VESSEL_DARK = "vessel_dark"
    VESSEL_STS = "vessel_sts"
    VESSEL_SPOOF = "vessel_spoof"
    VESSEL_STATIC = "vessel_static"
    # Aviation
    FLIGHT_POSITION = "flight_position"
    FLIGHT_DARK = "flight_dark"
    FLIGHT_ANOMALY = "flight_anomaly"
    # FInancial
    OPTIONS_FLOW = "options_flow"
    DARK_POOL = "dark_pool"
    FUTURES_COT = "futures_cot"
    PRICE_ANOMALY = "price_anomaly"
    INSIDER_TRADE = "insider_trade"
    # Information
    HEADLINE = "headline"
    SOCIAL_SIGNAL = "social_signal"
    NARRATIVE_CLUSTER = "narrative_cluster"
    # cyber
    BREACH_DETECTED = "breach_detected"
    INFRA_EXPOSED = "infra_exposed"
    BGP_ANOMALY = "bgp_anomaly"
    RANSOMWARE = "ransomware"
    # POlitical/economic
    CLIMATE_STRESS = "climate_stress"
    INFASTRUCTURE = "infrastructure"
    SPORTS_LINE_MOVEMENT = "sports_line_movement"
    PREDICTION_MARKET_TRADE = "prediction_market_trade"
    CRYPTO_LIQUIDATION = "crypto_liquidation"
    CRYPTO_PERP_FUNDING = "crypto_perp_funding"
    CUSTOM = "custom"

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

class AlertTier(int, Enum):
    WATCH = 1
    ALERT = 2
    INTELLIGENCE = 3

from enum import Enum
from pydantic import BaseModel
from typing import List

class ScenarioStatus(Enum):
    HYPOTHESIS = "HYPOTHESIS"
    CONFIRMED = "CONFIRMED"
    DENIED = "DENIED"

class ScenarioHypothesis(BaseModel):
    label: str
    probability: int
    mechanism: str
    beneficiaries: List[str]
    watch_signals: List[str]
    deny_signals: List[str]
    time_horizon: str

class Scenario(BaseModel):
    correlation_id: str
    status: ScenarioStatus
    headline: str
    significance: str
    hypotheses: List[ScenarioHypothesis]
    recommended_monitoring: List[str]
    confidence_overall: int
    confidence_rationale: str

    
class Entity(BaseModel):
    # Mapped to 'events' table columns via flattening:
    # primary_entity_id, primary_entity_type, primary_entity_name, primary_entity_flags
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
    # Stored in 'events.vessel_data' as JSONB (Polymorphism)
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
    # Stored in 'events.flight_data' as JSONB
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
    # draftkins/fandue;/pinnnacle
    mactchup: str
    market_type: str
    selection: str
    implied_probablity: float
    american_odds: int
    sharp_book_deviation: Optional[float] = None

class PredictionMarketData(BaseModel):
   market_id: str
   question: str
   outcome: str
   shares_traded: float
   price_usd: float
   liquidity_pool_size: Optional[float] = None

class CryptoData(BaseModel):
    pair: str
    trade_type: str
    side: str
    price: float
    size_tokens: float
    leverage: Optional[float] = None

class FinancialData(BaseModel):
    # Stored in 'events.financial_data' as JSONB
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
    exchange: Optional[str] = None
    volume_oi_ratio: Optional[float] = None
    otm_percentage: Optional[float] = None

class SecurityData(BaseModel):
    # Stored in 'events.security_data' as JSONB
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

# INTERMEDIATE MODEL: Not directly persisted in this form.
# Used by collectors before normalization.
class RawEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str
    collected_at: datetime = Field(default_factory=_utcnow)
    occurred_at: Optional[datetime] = None
    raw_payload: Dict[str, Any]

# DB TABLE: events (Hypertable)
class NormalizedEvent(BaseModel):
    # UUID Primary Key (distributed generation)
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4())) 
    type: EventType
    # Partition Key for TimescaleDB Hypertable
    occurred_at: datetime 
    collected_at: datetime = Field(default_factory=_utcnow)
    source: str
    source_reliability: float = 1.0
    # "Soft Foreign Key": Maps to primary_entity_id/type/name columns.
    # We do not use hard DB constraints here to allow high-speed ingestion of unknown entities.
    primary_entity: Entity
    related_entities: List[Entity] = Field(default_factory=list)
    # Maps to GEOGRAPHY(POINT, 4326) column 'coordinates' via PostGIS
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_ft: Optional[float] = None
    region: Optional[str] = None
    country_code: Optional[str] = None
    headline: Optional[str] = None
    summary: Optional[str] = None
    url: Optional[str] = None
    language: Optional[str] = "en"
    # POLYMORPHIC JSONB COLUMNS:
    # Allows storing domain-specific data without strict schema migrations.
    vessel_data: Optional[VesselData] = None
    flight_data: Optional[FlightData] = None
    financial_data: Optional[FinancialData] = None
    security_data: Optional[SecurityData] = None
    betting_data: Optional[BettingData]
    prediction_market_data: Optional[PredictionMarketData]
    crypto_data: Optional[CryptoData]

    tags: List[str] = Field(default_factory=list) # Maps to TEXT[] via GIN Index
    named_entities: List[str] = Field(default_factory=list) # Maps to TEXT[]
    sentiment: Optional[float] = None
    anomaly_score: float = 0.0
    # Links to 'correlations' table, but stored here to allow reverse lookups.
    correlation_ids: List[str] = Field(default_factory=list)

    @validator("anomaly_score")
    def clamp_anomaly_score(cls, v):
        return max(0.0, min(1.0, v))
    def has_locatuion(self) -> bool:
        return self.latitude is not None and self.longitude is not None
    
    def is_physical(self) -> bool:
        return self.type in [
            EventType.VESSEL_POSITION,
            EventType.VESSEL_DARK,
            EventType.VESSEL_STS,
            EventType.FLIGHT_POSITION,
            EventType.FLIGHT_DARK,
            EventType.FLIGHT_ANOMALY,
        
        ]
    def is_financial(self) -> bool:
        return self.type in [
            EventType.OPTIONS_FLOW,
            EventType.DARK_POOL,
            EventType.FUTURES_COT,
            EventType.PRICE_ANOMALY,
            EventType.INSIDER_TRADE,
        ]
    
    def to_summary(self) -> str:
        parts = [f"[{self.type.value}]", f"src: {self.source}"]
        if self.primary_entity.name:
            parts.append(f"entity:{self.primary_entity.name}")
        if self.region:
            parts.append(f"region:{self.region}")
        if self.headline:
            parts.append(f"headline:{self.headline[:80]}")
        if self.anomaly_score > 0.5:
            parts.append(f"ANOMALY:{self.anomaly_score:.2f}")
        return " | ".join(parts)

# DB TABLE: correlations
# The "Alert" layer grouping multiple events.
class CorrelationCluster(BaseModel):
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rule_id: str
    rule_name: str
    alert_tier: AlertTier
    detected_at: datetime = Field(default_factory=_utcnow)
    # Logical Link to events.event_id.
    # Note: DB does not enforce FK constraint to Hypertable 'events' for performance.
    trigger_event_id: str
    supporting_event_ids: List[str] = Field(default_factory=list)
    entity_ids: List[str] = Field(default_factory=list)
    description: str
    tags: List[str] = Field(default_factory=list)
    scenario: Optional[Dict[str, Any]] = None

# DB TABLE: scenarios
# The "Investigation" layer (Human-in-the-loop).
class Scenario(BaseModel):
    scenario_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    # HARD Foreign Key in DB: REFERENCES correlations(correlation_id)
    # Integrity is enforced here as volume is low and consistency is critical.
    correlation_id: str
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)
    status: ScenarioStatus = ScenarioStatus.HYPOTHESIS
    headline: str
    significance: str
    hypotheses: List[Dict[str, Any]]
    recommended_monitoring: List[str]
    confidence_overall: int
    confidence_rationale: str
    confidence_history: List[Dict] = Field(default_factory=list)
    supporting_event_ids: List[str] = Field(default_factory=list)