from __future__ import annotations
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, model_validator
import json

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
    INFRASTRUCTURE = "infrastructure"
    SPORTS_LINE_MOVEMENT = "sports_line_movement"
    PREDICTION_MARKET_TRADE = "prediction_market_trade"
    CRYPTO_LIQUIDATION = "crypto_liquidation"
    CRYPTO_PERP_FUNDING = "crypto_perp_funding"
    EARNINGS_REPORT = "earnings_report"
    EARNINGS_SURPRISE = "earnings_surprise"
    CUSTOM = "custom"
    CRYPTO_TRANSFER = "crypto_transfer"
    PREDICTION_MARKET = "prediction_market"
    VULNERABILITY = "vulnerability"
    INFRASTRUCTURE_DEGRADED = "infrastructure_degraded"

class EntityType(str, Enum):
    VESSEL = "vessel"
    AIRCRAFT = "aircraft"
    COMPANY = "company"
    PERSON = "person"
    COUNTRY = "country"
    INSTRUMENT = "instrument"
    INFRASTRUCTURE = "infrastructure"
    MEDIA_SOURCE = "media_source"
    VULNERABILITY = "vulnerability"
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
    latitude: Optional[float] = None
    longitude: Optional[float] = None
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
    implied_probability: Optional[float] = None
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
   ticker: Optional[str] = None
   total_volume: Optional[float] = None
   resolution_date: Optional[str] = None
   category: Optional[str] = None
   outcome_prices: Optional[dict] = None
   probability_delta_24h: Optional[float] = None

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
    funding_rate: Optional[float] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    basis_bps: Optional[float] = None
    open_interest: Optional[float] = None
    market_microstructure: Optional['MarketMicrostructure'] = None

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
    option_type: Optional[str] = None
    # Earnings calendar fields (Phase 4)
    earnings_report_date: Optional[str] = None
    earnings_session: Optional[str] = None
    eps_estimate: Optional[float] = None
    eps_actual: Optional[float] = None
    eps_surprise_pct: Optional[float] = None
    revenue_estimate: Optional[float] = None
    revenue_actual: Optional[float] = None
    # Reference data fields (Phase 2)
    sector: Optional[str] = None
    industry: Optional[str] = None
    index_membership: List[str] = Field(default_factory=list)
    market_cap_tier: Optional[str] = None

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

class ScoreAdjustment(BaseModel):
    """Records a single step in anomaly score derivation for provenance tracking."""
    reason: str
    delta: float

class AnomalyBreakdown(BaseModel):
    """Dimensional anomaly sub-scores — gives agents structured reasoning inputs."""
    composite_score: float = 0.0
    spatial_score: float = 0.0
    temporal_score: float = 0.0
    volume_z_score: float = 0.0
    volatility_z_score: float = 0.0
    cross_domain_correlation_score: float = 0.0
    ewma_volatility: float = 0.0
    is_significant: bool = False
    domain: str = "temporal"

class MarketMicrostructure(BaseModel):
    """Quantitative market metrics computed at enrichment time."""
    ewma_volatility: Optional[float] = None
    realized_volatility: Optional[float] = None
    parkinson_volatility: Optional[float] = None
    order_flow_imbalance: Optional[float] = None
    vwap: Optional[float] = None
    twap: Optional[float] = None
    kyle_lambda: Optional[float] = None
    amihud_illiquidity: Optional[float] = None
    realized_skewness: Optional[float] = None
    hurst_exponent: Optional[float] = None
    bid_ask_spread: Optional[float] = None

class CrossDomainSignal(BaseModel):
    """Pre-computed related signal from another domain, attached at enrichment time."""
    event_id: str
    event_type: str
    domain: str
    entity_id: str
    entity_name: Optional[str] = None
    headline: Optional[str] = None
    anomaly_score: float = 0.0
    occurred_at: Optional[datetime] = None
    region: Optional[str] = None

import hashlib

def compute_payload_hash(payload: Any) -> str:
    if isinstance(payload, dict):
        payload_str = json.dumps(payload, sort_keys=True, default=str)
    elif isinstance(payload, (str, bytes)):
        payload_str = payload if isinstance(payload, str) else payload.decode("utf-8", errors="ignore")
    else:
        payload_str = str(payload)
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

class RawIngestEnvelope(BaseModel):
    source_id: str = "unknown"
    ingest_timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    payload_hash: str = Field(default_factory=str)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _populate_envelope(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Accept source or source_id
            if "source_id" not in data and "source" in data:
                data["source_id"] = str(data["source"])
            elif "source_id" not in data:
                data["source_id"] = "unknown"

            # Accept payload or raw_payload
            if "payload" not in data and "raw_payload" in data:
                data["payload"] = data["raw_payload"]
            elif "payload" not in data:
                data["payload"] = {}

            # Handle ingest_timestamp / collected_at
            if "ingest_timestamp" not in data:
                cat = data.get("collected_at")
                if isinstance(cat, datetime):
                    data["ingest_timestamp"] = cat.isoformat()
                elif cat:
                    data["ingest_timestamp"] = str(cat)
                else:
                    data["ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
            elif isinstance(data["ingest_timestamp"], datetime):
                data["ingest_timestamp"] = data["ingest_timestamp"].isoformat()

            # Compute payload_hash if empty/missing
            if not data.get("payload_hash"):
                data["payload_hash"] = compute_payload_hash(data.get("payload", {}))

        return data

class RawEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str = "unknown"
    type: Optional[str] = "custom"
    collected_at: datetime = Field(default_factory=_utcnow)
    occurred_at: Optional[datetime] = None
    financial_data: Optional[Dict[str, Any]] = None
    raw_payload: Dict[str, Any] = Field(default_factory=dict)
    envelope: Optional[RawIngestEnvelope] = None

    @model_validator(mode="before")
    @classmethod
    def _ensure_envelope_and_sync(cls, data: Any) -> Any:
        if isinstance(data, dict):
            raw_env = data.get("envelope")
            if isinstance(raw_env, RawIngestEnvelope):
                env_dict = raw_env.model_dump()
            elif isinstance(raw_env, dict):
                env_dict = raw_env
            else:
                env_dict = None

            source_val = data.get("source") or data.get("source_id")
            if not source_val and env_dict:
                source_val = env_dict.get("source_id") or env_dict.get("source")
            if not source_val:
                source_val = "unknown"
            data["source"] = source_val

            payload_val = data.get("raw_payload") if "raw_payload" in data else data.get("payload")
            if payload_val is None and env_dict:
                payload_val = env_dict.get("payload") or env_dict.get("raw_payload")
            if payload_val is None:
                payload_val = {}
            data["raw_payload"] = payload_val

            cat_val = data.get("collected_at")
            if isinstance(cat_val, str):
                try:
                    cat_val = datetime.fromisoformat(cat_val)
                    data["collected_at"] = cat_val
                except ValueError:
                    cat_val = _utcnow()
                    data["collected_at"] = cat_val
            elif not cat_val and env_dict and env_dict.get("ingest_timestamp"):
                try:
                    cat_val = datetime.fromisoformat(env_dict["ingest_timestamp"])
                    data["collected_at"] = cat_val
                except ValueError:
                    cat_val = _utcnow()
                    data["collected_at"] = cat_val
            elif not cat_val:
                cat_val = _utcnow()
                data["collected_at"] = cat_val

            env_source = data.get("source_id") or source_val
            env_ts = data.get("ingest_timestamp")
            if isinstance(env_ts, datetime):
                env_ts = env_ts.isoformat()
            elif not env_ts and isinstance(cat_val, datetime):
                env_ts = cat_val.isoformat()
            elif not env_ts:
                env_ts = datetime.now(timezone.utc).isoformat()

            env_hash = data.get("payload_hash") or (env_dict.get("payload_hash") if env_dict else None) or compute_payload_hash(payload_val)

            data["envelope"] = RawIngestEnvelope(
                source_id=env_source,
                ingest_timestamp=env_ts,
                payload_hash=env_hash,
                payload=payload_val
            )

        return data

    @property
    def source_id(self) -> str:
        return self.envelope.source_id if self.envelope else self.source

    @property
    def ingest_timestamp(self) -> str:
        return self.envelope.ingest_timestamp if self.envelope else self.collected_at.isoformat()

    @property
    def payload_hash(self) -> str:
        return self.envelope.payload_hash if self.envelope else compute_payload_hash(self.raw_payload)


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
    anomaly_breakdown: Optional[AnomalyBreakdown] = None
    market_microstructure: Optional[MarketMicrostructure] = None
    cross_domain_signals: List[CrossDomainSignal] = Field(default_factory=list)
    correlation_ids: List[str] = Field(default_factory=list)
    score_adjustments: List['ScoreAdjustment'] = Field(default_factory=list)

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
            pe.flags if pe and pe.flags else [],
            self.longitude, 
            self.latitude,
            self.region, 
            self.country_code, 
            self.headline, 
            self.summary, 
            self.url,
            json.dumps(self.vessel_data.model_dump(mode='json')) if self.vessel_data else None,
            json.dumps(self.flight_data.model_dump(mode='json')) if self.flight_data else None,
            json.dumps(self.financial_data.model_dump(mode='json')) if self.financial_data else None,
            json.dumps(self.security_data.model_dump(mode='json')) if self.security_data else None,
            json.dumps(self.betting_data.model_dump(mode='json')) if self.betting_data else None,
            json.dumps(self.prediction_market_data.model_dump(mode='json')) if self.prediction_market_data else None,
            json.dumps(self.crypto_data.model_dump(mode='json')) if self.crypto_data else None,
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
            EventType.CRYPTO_PERP_FUNDING, EventType.EARNINGS_REPORT, EventType.EARNINGS_SURPRISE,
        ]
    
    def to_summary(self) -> str:
        parts = [f"[{self.type.value}]", f"src: {self.source}"]
        if self.primary_entity.name: parts.append(f"entity:{self.primary_entity.name}")
        if self.region: parts.append(f"region:{self.region}")
        if self.headline: parts.append(f"headline:{self.headline[:80]}")
        if self.anomaly_score > 0.5: parts.append(f"ANOMALY:{self.anomaly_score:.2f}")
        return " | ".join(parts)

    def to_readable_summary(self) -> str:
        """Formatted human-readable event summary for agent prompts, logs, and UI display."""
        parts = [f"[{self.type.value.upper()}]", f"Source: {self.source}"]
        ent_name = self.primary_entity.name or self.primary_entity.id if self.primary_entity else None
        if ent_name:
            parts.append(f"Entity: {ent_name}")
        if self.region:
            parts.append(f"Region: {self.region}")
        if self.headline:
            parts.append(f"Headline: '{self.headline}'")
        elif self.summary:
            parts.append(f"Summary: '{self.summary[:100]}'")

        # Domain specifics
        if self.financial_data and self.financial_data.ticker:
            parts.append(f"Ticker: {self.financial_data.ticker}")
            if self.financial_data.premium_usd:
                parts.append(f"Premium: ${self.financial_data.premium_usd:,.2f}")
        if self.vessel_data and self.vessel_data.mmsi:
            parts.append(f"MMSI: {self.vessel_data.mmsi}")
            if self.vessel_data.speed_knots is not None:
                parts.append(f"Speed: {self.vessel_data.speed_knots}kts")
        if self.flight_data and self.flight_data.icao24:
            parts.append(f"ICAO24: {self.flight_data.icao24}")
            if self.flight_data.callsign:
                parts.append(f"Callsign: {self.flight_data.callsign}")
        if self.security_data and self.security_data.cve_id:
            parts.append(f"CVE: {self.security_data.cve_id}")

        parts.append(f"AnomalyScore: {self.anomaly_score:.2f}")
        return " | ".join(parts)

class CorrelationCluster(BaseModel):
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rule_id: str
    rule_name: str
    alert_tier: AlertTier
    primary_domain: Optional[str] = None
    confidence_score: float = 0.85
    summary_headline: Optional[str] = None
    supporting_headlines: List[str] = Field(default_factory=list)
    metrics_summary: Dict[str, Any] = Field(default_factory=dict)
    detected_at: datetime = Field(default_factory=_utcnow)
    trigger_event_id: str
    supporting_event_ids: List[str] = Field(default_factory=list)
    primary_entity_id: Optional[str] = None
    primary_entity_name: Optional[str] = None
    entity_ids: List[str] = Field(default_factory=list)
    entity_names: List[str] = Field(default_factory=list)
    description: str
    tags: List[str] = Field(default_factory=list)
    scenario: Optional[Dict[str, Any]] = None

    @model_validator(mode="before")
    @classmethod
    def _ensure_primary_entity_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            e_ids = data.get("entity_ids") or []
            e_names = data.get("entity_names") or []
            if not data.get("primary_entity_id") and e_ids:
                data["primary_entity_id"] = str(e_ids[0])
            if not data.get("primary_entity_name") and e_names:
                data["primary_entity_name"] = str(e_names[0])
            elif not data.get("primary_entity_name") and data.get("primary_entity_id"):
                data["primary_entity_name"] = str(data["primary_entity_id"])
        return data

    def to_readable_summary(self) -> str:
        """Clean markdown-formatted cluster summary for LLM prompt injection and logs."""
        headline = self.summary_headline or self.rule_name or self.rule_id
        tier_str = self.alert_tier.value if hasattr(self.alert_tier, 'value') else str(self.alert_tier)
        domain_str = f" [{self.primary_domain.upper()}]" if self.primary_domain else ""
        primary_str = f" | Primary Entity: {self.primary_entity_name or self.primary_entity_id}" if (self.primary_entity_name or self.primary_entity_id) else ""
        entities_str = f" | Entities: {', '.join(self.entity_names or self.entity_ids)}" if (self.entity_names or self.entity_ids) else ""
        confidence_str = f" | Confidence: {self.confidence_score * 100:.0f}%" if self.confidence_score else ""
        
        summary = f"🚨 CORRELATION [{tier_str}]{domain_str}: {headline}{confidence_str}{primary_str}{entities_str}\n  Details: {self.description}"
        if self.supporting_headlines:
            summary += "\n  Supporting Evidence:\n" + "\n".join(f"    - {h}" for h in self.supporting_headlines[:5])
        return summary

# FIXED: Removed the shadowed class definition block. Combined the DB model with Hypotheses array.
class Scenario(BaseModel):
    scenario_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str
    primary_entity_id: Optional[str] = None
    primary_entity_name: Optional[str] = None
    entity_ids: List[str] = Field(default_factory=list)
    entity_names: List[str] = Field(default_factory=list)
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

    @model_validator(mode="before")
    @classmethod
    def _ensure_scenario_primary_entity(cls, data: Any) -> Any:
        if isinstance(data, dict):
            e_ids = data.get("entity_ids") or []
            e_names = data.get("entity_names") or []
            if not data.get("primary_entity_id") and e_ids:
                data["primary_entity_id"] = str(e_ids[0])
            if not data.get("primary_entity_name") and e_names:
                data["primary_entity_name"] = str(e_names[0])
            elif not data.get("primary_entity_name") and data.get("primary_entity_id"):
                data["primary_entity_name"] = str(data["primary_entity_id"])
        return data