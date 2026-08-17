"""
shared/models/provenance.py

UNIVERSAL PROVENANCE & INTEGRITY ENVELOPE (§3.1)
================================================
Centralized schema-enforced provenance metadata model for all Sentinel
quantitative signals, risk metrics, macro forecasts, and reasoning outputs.

Eliminates fabricated placeholder numbers by explicitly disclosing data source
types, mathematical methodologies, model confidence scores, and synthetic status.
"""

from enum import Enum
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field


class ProvenanceSourceType(str, Enum):
    """
    Standardized epistemological classification for all platform data points.
    """
    LIVE_MEASUREMENT = "live_measurement"            # Direct raw measurement from exchange/sensor API (Finnhub, AISStream, OpenSky, EDGAR)
    COMPUTED_DETERMINISTIC = "computed_deterministic" # Exact mathematical/closed-form calculation (Black-Scholes, ATR, VaR, Kelly)
    LLM_INFERENCE = "llm_inference"                  # Extracted, classified, or arbitrated by local LLM agent with confidence score
    DISCLOSED_PLACEHOLDER = "disclosed_placeholder"  # Cold-start state / insufficient history, explicitly disclosed (never fabricated)


class ProvenanceEnvelope(BaseModel):
    """
    Immutable provenance and confidence audit envelope attached to all outputs.
    """
    source_type: ProvenanceSourceType = Field(
        default=ProvenanceSourceType.COMPUTED_DETERMINISTIC,
        description="Epistemological category of the data point."
    )
    computed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the computation was performed."
    )
    methodology: str = Field(
        default="Closed-form mathematical computation",
        description="Exact mathematical formula, model specification, or algorithmic derivation."
    )
    model_name: Optional[str] = Field(
        default=None,
        description="Identifier of the executing model or engine (e.g. 'qwen2.5:14b', 'deterministic_risk_engine')."
    )
    model_confidence: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Statistical or LLM self-assessed confidence score (0.0 to 1.0)."
    )
    data_inputs: List[str] = Field(
        default_factory=list,
        description="Identifiers and timeframe tags of underlying telemetry inputs used."
    )
    is_synthetic: bool = Field(
        default=False,
        description="Flag indicating if the value was derived from historical simulation or backtest."
    )
    validation_gate_passed: Optional[bool] = Field(
        default=None,
        description="Whether this signal passed pre-deployment backtesting and threshold calibration gates."
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_type": self.source_type.value,
            "computed_at": self.computed_at.isoformat(),
            "methodology": self.methodology,
            "model_name": self.model_name,
            "model_confidence": self.model_confidence,
            "data_inputs": self.data_inputs,
            "is_synthetic": self.is_synthetic,
            "validation_gate_passed": self.validation_gate_passed,
        }


class ProvenanceMixin(BaseModel):
    """
    Reusable Pydantic mixin embedding standard provenance into response models.
    """
    provenance: ProvenanceEnvelope = Field(
        default_factory=lambda: ProvenanceEnvelope(
            source_type=ProvenanceSourceType.COMPUTED_DETERMINISTIC,
            methodology="Standard deterministic computation"
        )
    )
