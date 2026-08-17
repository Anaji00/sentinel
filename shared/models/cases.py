"""
shared/models/cases.py

Data models for Security & Market Investigation Case Management.
Enables multi-analyst collaboration, evidence linking, and incident response.
"""

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class CaseStatus(str, Enum):
    OPEN = "OPEN"
    IN_INVESTIGATION = "IN_INVESTIGATION"
    ESCALATED = "ESCALATED"
    RESOLVED = "RESOLVED"
    DISMISSED = "DISMISSED"


class CasePriority(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class CaseNote(BaseModel):
    note_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    author: str
    content: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class InvestigationCase(BaseModel):
    case_id: str = Field(default_factory=lambda: f"CASE-{uuid.uuid4().hex[:8].upper()}")
    title: str
    description: str
    status: CaseStatus = CaseStatus.OPEN
    priority: CasePriority = CasePriority.MEDIUM
    lead_analyst: str = "unassigned"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    linked_event_ids: List[str] = Field(default_factory=list)
    linked_correlation_ids: List[str] = Field(default_factory=list)
    linked_entities: List[str] = Field(default_factory=list)
    linked_filings: List[str] = Field(default_factory=list, description="SEC Form 8-K, 13F-HR accession numbers")
    linked_regulatory_docs: List[str] = Field(default_factory=list, description="Federal Register document numbers")
    linked_social_signals: List[str] = Field(default_factory=list, description="Telegram / Reddit / X post identifiers")
    linked_freight_metrics: List[str] = Field(default_factory=list, description="Baltic Dry Index or container freight index symbols")
    notes: List[CaseNote] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
