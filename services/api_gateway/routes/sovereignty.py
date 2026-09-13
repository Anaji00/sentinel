"""
services/api_gateway/routes/sovereignty.py

DATA SOVEREIGNTY & ANTI-SURVEILLANCE DISCLOSURE SURFACE (§3.4)
==============================================================
Exposes explicit, verifiable architectural disclosures of Sentinel's
local-first deployment posture, zero-telemetry boundary, and audit trail.
"""

import logging
from typing import Dict, List, Any
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from shared.models.ontology import PROHIBITED_DATA_CATEGORIES
from shared.utils.audit_ledger import AuditLedger, GENESIS_HASH
from shared.utils.quiet_failures import swallowed
from services.api_gateway.dependencies import get_db_optional, get_redis_optional

logger = logging.getLogger("api-gateway.sovereignty")

# The chain was actually walked and every link recomputed. Any other verdict --
# an empty ledger, no durable store, a broken link, an error -- is not
# tamper-evidence, and this endpoint used to report all of them as `true`.
VERIFIED_STATUS = "VERIFIED_VALID"

router = APIRouter(prefix="/api/v1/system/sovereignty", tags=["Data Sovereignty & Privacy Posture"])


class ServiceBoundaryItem(BaseModel):
    service_name: str
    location: str  # "Local / On-Premise" vs "Outbound Ingestion Only"
    data_direction: str  # "Internal Only", "Outbound Read-Only", "No Outbound Data"
    description: str
    user_data_exposed: bool = False


class SovereigntyManifest(BaseModel):
    architecture_model: str
    sovereignty_score_pct: float
    prohibited_categories: List[str]
    local_subsystems: List[ServiceBoundaryItem]
    external_ingest_feeds: List[ServiceBoundaryItem]
    cryptographic_guarantees: Dict[str, Any]
    statement_of_intent: str


@router.get("", response_model=SovereigntyManifest)
async def get_data_sovereignty_manifest(
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """
    Returns the platform's data sovereignty manifest: the declared service
    boundary, plus the audit ledger's verdict on its own chain.

    It used to say it was "proving zero user data leakage". A list of
    hand-written strings proves nothing, and two of the numbers it returned
    were literals: `sovereignty_score_pct` was the constant 100.0, and
    `tamper_evident` was the constant `True` -- on a platform that ships a
    `verify_chain()` able to answer that question and was never asked. Live,
    this endpoint reported `tamper_evident: true` while the ledger held zero
    rows.
    """
    local_subsystems = [
        ServiceBoundaryItem(
            service_name="Local LLM Reasoning Swarm (Ollama)",
            location="Local / On-Premise (Local GPU/CPU)",
            data_direction="Internal Only",
            description="Agent reasoning, hypothesis generation, and signal scoring execute strictly on the local host. Zero prompts or queries are sent to OpenAI, Anthropic, or external cloud LLM APIs.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="TimescaleDB / PostgreSQL",
            location="Local / Self-Hosted",
            data_direction="Internal Only",
            description="Continuous aggregates, trade orders, and historical bars are stored in an encrypted local relational hypertable. Zero telemetry is synchronized to cloud vendors.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="Neo4j Knowledge Graph",
            location="Local / Self-Hosted",
            data_direction="Internal Only",
            description="Supply chain topologies, ownership networks, and statistical causality edges reside on local disk.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="Qdrant Vector Database",
            location="Local / Self-Hosted",
            data_direction="Internal Only",
            description="384-dimensional dense semantic embeddings generated locally via SentenceTransformers.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="Kafka Event Streaming & Redis Bus",
            location="Local / Self-Hosted",
            data_direction="Internal Only",
            description="High-frequency pub/sub, rate-limiting token buckets, and state caches operate in-memory locally.",
            user_data_exposed=False,
        ),
    ]

    external_ingest = [
        ServiceBoundaryItem(
            service_name="SEC EDGAR Submissions API",
            location="Public Government Registry",
            data_direction="Outbound Read-Only",
            description="Fetches public Form 8-K, 13F-HR, and 10-K disclosures using anonymous user-agent headers. No user portfolio data transmitted.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="OpenSky Network (ADS-B)",
            location="Academic Sensor Network",
            data_direction="Outbound Read-Only",
            description="Ingests public flight state vectors. No user location or identity transmitted.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="AISStream (Maritime VHF)",
            location="Public Maritime Relay",
            data_direction="Outbound Read-Only",
            description="Ingests public commercial vessel transponder broadcasts.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="Federal Register API",
            location="Public Government API",
            data_direction="Outbound Read-Only",
            description="Monitors public notices on export controls, tariffs, and antitrust actions.",
            user_data_exposed=False,
        ),
        ServiceBoundaryItem(
            service_name="Market Data Provider (Finnhub / Alpaca)",
            location="Financial Exchange Gateway",
            data_direction="Outbound Read-Only (Market Quotes) / TLS Order Gateway",
            description="Streams market quotes. Trade orders are transmitted with TLS 1.3 encryption directly to the broker adapter.",
            user_data_exposed=False,
        ),
    ]

    # Asked, not asserted.
    verdict: Dict[str, Any] = {"status": "NOT_EVALUATED"}
    try:
        verdict = await AuditLedger(redis_client=redis, db_client=db).verify_chain()
    except Exception as _exc:
        swallowed("api_gateway.routes.sovereignty.verify_chain", _exc, logger)
        verdict = {"status": "VERIFICATION_ERROR"}

    tamper_evident = verdict.get("status") == VERIFIED_STATUS

    # The share of the declared boundary that exposes no user data. Still 100
    # today, and now for a reason: adding a boundary item with
    # `user_data_exposed=True` moves it, which a constant could not do. A score
    # that cannot fall is not a score.
    boundary = local_subsystems + external_ingest
    private = sum(1 for item in boundary if not item.user_data_exposed)
    score = round((private / len(boundary)) * 100.0, 2) if boundary else 0.0

    return SovereigntyManifest(
        architecture_model="Self-Hosted Air-Gappable Local-First Intelligence Platform",
        sovereignty_score_pct=score,
        prohibited_categories=sorted(list(PROHIBITED_DATA_CATEGORIES)),
        local_subsystems=local_subsystems,
        external_ingest_feeds=external_ingest,
        cryptographic_guarantees={
            "audit_trail": "SHA-256 Hash-Chained Merkle-like Ledger",
            "genesis_hash": GENESIS_HASH,
            # The ledger's own verdict, and what it was reached from. An empty
            # ledger and an unreachable store are not tamper-evidence, and the
            # status says which of them applies.
            "tamper_evident": tamper_evident,
            "audit_chain_status": verdict.get("status"),
            "audit_entries_verified": verdict.get("entries_checked", 0),
            "session_security": "HMAC-SHA256 Signed Sessions & Constant-Time Verification",
            "analytics_telemetry_trackers": "0 (Zero Google Analytics, Zero Mixpanel, Zero Segment)",
        },
        statement_of_intent=(
            "Sentinel is engineered as an anti-surveillance intelligence capability for citizens and independent analysts. "
            "It never monitors private citizens, never collects mobile carrier locations, never retains facial recognition data, "
            "and never transmits user prompts or portfolios to third-party AI APIs."
        ),
    )
