"""
tests/test_integrity_layer.py

Comprehensive test suite verifying Part 3: The Anti-Palantir Integrity Layer:
1. Universal Provenance Envelope Schema & Serialization (§3.1)
2. Provenance Mixin Inheritance (§3.1)
3. Zero-Fabrication Placeholder Disclosures (§3.1, §3.2)
4. Portfolio Risk Provenance Integration (§3.1)
5. Public Mathematical Methodology API (§3.3)
6. Data Sovereignty & Anti-Surveillance Manifest API (§3.4)
"""

import pytest
from datetime import datetime, timezone
from fastapi.testclient import TestClient

from shared.models.provenance import (
    ProvenanceSourceType,
    ProvenanceEnvelope,
    ProvenanceMixin,
)
from shared.models.ontology import PROHIBITED_DATA_CATEGORIES
from shared.utils.audit_ledger import GENESIS_HASH
from services.api_gateway.routes.main import app
from services.api_gateway.dependencies import create_jwt_token


def get_auth_cookies():
    token = create_jwt_token({"sub": "test-analyst-user", "role": "analyst"})
    return {"sentinel_session": token}


# ── 1. PROVENANCE ENVELOPE SCHEMA TESTS (§3.1) ────────────────────────────────

def test_provenance_envelope_types_and_serialization():
    # 1. Live measurement
    env_live = ProvenanceEnvelope(
        source_type=ProvenanceSourceType.LIVE_MEASUREMENT,
        methodology="Direct WebSocket feed from SEC EDGAR submissions API",
        data_inputs=["sec_edgar:cik_0001045810:8k"],
    )
    assert env_live.source_type == ProvenanceSourceType.LIVE_MEASUREMENT
    assert env_live.is_synthetic is False
    assert len(env_live.data_inputs) == 1

    # 2. Computed deterministic
    env_det = ProvenanceEnvelope(
        source_type=ProvenanceSourceType.COMPUTED_DETERMINISTIC,
        methodology="Black-Scholes analytical Greeks (Delta, Gamma, Theta, Vega)",
        model_name="deterministic_options_engine",
        data_inputs=["NVDA:quote", "options_chain:20260918"],
        validation_gate_passed=True,
    )
    assert env_det.source_type == ProvenanceSourceType.COMPUTED_DETERMINISTIC
    assert env_det.validation_gate_passed is True

    # 3. LLM inference
    env_llm = ProvenanceEnvelope(
        source_type=ProvenanceSourceType.LLM_INFERENCE,
        methodology="Zero-shot entity-sentiment synthesis and causal claim extraction",
        model_name="qwen2.5:14b",
        model_confidence=0.88,
        data_inputs=["telegram:osinttechnical:msg_9841"],
    )
    assert env_llm.source_type == ProvenanceSourceType.LLM_INFERENCE
    assert env_llm.model_confidence == 0.88

    # 4. Disclosed placeholder (zero-fabrication)
    env_ph = ProvenanceEnvelope(
        source_type=ProvenanceSourceType.DISCLOSED_PLACEHOLDER,
        methodology="Cold start: insufficient history (< 30 bars) to compute statistical Z-score",
    )
    assert env_ph.source_type == ProvenanceSourceType.DISCLOSED_PLACEHOLDER

    # Serialization dictionary
    d = env_llm.to_dict()
    assert d["source_type"] == "llm_inference"
    assert d["model_confidence"] == 0.88
    assert "computed_at" in d


# ── 2. PROVENANCE MIXIN INHERITANCE TESTS (§3.1) ──────────────────────────────

class CustomSignalResponse(ProvenanceMixin):
    signal_id: str
    ticker: str
    target_price: float


def test_provenance_mixin_embedding():
    resp = CustomSignalResponse(
        signal_id="SIG-001",
        ticker="AAPL",
        target_price=240.0,
        provenance=ProvenanceEnvelope(
            source_type=ProvenanceSourceType.COMPUTED_DETERMINISTIC,
            methodology="ATR(14) 2.0x target projection",
            model_name="quant_trading_engine",
        )
    )

    assert resp.signal_id == "SIG-001"
    assert resp.provenance.source_type == ProvenanceSourceType.COMPUTED_DETERMINISTIC
    assert resp.provenance.model_name == "quant_trading_engine"


# ── 3. PORTFOLIO RISK PROVENANCE TESTS (§3.1) ─────────────────────────────────

def test_portfolio_risk_endpoint_provenance():
    client = TestClient(app)
    cookies = get_auth_cookies()
    headers = {"X-User-Role": "ANALYST"}

    res = client.get("/api/v1/portfolio/risk", cookies=cookies, headers=headers)
    assert res.status_code == 200
    data = res.json()
    assert "provenance" in data
    assert "source_type" in data["provenance"]
    assert data["provenance"]["source_type"] in ("computed_deterministic", "disclosed_placeholder")


# ── 4. MATHEMATICAL METHODOLOGY SURFACE TESTS (§3.3) ──────────────────────────

def test_api_gateway_methodology_routes():
    client = TestClient(app)
    cookies = get_auth_cookies()

    # 1. Catalog of all methodologies
    res_cat = client.get("/api/v1/methodology", cookies=cookies)
    assert res_cat.status_code == 200
    catalog = res_cat.json()
    assert len(catalog) >= 5
    ids = [item["id"] for item in catalog]
    assert "covered_call_optimization" in ids
    assert "granger_causality" in ids
    assert "hawkes_point_process" in ids
    assert "parametric_var_cvar" in ids
    assert "half_kelly_position_sizing" in ids

    # 2. Individual signal methodology detail
    res_cc = client.get("/api/v1/methodology/covered_call_optimization", cookies=cookies)
    assert res_cc.status_code == 200
    cc_data = res_cc.json()
    assert "C(S, K, T)" in cc_data["formula_latex"]
    assert len(cc_data["parameters"]) >= 2
    assert len(cc_data["assumptions"]) >= 2
    assert "validation_gate" in cc_data
    assert "falsifiability_condition" in cc_data

    # 3. Granger causality methodology detail
    res_gc = client.get("/api/v1/methodology/granger_causality", cookies=cookies)
    assert res_gc.status_code == 200
    gc_data = res_gc.json()
    assert "Y_t" in gc_data["formula_latex"]
    assert "F =" in gc_data["formula_latex"]

    # 4. Unknown methodology 404
    res_404 = client.get("/api/v1/methodology/non_existent_signal", cookies=cookies)
    assert res_404.status_code == 404


# ── 5. DATA SOVEREIGNTY MANIFEST TESTS (§3.4) ─────────────────────────────────

def test_api_gateway_sovereignty_manifest():
    client = TestClient(app)
    cookies = get_auth_cookies()

    res = client.get("/api/v1/system/sovereignty", cookies=cookies)
    assert res.status_code == 200
    manifest = res.json()

    assert manifest["sovereignty_score_pct"] == 100.0
    assert len(manifest["local_subsystems"]) >= 5
    assert len(manifest["external_ingest_feeds"]) >= 4

    # Verify zero user data leakage claims
    for sub in manifest["local_subsystems"]:
        assert sub["user_data_exposed"] is False
    for ext in manifest["external_ingest_feeds"]:
        assert ext["user_data_exposed"] is False

    # Verify prohibited surveillance categories list matches ontology
    assert set(manifest["prohibited_categories"]) == PROHIBITED_DATA_CATEGORIES

    # Verify cryptographic guarantees
    assert manifest["cryptographic_guarantees"]["genesis_hash"] == GENESIS_HASH
    assert manifest["cryptographic_guarantees"]["tamper_evident"] is True
