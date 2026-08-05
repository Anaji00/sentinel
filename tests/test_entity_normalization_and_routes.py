"""
tests/test_entity_normalization_and_routes.py

Unit tests for API Gateway route helper utilities and entity field handling.
"""

from services.api_gateway.helpers import get_clean_entity_label
from shared.models import CorrelationCluster, Scenario, AlertTier


def test_get_clean_entity_label():
    assert get_clean_entity_label("Apple Inc", "AAPL") == "Apple Inc"
    assert get_clean_entity_label(None, "NVDA") == "NVDA"
    assert get_clean_entity_label(None, None) == "Global Entity"


def test_native_model_primary_entity_auto_derivation():
    cluster = CorrelationCluster(
        rule_id="RULE_01",
        rule_name="Test Rule",
        alert_tier=AlertTier.ALERT,
        trigger_event_id="evt_01",
        entity_ids=["NVDA"],
        entity_names=["NVIDIA Corp"],
        description="Test correlation",
    )
    assert cluster.primary_entity_id == "NVDA"
    assert cluster.primary_entity_name == "NVIDIA Corp"
