"""
services/api_gateway/helpers.py

Entity normalization and auto-derivation are natively handled by Pydantic models
in `shared.models.events` (NormalizedEvent, CorrelationCluster, Scenario) and TimescaleDB columns.
"""

def get_clean_entity_label(name: str | None, entity_id: str | None) -> str:
    """Helper utility for formatting entity display labels."""
    return str(name or entity_id or "Global Entity").strip()
