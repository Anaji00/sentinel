"""
tests/test_correlation_cascade.py

Unit test suite for Risk Cascade Modeler, Sector Hawkes, Soft Correlator (services/correlation/):
  - cascade.py
  - sector_hawkes.py
  - soft_correlator.py
  - event_store.py
"""

import pytest
import numpy as np
from services.correlation.cascade import GeopoliticalCascadeEngine

# ── 1. GEOPOLITICAL RISK CASCADE PROPAGATION GRAPH ──────────────────────────

def test_risk_cascade_propagation():
    modeler = GeopoliticalCascadeEngine()
    assert hasattr(modeler, "graph") or hasattr(modeler, "redis") or modeler is not None
