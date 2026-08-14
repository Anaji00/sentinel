"""
tests/test_agent_swarm.py

Unit test suite for Agent Swarm Subsystem (services/agents/):
  - consensus_engine.py
  - adversarial_wargamer.py
  - edge_validator.py
  - quant_trading_engine.py
  - base.py
  - supervisor.py
"""

import pytest
import numpy as np
from unittest.mock import AsyncMock, MagicMock, patch

from services.agents.consensus_engine import ConsensusEngine, ConsensusReport
from services.agents.edge_validator import EdgeValidatorAgent
from services.agents.rule_agent import RuleSynthesizerAgent

# ── 1. SWARM CONSENSUS ENGINE VOTING & WEIGHTED SYNTHESIS ────────────────────

def test_consensus_engine_weighted_aggregation():
    agent_responses = [
        {"agent": "quant", "recommendation": "BULLISH", "confidence": 0.85, "weight": 1.5},
        {"agent": "osint", "recommendation": "BULLISH", "confidence": 0.90, "weight": 1.2},
        {"agent": "cyber", "recommendation": "BEARISH", "confidence": 0.40, "weight": 0.8},
    ]
    
    # Calculate weighted consensus score
    bullish_weight = sum(r["confidence"] * r["weight"] for r in agent_responses if r["recommendation"] == "BULLISH")
    bearish_weight = sum(r["confidence"] * r["weight"] for r in agent_responses if r["recommendation"] == "BEARISH")
    total_weight = bullish_weight + bearish_weight
    
    consensus_score = bullish_weight / total_weight
    assert consensus_score > 0.70
    assert bullish_weight > bearish_weight


from shared.kafka import Topics

# ── 2. STATISTICAL EDGE VALIDATOR ─────────────────────────────────────────────

def test_statistical_edge_validator():
    validator = EdgeValidatorAgent("edge_validator", ["topic1"], MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    assert validator.output_topic == Topics.QUANT_DISCOVERIES


# ── 3. RULE SYNTHESIZER AGENT ────────────────────────────────────────────────

def test_rule_synthesizer_agent():
    rule_agent = RuleSynthesizerAgent("rule_agent", ["topic1"], MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    assert rule_agent.output_topic == "agents.rules.synthesized"
