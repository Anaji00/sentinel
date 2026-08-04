"""
services/agents/__init__.py
Exposes the agent swarm classes and consolidated engines.
"""

from .base import SentinelAgent, AgentBulletin, AgentPrediction, AgentScorecard
from .consensus_engine import (
    ConsensusEngine, ConsensusReport, ContradictionReport, ConsensusSignal,
    SubjectiveOpinion, ACHReport, CompetingHypothesis
)
from .macro_intelligence_engine import MacroIntelligenceEngine
from .quant_trading_engine import QuantTradingEngine
from .knowledge_graph_engine import KnowledgeGraphEngine
from .radar_agent import RadarAgent
from .rule_agent import RuleSynthesizerAgent
