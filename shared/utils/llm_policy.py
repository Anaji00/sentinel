"""Which agents run, and which of them may call the model.

One CPU-bound Ollama serves the whole swarm at roughly twenty-five completions
an hour. Measured on 5 October, seven agents and the reasoning service shared
it and each completion waited eight to sixteen minutes, so the stock-tier
agents -- the ones this deployment exists for -- queued behind a game-theory
simulation nobody read and a rule writer whose rules fired untested.

Two lists, each overridable from the environment with a comma-separated value
(an empty string means "none"):

  AGENTS_DISABLED      not launched at all; their only output was the model's.
  LLM_DISABLED_AGENTS  launched, but every model call answers "no verdict" at
                       once and the agent's deterministic path does the work.

The stock tier keeps running: stock_correlation_agent with its model, and
quant_trading_engine without its model-written advisory since 29 Oct 2026 (its
record is below).
"""
from __future__ import annotations

import os
from typing import FrozenSet

# Cut entirely.
#   adversarial_wargamer:  persona simulations published only to telemetry
#                          storage; nothing in trading, alerting or the UI reads them.
#   rule_synthesizer:      writes correlation rules the engine runs live with no
#                          backtest; its syn_1 fired 28 tier-3 alerts on 5 October.
#   knowledge_graph_engine: model-extracted entities and triples from news; the
#                          enrichment tier already maps headlines to tickers,
#                          sectors and zones deterministically.
DEFAULT_AGENTS_DISABLED = frozenset({
    "adversarial_wargamer",
    "rule_synthesizer",
    "knowledge_graph_engine",
})

# Run without the model.
#   macro_intelligence_engine: rates and vol-surface briefs are computed from
#                              measurements (shared.utils.deterministic_briefs);
#                              the hourly narrative review is dropped.
#   radar_agent:               escalation is a threshold on z-score and flow;
#                              the model was asked to judge exactly those numbers.
#   quant_trading_engine:      the model-written trading advisory (its only model
#                              call). Graded on prices it never earned: 280 calls,
#                              Brier 0.283 against a coin flip's 0.25, hit 49%,
#                              -26 bp a call net of SPY (29 Oct 2026); and on 28 Oct
#                              it lost every slot to the reasoning tier anyway. Peer
#                              discovery and insider clustering are deterministic
#                              and keep running. Re-enable through the environment
#                              if hosted inference makes a trial free.
DEFAULT_LLM_DISABLED = frozenset({
    "macro_intelligence_engine",
    "radar_agent",
    "quant_trading_engine",
})


def _names(env: str, default: FrozenSet[str]) -> FrozenSet[str]:
    raw = os.getenv(env)
    if raw is None:
        return default
    return frozenset(n.strip() for n in raw.split(",") if n.strip())


def agent_disabled(name: str) -> bool:
    return name in _names("AGENTS_DISABLED", DEFAULT_AGENTS_DISABLED)


def llm_disabled_for(name: str) -> bool:
    return name in _names("LLM_DISABLED_AGENTS", DEFAULT_LLM_DISABLED)


__all__ = [
    "DEFAULT_AGENTS_DISABLED", "DEFAULT_LLM_DISABLED", "agent_disabled", "llm_disabled_for",
]
