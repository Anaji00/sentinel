"""
shared/utils/rule_probation.py

Model-written correlation rules on probation.

The rule synthesizer used to install whatever a model wrote straight into the
live engine at the tier the model chose: syn_1 fired 28 top-tier alerts on
5 October 2026, and the agent was switched off. A synthesized rule now:

  - gets an id from its own definition, so two rules never share a key (the
    prompt's example rule was "syn_1" and the model copied it: 145 rules were
    written and three ids ever existed, each new one overwriting the last);
  - fires and is stored, so the grader can judge it on prices like every
    other rule, but no higher than PROBATION_TIER and marked as probation,
    until its family is graded to precede bigger moves (move_flagging_families);
  - is removed when the grader retires its family, with no model asked.

Pure: rule dicts and grades in, decisions out.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Mapping, Optional

from shared.utils import producer_grades as pg

PROBATION_TIER = "WATCH"
SYNTH_PREFIX = "syn_"


def synth_rule_id(rule: Mapping[str, Any]) -> str:
    """A stable id from what the rule matches, not from what the model called it."""
    key = {k: rule.get(k) for k in ("trigger_event_type", "conditions", "correlations")}
    return SYNTH_PREFIX + hashlib.sha1(json.dumps(key, sort_keys=True, default=str).encode()).hexdigest()[:10]


def on_probation(rule: Mapping[str, Any], flagging: set) -> bool:
    """A synthesized rule whose family has not been graded to precede bigger moves."""
    return bool(rule.get("probation")) and pg.rule_family(str(rule.get("rule_name") or "")) not in flagging


def capped_tier(declared: str, rule: Mapping[str, Any], flagging: set, order: List[str]) -> str:
    """The tier a rule may publish at: no higher than PROBATION_TIER while on
    probation. `order` lists tiers highest first, as the correlation engine does."""
    if not on_probation(rule, flagging) or declared not in order:
        return declared
    return declared if order.index(declared) >= order.index(PROBATION_TIER) else PROBATION_TIER


def graded_out(rules: Mapping[str, Mapping[str, Any]], grades: Optional[Dict[str, Any]]) -> List[str]:
    """Synthesized rules whose family the grader has retired on prices."""
    retired = pg.uninformative_rules(grades)
    return sorted(rid for rid, r in rules.items()
                  if r.get("probation") and pg.is_uninformative(str(r.get("rule_name") or ""), retired))


__all__ = ["PROBATION_TIER", "SYNTH_PREFIX", "capped_tier", "graded_out", "on_probation", "synth_rule_id"]
