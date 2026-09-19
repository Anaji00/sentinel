"""The correlation rule DSL, defined once, for everything that reads or writes it.

A rule is written in three places and read in one, and until this module existed
each of them carried its own copy of what a clause may contain:

  services/agents/rule_agent.py   the Pydantic model a synthesised rule is
                                  validated against before storage
  services/correlation/main.py    the evaluator, which looks keys up on a dict
  tests/                          a hand-written set asserting the two agree

Three copies of one contract, and they had already drifted the furthest they
can: the validator declared five of the eleven keys the evaluator reads, and
Pydantic drops what it is not told about. The synthesiser's prompt asks the
model to set `same_entity` by name, the model supplies it, and it was deleted
between the two -- so every synthetic rule the platform has ever written
reached the evaluator with no join at all, asserting that a shared 48-hour
window was the relationship.

Nothing here is a schema. It is the vocabulary and the two thresholds that
decide what a clause *means*, so the writer cannot offer a key the reader
ignores and the reader cannot require one the writer has no way to send.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

# ── What a clause may contain ───────────────────────────────────────────────

# What to look for, and how far back.
SELECTION_KEYS: tuple = (
    "event_types", "hours", "min_anomaly", "tags", "min_abs_move_pct",
)

# `min_anomaly` answers "was this unusual for this instrument".
# `min_abs_move_pct` answers "did the price actually move", and the two are not
# the same question: a thin name up 0.4% on ten times its normal volume scores
# higher on the first than a mega-cap down 9% does. Every rule about a
# repricing had to be written in terms of unusualness and hope the two agreed.
#
# Absolute, because a rule about a shock is about magnitude; direction is
# expressed by the event types and tags a clause already selects on.

# What connects the evidence to the trigger. A clause declaring none of these
# is asserting that falling in the same window is itself the relationship.
#
#   same_entity   one subject: a company, a ticker, a vessel, a wallet
#   region        one place -- a literal name, or `True` for "wherever the
#                 trigger is", which is how a chokepoint rule is written
#   shared_tags   the two sides name the same subject without sharing an
#                 identifier: a headline about a company and that company's
#                 stock, a CPI print and what repriced on it
#   proximity_km  within a distance, for events carrying coordinates
JOIN_KEYS: tuple = ("same_entity", "region", "shared_tags", "proximity_km")

# Which side of the trigger the evidence falls on, and how far from it.
ORDER_KEYS: tuple = ("precedes_trigger", "follows_trigger")
WINDOW_KEY: str = "within_minutes"

# Every key the evaluator looks up on a clause. A model declaring fewer drops
# the rest in silence; a model declaring more offers an operator a knob that
# changes nothing.
CLAUSE_KEYS: frozenset = frozenset(
    SELECTION_KEYS + JOIN_KEYS + ORDER_KEYS + (WINDOW_KEY,)
)


# ── The two thresholds that decide what a clause means ──────────────────────

# The longest window a purely temporal join may claim, in minutes.
#
# Contagion is the case that has no subject join and should not need one: an
# $840m liquidation cascade in BTC and COIN falling 9% forty minutes later is a
# finding, and requiring the two to share a name makes the claim unstateable --
# which is what happened to "Crypto Liquidation & Equity Spillover", whose
# equity leg the fallback join dropped on every match.
#
# What makes it a relationship rather than a coincidence is that the window is
# tight and directed. Six hours is inside one session; the 48-hour windows the
# fallback exists to reject are eight times that, and a clause declaring
# `within_minutes: 4320` has not constrained anything.
TEMPORAL_JOIN_MAX_MINUTES: int = 360

# Distinct terms a single-clause OR must match before it is a convergence.
#
# A clause listing several event types is an OR, and a rule firing on one term
# of it is not the convergence its name promises. Two, because that is the
# smallest number that makes the word true. Distinct *subjects* count the same
# way distinct types do -- three aircraft squawking anomalies in one corridor
# is a convergence of evidence, not of kinds.
CONVERGENCE_TYPE_TARGET: int = 2


# ── Seed rules ──────────────────────────────────────────────────────────────

# The key that marks a rule as part of the build rather than something the
# synthesiser wrote.
#
# The two live in one Redis hash, and until this existed nothing could tell
# them apart. Both the cheap ceiling and the LLM prune pass read that hash
# whole: the ceiling counted seventeen shipped rules against a budget named
# MAX_ACTIVE_SYNTHETIC_RULES, and the prune pass handed every one of them to a
# model with "identify any rules that are obsolete" and deleted whatever came
# back. A single prune could remove the only rule that lets the platform see a
# tanker go dark, and nothing would restore it until the correlation service
# next restarted.
SEED_RULE_KEY: str = "seed"


def is_seed_rule(rule: Any) -> bool:
    """Whether this rule ships with the build.

    A seed rule is the platform's floor: what it must be able to see on a cold
    start, before the synthesiser has observed anything. It is versioned in
    code, reconciled at startup, and not the agent's to retire.
    """
    return bool(_get(rule, SEED_RULE_KEY))


def _get(clause: Any, key: str) -> Any:
    """One clause accessor for both shapes it arrives in.

    The evaluator holds dicts loaded from Redis; the synthesiser holds Pydantic
    models on their way there. Everything below works on either, so a rule
    cannot be judged differently depending on which side of storage it is on.
    """
    if isinstance(clause, Mapping):
        return clause.get(key)
    return getattr(clause, key, None)


def declares_a_subject_join(clause: Any) -> bool:
    """Whether the clause names something the two sides share."""
    return any(_get(clause, k) for k in JOIN_KEYS)


def declares_a_temporal_join(clause: Any) -> bool:
    """Whether the clause's ordering is tight enough to be the relationship.

    Both halves are required. A direction with no bound says "afterwards, at
    some point in the next two days"; a bound with no direction says "nearby,
    either side". Together, and inside the session, they are a claim that one
    event followed another.
    """
    if not any(_get(clause, k) for k in ORDER_KEYS):
        return False
    try:
        return 0 < float(_get(clause, WINDOW_KEY)) <= TEMPORAL_JOIN_MAX_MINUTES
    except (TypeError, ValueError):
        return False


def declares_a_join(clause: Any) -> bool:
    """Whether the clause says what connects its evidence to the trigger."""
    return declares_a_subject_join(clause) or declares_a_temporal_join(clause)


def clause_event_types(clause: Any) -> Sequence[str]:
    types = _get(clause, "event_types") or []
    if isinstance(types, str):
        return [types]
    return [t for t in types if isinstance(t, str)]


def trigger_event_types(rule: Any) -> Sequence[str]:
    """A rule's trigger types, however it spelled them.

    Shipped rules use a list and the synthesiser's model once allowed only a
    bare string, so both shapes are in the stored rule set.
    """
    trigger = _get(rule, "trigger_event_type")
    if isinstance(trigger, str):
        return [trigger]
    if isinstance(trigger, (list, tuple)):
        return [t for t in trigger if isinstance(t, str)]
    return []


__all__ = [
    "CLAUSE_KEYS",
    "SEED_RULE_KEY",
    "CONVERGENCE_TYPE_TARGET",
    "JOIN_KEYS",
    "ORDER_KEYS",
    "SELECTION_KEYS",
    "TEMPORAL_JOIN_MAX_MINUTES",
    "WINDOW_KEY",
    "clause_event_types",
    "declares_a_join",
    "declares_a_subject_join",
    "declares_a_temporal_join",
    "is_seed_rule",
    "trigger_event_types",
]
