"""Which event types nothing in this platform emits, and why.

One definition, because three different things need the answer and had no way
to ask it:

  * `tests/test_event_type_producers.py` checks this table against the syntax
    tree -- every declared type is either constructed somewhere or recorded
    here, and the list may not silently grow.
  * `services/correlation/main.py` refuses at import to ship a rule whose
    clauses select on a type in it.
  * anyone reading a rule and wondering why it never fires.

The second of those is the reason this moved out of the test file. The table
was complete and correct, its docstring named three pieces of code that depend
on unproduced types -- the Telegram formatter, the materiality thresholds, the
cascade transitions -- and it did not name the rule engine, which was the
largest consumer of the gap by a wide margin. Thirteen of the fifteen shipped
rules selected on at least one of these: `price_anomaly` in eight,
`dark_pool` in six, twice as a trigger type. Every one of those clauses was
waiting for evidence this table already said would never arrive.

Removing an entry is the point: wire a producer, delete the line.
"""
from __future__ import annotations

from typing import Dict

# Declared with no producer. Each entry says why it is here, because "nothing
# emits this" is a different situation for each of them and the difference is
# what someone reading this needs.
UNPRODUCED: Dict[str, str] = {
    # VESSEL_STS and VESSEL_SPOOF were here and are not any more.
    #
    # Both entries described detection that stopped one step short: STS zones
    # scored a dark gap by proximity and nothing detected the transfer; the
    # Kalman residual turned an impossible jump into a score and nothing turned
    # it into a claim. `shared/utils/maritime_behaviour.py` closes both --
    # co-location with a dwell test, and an implied-speed bound -- and the
    # maritime enricher emits them. rule_maritime_chokepoint_evasion carried
    # one live evidence type; it now carries three.

    # Ingested and stored, but published under another type or not as events.
    #
    # Four entries were removed from this block when the detection above was
    # rewritten: HEADLINE, FLIGHT_POSITION, CRYPTO_TRANSFER and
    # CRYPTO_LIQUIDATION are all emitted, and were recorded here as unproduced
    # because a regex could not see a conditional or a `getattr`. The
    # explanations they carried read as findings and were not.
    "MARKET_CANDLE": "candles are written to tradfi_bars, not published as events",
    "PREDICTION_MARKET": "the collector emits PREDICTION_MARKET_TRADE",
    "DARK_POOL": "dark-pool prints arrive as EQUITY_BLOCK, tagged dark_pool_print",
    "PRICE_ANOMALY": "price moves arrive as MARKET_ANOMALY",
    "INFRASTRUCTURE": "superseded by INFRA_EXPOSED and INFRASTRUCTURE_DEGRADED",
    "VULNERABILITY": "CVEs arrive as INFRA_EXPOSED from the KEV path",

    # Declared for work not started. No consumer depends on these.
    "CLIMATE_STRESS": "no climate feed",
    "CUSTOM": "escape hatch for user-defined events; no producer by design",
    "FUTURES_COT": "no CFTC Commitments of Traders collector",
    "REGULATORY_EVENT": "no regulatory-action feed",
    "SPORTS_LINE_MOVEMENT": "no sportsbook collector",
    "INSIDER_CLUSTER": "clustering of INSIDER_TRADE is not implemented",
    "NARRATIVE_CLUSTER": "first-story detection scores novelty; it does not emit cluster events",
}

# Where the evidence actually arrives, for the entries that have somewhere.
#
# This is the half that makes the table actionable rather than merely honest. A
# rule asking for `dark_pool` is not asking for something impossible -- the
# platform detects dark-pool prints and stores them, under another type, one
# field away. A rule asking for `climate_stress` is asking for something that
# does not exist at all, and the difference matters when deciding whether to
# edit the rule or build the producer.
ARRIVES_AS: Dict[str, str] = {
    "DARK_POOL": "EQUITY_BLOCK",
    "PRICE_ANOMALY": "MARKET_ANOMALY",
    "PREDICTION_MARKET": "PREDICTION_MARKET_TRADE",
    "VULNERABILITY": "INFRA_EXPOSED",
}


def unproduced_values() -> frozenset:
    """The UNPRODUCED member names as their wire values (`dark_pool`, ...)."""
    from shared.models.events import EventType

    return frozenset(EventType[name].value for name in UNPRODUCED)


__all__ = ["UNPRODUCED", "ARRIVES_AS", "unproduced_values"]
