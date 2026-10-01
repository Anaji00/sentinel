"""
shared/utils/candidate_buffer.py

The best candidate seen lately, held for the next free slot.

Measured on the running deployment, 2026-09-20, over ten minutes on the fast
tier:

    messages processed                    ~1,970
    survive the agents' own cheap gates      ~700   candidates
    reach the admission bar                    44   6% of candidates
      held back by the percentile              37
      claimed a slot                            7
    generate calls at the model server          5

The percentile bar is sound and it is applied to six per cent of the
population. The other ninety-four per cent never reach it: `is_available()`
yields to whichever agent asked earliest, five agents queue for one slot, and a
candidate that arrives while somebody else is at the head of the queue is
dropped on the floor. So the platform reasons about whatever happened to arrive
in the instant a slot opened, and the most anomalous event of the hour is
unlikely to be among the forty-four the bar ever evaluates.

That is the difference this module exists to remove. It does not add capacity
and it does not change how many inferences run. It changes *which* ones: an
agent that cannot run now keeps its best candidate instead of discarding it,
and offers that one when its turn comes.

Three properties worth stating, because each is a decision:

  * Bounded and score-ordered. The buffer keeps the highest-scoring candidates
    and evicts the lowest, so a burst of noise cannot push out a real finding
    and memory cannot grow with the backlog.

  * Eviction is counted, not silent. A candidate that loses its place was a
    real observation the platform chose not to reason about, and the count is
    what distinguishes a healthy filter from a feed being thrown away.

  * It holds messages, not work. A drained candidate is re-dispatched through
    the agent's normal path, so staleness, type filters and dedup all re-apply.
    Nothing is bypassed to get it in front of the model -- if it has aged out
    in the meantime, it is dropped exactly as a fresh arrival would be.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

# How many candidates one agent holds.
#
# Small deliberately. This is a "best of the last few seconds" window, not a
# queue: a slot opens every couple of minutes, so anything below the top of the
# buffer will never be reached before it goes stale, and holding it only costs
# memory and makes the eviction count harder to read.
DEFAULT_MAX_ITEMS = 32

# How long a held candidate stays worth running.
#
# Shorter than the agent tier's MAX_EVENT_AGE_SEC, because that bound answers
# "is this event still current" and this one answers "is this still the best
# thing to spend the next slot on". A candidate older than this is not refused
# on merit; it is simply no longer the answer to the question being asked.
DEFAULT_MAX_AGE_SEC = 900.0

# The key marking a message that has already waited its turn.
#
# A drained candidate must not be offered back into the buffer -- that is a
# ping-pong that would let one message occupy the head position indefinitely.
# Deliberately underscore-prefixed and stripped before any payload is built.
DEFERRED_KEY = "_sentinel_deferred"

# The domain a candidate belongs to, for fair retention.
#
# Ranking on raw anomaly alone hands the slot to whichever collector is
# busiest. Measured over six hours: 887 of the 1,177 events clearing the
# admission bar were `aisstream` -- 75% -- not because AIS over-scores but
# because it emits 51,110 events in that window. AIS clears the bar on 1.73%
# of its events; okx_swap clears it on 26.8%, telegram on 25%, reddit on 20%.
#
# Worse, two detectors cannot compete at all: `macro_freight` scored a maximum
# of 0.583 over six hours and `aviation_gap_detector` 0.635, both permanently
# below anything AIS routinely produces. Freight rates and aviation gaps are
# real signal and a single global ranking means the swarm never sees one.
#
# So the buffer retains and drains per domain. Within a domain the best
# candidate still wins on score; across domains, no domain can be crowded out
# by another's volume.
_DOMAIN_KEYS = ("primary_domain", "domain", "asset_class")

# The payload block an enriched event carries says which domain it is in.
#
# `primary_domain` is declared on the event model and set by nothing: sampled
# live off enriched.events, it was None on every message, so reading it alone
# fell through to `source` every time and split one crypto domain into
# arbitrum_rpc, base_rpc and ethereum_rpc competing as three. These keys are
# structural -- the enricher that fills vessel_data is the maritime one -- so
# they identify the domain without a list of collector names to drift.
_DOMAIN_BY_PAYLOAD = (
    ("vessel_data", "maritime"),
    ("flight_data", "aviation"),
    ("crypto_data", "crypto"),
    ("financial_data", "financial"),
    ("security_data", "cyber"),
)

UNKNOWN_DOMAIN = "unknown"

# How often a domain may take a turn, relative to the others.
#
# Round-robin alone says every domain is equally worth an inference slot, and
# on this platform that is not the intent: it is an equities platform, and
# crypto was the majority of everything collected while producing no finding
# this audit recorded. A weight of 3 means the domain waits roughly three
# turns where an unweighted one waits one.
#
# Deliberately a small explicit table rather than a score adjustment. A domain
# that is deprioritised should be visibly deprioritised, not quietly given a
# worse number somewhere in the scoring path -- which is how a default came to
# stand in for a measurement three times over in this audit.
DOMAIN_TURN_COST = {
    "crypto": 3,
}
DEFAULT_TURN_COST = 1


def domain_of(message: Dict[str, Any]) -> str:
    """Which domain a candidate belongs to, from whatever it carries."""
    if not isinstance(message, dict):
        return UNKNOWN_DOMAIN
    for key in _DOMAIN_KEYS:
        value = message.get(key)
        if value:
            return str(value).lower()
    for key, domain in _DOMAIN_BY_PAYLOAD:
        if message.get(key):
            return domain
    source = message.get("source")
    if source:
        # Last resort. A source is a finer grain than a domain, which is the
        # safe direction: it can only split a domain, never merge two.
        return str(source).lower()
    return UNKNOWN_DOMAIN


class CandidateBuffer:
    """Score-ordered, bounded, and honest about what it drops."""

    def __init__(
        self,
        name: str,
        max_items: int = DEFAULT_MAX_ITEMS,
        max_age_sec: float = DEFAULT_MAX_AGE_SEC,
    ):
        self.name = name
        self.max_items = max(1, int(max_items))
        self.max_age_sec = float(max_age_sec)
        # (score, seq, offered_at, message). `seq` breaks ties so two equal
        # scores resolve by arrival rather than by comparing dicts, which
        # raises.
        self._items: List[Tuple[float, int, float, Dict[str, Any]]] = []
        self._seq = 0
        # Weighted fair queuing across domains. Each domain carries a virtual
        # time that advances by its turn cost when it is served, and the domain
        # with the smallest virtual time goes next -- so a quiet domain is not
        # starved by a loud one holding higher scores, and a deprioritised one
        # comes round proportionally less often without ever being locked out.
        #
        # An offset on a last-served tick was tried first and does nothing: with
        # N domains a turn already comes round every N ticks, so adding a
        # constant is swamped. Measured with three domains and a cost of 3, the
        # weighted domain still took exactly one third of the turns.
        self._virtual: Dict[str, float] = {}
        self.offered = 0
        self.evicted = 0
        self.drained = 0
        self.expired = 0

    def __len__(self) -> int:
        return len(self._items)

    def offer(self, message: Dict[str, Any], score: Optional[float]) -> bool:
        """Hold this candidate if it is better than what is already held.

        Returns True when it was kept. A message already marked as deferred is
        refused: it has had its turn.
        """
        if not isinstance(message, dict) or message.get(DEFERRED_KEY):
            return False
        try:
            value = float(score) if score is not None else 0.0
        except (TypeError, ValueError):
            value = 0.0

        self._seq += 1
        self.offered += 1
        self._items.append((value, self._seq, time.time(), message))
        # Highest score first; among equals, the one that arrived first.
        self._items.sort(key=lambda item: (-item[0], item[1]))
        if len(self._items) > self.max_items:
            # The lowest-scoring candidate loses its place -- but only within
            # the domain that is currently taking up the most room. A global
            # "evict the lowest" hands the whole buffer to the noisiest
            # collector, because its worst candidate still outscores another
            # domain's best. Counted either way, because a discarded
            # observation is a decision and not an absence.
            evicted = self._evict_one()
            self.evicted += 1
            return evicted[3] is not message
        return True

    def _evict_one(self):
        """Drop the weakest candidate from the domain crowding the buffer."""
        counts: Dict[str, int] = {}
        for item in self._items:
            counts[domain_of(item[3])] = counts.get(domain_of(item[3]), 0) + 1
        # The most-represented domain, ties broken by the lower score so the
        # choice is deterministic.
        crowded = max(counts, key=lambda d: (counts[d], -min(
            i[0] for i in self._items if domain_of(i[3]) == d
        )))
        for index in range(len(self._items) - 1, -1, -1):
            if domain_of(self._items[index][3]) == crowded:
                return self._items.pop(index)
        return self._items.pop()

    def take_best(self) -> Optional[Dict[str, Any]]:
        """The best candidate from the domain that has waited longest.

        Within a domain the highest score wins, which is what ranking is for.
        Across domains the least-recently-drained goes first, so a detector
        whose ceiling is below another's floor can still reach the model --
        `macro_freight` never scored above 0.583 in six hours and would never
        have been drained under a single global ranking.

        Expired entries are discarded on the way past and counted separately
        from evictions: one is the buffer being full, the other is the world
        having moved on, and they call for different answers.
        """
        cutoff = time.time() - self.max_age_sec
        while self._items:
            # Drop anything that aged out before choosing, so staleness is
            # never what makes a domain look like it is waiting its turn.
            fresh = [i for i in self._items if i[2] >= cutoff]
            self.expired += len(self._items) - len(fresh)
            self._items = fresh
            if not self._items:
                return None

            domains = {domain_of(i[3]) for i in self._items}
            # A domain seen for the first time starts at the current front of
            # the queue, so it is served soon without being handed every turn
            # it missed while it was absent.
            floor = min(self._virtual.values()) if self._virtual else 0.0
            for d in domains:
                self._virtual.setdefault(d, floor)
            oldest = min(domains, key=lambda d: (self._virtual[d], d))
            for index, item in enumerate(self._items):
                if domain_of(item[3]) != oldest:
                    continue
                self._items.pop(index)
                # BTC and ETH still reach the model, just not ahead of equities.
                self._virtual[oldest] += float(
                    DOMAIN_TURN_COST.get(oldest, DEFAULT_TURN_COST)
                )
                self.drained += 1
                item[3][DEFERRED_KEY] = True
                return item[3]
            return None
        return None

    def snapshot(self) -> Dict[str, Any]:
        """For a heartbeat, so a buffer that is quietly overflowing is visible."""
        return {
            "held": len(self._items),
            "offered": self.offered,
            "drained": self.drained,
            "evicted": self.evicted,
            "expired": self.expired,
            "best_score": round(self._items[0][0], 4) if self._items else None,
            # Which domains are represented, so a buffer holding thirty-two
            # AIS events and nothing else is visible as that rather than as a
            # healthy full buffer.
            "domains": sorted({domain_of(i[3]) for i in self._items}),
        }


def strip_deferred(message: Dict[str, Any]) -> Dict[str, Any]:
    """The message without this module's bookkeeping key."""
    if not isinstance(message, dict) or DEFERRED_KEY not in message:
        return message
    return {k: v for k, v in message.items() if k != DEFERRED_KEY}


__all__ = [
    "DEFAULT_MAX_AGE_SEC",
    "DEFAULT_MAX_ITEMS",
    "DEFERRED_KEY",
    "CandidateBuffer",
    "strip_deferred",
]
