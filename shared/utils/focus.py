"""
shared/utils/focus.py

Pointing more than one agent at the same subject.

The consensus engine fuses agent opinions into agreement or contradiction. It
has never had two opinions to fuse. Measured live on 4 September: six bulletins
across five agents, six distinct tickers, zero overlap, and one bulletin whose
ticker was None.

The cause is not spelling -- entity resolution fixed that, and the symbols are
clean. It is that each agent selects its own subject from its own watchlist and
nothing ever asks a second agent to look at what the first found. With roughly
one bulletin per agent per hour against hundreds of instruments, two agents
landing on the same name in the same window is close to impossible.

So Subjective Logic fusion, contradiction detection, the measured base rate and
the agent scorecards are all built, all correct, and all idle -- waiting on a
coordination that does not exist.

This is that coordination, and it is deliberately the smallest version of it:

  - Any agent that finds something interesting *offers* the subject.
  - Every agent, when choosing what to work on, consults the focus set first.
  - Nothing is compelled. An agent that has its own reason to look elsewhere
    still does; the focus set reorders preferences, it does not seize control.

It costs no inference. The agents already run and already choose; this only
changes what they choose, and the whole point is that a second opinion on a
subject already under examination is worth more than a first opinion on a
subject nobody else will ever look at.
"""

import logging
import time
from typing import Any, List, Optional

logger = logging.getLogger("shared.focus")

# Subjects currently worth a second opinion, scored by when they were offered.
FOCUS_KEY = "sentinel:focus:entities"

# How long an offer stands.
#
# Short: the value of a second opinion decays with the situation that prompted
# it, and a stale focus set would pin the swarm to yesterday's interesting name.
# Long enough that an agent on a thirty-minute review cycle sees it at least
# once.
FOCUS_TTL_SEC = 2700

# How many subjects the set holds. Small on purpose -- a focus list of fifty is
# a watchlist, and the platform already has watchlists. This is meant to be the
# handful of things worth converging on.
FOCUS_MAX = 12

# Below this conviction an agent's interest is not worth redirecting others.
# A radar escalation at 0.015 conviction, which the live system produced, should
# not pull four agents onto a ticker.
FOCUS_MIN_CONVICTION = 0.35


async def offer_focus(
    redis_client: Any,
    entity: str,
    conviction: float = 1.0,
    offered_by: str = "",
) -> bool:
    """Propose a subject as worth a second opinion.

    Best-effort and never raises: an agent's own work must not fail because the
    focus set was unreachable.
    """
    if not redis_client or not entity:
        return False
    try:
        if float(conviction) < FOCUS_MIN_CONVICTION:
            return False
    except (TypeError, ValueError):
        return False

    subject = str(entity).strip().upper()
    if not subject or subject in ("UNKNOWN", "NONE"):
        return False

    try:
        raw = getattr(redis_client, "raw", redis_client)
        now = time.time()
        pipe = raw.pipeline()
        pipe.zadd(FOCUS_KEY, {subject: now})
        # Drop anything older than the window, then anything beyond the cap,
        # oldest first.
        pipe.zremrangebyscore(FOCUS_KEY, "-inf", now - FOCUS_TTL_SEC)
        pipe.zremrangebyrank(FOCUS_KEY, 0, -(FOCUS_MAX + 1))
        pipe.expire(FOCUS_KEY, FOCUS_TTL_SEC)
        await pipe.execute()
        if offered_by:
            logger.debug("%s offered %s for a second opinion.", offered_by, subject)
        return True
    except Exception as e:
        logger.debug("Could not offer focus on %s: %s", entity, e)
        return False


async def current_focus(redis_client: Any, limit: int = FOCUS_MAX) -> List[str]:
    """Subjects another agent has found interesting recently, newest first."""
    if not redis_client:
        return []
    try:
        raw = getattr(redis_client, "raw", redis_client)
        now = time.time()
        members = await raw.zrangebyscore(
            FOCUS_KEY, now - FOCUS_TTL_SEC, "+inf",
        )
        out = [m.decode() if isinstance(m, bytes) else str(m) for m in (members or [])]
        return list(reversed(out))[:limit]
    except Exception as e:
        logger.debug("Could not read the focus set: %s", e)
        return []


async def prioritise(
    redis_client: Any,
    candidates: List[str],
    limit: Optional[int] = None,
) -> List[str]:
    """Reorder an agent's own candidates to put focused subjects first.

    Additive, not restrictive: every candidate the agent chose is still in the
    list and in its original relative order. What changes is that a subject
    another agent is already looking at rises to the front, so a swarm with
    limited inference spends it where a second opinion can actually be formed.
    """
    if not candidates:
        return []
    focused = set(await current_focus(redis_client))
    if not focused:
        return candidates[:limit] if limit else candidates

    front = [c for c in candidates if str(c).strip().upper() in focused]
    back = [c for c in candidates if str(c).strip().upper() not in focused]
    ordered = front + back
    return ordered[:limit] if limit else ordered
