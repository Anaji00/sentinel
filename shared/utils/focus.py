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
import os
import time
from typing import Any, Iterable, List, Optional

from shared.utils.quiet_failures import swallowed

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

# How long the single most recent subject in a domain survives.
#
# The TTL above is a statement about how fast a situation decays. It is also,
# accidentally, a statement about how often a domain has to produce -- and the
# domains do not produce at the same rate. Measured on the live focus set:
#
#   offered, lifetime   maritime 98  crypto 87  aviation 95  tradfi 39
#   present, right now  maritime  4  crypto  4  unknown   4  tradfi  0
#
# The per-domain cap already stops a busy domain crowding a quiet one out on
# count. It does nothing about recency: tradfi subjects are offered roughly
# once every eight hours, so they sit inside a forty-five minute window about
# nine per cent of the time and `stock_correlation_agent` -- whose domains are
# tradfi, market, equity and macro -- consults an empty set for the rest.
#
# So the quiet domains get the coordination least, which is backwards: they are
# the ones where two agents landing on one subject by chance is least likely,
# and therefore the ones a focus set exists to help.
#
# One subject per domain is kept past the TTL, up to this longer ceiling. It is
# one, not four, because the point is to give a quiet domain *something* to
# converge on rather than to pin the swarm to a stale list -- and `prioritise`
# only reorders, so a slightly old suggestion costs an ordering, not a slot.
FOCUS_FLOOR_TTL_SEC = int(os.getenv("FOCUS_FLOOR_TTL_SEC", str(6 * 3600)))

# How many subjects the set holds. Small on purpose -- a focus list of fifty is
# a watchlist, and the platform already has watchlists. This is meant to be the
# handful of things worth converging on.
FOCUS_MAX = 12

# And how many of those any one domain may hold.
#
# The set was a single global FIFO evicted oldest-first, and the correlation
# engine feeds it from every ELEVATED+ cluster regardless of domain -- so
# whichever domain was loud in a 45-minute window took every slot. Measured
# live, all seven entries were maritime, aviation and one commodity, while the
# three agents that read the set (quant, radar, stock-correlation) all draw
# their candidates from equity and crypto universes. A vessel name can never
# match a ticker candidate, so `prioritise` was a no-op on every call for every
# consumer.
#
# A per-domain cap means a burst in one domain evicts its own oldest entry
# rather than everyone else's, so the set stays usable by all of its readers.
FOCUS_MAX_PER_DOMAIN = 4

# Where each subject's domain is remembered, so the cap can be applied and a
# consumer can ask for the subjects it could actually act on.
FOCUS_DOMAIN_KEY = "sentinel:focus:domains"

# Below this conviction an agent's interest is not worth redirecting others.
# A radar escalation at 0.015 conviction, which the live system produced, should
# not pull four agents onto a ticker.
FOCUS_MIN_CONVICTION = 0.35


async def offer_focus(
    redis_client: Any,
    entity: str,
    conviction: float = 1.0,
    offered_by: str = "",
    domain: Optional[str] = None,
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
        dom = (str(domain).strip().lower() if domain else "unknown")

        await raw.zadd(FOCUS_KEY, {subject: now})
        await raw.hset(FOCUS_DOMAIN_KEY, subject, dom)
        await raw.zremrangebyscore(FOCUS_KEY, "-inf", now - FOCUS_TTL_SEC)

        # Evict within the domain first, so a burst in one cannot crowd out the
        # others. Only once every domain is inside its own cap does the global
        # cap apply, oldest-first as before.
        members = await raw.zrange(FOCUS_KEY, 0, -1)
        members = [m.decode() if isinstance(m, bytes) else str(m) for m in (members or [])]
        if members:
            domains = await raw.hmget(FOCUS_DOMAIN_KEY, members)
            by_domain: dict = {}
            for name, d in zip(members, domains or []):
                d = (d.decode() if isinstance(d, bytes) else d) or "unknown"
                by_domain.setdefault(d, []).append(name)
            surplus = []
            for d, names in by_domain.items():
                if len(names) > FOCUS_MAX_PER_DOMAIN:
                    # `members` is oldest-first, so the head of each list is.
                    surplus.extend(names[: len(names) - FOCUS_MAX_PER_DOMAIN])

            # The floor: the newest subject in each domain is re-scored so it
            # outlives the ordinary window. Without it a domain that offers
            # rarely is absent from the set most of the time, which is the
            # opposite of what a coordination mechanism should do -- see
            # FOCUS_FLOOR_TTL_SEC.
            for d, names in by_domain.items():
                newest = names[-1]
                if newest in surplus:
                    continue
                await raw.zadd(
                    FOCUS_KEY,
                    {newest: now - FOCUS_TTL_SEC + FOCUS_FLOOR_TTL_SEC},
                    xx=True, gt=True,
                )
            if surplus:
                await raw.zrem(FOCUS_KEY, *surplus)
                await raw.hdel(FOCUS_DOMAIN_KEY, *surplus)

        await raw.zremrangebyrank(FOCUS_KEY, 0, -(FOCUS_MAX + 1))
        # The keys themselves live as long as the longest thing in them.
        await raw.expire(FOCUS_KEY, FOCUS_FLOOR_TTL_SEC)
        await raw.expire(FOCUS_DOMAIN_KEY, FOCUS_FLOOR_TTL_SEC)
        if offered_by:
            logger.debug("%s offered %s for a second opinion.", offered_by, subject)
        return True
    except Exception as e:
        logger.debug("Could not offer focus on %s: %s", entity, e)
        return False


async def current_focus(
    redis_client: Any,
    limit: int = FOCUS_MAX,
    domains: Optional[Iterable[str]] = None,
) -> List[str]:
    """Subjects another agent has found interesting recently, newest first.

    `domains` narrows the answer to subjects a caller could actually act on. An
    equity agent asking for maritime subjects gets a list it can do nothing
    with, which is what made this mechanism a no-op for all three of its
    readers.
    """
    if not redis_client:
        return []
    try:
        raw = getattr(redis_client, "raw", redis_client)
        now = time.time()
        members = await raw.zrangebyscore(
            FOCUS_KEY, now - FOCUS_TTL_SEC, "+inf",
        )
        out = [m.decode() if isinstance(m, bytes) else str(m) for m in (members or [])]
        out = list(reversed(out))

        if domains:
            wanted = {str(d).strip().lower() for d in domains if d}
            try:
                tags = await raw.hmget(FOCUS_DOMAIN_KEY, out) if out else []
                kept = []
                for name, d in zip(out, tags or []):
                    d = (d.decode() if isinstance(d, bytes) else d) or "unknown"
                    # "unknown" is kept: a subject offered before domains were
                    # recorded should not vanish from every caller's view.
                    if d in wanted or d == "unknown":
                        kept.append(name)
                out = kept
            except Exception as _exc:
                # Counted rather than whispered: if the domain tags go missing
                # the filter silently widens to everything, which is the
                # behaviour this change exists to remove.
                swallowed("utils.focus.current_focus.domain_filter", _exc, logger)

        return out[:limit]
    except Exception as e:
        logger.debug("Could not read the focus set: %s", e)
        return []


async def prioritise(
    redis_client: Any,
    candidates: List[str],
    limit: Optional[int] = None,
    domains: Optional[Iterable[str]] = None,
) -> List[str]:
    """Reorder an agent's own candidates to put focused subjects first.

    Additive, not restrictive: every candidate the agent chose is still in the
    list and in its original relative order. What changes is that a subject
    another agent is already looking at rises to the front, so a swarm with
    limited inference spends it where a second opinion can actually be formed.
    """
    if not candidates:
        return []
    focused = set(await current_focus(redis_client, domains=domains))
    if not focused:
        return candidates[:limit] if limit else candidates

    front = [c for c in candidates if str(c).strip().upper() in focused]
    back = [c for c in candidates if str(c).strip().upper() not in focused]
    ordered = front + back
    return ordered[:limit] if limit else ordered
