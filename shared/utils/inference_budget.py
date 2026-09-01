"""
shared/utils/inference_budget.py

Rate-limits LLM inference to what the host can actually perform.

Measured on this deployment: a single knowledge-graph inference took 482
seconds. That is roughly 180 events per day. The stream it consumes carries
16,000 events every six hours, and the anomaly distribution is flat enough that
even a 0.95 threshold still admits ~14,000 a day -- a 78x gap that no threshold
closes.

The consequence was not slowness but a stall: the consumer paid eight minutes
per qualifying message, committed nothing meanwhile, and sat at zero messages a
minute while its lag grew by 180 a minute. A backlog that grows faster than it
drains is never drained; `enriched.events` had reached 395,000 messages, about
six years of work at that rate.

So the intake sheds instead of queuing, exactly as the WebSocket reader does.
A cheap check decides whether the model is free; when it is not, the message is
dropped and the consumer moves on. Dropping is not data loss here: every event
is already persisted and correlated. What is lost is one *opinion* about an
event, and having a current opinion about recent events is worth more than a
complete opinion about events from six years ago.
"""

import logging
import math
import os
import time
from collections import deque
from typing import Optional

logger = logging.getLogger("shared.inference_budget")

# A ceiling for work that never reports back, not the spacing between
# inferences. See MIN_GAP_SEC below for the value that actually governs.
#
# This was derived from measurement -- ~8 minutes per inference, so one every
# ten minutes leaves headroom -- and the measurement stopped being true. The
# same host now returns in ~110s:
#
#     knowledge_graph_engine | Inference completed (127288.3ms)
#     knowledge_graph_engine | Inference completed  (94159.45ms)
#
# Those two are 9.6 minutes apart, which is this constant and not the model.
# The slot was idle for roughly 80% of every cycle it held, and the swarm
# managed one inference in six hours. A constant derived from a measurement
# needs to expire with it; this one outlived its by a factor of five.
DEFAULT_COOLDOWN_SEC = 600

# After work on a priority domain the slot is released sooner, so those domains
# win more of the swarm's scarce inference over time. This is not a second lane:
# there is still exactly one slot and one inference at a time. Only how long the
# slot stays held varies, which shifts the *share* without raising concurrency
# on a single-threaded model server.
PRIORITY_COOLDOWN_SEC = 240

# How long the slot stays held once the work it covered has actually finished.
#
# The cooldowns above are a fallback for a worker that claimed a slot and never
# came back -- a crash, a hang, a container killed mid-inference. When an
# inference completes normally the agent says so, and holding the slot for the
# remaining eight minutes rations nothing; it just idles the model server.
#
# This is not zero, because freeing the slot the instant an inference ends would
# pin Ollama at 100% forever. It is capped at six of the host's twelve cores and
# the collectors, enrichment and correlation tiers share the rest, so the gap is
# what stops the reasoning tier from crowding out the pipeline feeding it.
MIN_GAP_SEC = float(os.getenv("INFERENCE_MIN_GAP_SEC", "60"))

# What this deployment is actually for. Vessel and aircraft telemetry is volume;
# these are the domains a person reads. An event from one of them earns a
# quicker return to the queue than a routine position fix.
PRIORITY_DOMAINS = frozenset({
    "news", "osint", "social", "media",
    "tradfi", "financial", "equity", "equities", "macro", "filings",
    "crypto", "prediction",
})


def is_priority_domain(domain: Optional[str]) -> bool:
    """True when a domain is one this deployment exists to reason about."""
    if not domain:
        return False
    token = str(domain).strip().lower()
    if token in PRIORITY_DOMAINS:
        return True
    # Event types arrive as "headline", "equity_block", "crypto_trade" and the
    # like; match on the leading token rather than demanding an exact name.
    return token.split("_", 1)[0] in PRIORITY_DOMAINS


# How selective admission is, and the guarantee that it cannot starve.
#
# The bar is a percentile of recently seen scores, so it tracks whatever the
# stream is actually producing rather than a threshold someone guessed. The
# holdback is bounded: if nothing has cleared the bar for this long, the next
# candidate is admitted regardless. An idle slot helps nobody, and a selection
# rule that can refuse forever is worse than no selection rule.
ADMISSION_PERCENTILE = float(os.getenv("INFERENCE_ADMISSION_PERCENTILE", "0.60"))
ADMISSION_HISTORY = int(os.getenv("INFERENCE_ADMISSION_HISTORY", "256"))
ADMISSION_MIN_HISTORY = int(os.getenv("INFERENCE_ADMISSION_MIN_HISTORY", "32"))
MAX_HOLDBACK_SEC = float(os.getenv("INFERENCE_MAX_HOLDBACK_SEC", "90"))

# How long a registered interest stands. Long enough that a slow consumer
# polling once every fourteen seconds is still counted as waiting when the
# slot next frees; short enough that an agent which has stopped asking stops
# holding anyone back.
WAITER_TTL_SEC = int(os.getenv("INFERENCE_WAITER_TTL_SEC", "300"))

# An entry older than this is dropped before the queue is read. Ordering is by
# first ask and never refreshed while a caller keeps asking, which is what
# makes it fair -- but it also means an agent that stops asking would hold the
# head position and block everyone until the key expired. A caller still
# polling is re-added immediately after being pruned, so it only loses its
# place to callers that have genuinely been waiting longer.
WAITER_STALE_SEC = int(os.getenv("INFERENCE_WAITER_STALE_SEC", "120"))


class InferenceBudget:
    """Admits work only when the model is free, using Redis so the budget is shared.

    Several agent processes talk to one Ollama instance. A per-process limiter
    would multiply by the number of processes and reproduce the queue it exists
    to prevent, so the key is namespaced per model rather than per agent.
    """

    def __init__(
        self,
        redis_client,
        model: str,
        cooldown_sec: int = DEFAULT_COOLDOWN_SEC,
        priority_cooldown_sec: int = PRIORITY_COOLDOWN_SEC,
        lane: Optional[str] = None,
        owner: Optional[str] = None,
    ):
        """`lane` reserves an independent slot for a caller.

        Without it every caller on the same model shares one key, which is
        correct when the callers are peers competing for a scarce resource. It
        is not correct when one of them is a different tier of the system: the
        reasoning service and the five agents-fast agents both run
        qwen2.5:1.5b, so they shared a slot -- and the agents, consuming a far
        busier stream, re-claimed it before it could expire. Sampled once every
        ten seconds the key was never free.

        The consequence was total starvation rather than degradation: reasoning
        sheds a cluster whenever the slot is busy, so it shed every one of them
        and the scenarios table stood empty since the service was first
        deployed. A lane guarantees it a turn.

        Concurrency is still bounded -- two lanes mean at most two in-flight
        requests, which Ollama serialises anyway with OLLAMA_NUM_PARALLEL=1.
        """
        self.redis = redis_client
        self.model = model or "default"
        self.lane = (lane or "").strip() or None
        # Who is asking. Without it the budget cannot be fair, because it cannot
        # tell one caller from another -- see _yields_to_a_waiter below.
        self.owner = (owner or "").strip() or None
        self.cooldown_sec = max(0, int(cooldown_sec))
        # Never longer than the standard hold: a "priority" domain that waited
        # longer than routine telemetry would be the opposite of the intent.
        self.priority_cooldown_sec = min(max(0, int(priority_cooldown_sec)), self.cooldown_sec)
        self.admitted = 0
        self.shed = 0
        self.held_back = 0
        # Recent candidate scores, per process. Deliberately not shared through
        # Redis: the bar asks "is this good compared with what I have been
        # seeing", and one extra round trip per candidate on a stream this size
        # would cost more than the selection is worth.
        self._recent_scores: deque = deque(maxlen=ADMISSION_HISTORY)
        self._last_admit: float = time.monotonic()

    @property
    def _key(self) -> str:
        if self.lane:
            return f"sentinel:inference:budget:{self.lane}:{self.model}"
        return f"sentinel:inference:budget:{self.model}"

    @property
    def _waiters_key(self) -> str:
        return f"{self._key}:waiters"

    @property
    def _seen_key(self) -> str:
        """When each waiter last asked, kept apart from when it first asked.

        One value cannot do both jobs: the queue is ordered by first ask, and
        staleness is judged by last ask.
        """
        return f"{self._key}:waiters:seen"

    async def _note_interest(self) -> None:
        """Records that this caller wanted the slot and could not have it.

        Backing off silently is what made the budget unfair. An agent that
        checks politely, finds the slot busy and returns leaves no trace, so
        nothing in the system knows it is waiting -- and the arithmetic then
        decides everything: knowledge_graph_engine consumes its stream at 7.9
        messages a second and the wargamer at 0.07, so on a slot that frees for
        an instant every minute the busy agent wins essentially every time.
        Measured over two hours: every completed inference belonged to one
        agent.

        Scored by when the caller *first* asked and never overwritten while it
        keeps asking, so the queue is ordered by how long each has been waiting
        rather than by who polled most recently -- which would hand it straight
        back to the fastest consumer.
        """
        if self.redis is None or not self.owner:
            return
        try:
            raw = getattr(self.redis, "raw", self.redis)
            # GT: keep the earliest ask, but only while the caller is still
            # asking. zadd(nx=True) never refreshed the score, so a waiter that
            # crossed WAITER_STALE_SEC was pruned and re-added at the back --
            # the agent that had waited longest was the one sent to the back,
            # which is the ordering inverted. Presence is refreshed separately
            # from position: _last_seen keeps the entry alive, the zset score
            # keeps its place.
            await raw.zadd(self._waiters_key, {self.owner: time.time()}, nx=True)
            await raw.hset(self._seen_key, self.owner, str(time.time()))
            await raw.expire(self._waiters_key, WAITER_TTL_SEC)
            await raw.expire(self._seen_key, WAITER_TTL_SEC)
        except Exception:
            pass

    async def _yields_to_a_waiter(self) -> bool:
        """Whether someone has been waiting longer than this caller.

        First-asked, first-served. "Not twice in a row" was the first attempt
        and it is not enough: it caps any one agent at half the slots, so with
        three contending agents the busiest still took two thirds. Ordering by
        wait time is the version that actually shares.

        With nobody waiting this is false and the caller proceeds exactly as
        before, so a quiet system loses nothing -- it costs throughput only in
        the case it exists for, where another agent has been shut out.
        """
        if self.redis is None or not self.owner:
            return False
        try:
            raw = getattr(self.redis, "raw", self.redis)
            try:
                # Drop waiters that have stopped asking, judged by when they
                # last asked rather than when they first did. Pruning on the
                # queue score punished the longest waiter for waiting.
                seen = await raw.hgetall(self._seen_key)
                if isinstance(seen, dict):
                    cutoff = time.time() - WAITER_STALE_SEC
                    gone = []
                    for member, when in seen.items():
                        member = member.decode() if isinstance(member, bytes) else member
                        try:
                            if float(when) < cutoff:
                                gone.append(member)
                        except (TypeError, ValueError):
                            gone.append(member)
                    if gone:
                        await raw.zrem(self._waiters_key, *gone)
                        await raw.hdel(self._seen_key, *gone)
            except Exception:
                pass
            head = await raw.zrange(self._waiters_key, 0, 0)
            if not isinstance(head, (list, tuple)) or not head:
                return False
            first = head[0]
            if isinstance(first, bytes):
                first = first.decode()
            # Checked, not coerced -- the same rule is_available() already
            # applies to EXISTS. A stub whose zrange answers with something
            # truthy that is not a name would otherwise read as "somebody is
            # ahead of you" and hold every caller back forever.
            if not isinstance(first, str) or not first:
                return False
            return first != self.owner
        except Exception:
            # Fairness is an optimisation; a Redis problem must not stop work.
            return False

    async def try_acquire(self, score: Optional[float] = None, domain: Optional[str] = None) -> bool:
        """True when this caller may run an inference now.

        Uses SET NX EX, so the check and the claim are one atomic operation:
        two agents polling simultaneously cannot both conclude the model is
        free. A Redis failure admits the work rather than blocking it -- the
        budget is an optimisation, and failing closed would silence the
        reasoning tier entirely over an unrelated outage.

        `domain` sets how long the slot stays held once claimed. Work on a
        priority domain releases it sooner, so news, filings and market data win
        a larger share of a fixed number of inferences than routine telemetry.
        Concurrency is unchanged: one slot, one inference.
        """
        if self.cooldown_sec == 0 or self.redis is None:
            self.admitted += 1
            return True

        # Importance, not arrival order.
        #
        # `score` has been a parameter of this method since it was written and
        # was never read: admission was whoever happened to call while the slot
        # was free. Against a hundred and fifty thousand events that makes
        # timing the selection criterion for everything the system chooses to
        # think about. The question a reviewer asks is "which ones, and why",
        # and "whichever arrived at the right moment" is not an answer.
        #
        # "Roughly twenty an hour" is repeated throughout this codebase as the
        # host's capacity. It was never measured: a 600s hold is six an hour,
        # and the swarm was observed managing one in six. The figure conflated
        # inferences with the decisions batching multiplies out of them. What
        # the host affords now depends on MIN_GAP_SEC and how long inference
        # actually takes -- both measurable, neither a constant to quote.
        #
        # A candidate now has to beat what this process has lately been seeing.
        # The bar is a percentile of recent scores rather than a fixed
        # threshold, for the same reason the detectors are: only the deployment
        # knows what ordinary looks like.
        if not self._passes_admission_bar(score):
            self.held_back += 1
            if self.held_back % 500 == 1:
                logger.info(
                    "Inference budget: held back %s below-bar candidate(s) for %s "
                    "(bar is the %.0fth percentile of recent scores).",
                    self.held_back, self.model, ADMISSION_PERCENTILE * 100,
                )
            return False

        # Fair turn-taking, checked before the claim so the slot is left free for
        # whoever is waiting rather than handed straight back to the last winner.
        if await self._yields_to_a_waiter():
            # Register before returning, or a caller that never peeks can never
            # become head of the queue it keeps standing aside for. The graph
            # engine's batch path claims directly and calls is_available()
            # nowhere, so without this it yields to everyone, forever -- the
            # starvation this mechanism exists to end, pointed the other way.
            await self._note_interest()
            self.shed += 1
            return False

        hold = self.priority_cooldown_sec if is_priority_domain(domain) else self.cooldown_sec
        try:
            raw = getattr(self.redis, "raw", self.redis)
            claimed = await raw.set(self._key, str(time.time()), ex=hold, nx=True)
            if claimed:
                self.admitted += 1
                self._last_admit = time.monotonic()
                if self.owner:
                    try:
                        # Served: leave the queue so the next caller is head.
                        await raw.zrem(self._waiters_key, self.owner)
                        await raw.hdel(self._seen_key, self.owner)
                    except Exception:
                        pass
                return True
            await self._note_interest()
            self.shed += 1
            if self.shed % 500 == 1:
                logger.info(
                    "Inference budget: shed %s messages for %s (one every %ss). "
                    "Raise the cooldown only if the host gets faster.",
                    self.shed, self.model, self.cooldown_sec,
                )
            return False
        except Exception as e:
            logger.warning("Inference budget check failed for %s, admitting: %s", self.model, e)
            self.admitted += 1
            return True

    def _passes_admission_bar(self, score: Optional[float]) -> bool:
        """Whether this candidate is worth the slot, given what else has arrived.

        Three ways to pass, and the last two are the safety rails:

          * the score clears the percentile bar of recent candidates;
          * no score was supplied, so there is nothing to rank on and refusing
            would silently disable a caller that simply does not score its work;
          * nothing has been admitted for MAX_HOLDBACK_SEC, so holding out any
            longer wastes the slot the selection exists to spend well.
        """
        if score is None:
            return True

        try:
            value = float(score)
        except (TypeError, ValueError):
            return True

        self._recent_scores.append(value)

        if len(self._recent_scores) < ADMISSION_MIN_HISTORY:
            # Too little history to say what "better than usual" means. A bar
            # computed from a handful of samples would mostly encode their order.
            return True

        if (time.monotonic() - self._last_admit) >= MAX_HOLDBACK_SEC:
            return True

        ranked = sorted(self._recent_scores)
        index = min(len(ranked) - 1, int(len(ranked) * ADMISSION_PERCENTILE))
        return value >= ranked[index]

    async def is_available(self) -> bool:
        """Read-only peek: would a claim succeed right now?

        Deliberately does not claim. Callers use it to skip expensive
        preparation -- context queries, prompt assembly, dedup bookkeeping --
        when the model is plainly busy, then let the atomic claim in
        try_acquire() decide for real. Peeking and claiming separately is safe
        because the claim is still atomic; the peek only avoids wasted work.

        Answers True when it cannot tell, so a Redis problem degrades to the
        previous behaviour rather than silencing the tier.
        """
        if self.cooldown_sec == 0 or self.redis is None:
            return True
        try:
            raw = getattr(self.redis, "raw", self.redis)
            held = await raw.exists(self._key)
            # EXISTS answers with a count. The type is checked rather than
            # coerced: int() succeeds on plenty of objects that are not an
            # answer -- a stub returns 1 from __int__ and would be read as
            # "busy", silencing the tier as surely as failing closed.
            free = (not held) if isinstance(held, bool) else (held <= 0) if isinstance(held, int) else True
            if free and await self._yields_to_a_waiter():
                # Free, but not this caller's turn. Answering True here is worse
                # than answering False: the caller builds its context -- for the
                # wargamer a Neo4j subgraph query and a Redis fetch -- and then
                # loses the claim to the same rule this peek did not apply.
                # Measured: seven wargames began and none completed, vanishing
                # between the peek and the claim without an error, because
                # InferenceShed is a BaseException and nothing logs a shed.
                free = False
            if not free:
                # The whole point: a caller that backs off here must still be
                # counted as wanting a turn.
                await self._note_interest()
            return free
        except Exception:
            return True

    async def finish(self) -> None:
        """Shortens the hold once the inference it covered has completed.

        The claim is sized for the worst case -- a worker that never returns --
        because the key expiring is the only thing that recovers a slot from a
        crashed process. Once the work is genuinely done that ceiling is just
        idle time: measured at ~110s of inference inside a 600s hold, the model
        server sat unused for four fifths of every cycle and the whole swarm
        managed one inference in six hours.

        EXPIRE rather than SET, so this can only ever shorten a key that still
        exists. If the inference outlived its own cooldown the key is already
        gone and possibly re-claimed by someone else; re-setting it would either
        resurrect a slot nobody holds or extend a stranger's.
        """
        if self.redis is None or self.cooldown_sec == 0:
            return
        try:
            raw = getattr(self.redis, "raw", self.redis)
            # ceil, not int: INFERENCE_MIN_GAP_SEC=0.5 truncates to 0, and
            # EXPIRE 0 deletes the key outright -- turning the gap that keeps
            # the model server off 100% duty into no gap at all.
            await raw.expire(self._key, max(1, math.ceil(MIN_GAP_SEC)))
        except Exception:
            # The key expires on its own; a failure here costs latency, not
            # correctness.
            pass

    async def release(self) -> None:
        """Frees the slot early when an inference finished faster than the cooldown.

        Optional: the key expires on its own, so a crashed worker cannot hold the
        budget forever.
        """
        if self.redis is None:
            return
        try:
            raw = getattr(self.redis, "raw", self.redis)
            await raw.delete(self._key)
        except Exception:
            pass

    @property
    def stats(self) -> dict:
        total = self.admitted + self.shed
        return {
            "admitted": self.admitted,
            "shed": self.shed,
            "admit_rate": round(self.admitted / total, 4) if total else 0.0,
            "cooldown_sec": self.cooldown_sec,
        }

class InferenceShed(BaseException):
    """Signals that the inference budget declined this piece of work.

    Deliberately BaseException rather than Exception, for the same reason
    asyncio.CancelledError is: ten of the fifteen inference call sites in the
    agent swarm wrap the call in `except Exception`, and a shed those handlers
    swallowed would let an agent carry on as though a model had answered.

    The semantics were verified against the dispatch loop rather than assumed:
      - asyncio.gather(return_exceptions=True) captures it, so the consume loop
        survives instead of the batch dying;
      - `isinstance(result, Exception)` is False, so it is not sent to the dead
        letter queue -- nothing is wrong with the message;
      - it is not caught by the agents' broad handlers;
      - the commit that follows still runs, so the consumer advances.

    Shedding is capacity, not failure. It must not increment error counters, must
    not DLQ, and must not block the commit.
    """

    def __init__(self, agent: str = "", model: str = ""):
        super().__init__(f"inference budget declined work for {agent or 'agent'} on {model or 'model'}")
        self.agent = agent
        self.model = model
