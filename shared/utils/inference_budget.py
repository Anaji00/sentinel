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
import time
from typing import Optional

logger = logging.getLogger("shared.inference_budget")

# Derived from measurement, not preference: ~8 minutes per inference means one
# every ten minutes leaves headroom and still keeps the consumer current.
DEFAULT_COOLDOWN_SEC = 600

# After work on a priority domain the slot is released sooner, so those domains
# win more of the swarm's scarce inference over time. This is not a second lane:
# there is still exactly one slot and one inference at a time. Only how long the
# slot stays held varies, which shifts the *share* without raising concurrency
# on a single-threaded model server.
PRIORITY_COOLDOWN_SEC = 240

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
        self.cooldown_sec = max(0, int(cooldown_sec))
        # Never longer than the standard hold: a "priority" domain that waited
        # longer than routine telemetry would be the opposite of the intent.
        self.priority_cooldown_sec = min(max(0, int(priority_cooldown_sec)), self.cooldown_sec)
        self.admitted = 0
        self.shed = 0

    @property
    def _key(self) -> str:
        if self.lane:
            return f"sentinel:inference:budget:{self.lane}:{self.model}"
        return f"sentinel:inference:budget:{self.model}"

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
        hold = self.priority_cooldown_sec if is_priority_domain(domain) else self.cooldown_sec
        try:
            raw = getattr(self.redis, "raw", self.redis)
            claimed = await raw.set(self._key, str(time.time()), ex=hold, nx=True)
            if claimed:
                self.admitted += 1
                return True
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
            if isinstance(held, bool):
                return not held
            if isinstance(held, int):
                return held <= 0
            return True
        except Exception:
            return True

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
