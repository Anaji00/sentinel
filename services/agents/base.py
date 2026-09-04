import asyncio
import json
import logging
import os
import time
import uuid

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Type

import aiohttp
from pydantic import BaseModel, Field, field_validator

from shared.utils.freshness import is_stale
from shared.utils.live_feed import agent_narrative
from shared.utils.inference_budget import (
    DEFAULT_COOLDOWN_SEC,
    InferenceBudget,
    InferenceShed,
)

# Ceiling on detached dispatches. Most finish in milliseconds; only the few
# the inference budget admits run long, so this is rarely approached. It
# exists so a slow model cannot convert backlog into unbounded memory.
MAX_INFLIGHT_DISPATCHES = 64


def _message_score(message: Dict[str, Any]) -> Optional[float]:
    """How important this message claims to be, for inference admission.

    Read from whatever the producer supplied. None when nothing usable is
    present, which the budget treats as "cannot rank, do not refuse" -- a
    caller that does not score its work must not be silently starved by a
    selection rule it never participated in.
    """
    for key in ("anomaly_score", "confidence_score", "severity", "conviction"):
        value = message.get(key)
        if value is None:
            continue
        try:
            score = float(value)
        except (TypeError, ValueError):
            continue
        # severity is authored on a 1-5 scale; the rest are already 0-1.
        return max(0.0, min(1.0, score / 5.0 if key == "severity" else score))
    trigger = message.get("trigger")
    if isinstance(trigger, dict):
        return _message_score(trigger)
    return None


def _message_domain(message: Dict[str, Any]) -> str:
    """Best-effort domain for a message, for inference prioritisation.

    Messages reach the swarm as correlations, briefs, scenarios and raw events,
    and each names its domain differently. Guessing wrong only changes how long
    a slot is held, never whether the work is correct, so this reads whatever is
    present rather than demanding one shape.
    """
    for key in ("primary_domain", "domain", "source_domain"):
        value = message.get(key)
        if value:
            return str(value)
    # Event types encode the domain in their prefix: "headline", "equity_block",
    # "vessel_position", "flight_anomaly".
    event_type = message.get("type") or message.get("event_type")
    if event_type:
        return str(event_type)
    source = message.get("source")
    return str(source) if source else ""

from shared.utils.tasks import safe_create_task
from shared.utils.focus import offer_focus, prioritise
# The same set the correlation layer refuses to treat as corroboration. One
# definition of "this is a position report, not a claim", used by both.
from shared.models.events import POSITION_TELEMETRY_TYPES as ROUTINE_TELEMETRY_TYPES
from shared.utils.heartbeat import touch_heartbeat
from shared.utils.text import clip
from shared.utils.ollama import (
    DEFAULT_MODEL,
    OllamaClient, SchemaViolationError, InferenceError,
    OLLAMA_MODEL, OLLAMA_FALLBACK_MODEL, OLLAMA_URL
)

# How long a subject stays "already considered", by how fast the subject moves.
#
# Five different windows were in use -- 600s in the stock correlation agent,
# 1800s in the quant engine and the volatility surface, 3600s in the rates
# engine and the knowledge graph -- and three were bare literals with nothing
# stating why one subject deserves ten minutes and another an hour. They may all
# have been right; there was no way to tell, and no way to change one without
# guessing at what it was for.
#
# Named by what they are about rather than by their duration, so a future change
# is an argument about the subject rather than about a number:
#
#   FAST    a price or a correlation, which can genuinely be new within minutes
#   MEDIUM  a position or a surface, which re-derives on a slower cadence
#   SLOW    a regime or an ontology, where a second look inside the hour is
#           almost always the same look
DEDUP_WINDOW_FAST_SEC = 600
DEDUP_WINDOW_MEDIUM_SEC = 1800
DEDUP_WINDOW_SLOW_SEC = 3600

# Task Queue Keys
TASK_QUEUE_HIGH   = "sentinel:tasks:high"
TASK_QUEUE_NORMAL = "sentinel:tasks:normal"
TASK_QUEUE_LOW    = "sentinel:tasks:low"

# Heartbeat interval in seconds for agent health reporting
HEARTBEAT_INTERVAL = 60

# ── STRUCTURED AGENT COMMUNICATION PROTOCOL ──────────────────────────────────

# Confidence assigned to an agent result that states none.
#
# Was 0.85, which put every silent agent above most measured findings.
AGENT_UNSTATED_CONFIDENCE = 0.30


class _NoBriefToPublish(Exception):
    """The agent returned no prose, so there is nothing to show a person."""


class _QuoteCacheMiss(Exception):
    """The one-hour quote cache had nothing. Storage is asked next."""


def _as_probability_value(value):
    """A model-supplied probability, normalised to 0-1.

    Shared by AgentPrediction and AgentBulletin so the two cannot diverge
    again. Normalises rather than raises: both recorders swallow exceptions and
    return quietly, so raising would convert a recoverable value into a
    silently missing record.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if 1.0 < number <= 100.0:
        number /= 100.0
    return min(1.0, max(0.0, number))


class AgentBulletin(BaseModel):
    """Typed inter-agent communication message. Replaces free-text episodic memory."""
    agent_name: str
    bulletin_type: str  # "regime_change", "signal", "alert", "thesis", "contradiction"
    primary_entity_id: Optional[str] = None
    primary_entity_name: Optional[str] = None
    ticker: Optional[str] = None
    conviction: float = 0.5  # 0.0 - 1.0

    @field_validator("conviction", mode="before")
    @classmethod
    def _bulletin_probability(cls, value):
        """The same contract AgentPrediction.conviction enforces.

        That validator was added because a model filled a bare float field with
        55.0 and every consumer read it as 0-1. AgentBulletin carried the
        identical field, with the identical "# 0.0 - 1.0" comment and no
        validator, and it is fed by the same models -- macro's
        tail_risk_conviction is a bare float the model writes, and it reaches
        publish_bulletin unchanged.

        Bulletins land in the consensus engine's Subjective Logic fusion, where
        a conviction above 1.0 does not merely overweight the opinion, it breaks
        the algebra. For a bearish bulletin at conviction 85:

            r = (1.0 - 85.0) * evidence_count * 0.3   ->  negative
            b = r / (r + s + W)                       ->  -0.419

        Belief, disbelief and uncertainty are masses in [0,1] summing to 1, so a
        negative belief is not a wrong answer, it is not an answer.
        """
        return _as_probability_value(value)
    expected_direction: Optional[str] = None  # "up", "down", "neutral"
    payload: Dict[str, Any] = Field(default_factory=dict)
    summary: str = ""
    published_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    expires_at: Optional[str] = None  # ISO timestamp
    ttl_seconds: int = 3600  # Default 1 hour


class AgentPrediction(BaseModel):
    """Tracked prediction for self-calibration."""
    prediction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_name: str
    ticker: str
    direction: str  # "up", "down", "flat" -- price predictions only
    conviction: float

    @field_validator("conviction", mode="before")
    @classmethod
    def _as_probability(cls, value):
        """Conviction is a probability. Some callers hand it a percentage.

        The first prediction this system ever recorded read:

            quant_trading_engine  DOTUSDT  up  conviction=55.0

        Every consumer treats this as 0-1 -- the wargamer divides its cascade
        probability by 100 before arriving here, and the quant engine's own risk
        tiering tests `< 0.6` and `< 0.8`. A 55.0 is therefore not merely
        weighted 55x too heavily: it is above every threshold, so a model saying
        "55% confident" was read as maximum conviction and given the widest
        risk-reward tier.

        The value came from a model filling a bare `float` field, so the fix
        belongs on the shared contract rather than on the one caller that
        happened to expose it -- the next recorder would have the same problem.
        Normalised rather than rejected: record_prediction swallows exceptions
        and returns "", so raising here would turn a recoverable value into a
        silently missing prediction, which is the failure this whole path was
        just dug out of.
        """
        return _as_probability_value(value)
    time_horizon_hours: int = 24
    entry_price: float = 0.0
    target_price: float = 0.0
    # Categorical predictions. Not every question the platform reasons about is a
    # two-sided price bet: a nomination race prices one leg per candidate, and
    # "up or down on candidate X" is not the claim an analyst is making. When
    # outcome_space is populated the prediction is about *which* outcome wins,
    # and `direction` does not describe it.
    # What kind of claim this is, so the resolver does not have to infer it.
    #
    # The wargamer names an entity it expects to be targeted next; that is not a
    # price bet, and it was being handed to the directional scorer, which asks
    # for a price on "airlines, airports" and gives up. It also passes
    # entry_price=0.0, and the scorer's first guard rejects a falsy entry price,
    # so those predictions returned None before anything was even looked up.
    # Recorded, stored, never once resolved.
    #
    # Defaulted to "price" so every prediction already in Redis keeps the exact
    # behaviour it had.
    prediction_kind: str = "price"
    outcome_space: List[str] = Field(default_factory=list)
    predicted_outcome: Optional[str] = None
    market_key: Optional[str] = None
    resolved_outcome: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    verified: bool = False
    outcome_correct: Optional[bool] = None


# A prediction record with nothing in it. 0.5 is the Brier score of a forecaster
# who says "even chance" every time -- uninformative rather than wrong.
UNPROVEN_BRIER = 0.5

# The floor keeps a consistently wrong agent contributing something rather than
# being silenced outright; being reliably wrong is information.
MIN_CONSENSUS_WEIGHT = 0.1


def consensus_weight_for(brier_score: float) -> float:
    """How much an agent's opinion counts, given its calibration.

    One function so the stored default and the value written at calibration
    time cannot drift apart, which is exactly what had happened: the default
    said 1.0 and this formula says 0.5 for the same Brier score.
    """
    return max(MIN_CONSENSUS_WEIGHT, 1.0 - float(brier_score))


class AgentScorecard(BaseModel):
    """Performance tracking for agent self-calibration."""
    agent_name: str
    predictions_made: int = 0
    predictions_correct: int = 0
    predictions_wrong: int = 0
    mean_conviction_on_correct: float = 0.0
    mean_conviction_on_wrong: float = 0.0
    brier_score: float = UNPROVEN_BRIER  # Lower is better (0=perfect, 1=worst)
    # Derived from the starting Brier rather than set to 1.0.
    #
    # It was 1.0, and update_scorecard sets it to consensus_weight_for(brier).
    # An agent that had never resolved a prediction therefore carried the weight
    # of a flawless one -- only a Brier of 0.0 reaches 1.0 -- and the moment it
    # resolved its first prediction the weight halved to 0.5.
    #
    # The consensus engine multiplies this by 10 to get an evidence count for
    # Subjective Logic fusion, so an unevaluated agent moved the fused opinion
    # twice as hard as one measured at the same Brier it starts from. Absence of
    # evidence was being read as evidence of accuracy, and it was not a
    # theoretical exposure: there are no scorecards in Redis at all, so every
    # agent in the swarm is currently weighted through this default.
    consensus_weight: float = consensus_weight_for(UNPROVEN_BRIER)
    last_calibrated_at: Optional[str] = None

class ThrottledLogger:
    """
    Prevents log swarming during high-frequency data bursts.
    Throttles repeated log entries by key so they log at most once per interval_sec seconds.
    """
    def __init__(self, logger_instance: logging.Logger, default_interval_sec: float = 10.0):
        self.logger = logger_instance
        self.default_interval = default_interval_sec
        self._last_logged: Dict[str, float] = {}

    def info(self, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        now = time.time()
        ttl = interval_sec if interval_sec is not None else self.default_interval
        if key not in self._last_logged or (now - self._last_logged[key]) >= ttl:
            self._last_logged[key] = now
            self.logger.info(msg, *args, **kwargs)

    def warning(self, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        now = time.time()
        ttl = interval_sec if interval_sec is not None else self.default_interval
        if key not in self._last_logged or (now - self._last_logged[key]) >= ttl:
            self._last_logged[key] = now
            self.logger.warning(msg, *args, **kwargs)

# How long a prediction outlives its horizon, waiting to be resolved.
#
# Twelve hours, not two: the window has to survive an outage, not just a sweep
# interval. An expired prediction is an inference already spent and a scorecard
# entry never made.
PREDICTION_RESOLUTION_BUFFER_SEC = int(
    os.getenv("PREDICTION_RESOLUTION_BUFFER_SEC", str(12 * 3600))
)


class InferenceBatcher:
    """Collects like questions so that one inference answers all of them.

    The scarce resource on this deployment is not tokens, it is *calls*. A
    single inference takes three to six minutes on a CPU-only host, and the
    shared budget releases one slot every 600 seconds -- so the swarm gets
    roughly twenty model decisions an hour against an input of about a hundred
    and fifty thousand events. RadarAgent was spending one whole slot deciding
    whether to track one ticker.

    Ten tickers in one prompt is ten decisions for the price of one, and the
    prompt grows by a line each while the expensive part -- loading the model,
    evaluating the system prompt, waiting for a slot -- is paid once. The same
    budget then buys an order of magnitude more coverage without asking the
    hardware for anything it cannot do.

    Callers do not see any of this. `submit()` awaits a single item's answer
    exactly as a direct call would, so an agent's handler reads the same way it
    did before.

    Flushes on whichever comes first:
      * `max_items`, so a busy stream does not build an unbounded prompt, and
      * `max_wait_sec`, so a quiet one does not leave a lone candidate waiting
        for company that never arrives.
    """

    def __init__(
        self,
        name: str,
        flush_fn,
        max_items: int = 10,
        max_wait_sec: float = 20.0,
        logger: Optional[logging.Logger] = None,
        max_waiters: Optional[int] = None,
        max_stall_sec: Optional[float] = None,
    ):
        self.name = name
        self._flush_fn = flush_fn
        # Clamped by the caller's stated capacity for simultaneous waiters. A
        # batch that needs more callers than can exist never fills by size and
        # silently degrades to "always wait the full timer", which is the slow
        # path plus a stall.
        if max_waiters is not None and max_items > max_waiters:
            max_items = max(1, int(max_waiters))
        self.max_items = max(1, int(max_items))
        self.max_wait_sec = max(0.5, float(max_wait_sec))
        # Ceiling on how long any one caller waits: the batch window plus room
        # for a real inference on this host, which measured 1-6 minutes.
        self.max_stall_sec = float(
            max_stall_sec if max_stall_sec is not None
            else os.getenv("BATCH_MAX_STALL_SEC", "420")
        )
        self.logger = logger or logging.getLogger(f"agents.batcher.{name}")
        self._pending: List[tuple] = []
        self._lock = asyncio.Lock()
        self._timer: Optional[asyncio.Task] = None
        # Whether a flush is currently inside the model call.
        #
        # The batch window is 20 seconds and an inference on this host takes
        # 400+, so the timer fired about twenty more times while the first call
        # was still running, and each firing started its own inference on
        # whatever had accumulated in the meantime. They did not run in
        # parallel -- the budget serialises them -- they queued, and every
        # caller in every queued batch was counting down the same 420-second
        # stall timeout while waiting for its turn.
        #
        # Measured over ninety minutes: 13 inferences answering 1, 1, 1, 3,
        # four-six-times, 9 and 10 candidates, against 149 callers that timed
        # out having never been answered at all. Three whole inferences spent
        # on one candidate each, while 149 candidates got nothing.
        #
        # Holding new candidates back while a call is in flight costs them
        # nothing -- a batch started now would queue behind that same call
        # anyway -- and it means the next inference answers ten candidates
        # instead of one.
        self._inflight = False

    async def submit(self, key: str, item: Any) -> Any:
        """Queues one question and waits for its answer.

        Returns None when the batch could not be answered -- shed by the
        budget, timed out, malformed. None means "no decision was reached",
        which is what the caller would have got from a failed direct call, and
        is deliberately distinct from a decision of False.
        """
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        async with self._lock:
            self._pending.append((key, item, future))
            should_flush = len(self._pending) >= self.max_items and not self._inflight
            if should_flush and self._timer:
                self._timer.cancel()
                self._timer = None
            elif not self._timer:
                self._timer = safe_create_task(self._flush_after_wait())

        if should_flush:
            # Detached, not awaited inline. Awaiting here put the whole flush --
            # a multi-minute inference -- ahead of the bounded wait below, so
            # the caller that happened to complete the batch was unbounded while
            # every other caller was protected. It also made one arbitrary
            # caller bear the cost of the batch on behalf of the rest.
            safe_create_task(self._flush())

        # Bounded, always.
        #
        # A caller parked here is holding a dispatch slot, and the consume loop
        # blocks once MAX_INFLIGHT_DISPATCHES of them accumulate -- so a batch
        # that never resolves does not merely lose its own answers, it stops the
        # agent reading its topic at all. Observed: radar_agent at processed=5
        # in 29 minutes, consumer live and assigned, offsets frozen across all
        # three partitions while lag climbed past 9,000.
        #
        # Timing out to None is the same outcome the caller already handles for
        # a shed or malformed batch: no verdict was reached. An unanswered
        # candidate costs one inference later; a wedged consumer costs
        # everything behind it.
        try:
            return await asyncio.wait_for(future, timeout=self.max_stall_sec)
        except asyncio.TimeoutError:
            self.logger.warning(
                "%s: no verdict for %s after %.0fs; releasing the dispatch slot. "
                "The batch is still in flight and its answer will be discarded.",
                self.name, key, self.max_stall_sec,
            )
            return None

    async def _flush_after_wait(self):
        try:
            await asyncio.sleep(self.max_wait_sec)
            await self._flush()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.warning("Batch timer for %s failed: %s", self.name, e)

    async def _flush(self):
        async with self._lock:
            # Never cancel the task we are running on.
            #
            # _flush_after_wait calls this, so self._timer is frequently the
            # current task; cancelling it delivered CancelledError at the next
            # await -- which is the inference itself, minutes long. The flush
            # died mid-call and the futures below were never resolved, so every
            # caller waited forever and the agent silently stopped producing
            # decisions. It survived unit testing because a flush that returns
            # instantly finishes before the cancellation is delivered; only real
            # latency exposes it.
            current = asyncio.current_task()
            if self._timer is not None and self._timer is not current:
                self._timer.cancel()
            self._timer = None

            # Someone is already inside the model call. Leave the pending queue
            # alone: it keeps filling, and the running flush re-arms below when
            # it finishes, so these candidates go out together in the next
            # inference instead of starting a rival one that would queue behind
            # it. The timer reference is cleared above, so the next submit()
            # arms a fresh one.
            #
            # This check has to come BEFORE the queue is drained. Draining
            # first and then returning would take the batch out of _pending and
            # abandon it -- its futures never resolved, every caller in it
            # waiting out the full stall timeout for an answer no longer being
            # computed. That is the exact failure the timer-cancel comment
            # above describes, reintroduced one line lower down.
            if self._inflight or not self._pending:
                return
            batch, self._pending = self._pending, []
            self._inflight = True

        keys = [k for k, _, _ in batch]
        try:
            answers = await self._flush_fn([(k, item) for k, item, _ in batch])
            answers = answers or {}
            self.logger.info(
                "%s: one inference answered %s candidate(s) -- %s",
                self.name, len(batch), ", ".join(keys[:8]) + ("..." if len(keys) > 8 else ""),
            )
        except Exception as e:
            # One failure resolves the whole batch to "no decision" rather than
            # leaving callers awaiting a future that will never complete. A
            # hung handler is a worse failure than an unanswered question.
            self.logger.warning("%s: batch of %s failed: %s", self.name, len(batch), e)
            answers = {}

        for key, _, future in batch:
            if not future.done():
                future.set_result(answers.get(key))

        # Released here, not before the resolution above: a flush that started
        # while these futures were still being settled would be answering a
        # queue this one had already taken.
        async with self._lock:
            self._inflight = False
            # Anything that arrived during the call goes out now. It has waited
            # the length of a full inference already; making it wait another
            # batch window on top of that is latency with nothing bought by it.
            if self._pending and self._timer is None:
                self._timer = safe_create_task(self._flush())


def _is_resolvable(pred) -> bool:
    """Whether a prediction could ever be scored, however long it is held.

    A directional price prediction needs a positive entry to compare against.
    Categorical and entity-appearance predictions do not, so they are never
    retired on this basis.
    """
    kind = str(getattr(pred, "prediction_kind", "") or "price")
    if kind != "price" or (getattr(pred, "outcome_space", None) or []):
        return True
    try:
        entry = float(getattr(pred, "entry_price", 0) or 0)
    except (TypeError, ValueError):
        return False
    return entry > 0


class SentinelAgent(ABC):
    _global_received_count = 0
    def __init__(self, agent_name: str, input_topics: List[str], redis_client, db_client, neo4j_client, producer, consumer, dlq, model=DEFAULT_MODEL, fallback_model: Optional[str] = None):
        self.name = agent_name
        self.input_topics = input_topics
        self.redis = redis_client 
        self.redis_client = redis_client
        self.db = db_client
        self.neo4j = neo4j_client
        self._producer = producer
        self._consumer = consumer
        self._dlq = dlq
        self.model = model
        self.fallback_model = fallback_model
        self.logger = logging.getLogger(f"agent.{agent_name}")
        self._processed = 0
        self._errors = 0
        self._started_at = datetime.now(timezone.utc)
        
        # Declared here for IDE support; instantiated inside the async event loop in run().
        self._session: Optional[aiohttp.ClientSession] = None
        self._llm: Optional[OllamaClient] = None
        
        # Concurrency bound: Limit inflight tasks to prevent memory explosion and LLM timeouts
        # Concurrency has to exceed the largest batch an agent assembles.
        #
        # A batching agent parks each caller on a future until the batch flushes,
        # and those callers hold a dispatch slot while they wait. With
        # concurrency 5 and a batch size of 10 the size trigger is unreachable
        # by construction: only five callers can ever be waiting, so every batch
        # falls through to its timer while holding every slot the agent has.
        # Observed live -- radar_agent's processed count froze at 5,292 with
        # 4,902 messages of lag behind it.
        #
        # These are idle awaits, not work, so the bound is about bookkeeping
        # rather than load; it needs to be comfortably above the batch size.
        self.dispatch_concurrency = int(os.getenv("AGENT_CONCURRENCY", "24"))
        self._dispatch_semaphore = asyncio.Semaphore(self.dispatch_concurrency)

        # Cross-agent state synchronization (§3.3):
        # Track recently processed event IDs and entities for context drift detection.
        # ConsensusEngine reads these digests to detect when agents have
        # divergent world-states before fusing their bulletins.
        from collections import deque
        self._recent_event_ids: deque = deque(maxlen=20)
        self._recent_entities: deque = deque(maxlen=20)
        self._current_regime: Optional[str] = None  # Set by subclass if applicable
    @abstractmethod
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pass

    @property
    @abstractmethod
    def output_topic(self) -> str:
        pass

    @property
    def producer(self):
        return self._producer

    async def run(self):
        from shared.utils.ollama import OLLAMA_TIMEOUT
        connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
        self._llm = OllamaClient(self._session, self.model, redis_client=self.redis_client)

        self.logger.info("=" * 60)
        self.logger.info(f"SENTINEL Agent: {self.name} | Model: {self.model} @ {OLLAMA_URL}")
        self.logger.info("=" * 60)

        await self._consumer.start()
        await self._producer.start()
        await self._dlq.start()
        heartbeat_task = safe_create_task(self._heartbeat_loop())
        # Without this, predictions expire unscored and every scorecard
        # stays at its default -- see _resolve_predictions_loop.
        resolver_task = safe_create_task(self._resolve_predictions_loop())

        # Run the scheduled review, if this agent defines one.
        #
        # `run_scheduled_review` is defined on the quant trading engine and on
        # the consensus engine and was called by nothing -- no scheduler, no
        # dynamic dispatch, no caller anywhere in the tree. So the quant
        # engine's sweep across watched equities had never run, the consensus
        # engine's scheduled analysis had never run, and the backtest refresh
        # added earlier in this audit sat inside a method that could not
        # execute, which is why sentinel:backtest:* was still empty after
        # thirty-five minutes of live running.
        # A one-time startup task, for agents that define one.
        #
        # Added because the graph type backfill was written into
        # start_supervisor(), which is only reachable from that module's
        # __main__ and which no compose service runs -- GraphSupervisor is
        # constructed through build_agent like every other agent. That is the
        # second time in this audit a repair was placed in a function with no
        # caller; a declared hook is what stops it being the third.
        if hasattr(self, "on_start"):
            self._on_start_task = safe_create_task(
                self.on_start(), name=f"{self.name}-on-start"
            )

        if hasattr(self, "run_scheduled_review"):
            # Held on the instance so the task is not garbage-collected mid-flight
            # and so shutdown can cancel it, matching how the resolver is kept.
            self._review_task = safe_create_task(
                self._scheduled_review_loop(), name=f"{self.name}-scheduled-review"
            )

        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            self.logger.info(f"{self.name} cancelled — shutting down")
        finally:
            heartbeat_task.cancel()
            resolver_task.cancel()
            # Dispatches now outlive the loop that started them, so give them a
            # bounded chance to finish before the HTTP session they are using is
            # closed underneath them. Anything still running after this is
            # cancelled rather than left to fail noisily on a dead session.
            inflight = set(getattr(self, "_inflight", set()))
            if inflight:
                self.logger.info(f"Draining {len(inflight)} in-flight dispatches...")
                done, pending = await asyncio.wait(inflight, timeout=30)
                for task in pending:
                    task.cancel()
            if self._session:
                await self._session.close()
            await self._consumer.close()
            await self._producer.close()
            await self._dlq.close()


    # Event types this agent can act on, or None for "everything".
    #
    # Declared per agent and enforced before the dispatch semaphore, so an agent
    # is not spending its concurrency limit deserialising and queueing telemetry
    # it will reject on the first line of handle(). None is the safe default:
    # an agent whose interests have not been written down keeps receiving
    # everything it did before.
    INTERESTED_EVENT_TYPES = None

    # Types an agent will still receive despite the routine-telemetry denylist.
    # Empty for every agent today; the hook exists so that adding one is a
    # declaration on the agent rather than an edit to the shared filter.
    ACCEPTS_TELEMETRY: frozenset = frozenset()

    async def _consume_loop(self):
        loop = asyncio.get_running_loop()
        self._inflight: set = getattr(self, "_inflight", set())
        while True:
            try:
                batches = await self._consumer.get_batch(timeout_ms=1000)
                if not batches:
                    continue
                for tp, msg_list in batches.items():
                    batch_size = len(msg_list)
                    SentinelAgent._global_received_count += batch_size
                    if SentinelAgent._global_received_count >= 500:
                        logging.getLogger("agents.swarm").info(f"Swarm processed 500 messages across all agent services.")
                        SentinelAgent._global_received_count = 0
                    tasks = []
                    for msg in msg_list:
                        try:
                            payload = json.loads(msg.value.decode('utf-8'))
                            tasks.append(safe_create_task(self._dispatch(payload)))
                        except json.JSONDecodeError as e:
                            self.logger.error(f"POISON PILL dropped: {e}")
                            await self._send_dlq({"raw": str(msg.value)}, "JSONDecodeError", self.input_topics[0])

                    # Reading is decoupled from thinking. Awaiting the batch here
                    # meant one slow inference froze the loop: a single measured
                    # call took 482 seconds, during which this consumer neither
                    # polled nor committed, so its lag grew while it sat idle
                    # holding one message. Agents that never call a model were
                    # stalled by agents that did.
                    #
                    # Tasks now run detached and are accounted for as they finish.
                    # Nothing about an agent's own logic changes -- the same
                    # handler runs with the same model on the same message -- only
                    # who waits for it.
                    topic_name = tp.topic if hasattr(tp, "topic") else (
                        self.input_topics[0] if self.input_topics else "unknown"
                    )
                    for task, msg in zip(tasks, msg_list):
                        self._inflight.add(task)
                        task.add_done_callback(self._inflight.discard)
                        task.add_done_callback(
                            lambda t, m=msg, tn=topic_name: self._account_for(t, m, tn)
                        )

                    # Bounded, so a slow model cannot turn unread backlog into
                    # unbounded memory. Waiting for the *first* completion rather
                    # than all of them keeps the loop moving as soon as capacity
                    # frees up.
                    while len(self._inflight) >= MAX_INFLIGHT_DISPATCHES:
                        await asyncio.wait(
                            set(self._inflight), return_when=asyncio.FIRST_COMPLETED
                        )

                # Committed once the work is accepted rather than once it is
                # finished. The at-least-once guarantee is deliberately not
                # extended to model output: the event itself is already persisted
                # and correlated upstream, so a crash mid-inference loses one
                # opinion, which is what the budget sheds by design anyway.
                try:
                    await self._consumer.commit()
                except Exception as commit_err:
                    self.logger.warning(f"Consumer commit skipped (partition rebalance/timeout): {commit_err}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.error(f"Consume loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    def _account_for(self, task: "asyncio.Task", msg, topic_name: str) -> None:
        """Records the outcome of a detached dispatch.

        Runs on the event loop as each task finishes, so the consume loop never
        waits on it. The DLQ send is itself scheduled rather than awaited,
        because a done-callback cannot await.
        """
        if task.cancelled():
            return
        err = task.exception()
        if err is None:
            return
        if isinstance(err, InferenceShed):
            # Capacity, not failure: no DLQ, no error counter.
            self._shed = getattr(self, "_shed", 0) + 1
            if self._shed % 1000 == 1:
                self.logger.info(
                    "%s: shed %s messages to stay within the inference budget",
                    self.name, self._shed,
                )
            return
        self.logger.error(f"Dispatch task failed with unhandled exception: {err}", exc_info=err)
        try:
            payload = json.loads(msg.value.decode("utf-8"))
        except Exception:
            payload = {"raw": str(msg.value)}
        safe_create_task(
            self._send_dlq(payload, f"UnhandledException: {type(err).__name__}: {err}", topic_name)
        )

    # A reserved slot in the inference budget, or None to share the common one.
    #
    # The default is to share: eight agents against a single-threaded model
    # server, one slot every ten minutes, and a shared key is what stops them
    # queueing behind each other. An agent gets its own lane only when sharing
    # demonstrably starves it -- see RadarAgent, whose batched decisions never
    # landed because knowledge_graph_engine, rule_synthesizer and
    # stock_correlation_agent kept winning the common slot.
    #
    # Each lane is one more inference that can be in flight, so this is a real
    # cost paid against a real host. Adding lanes without measuring the
    # starvation first would rebuild the queue the budget exists to prevent.
    INFERENCE_LANE: Optional[str] = None

    # How late an event may be and still be worth an agent's attention.
    #
    # An hour, not the correlation engine's fifteen minutes. Copying that window
    # here was wrong twice over. Correlation asks "what co-occurred inside a
    # sliding window", so fifteen minutes is the question itself; an agent asks
    # "is this worth watching", which an hour-old signal still answers.
    #
    # And agents consume the whole enriched.events firehose while caring about a
    # small slice of it, so they run steadily behind -- radar_agent sat ~15,600
    # messages back. Everything it read was therefore older than 900s and every
    # single event was dropped: "radar_agent skipped 10,001 event(s) older than
    # 900s", including a $479,668 GOOGL block that passed every other gate. A
    # guard meant to stop the system reasoning about yesterday was instead
    # stopping it reasoning at all.
    MAX_EVENT_AGE_SEC = int(os.getenv("AGENT_MAX_EVENT_AGE_SEC", "3600"))

    async def _dispatch(self, raw: Dict[str, Any]):
        # Refuse to act on history.
        #
        # An agent resuming after any interruption -- a deploy, a crash, a
        # laptop suspending overnight -- works forward from its committed
        # offset through everything that arrived while it was gone. Measured
        # after one such gap: the radar orchestrator held ~44,000 events. Acting
        # on those means escalating tickers whose flow finished hours ago and
        # spending the swarm's scarcest resource, an inference slot, to do it.
        #
        # Skipped, not seeked: the message is still consumed, committed and
        # counted, so the lag figure stays honest and the backlog drains at
        # parse speed instead of at inference speed.
        if is_stale(raw, self.MAX_EVENT_AGE_SEC):
            self._stale_skipped = getattr(self, "_stale_skipped", 0) + 1
            if self._stale_skipped % 2000 == 1:
                self.logger.warning(
                    "%s skipped %s event(s) older than %ss while catching up.",
                    self.name, self._stale_skipped, self.MAX_EVENT_AGE_SEC,
                )
            return

        # Type filtering before the dispatch slot, not inside handle().
        #
        # Every agent subscribes to ENRICHED_EVENTS, which is 90% position and
        # transfer telemetry, and each agent then rejects what it does not want
        # on the first lines of its own handle(). That rejection is correct and
        # it happens too late: the message has already taken a slot on the
        # dispatch semaphore, so the whole firehose is serialised through a
        # concurrency limit sized for real work.
        #
        # The radar orchestrator was 68,937 messages behind and diverging at 7.8
        # a second on exactly this. An agent that declares the event types it
        # can act on now drops the rest at parse speed. Agents that declare
        # nothing are unaffected, so this cannot silently narrow an agent whose
        # interests were never written down.
        # Routine telemetry, dropped for every agent.
        #
        # The allowlist below is opt-in and only radar_agent had ever filled it
        # in, so nine of ten agents took the whole firehose onto the dispatch
        # semaphore. An allowlist cannot safely be written for the others --
        # they route on source, ticker and payload shape as much as on type, so
        # declaring their interests as a type set would silently drop work they
        # currently do.
        #
        # A denylist can. These three types are position reports: a vessel or an
        # aircraft saying where it is. No agent's handle() references any of
        # them -- verified across all ten -- and they are the bulk of
        # ENRICHED_EVENTS. Dropping them here is the difference between an
        # agent's concurrency limit being sized for real work and being sized
        # for the firehose.
        #
        # An agent that later needs them declares them in ACCEPTS_TELEMETRY and
        # this stops applying to it.
        etype_raw = raw.get("type") or raw.get("event_type")
        if etype_raw and str(etype_raw) in ROUTINE_TELEMETRY_TYPES:
            if str(etype_raw) not in getattr(self, "ACCEPTS_TELEMETRY", frozenset()):
                self._telemetry_dropped = getattr(self, "_telemetry_dropped", 0) + 1
                if self._telemetry_dropped % 50000 == 1:
                    self.logger.debug(
                        "%s: %s routine position report(s) dropped before dispatch.",
                        self.name, self._telemetry_dropped,
                    )
                return

        interests = getattr(self, "INTERESTED_EVENT_TYPES", None)
        if interests:
            etype = raw.get("type") or raw.get("event_type")
            # Only filter messages that carry a type. A correlation cluster, a
            # bulletin and an agent brief have their own shapes, and an agent
            # subscribing to those topics must keep receiving them.
            if etype and str(etype) not in interests:
                self._filtered_out = getattr(self, "_filtered_out", 0) + 1
                if self._filtered_out % 50000 == 1:
                    self.logger.debug(
                        "%s: %s message(s) dropped before dispatch as uninteresting types.",
                        self.name, self._filtered_out,
                    )
                return

        async with self._dispatch_semaphore:
            t0 = time.monotonic()
            try:
                result = await self.handle(raw)
                if result is not None:
                    await self._producer.send(
                        self.output_topic,
                        result,
                        key=result.get("agent_run_id", str(uuid.uuid4())),
                    )
                    # Broadcast agent decision brief to Redis PubSub for live WebSocket UI streaming
                    try:
                        res_dict = result if isinstance(result, dict) else (result.model_dump() if hasattr(result, "model_dump") else {})

                        # An agent that produced no prose has no brief to show.
                        #
                        # The summary fell back to str(res_dict)[:200], so a
                        # graph-write confirmation reached the feed as an
                        # executive summary:
                        #
                        #   {'agent': 'supervisor', 'action': 'single_commit',
                        #    'entity_id': '8881EF'}
                        #
                        # scored 0.85 because that was the default when an
                        # agent stated no confidence. Bookkeeping presented as
                        # intelligence, ranked above most real findings.
                        narrative = agent_narrative(res_dict)
                        if not narrative:
                            raise _NoBriefToPublish

                        stated_confidence = res_dict.get("confidence")
                        if stated_confidence is None:
                            stated_confidence = res_dict.get("anomaly_score")

                        agent_pub_payload = {
                            "event_id": str(res_dict.get("agent_run_id") or uuid.uuid4()),
                            "type": f"agent_{self.name}",
                            "occurred_at": datetime.now(timezone.utc).isoformat(),
                            "source": f"Agent Swarm ({self.name})",
                            "primary_entity_id": str(res_dict.get("primary_entity") or res_dict.get("ticker") or self.name),
                            "primary_entity_name": str(res_dict.get("primary_entity") or res_dict.get("name") or self.name.replace("_", " ").title()),
                            "entity_name": str(res_dict.get("primary_entity") or res_dict.get("name") or self.name.replace("_", " ").title()),
                            "headline": f"🤖 AGENT [{self.name.upper()}]: {res_dict.get('headline') or clip(narrative, 120)}",
                            "summary": narrative,
                            # No invented confidence. An agent that did not
                            # state one gets the floor rather than 0.85, which
                            # ranked every silent agent above most measured
                            # findings.
                            "anomaly_score": float(stated_confidence) if stated_confidence is not None else AGENT_UNSTATED_CONFIDENCE,
                            "region": "GLOBAL",
                            "tags": ["agent_output", f"agent:{self.name}"],
                        }
                        await self.redis.raw.publish("sentinel:events:live", json.dumps(agent_pub_payload))
                    except _NoBriefToPublish:
                        self.logger.debug(
                            "%s produced no narrative; nothing to put in the feed.", self.name
                        )
                    except Exception as pub_err:
                        self.logger.debug(f"Agent live feed pub bypass: {pub_err}")
                self._processed += 1

                # Track event ID & entity for cross-agent state synchronization (§3.3)
                event_id = raw.get("event_id") or raw.get("agent_run_id")
                if event_id:
                    self._recent_event_ids.append(str(event_id))

                ent = raw.get("primary_entity_id") or raw.get("ticker") or raw.get("asset") or raw.get("primary_entity")
                if ent:
                    if isinstance(ent, dict):
                        ent = ent.get("id") or ent.get("name")
                    if ent:
                        self._recent_entities.append(str(ent))

                elapsed = time.monotonic() - t0
                if elapsed > 10:
                    self.logger.warning(f"Slow dispatch: {elapsed:.1f}s")
            except SchemaViolationError as e:
                self._errors += 1
                await self._send_dlq(raw, f"SchemaViolationError: {str(e)}", self.input_topics[0])
            except InferenceError as e:
                self._errors += 1
                self.logger.error(f"Inference error in agent {self.name}: {e}")
                await self._send_dlq(raw, f"InferenceError: {str(e)}", self.input_topics[0])
            except ValueError as e:
                self._errors += 1
                await self._send_dlq(raw, f"ValueError: {str(e)}", self.input_topics[0])
            except Exception as e:
                self.logger.error(f"Transient or unhandled dispatch error: {e}", exc_info=True)
                # Re-raise to crash the batch, skip commit, and preserve At-Least-Once delivery
                raise

    async def _send_dlq(self, raw: Dict, error: str, topic: str):
        try:
            await self._dlq.send("dead.letter", {"error": error, "topic": topic, "raw": raw, "agent": self.name})
        except Exception as e:
            self.logger.error(f"DLQ send failed: {e}")

    async def _heartbeat_loop(self):
        # Last sample, so the rate below can be windowed rather than lifetime.
        last_processed = self._processed
        last_sample_at = datetime.now(timezone.utc)
        stalled_since = None

        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            now = datetime.now(timezone.utc)
            elapsed = (now - self._started_at).total_seconds()

            # The rate was `processed / uptime` -- a lifetime average.
            #
            # Over a five-hour run a total stall moves that figure by a rounding
            # error, so radar_agent printed an unchanged processed=15738 on two
            # consecutive lines a minute apart and reported rate=0.80/s on both.
            # The heartbeats exist to make a silent agent audible and the metric
            # chosen could not express the condition they were added to detect.
            window_s = max(1e-9, (now - last_sample_at).total_seconds())
            window_delta = self._processed - last_processed
            window_rate = window_delta / window_s
            lifetime_rate = self._processed / elapsed if elapsed > 0 else 0.0

            if window_delta == 0:
                stalled_since = stalled_since or last_sample_at
            else:
                stalled_since = None
            stalled_seconds = (now - stalled_since).total_seconds() if stalled_since else 0.0

            stall_note = f" STALLED {int(stalled_seconds)}s" if stalled_seconds >= HEARTBEAT_INTERVAL else ""
            self.logger.info(
                f"♥ {self.name} | processed={self._processed} errors={self._errors} "
                f"rate={window_rate:.2f}/s (lifetime {lifetime_rate:.2f}/s){stall_note}"
            )

            last_processed = self._processed
            last_sample_at = now

            # What the platform health surface grades this agent on. Without it
            # a component is scored on liveness alone, which is what let a
            # consumer 68,000 messages behind report HEALTHY.
            try:
                error_rate = (self._errors / self._processed) if self._processed else 0.0
                await touch_heartbeat(
                    self.redis, self.name,
                    metadata={
                        "processed": self._processed,
                        "window_rate": round(window_rate, 4),
                        "stalled_seconds": round(stalled_seconds, 1),
                        "error_rate": round(error_rate, 4),
                        "consumer_lag": getattr(self, "_consumer_lag", None),
                        "lag_growing": getattr(self, "_lag_growing", None),
                    },
                )
            except Exception as hb_err:
                self.logger.debug(f"Progress heartbeat failed for {self.name}: {hb_err}")
            try:
                await self.redis.raw.set(
                    f"sentinel:agents:health:{self.name}",
                    json.dumps({
                        "processed": self._processed,
                        "errors":    self._errors,
                        "uptime_s":  int(elapsed),
                        "ts":        datetime.now(timezone.utc).isoformat(),
                    }),
                    ex=120, 
                )
            except Exception:
                pass

            # Publish state digest for cross-agent context drift detection (§3.3)
            try:
                digest = {
                    "agent_name": self.name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "recent_event_ids": list(self._recent_event_ids),
                    "recent_entities": list(self._recent_entities),
                    "input_topics": self.input_topics,
                    "processed_count": self._processed,
                    "current_regime": self._current_regime,
                    "model": self.model,
                }
                await self.redis.raw.set(
                    f"sentinel:agent:digest:{self.name}",
                    json.dumps(digest),
                    ex=300,  # 5-minute TTL: if not refreshed, agent is stale
                )
            except Exception:
                pass

    def state_key(self, *parts: str) -> str:
        return f"sentinel:agents:{self.name}:{':'.join(parts)}"

    async def write_agent_memory(self, memory_text: str, ttl: int = 86400):
        """
        Writes a timestamped episodic memory to a shared Redis sorted set.
        Allows cross-agent asynchronous communication (e.g. Quant -> News).
        """
        try:
            now = time.time()
            memory_payload = json.dumps({
                "agent": self.name,
                "text": memory_text,
                "ts": datetime.now(timezone.utc).isoformat()
            })
            
            # Use ZADD with current timestamp as score for easy chronological retrieval
            await self.redis.raw.zadd("sentinel:agents:episodic_memory", mapping={memory_payload: now})
            
            # Trim the memory stream to keep only the 100 most recent memories to prevent bloat
            await self.redis.raw.zremrangebyrank("sentinel:agents:episodic_memory", 0, -101)
            
            # Note: We don't set TTL on the ZSET itself because it's a shared stream, 
            # we just prune old entries by rank. Alternatively could prune by score.
            
            self.logger.debug(f"Episodic memory stored: {memory_text[:50]}...")
        except Exception as e:
            self.logger.warning(f"Failed to write agent memory: {e}")

    async def read_agent_memories(self, limit: int = 5) -> str:
        """
        Reads the most recent cross-agent episodic memories.
        Returns a formatted string ready for LLM prompt injection.
        """
        try:
            # ZREVRANGE to get most recent first
            raw_memories = await self.redis.raw.zrevrange("sentinel:agents:episodic_memory", 0, limit - 1)
            
            if not raw_memories:
                return "No recent agent memories."
                
            context = "\n### CROSS-AGENT MEMORIES ###\n"
            for raw in raw_memories:
                try:
                    mem = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                    ts = str(mem.get('ts', ''))[:19]
                    context += f"• [{ts}] {mem.get('agent', 'UnknownAgent')}: {mem.get('text', '')}\n"
                except Exception:
                    pass
            return context
        except Exception as e:
            self.logger.warning(f"Failed to read agent memories: {e}")
            return "Failed to fetch memories."

    async def get_cross_agent_context(self, ticker: Optional[str] = None, entity_id: Optional[str] = None, limit: int = 3) -> str:
        """
        Concise, fully dynamic helper for agents to retrieve active bulletins, cross-agent memories, and swarm consensus for LLM prompt injection.
        Filtering out self-memories ensures strictly peer-agent intelligence is provided.
        """
        lines = []
        lookup_key = ticker or entity_id
        try:
            bulletins = await self.read_bulletins(ticker=lookup_key)
            if bulletins:
                # Exclude self-bulletins for pure cross-agent context
                peer_bulletins = [b for b in bulletins if b.agent_name != self.name]
                if peer_bulletins:
                    bulletin_strs = [f"[{b.agent_name}->{b.bulletin_type}] {b.summary}" for b in peer_bulletins[:3]]
                    lines.append("Active Bulletins:\n- " + "\n- ".join(bulletin_strs))
        except Exception as e:
            self.logger.debug(f"Bulletin fetch error: {e}")

        try:
            raw_consensus = await self.redis.raw.get("sentinel:consensus:latest")
            if raw_consensus:
                cons = json.loads(raw_consensus if isinstance(raw_consensus, str) else raw_consensus.decode("utf-8"))
                summary = cons.get("summary") or cons.get("consensus_summary")
                if summary:
                    lines.append(f"Swarm Consensus: {clip(summary, 200)}")
        except Exception as e:
            self.logger.debug(f"Consensus fetch error: {e}")

        try:
            raw_mems = await self.redis.raw.zrevrange("sentinel:agents:episodic_memory", 0, limit * 2)
            if raw_mems:
                mem_strs = []
                for raw in raw_mems:
                    try:
                        m = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                        text = m.get('text', '')
                        agent_name = m.get('agent', 'Agent')
                        if text and agent_name != self.name:
                            mem_strs.append(f"[{agent_name}]: {text}")
                    except Exception:
                        pass
                if mem_strs:
                    lines.append("Cross-Agent Memories:\n- " + "\n- ".join(mem_strs[:limit]))
        except Exception as e:
            self.logger.debug(f"Memory fetch error: {e}")

        return "\n".join(lines) if lines else ""

    # ── STRUCTURED BULLETIN SYSTEM ──────────────────────────────────────────

    async def publish_bulletin(
        self,
        bulletin_type: str,
        summary: str,
        ticker: Optional[str] = None,
        primary_entity_id: Optional[str] = None,
        primary_entity_name: Optional[str] = None,
        conviction: float = 0.5,
        expected_direction: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = 3600,
    ) -> None:
        """
        Publishes a structured AgentBulletin to Redis.
        Other agents can query this via read_bulletins() or subscribe_bulletins().
        """

        # Offer this subject to the rest of the swarm.
        #
        # Consensus fuses by entity and has never had two opinions on one --
        # six live bulletins, five agents, six distinct tickers. Nothing asked a
        # second agent to look at what the first found. This does, without
        # compelling anything: the other agents consult the focus set when
        # choosing and are free to ignore it.
        safe_create_task(
            offer_focus(self.redis, ticker, conviction, offered_by=self.name),
            name=f"{self.name}-offer-focus",
        )
        try:
            ent_id = primary_entity_id or ticker
            ent_name = primary_entity_name or ent_id
            bulletin = AgentBulletin(
                agent_name=self.name,
                bulletin_type=bulletin_type,
                primary_entity_id=ent_id,
                primary_entity_name=ent_name,
                ticker=ticker,
                conviction=conviction,
                expected_direction=expected_direction,
                payload=payload or {},
                summary=summary,
                ttl_seconds=ttl_seconds,
            )
            key = f"sentinel:bulletins:{self.name}:{bulletin_type}"
            if ticker:
                key += f":{ticker.upper()}"
            await self.redis.raw.set(key, bulletin.model_dump_json(), ex=ttl_seconds)

            # A bulletin is the agent's conclusion, so it is also the memory
            # worth sharing.
            #
            # write_agent_memory had no call site anywhere, so
            # sentinel:agents:episodic_memory held zero entries -- while
            # read_agent_memories is called by the macro engine on every
            # relevant dispatch and therefore returned nothing, every time.
            # Both halves were built; only the write was never wired.
            #
            # Hooking it here rather than asking each agent to remember
            # separately keeps the two in step: an agent that publishes a
            # conclusion has, by construction, something worth recalling.
            await self.write_agent_memory(
                f"[{bulletin_type}] {summary}"
                + (f" (conviction {bulletin.conviction:.2f})" if bulletin.conviction else "")
            )

            # Also publish to PubSub for real-time listeners
            await self.redis.raw.publish(
                "sentinel:bulletins:stream",
                bulletin.model_dump_json(),
            )
            self.logger.debug(f"Published bulletin: {bulletin_type} | {summary[:60]}")
        except Exception as e:
            self.logger.warning(f"Failed to publish bulletin: {e}")

    def safe_create_task(self, coro, task_name: Optional[str] = None) -> asyncio.Task:
        """
        Launches an async coroutine as a background task with exception logging callback.
        """
        task = safe_create_task(coro, name=task_name)
        def _on_done(t: asyncio.Task):
            try:
                if not t.cancelled() and t.exception():
                    self.logger.warning(f"Background task '{t.get_name()}' failed: {t.exception()}")
            except Exception as ex:
                self.logger.debug(f"Task callback error: {ex}")
        task.add_done_callback(_on_done)
        return task

    async def read_bulletins(
        self,
        agent_name: Optional[str] = None,
        bulletin_type: Optional[str] = None,
        ticker: Optional[str] = None,
    ) -> List[AgentBulletin]:
        """
        Reads active agent bulletins from Redis.
        Filters by agent_name, bulletin_type, and/or ticker.
        Returns typed AgentBulletin objects with automatic expiry filtering.
        """
        try:
            pattern_parts = ["sentinel:bulletins"]
            pattern_parts.append(agent_name if agent_name else "*")
            pattern_parts.append(bulletin_type if bulletin_type else "*")
            if ticker:
                pattern_parts.append(ticker.upper())
            else:
                pattern_parts.append("*")
            pattern = ":".join(pattern_parts)

            bulletins = []
            cursor = 0
            while True:
                cursor, keys = await self.redis.raw.scan(cursor=cursor, match=pattern, count=50)
                if keys:
                    values = await self.redis.raw.mget(keys)
                    for val in values:
                        if val:
                            try:
                                raw = val if isinstance(val, str) else val.decode("utf-8")
                                bulletins.append(AgentBulletin(**json.loads(raw)))
                            except Exception:
                                pass
                if cursor == 0:
                    break

            # Sort by published_at descending
            bulletins.sort(key=lambda b: b.published_at, reverse=True)
            return bulletins
        except Exception as e:
            self.logger.warning(f"Failed to read bulletins: {e}")
            return []

    async def subscribe_bulletins(self, types: List[str], limit: int = 10) -> List[AgentBulletin]:
        """
        Convenience: reads bulletins matching any of the given types across all agents.
        Returns sorted by recency, limited to `limit` results.
        """
        all_bulletins = []
        for btype in types:
            bulletins = await self.read_bulletins(bulletin_type=btype)
            all_bulletins.extend(bulletins)
        all_bulletins.sort(key=lambda b: b.published_at, reverse=True)
        return all_bulletins[:limit]

    async def get_bulletins_for_prompt(self, types: Optional[List[str]] = None, limit: int = 5) -> str:
        """
        Returns a formatted string of active bulletins for LLM prompt injection.
        """
        types = types or ["regime_change", "signal", "alert", "thesis"]
        bulletins = await self.subscribe_bulletins(types, limit=limit)
        if not bulletins:
            return ""
        context = "\n### ACTIVE AGENT BULLETINS ###\n"
        for b in bulletins:
            direction_str = f" ({b.expected_direction})" if b.expected_direction else ""
            ticker_str = f" [{b.ticker}]" if b.ticker else ""
            context += (
                f"- [{b.agent_name}] {b.bulletin_type}{ticker_str}{direction_str} "
                f"(conviction: {b.conviction:.0%}): {b.summary}\n"
            )
        return context + "\n"

    # ── AGENT SELF-CALIBRATION & PREDICTION TRACKING ───────────────────────

    async def record_prediction(
        self,
        ticker: str,
        direction: str,
        conviction: float,
        entry_price: float,
        target_price: float = 0.0,
        time_horizon_hours: int = 24,
        prediction_kind: str = "price",
    ) -> str:
        """
        Records a prediction for later verification.
        Returns the prediction_id.

        A repeat of a standing call is not a second forecast. The quant engine
        re-derives the same plays on every run, and with nothing to stop it the
        same claim was stored again each time: of six predictions recorded, two
        pairs were byte-identical duplicates of one another. That inflates the
        count, and it double-weights the scorecard that the consensus engine
        reads -- an agent repeating itself would outrank one that was right.
        """
        try:
            # One standing call per agent, ticker and direction. A genuine
            # change of view -- a reversal, or a new entry after the last
            # horizon lapsed -- still gets through, because the claim expires
            # with the prediction it guards.
            claim_key = (
                f"sentinel:predictions:claim:{self.name}:"
                f"{str(ticker).upper()}:{str(direction).lower()}"
            )
            claim_ttl = max(int(time_horizon_hours) * 3600, 3600)
            try:
                is_new = await self.redis.raw.set(claim_key, "1", nx=True, ex=claim_ttl)
                if not is_new:
                    self.logger.debug(
                        "Prediction for %s %s already stands; not recording a duplicate.",
                        ticker, direction,
                    )
                    return ""
            except Exception as e:
                # A failed claim must not lose the prediction. Recording a
                # duplicate is a smaller harm than dropping a forecast.
                self.logger.debug(f"Prediction dedupe claim failed for {ticker}: {e}")

            pred = AgentPrediction(
                agent_name=self.name,
                ticker=ticker.upper(),
                direction=direction,
                conviction=conviction,
                entry_price=entry_price,
                target_price=target_price,
                time_horizon_hours=time_horizon_hours,
                prediction_kind=prediction_kind,
            )
            key = f"sentinel:predictions:{self.name}:{pred.prediction_id}"
            # Horizon plus a generous window to be resolved in.
            #
            # This was a two-hour buffer. The resolver sweeps every fifteen
            # minutes, so two hours is ample while the agent is running -- and
            # the agent is frequently not: deploys, restarts, a laptop
            # suspending overnight. Any of those spanning the wrong two hours
            # expires the prediction unresolved, and an unresolved prediction is
            # a wasted inference on a host that affords about twenty an hour.
            # The scorecards it would have fed are what weight the consensus
            # engine, so the loss compounds.
            #
            # Redis storage for a few extra hours is the cheapest thing in this
            # system. The buffer is sized for the outage, not the sweep.
            ttl = max(
                time_horizon_hours * 3600 + PREDICTION_RESOLUTION_BUFFER_SEC,
                86400,
            )
            await self.redis.raw.set(key, pred.model_dump_json(), ex=ttl)

            # Index by ticker for fast lookup
            idx_key = f"sentinel:predictions:by_ticker:{ticker.upper()}"
            await self.redis.raw.sadd(idx_key, key)
            await self.redis.raw.expire(idx_key, ttl)

            # A categorical call on a market that prices the same outcome is a
            # paired forecast: Sentinel's probability for a named outcome beside
            # the market's, on one proposition and on the same 0-1 scale. That
            # is the only pairing in this system where both sides are answering
            # an identical question, so it is the one recorded.
            await self._record_paired_forecast(pred)

            self.logger.debug(f"Recorded prediction {pred.prediction_id}: {ticker} {direction} @ {conviction:.0%}")
            return pred.prediction_id
        except Exception as e:
            self.logger.warning(f"Failed to record prediction: {e}")
            return ""

    async def _record_paired_forecast(self, pred: "AgentPrediction") -> bool:
        """Files a Sentinel/market probability pair, when one genuinely exists.

        Only categorical predictions qualify. A price-direction call ("up on
        NVDA") has no market quoting the same proposition, and grading it
        against one would produce a Brier score for a question nobody asked --
        which is why MarketCalibrationTracker had no callers rather than a
        convenient one.
        """
        if not pred.outcome_space or not pred.predicted_outcome:
            return False
        market_key = pred.market_key or pred.ticker
        distribution = await self._latest_outcome_distribution(market_key)
        if not distribution:
            return False

        # The market's price for the very outcome the agent named.
        market_p = None
        target = pred.predicted_outcome.strip().lower()
        for name, price in distribution.items():
            if str(name).strip().lower() == target:
                market_p = float(price)
                break
        if market_p is None:
            return False

        try:
            from services.reasoning.market_calibration import (
                MarketCalibrationTracker,
                PairedForecast,
            )
        except Exception as e:
            self.logger.debug(f"Calibration tracker unavailable: {e}")
            return False

        tracker = MarketCalibrationTracker(self.redis)
        return await tracker.record_forecast(PairedForecast(
            market_id=f"{market_key}:{pred.predicted_outcome}",
            question=f"{market_key}: does '{pred.predicted_outcome}' win?",
            sentinel_probability=max(0.0, min(1.0, float(pred.conviction))),
            market_probability=max(0.0, min(1.0, market_p)),
            ticker=pred.ticker,
        ))

    async def current_regime(self) -> str:
        """The prevailing market regime, or "unknown".

        Published by the macro intelligence engine. Used to partition
        performance history: a hit rate earned in a bull-steepening regime says
        little about the same strategy under inversion, and Kelly sizing treats
        whatever it is handed as the true win probability.
        """
        try:
            raw = await self.redis.raw.get("sentinel:macro:rates_regime:latest")
            if not raw:
                return "unknown"
            brief = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            regime = brief.get("regime") or brief.get("rates_regime") or brief.get("state")
            return str(regime).lower().strip().replace(" ", "_") if regime else "unknown"
        except Exception as e:
            self.logger.debug(f"Regime lookup failed: {e}")
            return "unknown"

    def _scorecard_key(self, strategy: Optional[str] = None, regime: Optional[str] = None) -> str:
        """Redis key for a scorecard partition.

        The unpartitioned key is preserved so existing history is not orphaned
        and remains the fallback when a partition is too thin to trust.
        """
        if not strategy and not regime:
            return f"sentinel:agents:scorecard:{self.name}"
        return f"sentinel:agents:scorecard:{self.name}:{strategy or 'any'}:{regime or 'any'}"

    async def get_scorecard(
        self,
        strategy: Optional[str] = None,
        regime: Optional[str] = None,
    ) -> AgentScorecard:
        """Retrieves a performance scorecard, optionally partitioned.

        With no arguments this returns the global card, unchanged. Passing a
        strategy and/or regime returns performance conditioned on that context,
        which is what a position sizer actually needs: pooling a macro equity
        call with a crypto funding trade makes both denominators meaningless.
        """
        key = self._scorecard_key(strategy, regime)
        try:
            raw = await self.redis.raw.get(key)
            if raw:
                data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                return AgentScorecard(**data)
        except Exception as e:
            self.logger.debug(f"Scorecard fetch failed for {key}: {e}")
        return AgentScorecard(agent_name=self.name)

    async def get_conditional_scorecard(
        self,
        strategy: Optional[str] = None,
        min_samples: int = 20,
    ) -> Tuple[AgentScorecard, str]:
        """Best available scorecard for the current regime, and its provenance.

        Falls back deliberately rather than silently: a partition with fewer
        than *min_samples* observations produces a win rate too noisy to size
        on, so the broader card is used instead and the caller is told which it
        got. Returns (card, source) where source is one of
        "strategy_regime", "strategy", or "global".
        """
        regime = await self.current_regime()

        if strategy and regime != "unknown":
            card = await self.get_scorecard(strategy=strategy, regime=regime)
            if card.predictions_made >= min_samples:
                return card, "strategy_regime"

        if strategy:
            card = await self.get_scorecard(strategy=strategy)
            if card.predictions_made >= min_samples:
                return card, "strategy"

        return await self.get_scorecard(), "global"

    async def update_scorecard(
        self,
        prediction_correct: bool,
        conviction: float,
        strategy: Optional[str] = None,
    ) -> None:
        """Updates the agent's scorecard with a verified prediction outcome.

        Writes both the global card and, when a strategy is named, the
        strategy/regime partition. Without the partitioned write the conditional
        cards would stay permanently empty and always fall back to global.
        """
        await self._apply_outcome(self._scorecard_key(), prediction_correct, conviction)

        if strategy:
            regime = await self.current_regime()
            await self._apply_outcome(
                self._scorecard_key(strategy=strategy), prediction_correct, conviction
            )
            if regime != "unknown":
                await self._apply_outcome(
                    self._scorecard_key(strategy=strategy, regime=regime),
                    prediction_correct, conviction,
                )

    async def _apply_outcome(
        self,
        key: str,
        prediction_correct: bool,
        conviction: float,
    ) -> None:
        """Applies one outcome to the scorecard stored at *key*."""
        try:
            raw = await self.redis.raw.get(key)
            card = (AgentScorecard(**json.loads(raw if isinstance(raw, str) else raw.decode("utf-8")))
                    if raw else AgentScorecard(agent_name=self.name))
            card.predictions_made += 1

            if prediction_correct:
                card.predictions_correct += 1
                # Running average of conviction on correct predictions
                n = card.predictions_correct
                card.mean_conviction_on_correct = (
                    card.mean_conviction_on_correct * (n - 1) + conviction
                ) / n
            else:
                card.predictions_wrong += 1
                n = card.predictions_wrong
                card.mean_conviction_on_wrong = (
                    card.mean_conviction_on_wrong * (n - 1) + conviction
                ) / n

            # Brier score update: BS = (1/N) Σ (forecast - outcome)²
            outcome = 1.0 if prediction_correct else 0.0
            total = card.predictions_made
            card.brier_score = (
                card.brier_score * (total - 1) + (conviction - outcome) ** 2
            ) / total

            # Adjust consensus weight based on calibration
            # Well-calibrated agents (low Brier) get higher weight
            card.consensus_weight = consensus_weight_for(card.brier_score)
            card.last_calibrated_at = datetime.now(timezone.utc).isoformat()

            await self.redis.raw.set(key, card.model_dump_json(), ex=604800)  # 7 day TTL

            if card.predictions_made % 10 == 0:
                accuracy = card.predictions_correct / max(1, card.predictions_made)
                self.logger.info(
                    f"📊 SCORECARD [{self.name}] | Accuracy: {accuracy:.0%} "
                    f"| Brier: {card.brier_score:.3f} | Weight: {card.consensus_weight:.2f} "
                    f"| Correct Conviction: {card.mean_conviction_on_correct:.0%} "
                    f"| Wrong Conviction: {card.mean_conviction_on_wrong:.0%}"
                )

                # Overconfidence alert
                if card.mean_conviction_on_wrong > 0.7 and card.predictions_wrong > 5:
                    self.logger.warning(
                        f"⚠️ OVERCONFIDENCE DETECTED [{self.name}]: Mean conviction on wrong predictions "
                        f"is {card.mean_conviction_on_wrong:.0%}. Consider reducing conviction thresholds."
                    )
        except Exception as e:
            self.logger.warning(f"Failed to update scorecard: {e}")

    async def is_recently_processed(self, entity_id: str, window_seconds: int = 3600) -> bool:
        res = await self.redis.raw.exists(self.state_key("seen", entity_id))
        return bool(res) if isinstance(res, (bool, int)) else False

    async def mark_processed(self, entity_id: str, window_seconds: int = 3600):
        await self.redis.raw.set(self.state_key("seen", entity_id), "1", ex=window_seconds)

    async def claim_processing(self, entity_id: str, ttl_seconds: int) -> bool:
        """Take an exclusive claim on an entity, or report that someone holds it.

        is_recently_processed() + mark_processed() is a check followed by an act,
        and the gap between them is however long the work takes. In the radar
        agent that gap is an LLM dispatch: 440 seconds, live. Every message for
        the same ticker arriving inside those seven minutes read an absent key,
        passed the guard, and bought its own inference slot -- META was
        evaluated three times in two seconds, at Z=4.20, Z=0.77 and Z=2.55,
        three separate dispatches of the scarcest resource the platform has to
        answer one question.

        SET NX collapses the two operations into one. The first caller gets the
        key and proceeds; the rest are told it exists and stop.

        The TTL is deliberately the in-flight window rather than the full
        cooldown. A claim is not a verdict: if the work escalates, the caller
        promotes it with mark_processed() and the entity is quiet for the whole
        cooldown; if the work declines or raises, the claim expires on its own
        and the entity can be reconsidered. Nothing has to release it, which
        matters because the paths that decline are the ones most likely to
        return early or raise.
        """
        res = await self.redis.raw.set(
            self.state_key("seen", entity_id), "1", ex=ttl_seconds, nx=True
        )
        return bool(res)

    async def enqueue_task(self, task_type: str, payload: Dict, priority: str = "normal"):
        queue = {"high": TASK_QUEUE_HIGH, "normal": TASK_QUEUE_NORMAL, "low": TASK_QUEUE_LOW}.get(priority, TASK_QUEUE_NORMAL)
        task = {
            "task_id": str(uuid.uuid4()), "task_type": task_type, "agent": self.name,
            "payload": payload, "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.redis.raw.rpush(queue, json.dumps(task))

    async def fetch_entity_context(self, entity_name: str) -> str:
        """
        Surgically fetches recent ML anomalies and news that explicitly mention the entity.
        Prevents cross-domain context pollution while giving the LLM deep awareness.
        """
        try:
            # We use TimescaleDB directly for the absolute source of truth.
            query = """
                SELECT type, headline, anomaly_score, occurred_at
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '24 hours'
                  AND (
                    primary_entity_id ILIKE $1 
                    OR headline ILIKE $2
                  )
                  AND (anomaly_score >= 0.5 OR type = 'headline')
                ORDER BY occurred_at DESC
                LIMIT 5
            """
            rows = await self.db.query(query, entity_name, f"%{entity_name}%")
            if not rows:
                return ""
                
            context = f"\n### RECENT ML ANOMALIES & NEWS FOR {entity_name.upper()} ###\n"
            for r in rows:
                score = f"(Anomaly Score: {r['anomaly_score']:.2f})" if r.get('anomaly_score') else ""
                context += f"- [{r['type']}] {r['headline']} {score}\n"
            return context + "\n"
        except Exception as e:
            self.logger.error(f"Failed to fetch entity context for {entity_name}: {e}")
            return ""

    async def fetch_global_context(self) -> str:
        """
        Fetches the top ML anomalies, latest news, and live outputs from upstream swarm agents
        (Yield Curve Rates, News Intel, Quant Researcher) to build a unified World State context.
        """
        try:
            anomaly_query = """
                SELECT type, headline, anomaly_score, occurred_at
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '24 hours'
                  AND anomaly_score >= 0.65
                ORDER BY anomaly_score DESC
                LIMIT 5
            """
            anomalies = await self.db.query(anomaly_query)
            
            news_query = """
                SELECT type, headline, occurred_at
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '24 hours'
                  AND type = 'headline'
                ORDER BY occurred_at DESC
                LIMIT 5
            """
            news = await self.db.query(news_query)
            
            context = "\n### GLOBAL SENTINEL SWARM WORLD STATE (LAST 24 HOURS) ###\n"
            context += "TOP ML ANOMALIES:\n"
            for r in anomalies:
                context += f"- [{r['type']}] {r['headline']} (Score: {r['anomaly_score']:.2f})\n"
                
            context += "\nLATEST GLOBAL NEWS:\n"
            for r in news:
                context += f"- {r['headline']}\n"

            # Ingest live rate regime & macro state from Redis cache
            try:
                rates_raw = await self.redis.raw.get("sentinel:macro:rates_regime:latest")
                if rates_raw:
                    rates_data = json.loads(rates_raw.decode("utf-8") if isinstance(rates_raw, bytes) else rates_raw)
                    context += f"\nLATEST RATES & CREDIT REGIME (YieldCurveMacroRatesAgent):\n"
                    context += f"- Curve State: {rates_data.get('curve_state', 'N/A')} | Spread: {rates_data.get('yield_spread_2y10y_bps', 0):+.1f} bps\n"
                    context += f"- Breakeven Inflation: {rates_data.get('breakeven_inflation_bps', 0):.1f} bps | TIPS Yield: {rates_data.get('tips_yield', 0):.2f}%\n"
                    context += f"- Credit Risk: {rates_data.get('credit_spread_widening_signal', 'Stable')} | Macro Risk: {rates_data.get('macro_risk_level', 'LOW')}\n"
            except Exception as rx:
                self.logger.debug(f"Rates regime cache miss in global context: {rx}")

            # The macro engine's latest synthesis, as ambient context.
            #
            # This brief already goes to Topics.INTEL_BRIEFS, which six agents
            # subscribe to -- but that is event-driven, so an agent sees it only
            # while it happens to be processing that message. The Redis copy was
            # written on every macro review and read by nothing, which left the
            # other nine agents with no view of the macro regime unless a brief
            # arrived in their queue at the right moment. Reading it here makes
            # the macro engine's conclusions available to every agent building
            # any prompt, which is what the key was evidently written for.
            try:
                brief_raw = await self.redis.raw.get("sentinel:macro:latest_brief")
                if brief_raw:
                    payload = json.loads(
                        brief_raw.decode("utf-8") if isinstance(brief_raw, bytes) else brief_raw
                    )
                    brief_body = payload.get("brief") or {}
                    headline = brief_body.get("headline") or brief_body.get("strategic_summary")
                    if headline:
                        context += "\nLATEST STRATEGIC MACRO BRIEF (MacroIntelligenceEngine):\n"
                        context += f"- {clip(headline, 200)}\n"
                        summary = brief_body.get("summary") or brief_body.get("strategic_summary")
                        if summary and summary != headline:
                            context += f"- {clip(summary, 240)}\n"
                        context += f"- Severity: {payload.get('computed_severity', 'N/A')} | As of: {payload.get('created_at', 'unknown')}\n"
            except Exception as bx:
                self.logger.debug(f"Macro brief cache miss in global context: {bx}")

            # Ingest recent shared agent memories
            try:
                # The key that is actually written. This read sentinel:memory:shared,
                # which appears exactly once in the tree -- here -- and is written
                # by nothing, so this section has always been empty while
                # write_agent_memory wrote to a key nothing in this path read.
                mems = await self.redis.raw.zrevrange("sentinel:agents:episodic_memory", 0, 4)
                if mems:
                    context += "\nSHARED SWARM MEMORIES & INTEL:\n"
                    for m in mems:
                        text = m.decode("utf-8") if isinstance(m, bytes) else str(m)
                        # Stored as JSON by write_agent_memory.
                        try:
                            _entry = json.loads(text)
                            text = f"[{_entry.get('agent', '?')}] {_entry.get('text', '')}"
                        except (ValueError, TypeError):
                            pass
                        context += f"- {text}\n"
            except Exception as mx:
                self.logger.debug(f"Shared memory miss in global context: {mx}")

            # Other agents' active conclusions.
            #
            # get_bulletins_for_prompt builds exactly this block and had no
            # call site anywhere, so no agent has ever seen another agent's
            # bulletins in its own prompt -- the mechanism by which the swarm
            # would be a swarm rather than ten independent readers of one
            # event stream.
            try:
                context += await self.get_bulletins_for_prompt(limit=5)
            except Exception as bx:
                self.logger.debug(f"Bulletin context unavailable: {bx}")

            return context + "\n"
        except Exception as e:
            self.logger.error(f"Failed to fetch global context: {e}")
            return ""

    @property
    def _inference_budget(self) -> InferenceBudget:
        """Model-scoped budget, created on first use and shared through Redis."""
        budget = getattr(self, "_budget_instance", None)
        if budget is None:
            # getattr rather than attribute access: agents are sometimes built
            # partially (tests, out-of-band dispatch) and a budget that raises
            # on a missing model would break callers that never reach a model.
            budget = InferenceBudget(
                getattr(self, "redis", None),
                getattr(self, "model", None) or "default",
                cooldown_sec=(
                    int(os.getenv("AGENT_LANE_COOLDOWN_SEC", "300"))
                    if self.INFERENCE_LANE else DEFAULT_COOLDOWN_SEC
                ),
                lane=self.INFERENCE_LANE,
                # So the budget can tell callers apart and take turns between
                # them. Without a name every claim looks the same, and the
                # agent with the busiest input stream wins every slot.
                owner=getattr(self, "name", None),
            )
            self._budget_instance = budget
        return budget


    # ── Closing the prediction loop ──────────────────────────────────────────
    #
    # Predictions were recorded with a TTL and then expired unresolved:
    # update_scorecard() existed and had no callers anywhere in the codebase.
    # Every agent's scorecard therefore sat at its constructed default forever,
    # which matters beyond bookkeeping -- the consensus engine fuses opinions
    # *weighted by these scorecards*, so a weighting that never moves is no
    # weighting at all, and an agent that is consistently wrong carried exactly
    # as much influence as one that is consistently right.
    #
    # Each agent resolves its own predictions. That keeps the work where the
    # scorecard and the agent name already are, and needs no cross-process
    # coordination.
    PREDICTION_SWEEP_INTERVAL_SEC = 900

    # Moves smaller than this are noise, not a direction. Scoring them credits an
    # agent for a call the market never made.
    FLAT_BAND = 0.002

    # A categorical market is only treated as settled when the leader is clear of
    # the field by this much. Two candidates at 0.34 and 0.33 have not resolved
    # anything.
    OUTCOME_DECISION_MARGIN = 0.15

    # How often an agent that defines run_scheduled_review is asked to run it.
    #
    # Long, because a review is a sweep rather than a reaction and each one may
    # spend inference. The first run waits a full interval so an agent starting
    # up is not competing with its own backlog for the model.
    SCHEDULED_REVIEW_INTERVAL_SEC = int(os.getenv("AGENT_SCHEDULED_REVIEW_SEC", "1800"))

    # How long after start the first review runs.
    #
    # Sleeping a whole interval first meant an agent redeployed more often than
    # every thirty minutes never ran its review at all. Short enough that a
    # restart does not lose the cycle, long enough that the agent is not
    # sweeping while its consumer and pools are still coming up.
    SCHEDULED_REVIEW_FIRST_RUN_SEC = int(os.getenv("AGENT_SCHEDULED_REVIEW_FIRST_SEC", "180"))

    async def _scheduled_review_loop(self) -> None:
        """Calls run_scheduled_review on an interval, for agents that define it."""
        await asyncio.sleep(self.SCHEDULED_REVIEW_FIRST_RUN_SEC)
        while True:
            try:
                result = await self.run_scheduled_review()
                if result:
                    self.logger.info(
                        "%s scheduled review produced a result.", self.name
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # A failed review must not end the loop; the next interval
                # tries again.
                self.logger.warning("%s scheduled review failed: %s", self.name, e)
            await asyncio.sleep(self.SCHEDULED_REVIEW_INTERVAL_SEC)

    async def _resolve_predictions_loop(self) -> None:
        while True:
            await asyncio.sleep(self.PREDICTION_SWEEP_INTERVAL_SEC)
            try:
                await self.resolve_due_predictions()
            except Exception as e:
                self.logger.warning(f"Prediction resolution sweep failed: {e}")

    async def _latest_price(self, ticker: str) -> Optional[float]:
        """Most recent quote for a ticker, or None when nothing is known.

        None is a real answer here: resolving a prediction against a price we do
        not have would manufacture a track record out of nothing.
        """
        try:
            raw = await self.redis.raw.get(f"sentinel:quotes:latest:{ticker.upper()}")
            if not raw:
                # A cache miss is the ordinary case, not the end of the search:
                # this key expires after an hour and predictions are resolved a
                # day later. Returning here is what made the fallback below
                # unreachable for exactly the tickers that needed it.
                raise _QuoteCacheMiss
            text = raw if isinstance(raw, str) else raw.decode("utf-8")

            # The collectors write a bare number here -- "93.23" -- not an
            # object. json.loads() parses that to a float perfectly happily, and
            # the old code then called .get("price") on it, raising
            # AttributeError into a bare `except Exception: return None`. So
            # this returned None for every ticker that existed, every time, and
            # the prediction resolver read that as "unverifiable, so uncounted":
            # no prediction was ever scored and no scorecard ever moved.
            try:
                quote = json.loads(text)
            except (ValueError, TypeError):
                quote = text

            if isinstance(quote, (int, float)):
                return float(quote)
            if isinstance(quote, str):
                return float(quote.strip())
            if isinstance(quote, dict):
                for field in ("price", "close", "last", "c"):
                    if quote.get(field) is not None:
                        return float(quote[field])
        except _QuoteCacheMiss:
            pass
        except Exception:
            pass

        # The cache is not a price history.
        #
        # sentinel:quotes:latest carries a one-hour TTL, and a prediction with a
        # 24-hour horizon is resolved the next day -- by which time the key has
        # expired. Measured after the close: two quote keys survived for a
        # fifty-symbol watchlist. _score_directional reads None as "unverifiable,
        # so uncounted", so a prediction that survived eviction would still
        # never be scored, and no scorecard could ever move.
        #
        # Both fallbacks read what the system already stores durably rather than
        # adding anything: the crypto candle lists are trimmed rather than
        # expired, and tradfi_bars is the equity history the rest of the
        # platform measures against.
        return await self._durable_price(ticker)

    async def _durable_price(self, ticker: str) -> Optional[float]:
        """Last known close from storage, when the quote cache has expired."""
        symbol = str(ticker or "").upper().strip()
        if not symbol:
            return None

        # Crypto: newest entry of the one-minute candle list.
        try:
            newest = await self.redis.raw.lindex(f"sentinel:candles:1m:{symbol}", 0)
            if newest:
                text = newest if isinstance(newest, str) else newest.decode("utf-8")
                close = json.loads(text).get("close")
                if close is not None:
                    return float(close)
        except Exception:
            pass

        # Equities: the most recent bar this platform recorded.
        try:
            if self.db:
                rows = await self.db.query(
                    "SELECT close FROM tradfi_bars WHERE ticker = $1 "
                    "ORDER BY time DESC LIMIT 1",
                    symbol,
                )
                if rows and rows[0].get("close") is not None:
                    return float(rows[0]["close"])
        except Exception as e:
            self.logger.debug(f"Durable price lookup failed for {symbol}: {e}")

        return None

    async def _retire_prediction(self, key: str, pred, reason: str) -> None:
        """Moves a permanently unresolvable prediction out of the resolver's path."""
        try:
            payload = pred.model_dump_json()
        except Exception:
            payload = "{}"
        try:
            pipe = self.redis.raw.pipeline()
            pipe.set(
                f"sentinel:predictions:retired:{self.name}:{pred.prediction_id}",
                payload, ex=7 * 86400,
            )
            pipe.delete(key)
            await pipe.execute()
            self.logger.info(
                "Retired prediction %s on %s: %s. It could not resolve at any "
                "point in the future, so it is no longer swept.",
                str(pred.prediction_id)[:8], pred.ticker, reason,
            )
        except Exception as e:
            self.logger.debug("Could not retire prediction %s: %s", key, e)

    async def _score_directional(self, pred: "AgentPrediction") -> Optional[bool]:
        """Was a price-direction call right? None when it cannot be judged.

        A flat close is not a win for "down". The original scoring said
        `moved_up = current > entry`, so an unchanged price scored every bearish
        call correct -- a free record for predicting nothing happens. An
        unchanged price answers no directional question and is left uncounted,
        unless the agent actually predicted flat.
        """
        current = await self._latest_price(pred.ticker)
        if current is None:
            return None

        # `not pred.entry_price` was the guard here, and 0.0 is falsy.
        #
        # That was written for the wargamer, which records an entity claim with
        # entry_price=0.0 and has since been given its own scoring path. The
        # quant engine then began publishing price predictions with a zero
        # entry, and every one of them returned here before a price was looked
        # up -- unjudgeable, uncounted, so no scorecard was ever written and
        # every agent stayed pinned at the 0.5 unproven default in the fusion.
        #
        # A zero entry price is now a defect to report rather than a silent
        # skip: the producer is guarded, so one arriving means the guard was
        # bypassed. None is still returned, because scoring against a zero
        # denominator would be arithmetic on nothing.
        if pred.entry_price is None:
            return None
        if not isinstance(pred.entry_price, (int, float)) or pred.entry_price <= 0:
            self.logger.warning(
                "Prediction %s on %s carries a non-positive entry price (%r) and "
                "cannot be resolved. The producer should not have recorded it.",
                getattr(pred, "prediction_id", "?"), pred.ticker, pred.entry_price,
            )
            return None

        direction = (pred.direction or "").strip().lower()
        # Relative, so the threshold means the same thing for a $3 stock and a
        # $3,000 one.
        move = (current - pred.entry_price) / abs(pred.entry_price)

        if direction in ("flat", "neutral", "unchanged", "hold"):
            return abs(move) <= self.FLAT_BAND
        if abs(move) <= self.FLAT_BAND:
            return None             # no move to judge a directional call against
        if direction in ("up", "long", "bullish", "buy"):
            return move > 0
        if direction in ("down", "short", "bearish", "sell"):
            return move < 0
        # An unrecognised direction used to be silently scored as "down", which
        # credited the agent for a word the resolver did not understand.
        self.logger.debug("Unscoreable direction %r on %s", pred.direction, pred.ticker)
        return None

    # What counts as an entity having been targeted. Matches the partial index
    # on events(anomaly_score DESC, occurred_at DESC) WHERE anomaly_score > 0.5,
    # so the lookup below uses it rather than scanning.
    APPEARANCE_ANOMALY_FLOOR = 0.5

    @staticmethod
    def _entity_claim(text: str) -> str:
        """The entity a free-text prediction is actually naming.

        Deliberately narrow. Two annotations recur because the model appends its
        own commentary to the name -- "CSN5086 (Centrality Multiplier: 1.00x)"
        and "MEA305 Entity" -- and both name a real entity that an exact match
        would miss. Nothing fuzzier is done here on purpose: keyword and
        substring matching is what made the old resolution signals fire on 89%
        of events and resolve nothing honestly.
        """
        claim = str(text or "").strip()
        if "(" in claim:
            claim = claim.split("(", 1)[0].strip()
        if claim.lower().endswith(" entity"):
            claim = claim[: -len(" entity")].strip()
        return claim

    async def _score_entity_appearance(self, pred: "AgentPrediction") -> Optional[bool]:
        """Was the entity the agent named actually targeted inside the horizon?

        Unlike a price call, absence is a real answer here: the horizon elapsed
        and the named entity did not turn up in anything anomalous. That is what
        lets these predictions score wrong rather than merely go unresolved, and
        it is the whole reason the wargamer's record could never move.

        The distinction that makes the negative honest is between an entity the
        platform never sees at all and one it sees and had nothing to report.
        The first cannot be judged and returns None; only the second is False.
        Scoring an unmatchable name as wrong would grade the agent on whether
        its phrasing happened to match a database column.
        """
        if not self.db:
            return None

        claim = self._entity_claim(pred.ticker)
        if not claim:
            return None

        try:
            created = datetime.fromisoformat(pred.created_at)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            deadline = created + timedelta(hours=pred.time_horizon_hours)

            # Is this an entity this platform can observe at all? Asked without
            # a time bound, because the question is whether it is visible, not
            # whether it was busy.
            known = await self.db.query(
                """
                SELECT 1 FROM events
                WHERE upper(primary_entity_name) = upper($1)
                   OR upper(primary_entity_id)   = upper($1)
                LIMIT 1
                """,
                claim,
            )
            if not known:
                self.logger.debug(
                    "Prediction %s names %r, which this platform has never "
                    "observed; left unresolved rather than scored wrong.",
                    pred.prediction_id[:8], claim,
                )
                return None

            hit = await self.db.query(
                """
                SELECT 1 FROM events
                WHERE occurred_at > $1 AND occurred_at <= $2
                  AND anomaly_score > $3
                  AND (upper(primary_entity_name) = upper($4)
                       OR upper(primary_entity_id) = upper($4))
                LIMIT 1
                """,
                created, deadline, self.APPEARANCE_ANOMALY_FLOOR, claim,
            )
            return bool(hit)
        except Exception as e:
            # Uncounted rather than guessed, the same rule the price scorers use.
            self.logger.debug("Could not score entity appearance for %s: %s",
                              pred.prediction_id[:8], e)
            return None

    async def _score_categorical(self, pred: "AgentPrediction") -> Optional[bool]:
        """Did the predicted outcome lead its field at the horizon?

        Resolved against the market's own closing distribution, which is the only
        settlement signal this deployment actually receives. None whenever the
        distribution is missing or too close to call, so an unresolved race is
        uncounted rather than guessed.
        """
        if not pred.predicted_outcome:
            return None
        distribution = await self._latest_outcome_distribution(pred.market_key or pred.ticker)
        if not distribution:
            return None

        ranked = sorted(distribution.items(), key=lambda kv: kv[1], reverse=True)
        leader, leader_p = ranked[0]
        runner_up_p = ranked[1][1] if len(ranked) > 1 else 0.0
        if leader_p - runner_up_p < self.OUTCOME_DECISION_MARGIN:
            return None             # still a contest, not a result

        pred.resolved_outcome = leader
        correct = leader.strip().lower() == pred.predicted_outcome.strip().lower()

        # Close the calibration loop. Without a settled outcome the tracker can
        # compute divergence but never a Brier score, which is why skill_report()
        # answered with an explicit "no resolved markets" note instead of a
        # number -- the one thing it exists to produce.
        try:
            from services.reasoning.market_calibration import MarketCalibrationTracker
            tracker = MarketCalibrationTracker(self.redis)
            await tracker.resolve(
                f"{pred.market_key or pred.ticker}:{pred.predicted_outcome}",
                1 if correct else 0,
            )
        except Exception as e:
            self.logger.debug(f"Could not resolve paired forecast: {e}")

        return correct

    async def _latest_outcome_distribution(self, market_key: str) -> Optional[Dict[str, float]]:
        """Current odds across a market's outcomes, as written by enrichment."""
        if not market_key:
            return None
        try:
            raw = await self.redis.raw.get(f"sentinel:prediction:outcomes:{market_key}")
            if not raw:
                return None
            parsed = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            if not isinstance(parsed, dict) or not parsed:
                return None
            out: Dict[str, float] = {}
            for name, prob in parsed.items():
                try:
                    out[str(name)] = float(prob)
                except (TypeError, ValueError):
                    continue
            return out or None
        except Exception:
            return None

    async def resolve_due_predictions(self) -> int:
        """Scores this agent's predictions whose horizon has elapsed.

        Returns how many were resolved. A prediction with no price to check
        against is left alone rather than guessed at; it expires on its own TTL
        and simply never counts, which is the honest outcome for something that
        cannot be verified.
        """
        resolved = 0
        try:
            pattern = f"sentinel:predictions:{self.name}:*"
            keys = [k async for k in self.redis.raw.scan_iter(match=pattern, count=200)]
        except Exception as e:
            self.logger.debug(f"Could not scan predictions: {e}")
            return 0

        now = datetime.now(timezone.utc)
        for key in keys:
            try:
                raw = await self.redis.raw.get(key)
                if not raw:
                    continue
                pred = AgentPrediction(**json.loads(
                    raw if isinstance(raw, str) else raw.decode("utf-8")
                ))
                if pred.verified:
                    continue

                created = datetime.fromisoformat(pred.created_at)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                if (now - created).total_seconds() < pred.time_horizon_hours * 3600:
                    continue        # still open; the market has not answered yet

                if pred.prediction_kind == "entity_appearance":
                    correct = await self._score_entity_appearance(pred)
                elif pred.outcome_space:
                    correct = await self._score_categorical(pred)
                else:
                    correct = await self._score_directional(pred)
                if correct is None:
                    # Unverifiable *this time* is not the same as unverifiable
                    # forever. A prediction whose entry price is non-positive
                    # can never resolve however long it is kept, and six of the
                    # eight records in the live corpus were exactly that --
                    # re-read, re-judged and re-logged on every fifteen-minute
                    # sweep, keeping three quarters of the corpus the scorecards
                    # depend on permanently unusable.
                    #
                    # Retired rather than deleted: the record is kept briefly
                    # under a distinct key so a person can see what was
                    # discarded and why, and it stops being offered to the
                    # resolver.
                    if not _is_resolvable(pred):
                        await self._retire_prediction(key, pred, "non-positive entry price")
                        continue
                    continue        # unverifiable for now, so uncounted

                await self.update_scorecard(
                    prediction_correct=correct,
                    conviction=pred.conviction,
                )

                pred.verified = True
                pred.outcome_correct = correct
                # Kept briefly after resolution so a scorecard dispute can be
                # traced back to the predictions behind it.
                await self.redis.raw.set(key, pred.model_dump_json(), ex=86400)
                resolved += 1
            except Exception as e:
                self.logger.debug(f"Could not resolve prediction {key}: {e}")

        if resolved:
            self.logger.info(
                "Resolved %s prediction(s) for %s against realised prices", resolved, self.name
            )
        return resolved

    async def _execute_with_telemetry(
        self,
        message: dict,
        system_prompt: str,
        user_prompt: str,
        schema: Optional[Type[BaseModel]] = None,
        temperature: float = 0.1,
        model: Optional[str] = None,
        fallback_model: Optional[str] = OLLAMA_FALLBACK_MODEL,
        num_predict: Optional[int] = None,
    ) -> Any:
        
        # Budget first: before telemetry, before the producer starts, before any
        # work at all. One inference on this host measured 482 seconds, so the
        # swarm can perform roughly 180 a day against an input of tens of
        # thousands. Without a limit each agent queued behind an eight-minute
        # call, committed nothing, and its consumer fell further behind than it
        # advanced -- 395,000 messages of backlog on one topic alone.
        #
        # The budget is shared per model, not per agent: these processes talk to
        # one single-threaded Ollama, so a per-agent limit would multiply by the
        # number of agents and rebuild the queue it exists to prevent.
        # The message's own anomaly score decides whether this candidate is
        # worth the slot. Passing it is what turns the budget from
        # first-come-first-served into a selection: the parameter existed and no
        # caller had ever filled it, so "which twenty events of a hundred and
        # fifty thousand get analysed" was answered by arrival timing alone.
        if not await self._inference_budget.try_acquire(
            score=_message_score(message),
            domain=_message_domain(message),
        ):
            raise InferenceShed(self.name, model or self.model)

        start_time = time.monotonic()
        # Fallback to a UUID if no event_id is present (e.g., scheduled tasks)
        run_id = message.get("event_id", str(uuid.uuid4())[:8])
        
        if not getattr(self._producer, "_started", False):
            await self._producer.start()

        await self._producer.send(
            "agents.telemetry", 
            {
                "agent": self.name, 
                "status": "THINKING", 
                "task_id": run_id,
                "trace_id": message.get("trace_id", "unknown"),
                "system_prompt_length": len(system_prompt),
                "user_prompt_length": len(user_prompt)
            }
        )
        
        # Lazy initialize LLM client if agent was dispatched out-of-band without run()
        if self._llm is None:
            from shared.utils.ollama import OllamaClient, OLLAMA_TIMEOUT
            if self._session is None or self._session.closed:
                connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
            self._llm = OllamaClient(self._session, self.model, redis_client=self.redis_client)

        # 2. Execute LLM with Pydantic Enforcement & Truncation Fallback Retry
        try:
            response = await self._llm.infer(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema=schema,
                temperature=temperature,
                model=model or self.model,
                fallback_model=fallback_model or self.fallback_model,
                num_predict=num_predict or 512,
            )
        except (SchemaViolationError, InferenceError) as err:
            self.logger.warning(f"LLM inference schema/truncation error ({err}). Executing temperature-decayed fallback retry...")
            try:
                response = await self._llm.infer(
                    system_prompt=system_prompt + "\nIMPORTANT: Return valid, complete, and un-truncated JSON adhering strictly to the schema.",
                    user_prompt=user_prompt,
                    schema=schema,
                    temperature=0.0,
                    model=fallback_model or self.fallback_model or self.model,
                    num_predict=max(768, (num_predict or 512) * 2),
                )
            except Exception as retry_err:
                await self._producer.send(
                    "agents.telemetry",
                    {
                        "agent": self.name,
                        "status": "FAILED",
                        "task_id": run_id,
                        "latency_ms": round((time.monotonic() - start_time) * 1000, 2),
                        "error": f"Original: {err} | Retry: {retry_err}",
                    }
                )
                raise
        except Exception as err:
            await self._producer.send(
                "agents.telemetry",
                {
                    "agent": self.name,
                    "status": "FAILED",
                    "task_id": run_id,
                    "latency_ms": round((time.monotonic() - start_time) * 1000, 2),
                    "error": str(err),
                }
            )
            raise
        finally:
            # The slot was claimed for the worst case -- a worker that claims and
            # never returns -- and that is the only thing the full cooldown is
            # protecting against. This inference has now either produced
            # something or failed, so continuing to hold the slot for the
            # remaining minutes rations nothing and idles the model server.
            #
            # In `finally` rather than after the success path because a failed
            # inference held the budget exactly as long as a successful one, and
            # a tier that is erroring is the last one that should also be
            # blocking every other agent from trying.
            await self._inference_budget.finish()

        if hasattr(response, "model_dump"):
            output_payload = response.model_dump()
        elif hasattr(response, "dict"):
            output_payload = response.dict()
        else:
            output_payload = {"raw_text": str(response)}

        # 3. Log beginning snippet of agent output and emit telemetry
        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        try:
            out_str = json.dumps(output_payload, separators=(',', ':'), default=str)
        except Exception:
            out_str = str(output_payload)
        preview_text = out_str[:10] + "..." if len(out_str) > 10 else out_str
        self.logger.info(f"✅ [{self.name}] Inference completed ({elapsed_ms}ms) | Output: {preview_text}")

        await self._producer.send(
            "agents.telemetry", 
            {
                "agent": self.name, 
                "status": "COMPLETE",
                "task_id": run_id,
                "latency_ms": elapsed_ms,
                "output_payload": output_payload
            }
        )
        return response

    async def verify_ticker_with_reasoning(self, ticker: str) -> bool:
        """
        Reasoning Service:
        Uses an LLM agentic verification step to double-check that a symbol is a valid 
        primary US common equity (or BTC) and NOT a derivative ETF, option, or crypto altcoin.
        """
        from shared.utils.equities import is_major_crypto, is_supported_asset, is_valid_primary_equity

        if not ticker or not isinstance(ticker, str):
            return False

        clean_ticker = ticker.strip().upper()
        if not is_supported_asset(clean_ticker):
            return False

        # A crypto major needs no model call: membership of the collected set is
        # the whole question, and it is answered deterministically above.
        if is_major_crypto(clean_ticker):
            return True

        class TickerVerificationDecision(BaseModel):
            valid: bool
            asset_type: str
            rationale: str

        prompt = f"""
        You are an institutional market metadata verification service.
        Verify if the symbol '{clean_ticker}' is a valid primary US common equity (e.g. AAPL, NVDA, TSLA) or Bitcoin (BTC).
        
        Strict Rules:
        - If '{clean_ticker}' is a YieldMax, Roundhill, Defiance, T-REX, GraniteShares, or any derivative ETF of a primary equity, set valid=false.
        - If '{clean_ticker}' is a crypto token this platform does not collect, set valid=false.
        - If '{clean_ticker}' is an option, warrant, preferred share, or invalid token, set valid=false.
        - If '{clean_ticker}' is a legitimate primary operating company stock or BTC, set valid=true.
        
        Return ONLY valid JSON.
        Schema: {{"valid": boolean, "asset_type": "string", "rationale": "string"}}
        """

        try:
            decision = await self._execute_with_telemetry(
                message={"system": "ticker_verification", "ticker": clean_ticker},
                system_prompt="You are an institutional market metadata verification service.",
                user_prompt=prompt,
                schema=TickerVerificationDecision,
                temperature=0.0,
                num_predict=128,
                fallback_model="gemma3:1b"
            )

            if decision.valid:
                self.logger.info(f"✅ REASONING VERIFICATION PASSED: {clean_ticker} verified as {decision.asset_type}. Rationale: {decision.rationale}")
                return True
            else:
                self.logger.warning(f"⚠️ REASONING VERIFICATION REJECTED: {clean_ticker} rejected as {decision.asset_type}. Rationale: {decision.rationale}")
                return False
        except Exception as e:
            self.logger.warning(f"Ticker reasoning verification fallback for {clean_ticker}: {e}")
            return is_valid_primary_equity(clean_ticker)

    async def close(self):
        """Cleanly close all Kafka connections upon agent shutdown."""
        if hasattr(self, "_consumer") and self._consumer:
            try:
                await self._consumer.close()
            except Exception:
                pass
        if hasattr(self, "_producer") and self._producer:
            try:
                await self._producer.close()
            except Exception:
                pass
        if hasattr(self, "_dlq") and self._dlq:
            try:
                await self._dlq.close()
            except Exception:
                pass