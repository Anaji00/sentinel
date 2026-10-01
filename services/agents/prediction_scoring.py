"""Recording a prediction, and finding out whether it was right.

Extracted from `base.py`, which carried 3,356 lines and 53 methods on one class
that every agent inherits whole. Four defects in this audit were changes to
shared behaviour that could not be reasoned about locally, and this group is
the most self-contained seam in it: a prediction is recorded, a horizon
elapses, a price or an outcome settles it, and a scorecard moves.

A mixin rather than a collaborator object, deliberately. The methods here call
siblings that stayed behind -- `_scorecard_key`, `_durable_price`,
`_score_categorical`, `_execute_with_telemetry`, `current_regime` -- and read
`self.redis`, `self.db`, `self.name` and `self.logger`. A mixin keeps every one
of those resolving exactly as before, so this move changes no behaviour and no
call site. Threading those dependencies through a constructor would have been a
rewrite wearing a refactor's clothes.

`base.py` imports the module-level names back, so every existing
`from services.agents.base import AgentPrediction` still resolves.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from shared.utils.ollama import DEFAULT_MODEL
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.quote_cache import quote_key

logger = logging.getLogger("agent.predictions")


DIRECTION_UP = frozenset({"up", "bullish", "long", "buy", "positive"})


DIRECTION_DOWN = frozenset({"down", "bearish", "short", "sell", "negative"})


DIRECTION_FLAT = frozenset({"flat", "neutral", "unchanged", "hold", "sideways"})


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


UNPROVEN_BRIER = 0.5


MIN_CONSENSUS_WEIGHT = 0.1




def canonical_direction(value) -> Optional[str]:
    """"up", "down", "flat", or None when the word says nothing directional.

    None is a real answer: "uncertain" is not a direction, and turning it into
    one would invent a claim in order to score it.
    """
    token = str(value or "").strip().lower()
    if token in DIRECTION_UP:
        return "up"
    if token in DIRECTION_DOWN:
        return "down"
    if token in DIRECTION_FLAT:
        return "flat"
    return None


class _QuoteCacheMiss(Exception):
    """The one-hour quote cache had nothing. Storage is asked next."""


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
    # Which performance partition this claim belongs to.
    #
    # `update_scorecard(strategy=...)` writes the strategy and strategy/regime
    # cards that `get_conditional_scorecard` reads, and nothing in the tree ever
    # passed one -- so those partitions never existed, every lookup fell through
    # to the global card, and the quant engine's Kelly sizing used the 0.55
    # prior for every position it has ever sized. The prediction has to carry
    # the partition for the resolver to be able to write it.
    strategy: Optional[str] = None
    outcome_space: List[str] = Field(default_factory=list)
    predicted_outcome: Optional[str] = None
    market_key: Optional[str] = None
    resolved_outcome: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    verified: bool = False
    outcome_correct: Optional[bool] = None


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


PREDICTION_RESOLUTION_BUFFER_SEC = int(
    os.getenv("PREDICTION_RESOLUTION_BUFFER_SEC", str(12 * 3600))
)


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


class PredictionScoringMixin:
    """Everything about making a forecast and being graded on it."""

    async def record_prediction(
        self,
        ticker: str,
        direction: str,
        conviction: float,
        entry_price: float,
        target_price: float = 0.0,
        time_horizon_hours: int = 24,
        prediction_kind: str = "price",
        strategy: Optional[str] = None,
        outcome_space: Optional[List[str]] = None,
        predicted_outcome: Optional[str] = None,
        market_key: Optional[str] = None,
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
                strategy=strategy,
                # A categorical claim -- which of several outcomes wins -- is
                # the only shape this platform can compare against a prediction
                # market, and the recorder had no parameter for it. So
                # outcome_space was always empty, `_record_paired_forecast`
                # returned at its first guard, `_score_categorical` was
                # unreachable, and MarketCalibrationTracker never recorded or
                # resolved anything, against thirteen live market-odds keys.
                outcome_space=list(outcome_space or []),
                predicted_outcome=predicted_outcome,
                market_key=market_key,
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

    async def _latest_price(self, ticker: str) -> Optional[float]:
        """Most recent quote for a ticker, or None when nothing is known.

        None is a real answer here: resolving a prediction against a price we do
        not have would manufacture a track record out of nothing.
        """
        try:
            # The shared helper, which also strips. Building the key by hand here
            # dropped the .strip() the writer applies, so a ticker arriving
            # with whitespace looked up a key that is present and missed it.
            raw = await self.redis.raw.get(quote_key(ticker))
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
        except Exception as _exc:
            swallowed("agents.base._latest_price", _exc)

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

        direction = canonical_direction(pred.direction)
        # Relative, so the threshold means the same thing for a $3 stock and a
        # $3,000 one.
        move = (current - pred.entry_price) / abs(pred.entry_price)

        if direction == "flat":
            return abs(move) <= self.FLAT_BAND
        if abs(move) <= self.FLAT_BAND:
            return None             # no move to judge a directional call against
        if direction == "up":
            return move > 0
        if direction == "down":
            return move < 0
        # An unrecognised direction used to be silently scored as "down", which
        # credited the agent for a word the resolver did not understand.
        self.logger.debug("Unscoreable direction %r on %s", pred.direction, pred.ticker)
        return None

    async def _record_unresolvable(self, pred) -> None:
        """Track predictions that reached their horizon and could not be scored.

        Exposed beside the scorecard so a reader can tell a 70% hit rate over
        everything from a 70% hit rate over the two thirds that happened to be
        checkable.
        """
        try:
            raw = getattr(self.redis, "raw", self.redis)
            day = datetime.now(timezone.utc).strftime("%Y%m%d")
            key = f"sentinel:scorecard:{self.name}:unresolvable:{day}"
            pipe = raw.pipeline()
            pipe.incr(key)
            pipe.expire(key, 30 * 86400)
            await pipe.execute()
        except Exception as e:
            self.logger.debug("Could not record an unresolvable prediction: %s", e)

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

                    # Uncounted, and now counted as uncounted.
                    #
                    # Skipping these silently is a selection filter if
                    # verifiability correlates with outcome, and it does:
                    # _score_directional returns None when a durable price
                    # cannot be fetched, and prices go missing for illiquid,
                    # halted and delisted names -- disproportionately where a
                    # directional call goes wrong. Failures were therefore
                    # dropped from the denominator more often than successes,
                    # and the resulting win rate feeds kelly_criterion as
                    # win_probability. A survivorship-biased hit rate becomes a
                    # position size.
                    #
                    # The prediction still is not scored -- inventing an outcome
                    # would be worse -- but the rate is now visible beside the
                    # scorecard, so the bias can be seen instead of inferred.
                    await self._record_unresolvable(pred)
                    continue        # unverifiable for now, so uncounted

                await self.update_scorecard(
                    prediction_correct=correct,
                    conviction=pred.conviction,
                    # The partition the claim was made under, so the
                    # strategy and strategy/regime cards actually get written.
                    strategy=pred.strategy,
                )

                pred.verified = True
                pred.outcome_correct = correct
                # Durable, not only in Redis.
                #
                # update_scorecard writes to Redis; three readers query
                # `agent_predictions` in Postgres for resolved_at, direction,
                # ticker and outcome_correct, and nothing ever wrote them.
                # measured_base_rate returned the non-informative 0.5 prior for
                # every Subjective Logic projection the platform has ever made,
                # _observed_win_rate returned None for every Kelly fraction, and
                # the swarm route saw nothing. The outcome exists here; this is
                # where it becomes durable.
                await self._persist_resolved_prediction(pred, correct)
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

    async def _persist_resolved_prediction(self, pred: "AgentPrediction", correct: bool) -> None:
        """Writes a resolved prediction to the durable record its readers query.

        Best-effort: a scorecard that has already been updated must not be lost
        because the archive write failed, and the Redis copy remains the
        authority for the resolver itself.
        """
        if not self.db:
            return
        try:
            brier = (float(pred.conviction) - (1.0 if correct else 0.0)) ** 2
            await self.db.execute(
                """
                INSERT INTO agent_predictions (
                    prediction_id, predicted_target, confidence, occurred_at,
                    agent_name, ticker, direction, entry_price, horizon_hours,
                    resolved_at, outcome_correct, brier_score
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10, $11)
                """,
                str(pred.prediction_id),
                str(pred.ticker),
                float(pred.conviction),
                datetime.fromisoformat(pred.created_at)
                if isinstance(pred.created_at, str) else pred.created_at,
                str(self.name),
                str(pred.ticker),
                canonical_direction(pred.direction) or str(pred.direction or ""),
                float(pred.entry_price or 0.0),
                int(pred.time_horizon_hours or 0),
                bool(correct),
                round(brier, 6),
            )
        except Exception as e:
            # Counted, not whispered. This is the durable record three separate
            # readers depend on -- the measured base rate, the per-ticker win
            # rate behind Kelly, and the swarm view -- so losing a write here
            # silently is how they came to be empty in the first place.
            swallowed(f"agents.{self.name}._persist_resolved_prediction", e, self.logger)

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
                fallback_model=DEFAULT_MODEL
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

