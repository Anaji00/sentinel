"""
services/agents/edge_validator.py

EMPIRICAL GRAPH EDGE VALIDATOR & BACKTESTING ENGINE (§3.4, Task 3)
=================================================================

Periodically backtests LLM-proposed ontology edges (SUPPLIES, COMMODITY_EXPOSURE,
POSITIVE_EXPOSURE_TO, INVERSE_EXPOSURE_TO) against realized market outcomes in
TimescaleDB, and adjusts each edge's `confidence` property based on whether it
actually predicted anything. This makes edge confidence earned rather than
self-reported by a one-shot LLM call.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import math
import re

from scipy.stats import binom

from shared.kafka import Topics
from shared.models.events import UNRATED_EDGE_CONFIDENCE
from services.agents.base import SentinelAgent
from shared.utils.quiet_failures import swallowed
from shared.utils.tasks import safe_create_task

logger = logging.getLogger("agent.edge_validator")

# Which relationships this validator is willing to grade.
#
# It listed four, and live those matched three edges in the whole graph -- two
# COMMODITY_EXPOSURE and one POSITIVE_EXPOSURE_TO, none of which had ever been
# validated, while SUPPLIES and INVERSE_EXPOSURE_TO did not exist at all. The
# predicates that actually carry weight in the prompts had no path to being
# checked: 124 SYMPATHY_MOVER, 53 PEER_OF, 31 GRANGER_CAUSES, 13
# STATISTICALLY_CORRELATED_WITH, 10 MACRO_CORRELATED.
#
# These are the asserted, directional, instrument-to-instrument claims -- the
# ones whose confidence is meant to be earned rather than declared. Deliberately
# not RELATED_TO or LOCATED_IN: a co-occurrence edge and a geographical fact are
# not predictions, and grading them against a price reaction would be scoring
# the wrong thing.
EXPOSURE_PREDICATES = [
    "SUPPLIES",
    "COMMODITY_EXPOSURE",
    "POSITIVE_EXPOSURE_TO",
    "INVERSE_EXPOSURE_TO",
    "SYMPATHY_MOVER",
    "PEER_OF",
    "GRANGER_CAUSES",
    "STATISTICALLY_CORRELATED_WITH",
    "MACRO_CORRELATED",
]
REACTION_WINDOW_HOURS = 24          # how long after the source-entity event to look for a reaction
MIN_SAMPLES_BEFORE_TRUST = 5        # don't promote/decay off one data point
CONFIDENCE_STEP = 0.05              # EWMA learning rate for confidence updates
LOOKBACK_DAYS = 30                  # evidence window, shared by the test and its null

# An edge is only evidence if the ticker reacts more often after the source
# event than it does anyway.
#
# The hit rate this used to score on counted how often the ticker had *any*
# anomaly >= 0.5 within 24 hours of a source event -- with no comparison to how
# often it has one in any 24 hours at all. For a liquid name that is most days,
# so a spurious edge scored near 1.0, and since the sweep runs every five
# minutes over the same fixed window the EWMA reached that rate within hours.
# Every exposure edge on an active ticker converged to confidence 1.0 and
# published a `quant_discovery` announcing it had been empirically validated.
# The mechanism built to make confidence earned was handing it out.
#
# The null is the ticker's own arrival rate: fit lambda over the lookback,
# take P(at least one reaction in 24h) = 1 - exp(-24*lambda), and ask how
# unlikely the observed hit count is under it. Confidence tracks 1 - p, so an
# edge earns it by beating its own base rate rather than by pointing at
# something busy.
REACTION_THRESHOLD = 0.5            # anomaly score that counts as a reaction

# Above this base rate the ticker reacts so often that no window can
# discriminate: 19 times in 20 a random day would "confirm" any edge. Scoring
# these produces confidence from arithmetic rather than evidence, so they are
# left where they are instead.
MAX_TESTABLE_BASE_RATE = 0.95

# How many edges one sweep grades.
#
# The query below had never returned a row, so the cost of this loop had never
# been paid. It is real: each edge costs one source-event query, one base-rate
# fit and up to 50 reaction queries, and there are 1,090 edges on the listed
# predicates. Ungated that is ~55,000 queries every five minutes against the
# same Timescale the enrichment path is using.
#
# Least-recently-validated first, so the whole set cycles rather than the same
# head of it being re-graded: 1,090 edges at 50 a sweep is a full pass roughly
# every two hours, against a 30-day evidence window that does not move faster
# than that.
#
# Ten, not fifty. Fifty was the first guess and the first sweep ran past twelve
# minutes against a five-minute cadence. The queries underneath have since been
# repaired -- see `_base_rate` -- so ten is now conservative rather than
# necessary, and it stays there until a completed sweep has been measured at
# this size. A full cycle of 1,090 edges takes about nine hours, against a
# thirty-day evidence window that does not move faster than that.
EDGES_PER_SWEEP = int(os.getenv("EDGE_VALIDATOR_BATCH", "10"))

# The Cypher alternation, built from the list above rather than written out.
#
# These two had drifted: the list was widened from four predicates to nine --
# with a comment recording that the original four matched three edges in the
# entire graph -- and the query kept matching only the original four. Measured
# 2026-09-20: 713 of 1,090 edges on the listed predicates were unreachable,
# including all 242 GRANGER_CAUSES, the only predicate that asserts direction.
# Deriving one from the other is what stops that recurring.
_PREDICATE_ALTERNATION = "|".join(EXPOSURE_PREDICATES)


def _word_pattern(symbol: str) -> str:
    """A Postgres regex matching `symbol` as a whole word.

    `headline ILIKE '%BP%'` matched "abrupt", "BPO" and "subpoena". Short
    tickers are common and this ran on every source event and every reaction
    window, so the false positives went straight into the hit count the
    confidence was derived from.
    """
    return r"\m" + re.escape(symbol) + r"\M"


# Why the reaction side matches on the entity id alone.
#
# `primary_entity_id = $1 OR headline ~* $2` cannot use an index: the equality
# is served by events_entity_time_idx and the regex is not, and an OR across
# the two lets Postgres use neither. Over thirty days of a 10.2M-row table that
# is a sequential scan, and it runs once for the base rate plus once per source
# event -- 51 times per edge.
#
# Measured 2026-09-20, what the regex branch actually adds:
#
#     GOOGL    entity id only 2,795   with regex 2,797   (+0.07%)
#     BTCUSD   entity id only     0   with regex     0   (+0)
#
# Against that, BTCUSD timed out outright and no sweep completed at all in the
# first fifteen minutes after this validator was repaired. Two rows in 2,797 is
# not worth a mechanism that never finishes.
#
# The source-event query below keeps the regex: it runs once per edge rather
# than fifty-one times, and finding the events that *mention* an entity is the
# looser question where a headline match earns its cost.
async def _base_rate(timescale_client: Any, ticker: str) -> Optional[float]:
    """P(at least one reaction from `ticker` in a 24h window), from its own history.

    Poisson rather than a day count: reactions cluster, and the arrival rate is
    what a 24-hour window actually integrates. Returns None when there is no
    history to fit, which is not the same as a rate of zero.
    """
    # The window is the denominator, not the time since the first hit.
    #
    # This fitted lambda as n / (NOW() - min(occurred_at)) over rows already
    # filtered to anomaly_score >= threshold, so the denominator was the time
    # since the ticker's *first qualifying event*. Conditioning it on the first
    # arrival is length-biased sampling: it inflates lambda by T/(T - t_first),
    # and for a ticker whose single reaction was an hour ago it drives p0 to
    # 1.0. That then meets MAX_TESTABLE_BASE_RATE, whose purpose is to exclude
    # tickers that react so often no window can discriminate -- so the sparsest
    # names were being excluded for being too busy. Measured over 30 days of
    # live events: 2,108 tickers ruled untestable as fitted, against 904 using
    # the lookback window.
    #
    # `observed_sec` is how long this ticker has been observable at all, which
    # is the honest denominator for a name the platform only started seeing
    # recently, capped at the lookback.
    # Two queries, because the expensive half is only needed half the time.
    #
    # This was one statement with an uncorrelated subquery computing
    # `min(occurred_at)` over the ticker's whole history. Measured 2026-09-20:
    #
    #     GOOGL   (has qualifying rows)   1,415ms
    #     BTCUSD  (has none)             40,000ms+, hit the statement timeout
    #
    # An entity with no matching rows is the pathological case for `min()`:
    # proving there is no minimum means visiting every chunk, including the
    # compressed ones. And the result is discarded in exactly that case -- the
    # `n <= 0` branch below overwrites `span` with the full lookback. So the
    # query spent forty seconds computing a number it then threw away, and the
    # sweep never finished.
    #
    # Counting first is cheap and indexed (events_entity_time_idx). The span is
    # computed only for a ticker already known to have qualifying events, where
    # `min()` finds a row and stops.
    count_query = """
        SELECT count(*)::float AS n
        FROM events
        WHERE occurred_at > NOW() - INTERVAL '%s days'
          AND primary_entity_id = $1
          AND anomaly_score >= $2
    """ % LOOKBACK_DAYS
    try:
        rows = await timescale_client.query(count_query, ticker, REACTION_THRESHOLD)
    except Exception as e:
        swallowed("edge_validator.base_rate_query", e, logger, detail=ticker)
        return None

    if not rows:
        return None
    n = float(rows[0].get("n") or 0.0)

    if n <= 0:
        # Nothing qualifying. Two very different reasons, and the difference
        # decides whether this edge can be graded at all.
        #
        # Measured on the first sweep that ever completed: all seven edges
        # decayed on "0/50 hits vs base 1.7%", and 1.7% is this floor. Their
        # targets -- US10Y, GC=F, BTCUSD, VOLATILE_1M_CANDLE -- have *zero*
        # rows in the events table, not zero qualifying rows. The platform
        # quotes those instruments but never writes an event keyed to them, so
        # a reaction cannot be observed whether or not one occurred. Grading
        # them produced a decay on every sweep, and a decayed confidence reads
        # as earned, so the mechanism built to make confidence evidential would
        # have ground every macro-target edge to zero on no evidence.
        #
        # This function's docstring already draws the distinction -- "returns
        # None when there is no history to fit, which is not the same as a rate
        # of zero" -- and the caller already skips on None. It was the check
        # that was missing, not the contract.
        try:
            observed = await timescale_client.query(
                """
                SELECT count(*)::float AS n FROM events
                WHERE occurred_at > NOW() - INTERVAL '%s days'
                  AND primary_entity_id = $1
                """ % LOOKBACK_DAYS,
                ticker,
            )
        except Exception as e:
            swallowed("edge_validator.base_rate_observed", e, logger, detail=ticker)
            return None
        if float((observed or [{}])[0].get("n") or 0.0) <= 0:
            # The platform has never recorded this entity. Untestable, which
            # is not the same as refuted.
            return None

        # It is observed and simply reacts rarely. That is a real signal, and
        # the null must not be degenerate: use the smallest rate the window can
        # resolve rather than zero, which would make any single hit infinitely
        # significant.
        span = LOOKBACK_DAYS * 86400.0
        n = 0.5
    else:
        # How long this ticker has been observable at all, capped at the
        # lookback -- deliberately NOT conditioned on the first *qualifying*
        # event, which is length-biased and once ruled 2,108 tickers untestable
        # for being too busy.
        span_query = """
            SELECT LEAST(
                       EXTRACT(EPOCH FROM (NOW() - COALESCE(
                           min(occurred_at), NOW() - INTERVAL '%s days'
                       ))),
                       %s * 86400.0
                   ) AS span_sec
            FROM events
            WHERE occurred_at > NOW() - INTERVAL '%s days'
              AND primary_entity_id = $1
        """ % (LOOKBACK_DAYS, LOOKBACK_DAYS, LOOKBACK_DAYS)
        try:
            span_rows = await timescale_client.query(span_query, ticker)
        except Exception as e:
            swallowed("edge_validator.base_rate_span", e, logger, detail=ticker)
            return None
        span = float((span_rows or [{}])[0].get("span_sec") or 0.0)
        if span <= 0:
            span = LOOKBACK_DAYS * 86400.0

    lam = n / span
    p0 = 1.0 - math.exp(-lam * REACTION_WINDOW_HOURS * 3600.0)
    return min(max(p0, 1e-6), 1.0 - 1e-6)


def _evidence(hits: int, trials: int, base_rate: float) -> float:
    """How much better than chance, as a number in [0, 1].

    One-sided binomial tail: P(X >= hits | trials, base_rate). Confidence
    tracks 1 - p, so an edge whose hits are exactly what the base rate predicts
    earns about 0.5 and decays toward it, while one that beats the rate earns
    the difference.
    """
    if trials <= 0:
        return 0.0
    p_value = float(binom.sf(hits - 1, trials, base_rate))
    return min(max(1.0 - p_value, 0.0), 1.0)


async def validate_edges(
    neo4j_client: Any,
    timescale_client: Any,
    redis_client: Optional[Any] = None,
    producer: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Periodically backtests LLM-proposed ontology edges against realized market outcomes in TimescaleDB.
    Nudges confidence up for predictive edges and decays unproven guesses.

    Strictly uses parameterized Cypher queries to prevent injection vulnerabilities.
    """
    if neo4j_client is None or timescale_client is None:
        logger.warning("Neo4j or TimescaleDB client uninitialized. Skipping edge validation.")
        return {"validated": 0, "promoted": 0, "decayed": 0}

    # Every edge on a graded predicate, oldest-validated first.
    #
    # Three constraints were removed because each matched nothing. `(a:Entity)`
    # required the generic label, and the sources are :Company, :Commodity,
    # :MacroFactor and :Region. `(b:Entity {type: 'instrument'})` required a
    # type value that does not occur in this graph at all -- the targets carry
    # Commodity, Company, MacroFactor, Index and CryptoAsset. Together with the
    # four-of-nine predicate list, the query returned zero rows, and had
    # returned zero rows for the life of the deployment: no edge in the graph
    # has ever carried a `validation_samples` property.
    #
    # An edge is identified by its endpoints' ids, so an id is the one thing
    # that is actually required.
    query = f"""
    MATCH (a)-[r:{_PREDICATE_ALTERNATION}]->(b)
    WHERE a.id IS NOT NULL AND b.id IS NOT NULL
    RETURN a.id AS source_id, type(r) AS predicate, b.id AS ticker,
           coalesce(r.confidence, $unrated) AS confidence,
           coalesce(r.validation_samples, 0) AS samples,
           coalesce(r.last_validated, 0) AS last_validated,
           elementId(r) AS rid
    ORDER BY last_validated ASC
    LIMIT $batch
    """

    try:
        edges = await neo4j_client.query(
            query, {"unrated": UNRATED_EDGE_CONFIDENCE, "batch": EDGES_PER_SWEEP}
        )
    except Exception as e:
        logger.error(f"Failed querying Neo4j exposure edges: {e}")
        return {"validated": 0, "promoted": 0, "decayed": 0}

    if not edges:
        logger.debug("No exposure edges found in Neo4j graph to validate.")
        return {"validated": 0, "promoted": 0, "decayed": 0}

    validated_count = 0
    promoted_count = 0
    decayed_count = 0
    # Why the rest were not graded.
    #
    # The first working sweep read "2 edge(s) evaluated" out of ten, and the
    # other eight were invisible: an unobserved target returns None with no
    # log, and the too-busy branch logs at DEBUG, which this deployment does
    # not emit. "Evaluated 2" and "evaluated 2, skipped 8 for want of a
    # measurable target" are different reports, and only the second says
    # whether the validator is working.
    skipped = {"unobserved_or_unfittable": 0, "reacts_too_often": 0,
               "no_source_events": 0, "too_few_trials": 0}

    for edge in edges:
        source_id = str(edge.get("source_id", "")).strip().upper()
        predicate = str(edge.get("predicate", "")).strip()
        ticker = str(edge.get("ticker", "")).strip().upper()
        old_conf = float(edge.get("confidence", 0.5))
        prev_samples = int(edge.get("samples", 0))
        rid = edge.get("rid")

        if not source_id or not ticker or predicate not in EXPOSURE_PREDICATES:
            continue

        # The null this edge has to beat, fitted once per ticker rather than
        # once per event.
        #
        # Checked before the source-event scan below, not after. That scan
        # still carries a headline regex and cannot use an index, and it timed
        # out on META and SNDK -- both of which are then skipped here anyway
        # for reacting too often to be testable. Paying for a sequential scan
        # to reach a verdict of "no power" is the wrong order.
        base_rate = await _base_rate(timescale_client, ticker)
        if base_rate is None:
            skipped["unobserved_or_unfittable"] += 1
            continue
        if base_rate > MAX_TESTABLE_BASE_RATE:
            skipped["reacts_too_often"] += 1
            logger.debug(
                "Skipping %s -[%s]-> %s: base reaction rate %.3f leaves the test no power.",
                source_id, predicate, ticker, base_rate,
            )
            continue

        # 1. Pull historical events tagged to source_id from TimescaleDB
        #
        # Exact match on the entity id and a word-boundary regex on the
        # headline. The ILIKE '%%SOURCE%%' this replaces matched any headline
        # containing the letters anywhere, which for a two-letter ticker is
        # most of them.
        events_query = """
            SELECT event_id, type, headline, anomaly_score, occurred_at
            FROM events
            WHERE occurred_at > NOW() - INTERVAL '%s days'
              AND (
                primary_entity_id = $1
                OR headline ~* $2
              )
            ORDER BY occurred_at DESC
            LIMIT 50
        """ % LOOKBACK_DAYS
        try:
            rows = await timescale_client.query(
                events_query, source_id, _word_pattern(source_id)
            )
        except Exception as e:
            swallowed("edge_validator.source_events_query", e, logger, detail=source_id)
            rows = []

        if not rows:
            skipped["no_source_events"] += 1
            continue

        hits = 0
        trials = 0

        # 2. For each source event, check if target ticker had a reaction in 24h post-event
        for event in rows:
            event_ts = event.get("occurred_at")
            if not event_ts:
                continue
            # asyncpg wants a datetime for a timestamptz parameter and this
            # client hands back ISO strings. Every reaction query raised
            # `DataError: invalid input for query argument $2 ... got 'str'`
            # -- 300 of them in one sweep -- and the handler counts a failed
            # query as "not a trial", so `trials` stayed 0 and every edge was
            # skipped with nothing written. The query had never run before the
            # repair above, so this had never been reachable.
            if isinstance(event_ts, str):
                try:
                    event_ts = datetime.fromisoformat(event_ts)
                except ValueError:
                    swallowed(
                        "edge_validator.event_ts_parse", ValueError(event_ts),
                        logger, detail=f"{source_id}->{ticker}",
                    )
                    continue

            # Query TimescaleDB for ticker events in the [event_ts, event_ts + 24h] reaction window
            reaction_query = """
                SELECT event_id, type, anomaly_score
                FROM events
                WHERE primary_entity_id = $1
                  AND occurred_at >= $2
                  AND occurred_at <= $2 + INTERVAL '%s hours'
                  AND anomaly_score >= $3
                LIMIT 1
            """ % REACTION_WINDOW_HOURS
            try:
                rx_rows = await timescale_client.query(
                    reaction_query, ticker, event_ts, REACTION_THRESHOLD
                )
            except Exception as e:
                # A failed query is not a miss. Counting it as one biased every
                # confidence downward in exactly the conditions -- database
                # under load -- where the bias is largest, and said nothing.
                swallowed("edge_validator.reaction_query", e, logger, detail=f"{source_id}->{ticker}")
                continue
            trials += 1
            if rx_rows:
                hits += 1

        if trials <= 0:
            skipped["too_few_trials"] += 1
            continue

        # `validation_samples` is the size of the evidence, not a running total.
        #
        # It was `prev_samples + len(rows)`, and the sweep re-reads the same
        # fixed 30-day window every five minutes -- so it grew by up to 50 per
        # sweep, roughly 14,000 a day, off one unchanged body of evidence. The
        # MIN_SAMPLES_BEFORE_TRUST gate it feeds was satisfied within minutes of
        # startup for every edge, permanently.
        new_total_samples = trials
        hit_rate = hits / trials
        evidence = _evidence(hits, trials, base_rate)

        # 3. Only promote/decay if sample size >= MIN_SAMPLES_BEFORE_TRUST
        if new_total_samples >= MIN_SAMPLES_BEFORE_TRUST:
            # EWMA step towards how much the hits beat the ticker's own base rate
            new_conf = round(max(0.0, min(1.0, (1.0 - CONFIDENCE_STEP) * old_conf + CONFIDENCE_STEP * evidence)), 4)

            if abs(new_conf - old_conf) >= 0.001:
                # 4. Write back via fully parameterized Cypher
                # By the relationship's own identity.
                #
                # This re-matched the endpoints, and carried the same
                # `(a:Entity)` / `type: 'instrument'` constraints as the read --
                # so even had the read returned rows, every write-back would
                # have matched nothing. Re-deriving an edge from properties
                # also means a full relationship scan; elementId is exact and
                # was already in hand from the read.
                update_query = """
                MATCH ()-[r]->()
                WHERE elementId(r) = $rid
                SET r.confidence = $new_confidence,
                    r.validation_samples = $samples,
                    r.last_validated = timestamp()
                """
                try:
                    await neo4j_client.query(update_query, {
                        "rid": rid,
                        "new_confidence": new_conf,
                        "samples": new_total_samples,
                    })

                    logger.info(
                        f"📊 Edge Confidence Updated | {source_id} -[{predicate}]-> {ticker} | "
                        f"Confidence: {old_conf:.3f} → {new_conf:.3f} | "
                        f"Hit Rate: {hit_rate:.1%} vs base {base_rate:.1%} | "
                        f"Evidence: {evidence:.3f} ({hits}/{trials} hits)"
                    )

                    # Invalidate Redis exposure cache key so macro engine picks up change
                    if redis_client and hasattr(redis_client, "raw"):
                        try:
                            # SCAN and delete in batches. KEYS blocks the whole
                            # server, and this runs on every promoted edge.
                            stale = []
                            async for k in redis_client.raw.scan_iter(
                                match=f"sentinel:cache:exposure:{source_id}:*", count=500
                            ):
                                stale.append(k)
                                if len(stale) >= 500:
                                    await redis_client.raw.delete(*stale)
                                    stale = []
                            if stale:
                                await redis_client.raw.delete(*stale)
                        except Exception as cache_err:
                            # Said out loud: a cache that fails to invalidate
                            # serves a stale exposure figure to the macro
                            # engine, which is a wrong answer rather than a
                            # missing one.
                            logger.warning(
                                "Could not invalidate exposure cache for %s: %s",
                                source_id, cache_err,
                            )

                    # If edge promoted above 0.80, publish quant discovery event for RuleSynthesizer
                    if new_conf >= 0.80 and old_conf < 0.80 and producer:
                        discovery_payload = {
                            "type": "quant_discovery",
                            "source": "edge_validator",
                            "description": f"Empirically validated high-confidence relationship: {source_id} {predicate} {ticker} (confidence: {new_conf:.2f})",
                            "correlated_assets": [source_id, ticker],
                            "confidence": new_conf,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                        await producer.send(Topics.QUANT_DISCOVERIES, discovery_payload, key=ticker)

                    if new_conf > old_conf:
                        promoted_count += 1
                    else:
                        decayed_count += 1
                    validated_count += 1

                except Exception as e:
                    logger.error(f"Failed updating Neo4j edge confidence for {source_id}->{ticker}: {e}")

    return {
        "validated": validated_count,
        "promoted": promoted_count,
        "decayed": decayed_count,
        "considered": len(edges),
        "skipped": skipped,
    }


class EdgeValidatorAgent(SentinelAgent):
    """
    Agent wrapper running EdgeValidator as a scheduled 5-minute background loop.
    """

    @property
    def output_topic(self) -> str:
        return Topics.QUANT_DISCOVERIES

    async def run(self):
        """Launches the periodic edge validation loop alongside reactive handler."""
        validation_task = safe_create_task(self._run_scheduled_validation())
        try:
            await super().run()
        finally:
            validation_task.cancel()

    async def _run_scheduled_validation(self):
        await asyncio.sleep(10)  # Initial startup delay
        while True:
            try:
                res = await validate_edges(
                    neo4j_client=self.neo4j,
                    timescale_client=self.db,
                    redis_client=self.redis,
                    producer=self._producer,
                )
                # Logged even at zero, on the first sweep and then each
                # hundredth. Gating this on `> 0` is why a validator that had
                # never graded an edge in the platform's lifetime was silent
                # about it: a sweep that does nothing looked exactly like a
                # sweep that found nothing to do.
                self._sweeps = getattr(self, "_sweeps", 0) + 1
                if res.get("validated", 0) > 0 or self._sweeps % 100 == 1:
                    sk = res.get("skipped") or {}
                    logger.info(
                        "⚡ Edge Validation Sweep #%s: %s of %s edge(s) evaluated "
                        "(%s promoted, %s decayed); skipped %s",
                        self._sweeps, res.get("validated", 0),
                        res.get("considered", 0),
                        res.get("promoted", 0), res.get("decayed", 0),
                        ", ".join(f"{k}={v}" for k, v in sk.items() if v) or "none",
                    )
            except Exception as e:
                logger.error(f"Edge validation loop failed: {e}", exc_info=True)
            await asyncio.sleep(300)  # 5-minute cadence

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # EdgeValidator operates on a scheduled cadence; reactive messages return None
        return None
