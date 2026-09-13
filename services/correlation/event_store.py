"""
services/correlation/event_store.py

TimescaleDB query interface used by all correlation rules.
Rules call get_recent() to ask "what events exist in this domain, region,
and time window?" — this translates those questions into SQL.

All queries are parameterized. The only f-string interpolation is the safe
`LIMIT {int(limit)}` cast and the `AND`-join of hardcoded condition strings.
"""

import asyncio
import time
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from shared.db import get_timescale
from shared.utils.metrics import MetricsCollector
from shared.utils.tasks import safe_create_task
from shared.models.events import event_domain as canonical_domain
from shared.models.events import resolve_event_domain

logger = logging.getLogger("correlation.store")

# Below this an event is unreachable by every correlation rule.
#
# Kept under the lowest min_anomaly any rule uses (0.20) with margin, so a
# slightly more permissive rule added later still finds its inputs. The
# accompanying test asserts the ordering rather than trusting this comment.
RECENT_WINDOW_MIN_ANOMALY = 0.15

# Members fetched per round trip when scanning the recent window.
#
# Large enough that the scan is a handful of round trips rather than hundreds,
# small enough that the reply Redis has to build stays in the low megabytes
# instead of the 98.7 MB the unbatched read required.
RECENT_WINDOW_SCAN_BATCH = 2000

# How long the Redis window holds, in hours. A clause asking for more than this
# is asking for evidence the cache has already evicted.
CACHED_WINDOW_HOURS = 48

# Rows the deep-window read may return per clause.
#
# The Redis path already caps at `limit` (50) after ranking; this bounds what
# the database is asked to materialise before the same ranking runs over the
# union. Larger than the limit so the merge has something to choose between,
# small enough that a 7-day clause is one indexed scan and not a table read.
DEEP_WINDOW_MAX_ROWS = 200

# How long a deep-window read may take before the clause gives up on it.
#
# This read sits inside the per-event rule loop, so it is the only part of the
# correlation path that can be made slow by something outside the process. Two
# seconds is generous for an indexed range scan and short enough that a
# degraded database costs throughput rather than stopping it.
DEEP_WINDOW_TIMEOUT_SEC = 2.0



def _select_diverse_evidence(ranked: list, limit: int) -> list:
    """The top `limit` events, with every matched type represented first.

    Evidence was taken as `sorted_by_anomaly[:10]`, which lets one detector own
    every slot whenever its scores run higher than its neighbours'. That is not
    hypothetical: a rule naming four event types in one clause produced clusters
    whose evidence was entirely flight_anomaly, averaging 0.956, while
    flight_dark averaged 0.632 -- and flight_dark's 10,370 events in 48 hours
    appeared in no cluster at all. The rule matched them; the ranking hid them.

    A cluster's job is to show what co-occurred, so the first pass gives each
    distinct type its strongest example and only then fills the remaining slots
    by score. Within a type the score order is preserved, so the ranking still
    decides which flight_dark event is shown -- it just no longer decides
    whether any is.
    """
    if limit <= 0 or not ranked:
        return []
    if len(ranked) <= limit:
        return ranked

    by_type: dict = {}
    for event in ranked:
        by_type.setdefault(event.get("type") or "unknown", []).append(event)

    selected, seen = [], set()
    # Round-robin across types, strongest first within each, so the slots are
    # shared before any type takes a second one.
    while len(selected) < limit:
        progressed = False
        for bucket in by_type.values():
            if not bucket:
                continue
            event = bucket.pop(0)
            key = id(event)
            if key in seen:
                continue
            seen.add(key)
            selected.append(event)
            progressed = True
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return selected


# How far ahead of now an event may claim to have happened.
#
# Clock skew between a collector host and this one is real and small; anything
# beyond this is a parsing error or a fabricated timestamp.
FUTURE_TOLERANCE_SEC = 300.0

# How far behind now an event may be and still be worth caching. The correlation
# window is 48 hours, so anything older cannot participate in a rule anyway.
MAX_BACKDATE_SEC = 7 * 24 * 3600.0

# Writes between sliding-window prunes. The window moves by seconds and the
# structure holds hundreds of thousands of members; pruning per write spent a
# ZREMRANGEBYSCORE on every ingested event to remove almost nothing.
PRUNE_EVERY_N_WRITES = 250

# Correlations that could not be written to the database, kept so a transient
# outage does not destroy findings the engine has already moved past.
FAILED_CORRELATIONS_KEY = "sentinel:correlations:failed"
FAILED_CORRELATIONS_MAX = 5000


def _sane_epoch(occurred_at, event_id=None):
    """A UTC epoch for an event, or None if the timestamp cannot be trusted.

    Two failures this guards against, both of which poison the sorted set that
    every correlation window is read from.

    A naive datetime silently takes the host's local offset when `.timestamp()`
    is called, so the same event ingested on two differently-configured hosts
    lands hours apart. Naive input is read as UTC here, which is what every
    collector actually means.

    And a timestamp far in the future is never evicted by a sliding window that
    prunes from below, so one bad value occupies rank 0 of a descending read
    permanently and is served as the newest evidence to every rule that queries
    the window.

    Rejected rather than clamped: clamping invents a time the event did not
    happen at, and the event is still in the database and the Kafka log -- only
    the correlation cache declines it.
    """
    if occurred_at is None:
        return None
    try:
        dt = occurred_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        epoch = dt.timestamp()
    except (AttributeError, ValueError, OSError, OverflowError) as e:
        logger.warning("Unusable occurred_at on event %s: %s", event_id, e)
        MetricsCollector.increment("event_store_timestamp_unusable_total")
        return None

    now = time.time()
    if epoch > now + FUTURE_TOLERANCE_SEC:
        logger.warning(
            "Event %s claims to occur %.0fs in the future; not cached. A future "
            "timestamp is never evicted by a sliding window and would be served "
            "as the newest evidence indefinitely.",
            event_id, epoch - now,
        )
        MetricsCollector.increment("event_store_timestamp_future_total")
        return None
    if epoch < now - MAX_BACKDATE_SEC:
        MetricsCollector.increment("event_store_timestamp_stale_total")
        return None
    return epoch


class EventStore:

    def __init__(self, redis_client, db_client):
        self._redis = redis_client
        self._db = db_client
        self.cache_key = "events:recent_window"
        # Derived, not repeated. `get_recent` decides where the cache stops and
        # the database starts from CACHED_WINDOW_HOURS; if the prune horizon
        # were written separately the two would drift and the deep read would
        # either duplicate rows or skip a band of them.
        self.window_seconds = CACHED_WINDOW_HOURS * 3600
        # The counter the prune below reads.
        #
        # It was never initialised, so `self._writes_since_prune += 1` raised
        # AttributeError on *every* write for the life of the deployment --
        # 7,209 log lines and a counter at 7,400 in 24 hours, all one bug. The
        # `zadd` above it succeeds, so events still reached the window; what
        # never ran was the eviction.
        #
        # Measured before this: 182,923 members of which 122,027 (67%) were past
        # the 48-hour window, oldest 8.2 days, and the key held 135 MB of a
        # 174 MB Redis instance capped at 400 MB. The comment on the prune
        # describes the same structure reaching 321,535 members and 196 MB,
        # forcing every other key out and failing writes with "command not
        # allowed when used memory > 'maxmemory'". It was on that path again.
        #
        # Started at the threshold rather than zero so the first write of a new
        # process prunes immediately: a restart is exactly when a backlog from
        # the previous one is waiting, and waiting 250 more writes to discover
        # that is the wrong default.
        self._writes_since_prune = PRUNE_EVERY_N_WRITES

    async def add_event(self, event: Any):
        """Add a normalized event to the Redis Sliding Window cache."""
        try:
            # Nothing stores what no rule can ask for.
            #
            # This window held 321,535 members and 196MB -- 86% of the whole
            # Redis instance, in one key with no TTL, which under volatile-lru
            # can never be reclaimed. It therefore forced everything else out
            # and writes began failing with "command not allowed when used
            # memory > 'maxmemory'", taking the supervisor's dispatch with them.
            #
            # The lowest min_anomaly any rule requests is 0.20, so an event
            # below that floor is unreachable by every rule in the system and
            # was being kept for forty-eight hours regardless. flight_position
            # averages 0.107 and vessel_static is 0.000, and between them they
            # are thousands of members an hour.
            #
            # The floor sits below the lowest rule threshold with margin, and
            # tests/test_recent_window_floor.py pins that relationship: if a
            # rule is ever written below it, the test fails rather than the
            # rule silently matching nothing.
            if (event.anomaly_score or 0.0) < RECENT_WINDOW_MIN_ANOMALY:
                return

            timestamp = _sane_epoch(event.occurred_at, event.event_id)
            if timestamp is None:
                return
            payload = json.dumps({
                "event_id": event.event_id,
                "type": event.type.value,
                # Canonical, so the Hawkes history loader and the correlation
                # engine agree about what domain this event was.
                "domain": resolve_event_domain(event),
                # Carried so the correlation layer can ask whether its evidence
                # is independent. Without it, breadth counts events and cannot
                # tell three reports from one collector apart from three
                # collectors agreeing -- and measured over 24 hours, 1,601 of
                # 1,632 clusters drew every supporting event from a single
                # source.
                "source": getattr(event, "source", None),
                "anomaly_score": event.anomaly_score,
                "tags": event.tags,
                "region": event.region,
                "latitude": event.latitude,
                "longitude": event.longitude,
                # Truncated at write. Both are read back, so they cannot be
                # dropped, but the consumer already cuts them to 200 and
                # storing them in full multiplied a 321,535-member structure.
                "headline": (event.headline or "")[:160] or None,
                "summary": (getattr(event, "summary", None) or "")[:200] or None,
                "named_entities": event.named_entities,
                "entity_name": (event.primary_entity.name if event.primary_entity and event.primary_entity.name else
                                event.primary_entity.id if event.primary_entity else None),
                "entity_type": (event.primary_entity.type.value if event.primary_entity and hasattr(event.primary_entity.type, 'value') else None),
                "entity_id": (event.primary_entity.id if event.primary_entity else None),
            })
            await self._redis.zadd(self.cache_key, {payload: timestamp})

            # Sliding Window Maintenance
            #
            # Pruned from both ends, and not on every write.
            #
            # The old cleanup was `zremrangebyscore(key, "-inf", cutoff)` on
            # every add_event. It only removed from below, so an event dated in
            # the future was never evicted -- and because the window is read
            # with desc=True, a single bad timestamp sat at rank 0 permanently
            # and was returned first as the "most recent" evidence for every
            # rule, forever. The forward sweep is what makes that recoverable
            # for anything already stored.
            #
            # Running both on every write also meant a ZREMRANGEBYSCORE per
            # ingested event against a structure holding hundreds of thousands
            # of members. The window moves by seconds; pruning it every
            # PRUNE_EVERY_N_WRITES is the same window with a fraction of the work.
            self._writes_since_prune += 1
            if self._writes_since_prune >= PRUNE_EVERY_N_WRITES:
                self._writes_since_prune = 0
                now = time.time()
                pipe = self._redis.raw.pipeline()
                pipe.zremrangebyscore(self.cache_key, "-inf", now - self.window_seconds)
                pipe.zremrangebyscore(self.cache_key, now + FUTURE_TOLERANCE_SEC, "+inf")
                await pipe.execute()
        except Exception as e:
            # An event that never reaches the cache cannot be correlated with
            # anything. Counted so a rising drop rate is visible rather than
            # inferred from correlations that stopped appearing.
            logger.error("EventStore.add_event to redis cache failed: %s", e, exc_info=True)
            MetricsCollector.increment("pipeline_errors_total:event_store_add")
            

    async def get_recent(
        self,
        event_types: List[str],
        exclude_event_id: str = None,
        hours:       int   = 48,
        region:      str   = None,
        min_anomaly: float = 0.0,
        tags:        List[str] = None,
        limit:       int   = 50,
        entity_id:   Optional[str] = None,
        exclude_types: Optional[Sequence[str]] = None,
        after_epoch:   Optional[float] = None,
        before_epoch:  Optional[float] = None,
    ) -> List[Dict]:
        """Fetch historical events instantly from RAM instead of Postgres."""

        try:
            cutoff = time.time() - (hours * 3600)
            entity_id = str(entity_id).upper() if entity_id else None

            # Read in batches rather than materialising the whole window.
            #
            # This was a single ZRANGE over the entire 48-hour set: 144,212
            # members, 98.7 MB, fetched and JSON-parsed in full so that Python
            # could filter it down and return at most 50 rows. It runs once per
            # correlation clause, per rule, per event.
            #
            # Redis has to build the whole reply in the client output buffer
            # before sending it, so each call pushed used_memory from 114 MB to
            # 273-287 MB against a 419 MB ceiling. Measured live: a spike every
            # ~55 seconds, and evicted_keys rising by ~2,300 at each one --
            # about 47 keys a second, continuously.
            #
            # What that evicted is the point. events:recent_window carries no
            # TTL, so under volatile-lru it cannot be evicted; everything that
            # *can* be is the small TTL'd keys, which is where the anomaly
            # baselines live. 331 tickers traded through the financial scorers
            # in 24 hours and 4 of them still had a stored mean and variance.
            # A normaliser with no history returns 0 for its first observation,
            # so the financial z-scores were being computed from a baseline that
            # eviction kept resetting.
            #
            # The scan still reads every member -- the caller ranks by anomaly
            # score and takes the top N, so an early exit would silently change
            # which rows come back. Only the peak buffer changes, from one 98 MB
            # reply to a few megabytes at a time.
            results = []
            offset = 0
            while True:
                # With scores, because the score *is* the event time.
                #
                # The stored payload carries no timestamp -- occurred_at is the
                # sorted-set score and nothing else -- so every hit reached the
                # rule evaluator with no notion of when it happened, and the
                # rules could only ever express co-occurrence inside a window.
                # Reading the score costs nothing extra and it is already
                # authoritative for members written before this change, which a
                # payload field would not be.
                raw_results = await self._redis.raw.zrange(
                    self.cache_key,
                    "+inf",
                    cutoff,
                    desc=True,
                    byscore=True,
                    offset=offset,
                    num=RECENT_WINDOW_SCAN_BATCH,
                    withscores=True,
                )
                if not raw_results:
                    break
                offset += len(raw_results)
                for raw, score in raw_results:
                    e = json.loads(raw)
                    e["occurred_at_epoch"] = float(score)
                
                    # FILTER FIX: Translated SQL conditions into native Python checks
                    if exclude_event_id and e["event_id"] == exclude_event_id:
                        continue
                    if min_anomaly > 0 and e["anomaly_score"] < min_anomaly:
                        continue
                    if event_types and e["type"] not in event_types:
                        continue
                    # Same-name correlation, for rules that mean one company.
                    #
                    # "Equity Block & Options Convergence" is a claim about a
                    # block trade and options activity in the same name. Without
                    # this the rule correlated an AAPL block with whatever else
                    # had traded in 48 hours, and published the result headlined
                    # AAPL over supporting evidence reading MTZ, KKR and DELL.
                    # Filtered here, not by the caller after truncation.
                    #
                    # get_recent sorts by anomaly score and keeps the top 50.
                    # The caller then stripped position telemetry and applied
                    # the rule's temporal bound to whatever survived -- so if
                    # the fifty highest-scoring events in the window were all
                    # vessel position fixes, the rule saw no evidence at all
                    # while qualifying events sat below the cut. A rule that
                    # excludes a noisy type was therefore most likely to find
                    # nothing precisely when that type was busiest.
                    if exclude_types and str(e.get("type", "")) in exclude_types:
                        continue
                    ts = e.get("occurred_at_epoch")
                    if ts is not None:
                        if after_epoch is not None and float(ts) < after_epoch:
                            continue
                        if before_epoch is not None and float(ts) > before_epoch:
                            continue

                    if entity_id and str(e.get("entity_id") or "").upper() != entity_id:
                        continue
                    if region and e.get("region") != region:
                        continue
                    if tags:
                        # Python equivalent of PostgreSQL's "tags && %s" (array overlap check)
                        # Returns True if ANY tag in the required 'tags' list exists in the event's tags.
                        event_tags = e.get("tags") or []
                        if not any(t in event_tags for t in tags):
                            continue

                    results.append(e)
                
            
                    
            # The SQL query ordered by anomaly_score DESC, then occurred_at DESC.
            # Redis sorted them by occurred_at DESC natively. Now we just sort by anomaly.
            # The part of the window Redis does not have.
            #
            # The cache holds 48 hours and prunes anything older. Two shipped
            # rules ask for 168: the informed-trading sequence, whose insider
            # leg is a Form 4 with a two-business-day filing deadline, and the
            # institutional-holdings rule, whose trigger is a quarterly
            # disclosure. Both were querying a seven-day window against a
            # two-day store, so every clause silently truncated at 48 hours and
            # the evidence those rules exist to find was unreachable -- not
            # rejected, not logged, simply absent.
            #
            # The events table keeps 90 days and is indexed on (type,
            # occurred_at) and (primary_entity_id, occurred_at). Reading the
            # remainder from there costs one indexed scan for the rules that
            # ask beyond the cache and nothing at all for the rules that do not.
            if hours > CACHED_WINDOW_HOURS:
                deep = await self._deep_window(
                    event_types,
                    older_than=time.time() - (CACHED_WINDOW_HOURS * 3600),
                    newer_than=cutoff,
                    min_anomaly=min_anomaly,
                    region=region,
                    tags=tags,
                    entity_id=entity_id,
                    exclude_types=exclude_types,
                    exclude_event_id=exclude_event_id,
                    after_epoch=after_epoch,
                    before_epoch=before_epoch,
                )
                if deep:
                    seen = {r["event_id"] for r in results}
                    results.extend(r for r in deep if r["event_id"] not in seen)

            results.sort(key=lambda x: x["anomaly_score"], reverse=True)
            return _select_diverse_evidence(results, limit)

        except Exception as e:
            logger.error(f"Redis cache fetch failed: {e}")
            return []

    async def _deep_window(
        self,
        event_types: Optional[List[str]],
        *,
        older_than: float,
        newer_than: float,
        min_anomaly: float = 0.0,
        region: Optional[str] = None,
        tags: Optional[List[str]] = None,
        entity_id: Optional[str] = None,
        exclude_types: Optional[Sequence[str]] = None,
        exclude_event_id: Optional[str] = None,
        after_epoch: Optional[float] = None,
        before_epoch: Optional[float] = None,
    ) -> List[Dict]:
        """Events between the cache's horizon and the clause's, from Postgres.

        Every filter the Redis path applies in Python is applied here in SQL,
        so a clause returns the same evidence whichever side of the 48-hour
        boundary it falls on. Rows are shaped exactly like the cached payload:
        the caller cannot tell them apart, and must not have to.

        Returns nothing rather than raising. The cached window is the primary
        source and a database that is slow or down must degrade a seven-day
        clause to a two-day one, not fail the correlation.
        """
        if not self._db:
            return []
        lo = max(0.0, newer_than if after_epoch is None else max(newer_than, after_epoch))
        hi = older_than if before_epoch is None else min(older_than, before_epoch)
        if hi <= lo:
            return []

        # The cache's own floor, applied here too.
        #
        # `add_event` declines anything below RECENT_WINDOW_MIN_ANOMALY, so the
        # cached half of a window has never contained one. Without this the
        # same clause would see two different populations either side of the
        # 48-hour line -- everything above 0.15 for the recent part and
        # everything at all for the older part -- and a rule asking for
        # `min_anomaly: 0.0` would draw evidence from last week that it could
        # not draw from yesterday.
        floor = max(float(min_anomaly or 0.0), RECENT_WINDOW_MIN_ANOMALY)

        params: List[Any] = [
            datetime.fromtimestamp(lo, tz=timezone.utc),
            datetime.fromtimestamp(hi, tz=timezone.utc),
            floor,
        ]
        where = [
            "occurred_at >= $1",
            "occurred_at < $2",
            "COALESCE(anomaly_score, 0) >= $3",
        ]

        def _bind(value) -> str:
            params.append(value)
            return f"${len(params)}"

        if event_types:
            where.append(f"type = ANY({_bind(list(event_types))})")
        if exclude_types:
            where.append(f"NOT (type = ANY({_bind(list(exclude_types))}))")
        if entity_id:
            where.append(f"UPPER(primary_entity_id) = {_bind(str(entity_id).upper())}")
        if region:
            where.append(f"region = {_bind(region)}")
        if tags:
            where.append(f"tags && {_bind(list(tags))}")
        if exclude_event_id:
            where.append(f"event_id::text <> {_bind(str(exclude_event_id))}")

        sql = f"""
            SELECT event_id::text AS event_id, type, source, anomaly_score, tags,
                   region, headline, summary, named_entities,
                   primary_entity_id AS entity_id,
                   primary_entity_name AS entity_name,
                   primary_entity_type AS entity_type,
                   COALESCE(latitude, ST_Y(coordinates::geometry)) AS latitude,
                   COALESCE(longitude, ST_X(coordinates::geometry)) AS longitude,
                   EXTRACT(EPOCH FROM occurred_at) AS occurred_at_epoch
            FROM events
            WHERE {' AND '.join(where)}
            ORDER BY anomaly_score DESC, occurred_at DESC
            LIMIT {DEEP_WINDOW_MAX_ROWS}
        """
        try:
            # Bounded, because this runs on the hot path.
            #
            # The correlation engine evaluates every rule against every event,
            # so a clause reaching past the cache adds a database round trip to
            # that loop. The cached window is the primary source; a slow
            # database must cost a seven-day clause its older five days, not
            # stall the consumer behind it.
            rows = await asyncio.wait_for(
                self._db.query(sql, *params), timeout=DEEP_WINDOW_TIMEOUT_SEC
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Deep correlation window read exceeded %.1fs; falling back to the "
                "cached window for this clause.", DEEP_WINDOW_TIMEOUT_SEC,
            )
            MetricsCollector.increment("pipeline_errors_total:deep_window_timeout")
            return []
        except Exception as exc:
            # Counted, because a deep window that silently stops working
            # returns a rule to exactly the truncation this method exists to
            # fix -- and that failure is invisible in the rule's own output.
            logger.warning("Deep correlation window read failed: %s", exc)
            MetricsCollector.increment("pipeline_errors_total:deep_window_read")
            return []

        out = []
        for row in rows or []:
            e = dict(row)
            e["anomaly_score"] = float(e.get("anomaly_score") or 0.0)
            e["tags"] = list(e.get("tags") or [])
            e["named_entities"] = list(e.get("named_entities") or [])
            e["occurred_at_epoch"] = float(e.get("occurred_at_epoch") or 0.0)
            # The cached payload carries the canonical domain; derive the same
            # one here rather than leaving the key absent, which reads as
            # "unknown domain" to every consumer of this list.
            e["domain"] = canonical_domain(e.get("type") or "")
            out.append(e)
        return out

    async def save_correlation(self, cluster) -> None:
        """
        Persist a CorrelationCluster to the correlations table.
        Errors are logged but not re-raised — a failed save doesn't block
        the correlation engine from processing the next event.
        """
        try:
            tier_map = {"WATCH": 1, "ALERT": 2, "ELEVATED": 3, "INTELLIGENCE": 4, "CRITICAL": 5}
            tier_str = cluster.alert_tier.value if hasattr(cluster.alert_tier, 'value') else str(cluster.alert_tier)
            tier_int = tier_map.get(str(tier_str).upper(), 2)

            # Eight fields the cluster is published with were not named here,
            # so they were dropped in silence on every row. Nothing reading a
            # correlation from the database could see its confidence, its
            # domain, how many events supported it, or the headline the operator
            # was shown -- the writer names the columns it knows about, and the
            # ones it does not raise nothing.
            metrics = getattr(cluster, "metrics_summary", None)
            await self._db.execute("""
                INSERT INTO correlations (
                    correlation_id, rule_id, rule_name, alert_tier,
                    detected_at, trigger_event_id, supporting_event_ids,
                    entity_ids, description, tags,
                    confidence_score, primary_domain, summary_headline,
                    supporting_headlines, metrics_summary,
                    primary_entity_id, primary_entity_name, entity_names
                ) VALUES ($1::uuid, $2, $3, $4, $5, $6::uuid, $7::uuid[], $8, $9, $10,
                          $11, $12, $13, $14, $15::jsonb, $16, $17, $18)
            """, 
                cluster.correlation_id,
                cluster.rule_id,
                cluster.rule_name,
                tier_int,
                cluster.detected_at,
                cluster.trigger_event_id,
                cluster.supporting_event_ids,
                cluster.entity_ids,
                cluster.description,
                cluster.tags,
                getattr(cluster, "confidence_score", None),
                getattr(cluster, "primary_domain", None),
                getattr(cluster, "summary_headline", None),
                getattr(cluster, "supporting_headlines", None),
                # Passed as a mapping, not a pre-serialised string: the pool
                # registers a jsonb codec whose encoder is already json.dumps,
                # so serialising here would store a jsonb *string* rather than
                # the object -- the double-encoding defect repaired elsewhere in
                # this audit, which made half the scenario corpus unqueryable.
                metrics if isinstance(metrics, dict) else None,
                getattr(cluster, "primary_entity_id", None),
                getattr(cluster, "primary_entity_name", None),
                getattr(cluster, "entity_names", None),
            )
            logger.info(f"💾 Persisted correlation {cluster.correlation_id} to TimescaleDB.")

            # The back-link, from every event this cluster cites to the cluster.
            #
            # `supporting_event_ids` points one way and nothing ever pointed
            # back: of 20,271 events scoring 0.5 or above in 24 hours, 20,256
            # carried no `correlation_ids` at all. The only type with any was
            # market_anomaly, at 4 of 73 -- and that is precisely the one path
            # repaired by name, for the radar enricher alone.
            #
            # Without it the question "what did this event contribute to?" has
            # no answer from the event side: the inspector cannot show it,
            # /explain/event/{id} cannot trace it, and an analyst starting from
            # a suspicious transfer cannot reach the cluster that cited it.
            #
            # Detached and best-effort: a correlation that is already persisted
            # must not be lost because its back-link failed, and the forward
            # reference remains authoritative either way.
            safe_create_task(
                self._link_events_to_correlation(cluster),
                name="correlation-backlink",
            )
        except Exception as e:
            # Logged with a traceback, counted, and kept.
            #
            # The previous handler logged one line and returned. That is not
            # silence, but it is not recoverable either: a correlation that
            # fails to persist is gone -- the engine has already moved on, and
            # nothing retries it -- so a database in trouble loses every finding
            # it produces while the service reports healthy throughput.
            #
            # The cluster is parked in Redis so it can be replayed, and the
            # counter is what a monitor can alert on. A single failure is
            # ordinary; a rising count means findings are being dropped.
            logger.error(
                "save_correlation failed (%s): %s", cluster.correlation_id, e, exc_info=True
            )
            MetricsCollector.increment("pipeline_errors_total:correlation_persist")
            try:
                raw = getattr(self._redis, "raw", self._redis)
                await raw.lpush(
                    FAILED_CORRELATIONS_KEY,
                    json.dumps(cluster.model_dump(mode="json") if hasattr(cluster, "model_dump") else str(cluster), default=str),
                )
                await raw.ltrim(FAILED_CORRELATIONS_KEY, 0, FAILED_CORRELATIONS_MAX - 1)
            except Exception as park_err:
                # Both stores unavailable. Nothing further can be done here, but
                # it is said plainly rather than swallowed.
                logger.error(
                    "Correlation %s could not be persisted or parked: %s",
                    cluster.correlation_id, park_err,
                )
                MetricsCollector.increment("pipeline_errors_total:correlation_lost")

    async def _link_events_to_correlation(self, cluster) -> None:
        """Append this correlation's id to each event it cites.

        `array_append` guarded by `NOT correlation_ids @> ARRAY[...]` so a
        replayed or re-published cluster does not accumulate duplicates, and
        `COALESCE` because the column is null on every row written before this
        existed.
        """
        ids = [i for i in (list(getattr(cluster, "supporting_event_ids", None) or [])
                           + [getattr(cluster, "trigger_event_id", None)]) if i]
        if not ids or self._db is None:
            return
        try:
            await self._db.execute(
                """
                UPDATE events
                   SET correlation_ids =
                         array_append(COALESCE(correlation_ids, ARRAY[]::uuid[]), $1::uuid)
                 WHERE event_id = ANY($2::uuid[])
                   -- Bounded in time so TimescaleDB can exclude chunks.
                   --
                   -- Without this the UPDATE scans all nineteen chunks of a
                   -- 3.5 GB hypertable for a handful of ids and times out --
                   -- which is exactly what it did, silently, on every cluster:
                   -- "Correlation back-link failed" with an empty asyncpg
                   -- TimeoutError message. The events a cluster cites are
                   -- recent by construction; the correlation window itself is
                   -- 48 hours, so nothing older can be supporting evidence.
                   AND occurred_at > NOW() - ($3 || ' seconds')::interval
                   AND NOT (COALESCE(correlation_ids, ARRAY[]::uuid[]) @> ARRAY[$1::uuid])
                """,
                cluster.correlation_id,
                list({str(i) for i in ids}),
                str(int(self.window_seconds + FUTURE_TOLERANCE_SEC)),
            )
        except Exception as e:
            # Counted rather than only whispered: a back-link that stops working
            # is invisible from the forward side, which is how it came to be
            # missing on 99.9% of events without anything reporting it.
            logger.warning("Correlation back-link failed for %s: %s", cluster.correlation_id, e)
            MetricsCollector.increment("pipeline_errors_total:correlation_backlink")
