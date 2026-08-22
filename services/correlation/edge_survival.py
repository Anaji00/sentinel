"""
services/correlation/edge_survival.py

Out-of-sample survival tracking for discovered correlation edges (INN-04).

The discovery engine emits edges that pass a significance test with FDR
correction, and those edges decay on a 30-day half-life. What was never
recorded is whether a discovered edge *replicates*: whether the relationship
found on one window is still there on the next.

That number is the difference between a correlation engine and a correlation
generator. A discovery process with a 10% survival rate is producing noise with
p-values attached, and no amount of multiple-comparison correction reveals that
-- only re-testing does.

The design deliberately avoids a second statistics implementation: an edge is
registered at discovery with the statistic that justified it, and re-evaluated
later against a freshly computed statistic supplied by the caller. This module
owns the bookkeeping, not the mathematics.
"""

import json
import logging
import math
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("correlation.edge_survival")

# Redis keyspace.
COHORT_KEY = "sentinel:edges:cohort"          # hash: edge_id -> registration JSON
SUMMARY_KEY = "sentinel:edges:survival"       # hash: rolling aggregate

# An edge is eligible for re-test once the out-of-sample window has elapsed.
# One week: long enough that the re-test window shares little data with the
# discovery window, short enough to accumulate verdicts at a useful rate.
DEFAULT_RETEST_AFTER_SEC = 7 * 24 * 3600

# Survival is judged on the relationship persisting, not on reproducing the
# original coefficient exactly. A correlation that was 0.62 and is now 0.48 has
# survived; one that is now 0.03 has not.
SURVIVAL_MIN_ABS_COEFFICIENT = 0.20
SURVIVAL_MAX_P_VALUE = 0.05

# Retention for registered edges, so an un-retested cohort cannot grow forever.
COHORT_TTL_SEC = 90 * 24 * 3600


@dataclass
class EdgeRegistration:
    """An edge as it stood at discovery, for later comparison."""
    edge_id: str
    source: str
    target: str
    relation: str
    method: str                     # "pearson", "granger", "cointegration", "hawkes"
    coefficient: float
    p_value: float
    q_value: Optional[float] = None
    discovered_at: float = field(default_factory=time.time)
    window_bars: Optional[int] = None

    def is_due(self, now: Optional[float] = None,
               retest_after: float = DEFAULT_RETEST_AFTER_SEC) -> bool:
        return (now or time.time()) - self.discovered_at >= retest_after


@dataclass
class SurvivalVerdict:
    """The outcome of re-testing one edge out of sample."""
    edge_id: str
    method: str
    survived: bool
    original_coefficient: float
    retest_coefficient: Optional[float]
    retest_p_value: Optional[float]
    coefficient_decay: Optional[float]  # fraction of original magnitude retained
    days_elapsed: float
    reason: str


class EdgeSurvivalTracker:
    """Registers discovered edges and scores whether they replicate.

    Usage sits either side of the discovery cycle:

        # at discovery
        await tracker.register(EdgeRegistration(...))

        # later, once the caller has recomputed the statistic
        verdict = await tracker.evaluate(edge_id, coefficient=r, p_value=p)
    """

    def __init__(self, redis_client: Any = None):
        self.redis = redis_client

    # ── registration ─────────────────────────────────────────────────────────

    @staticmethod
    def edge_id(source: str, target: str, method: str) -> str:
        """Stable identity for an edge, direction-preserving.

        Granger is directional -- A causing B is a different claim from B
        causing A -- so the pair is not sorted.
        """
        return f"{method}:{source.upper()}:{target.upper()}"

    async def register(self, reg: EdgeRegistration) -> bool:
        """Records an edge at discovery time. Re-registration is ignored.

        Keeping the first observation matters: overwriting on re-discovery would
        continually reset the clock and no edge would ever become due, which
        silently disables the whole measurement.
        """
        if not self.redis:
            return False
        try:
            raw = getattr(self.redis, "raw", self.redis)
            existing = await raw.hget(COHORT_KEY, reg.edge_id)
            if existing:
                return False
            await raw.hset(COHORT_KEY, reg.edge_id, json.dumps(asdict(reg)))
            await raw.expire(COHORT_KEY, COHORT_TTL_SEC)
            return True
        except Exception as e:
            logger.warning("Edge registration failed for %s: %s", reg.edge_id, e)
            return False

    async def due_for_retest(
        self,
        limit: int = 200,
        retest_after: float = DEFAULT_RETEST_AFTER_SEC,
    ) -> List[EdgeRegistration]:
        """Registered edges whose out-of-sample window has elapsed."""
        if not self.redis:
            return []
        try:
            raw = getattr(self.redis, "raw", self.redis)
            entries = await raw.hgetall(COHORT_KEY)
            now = time.time()
            due: List[EdgeRegistration] = []
            for _, blob in (entries or {}).items():
                try:
                    data = json.loads(blob.decode() if isinstance(blob, bytes) else blob)
                    reg = EdgeRegistration(**data)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue  # a malformed entry must not stall the whole sweep
                if reg.is_due(now, retest_after):
                    due.append(reg)
                if len(due) >= limit:
                    break
            return due
        except Exception as e:
            logger.warning("Could not list edges due for re-test: %s", e)
            return []

    # ── evaluation ───────────────────────────────────────────────────────────

    async def evaluate(
        self,
        edge_id: str,
        coefficient: Optional[float],
        p_value: Optional[float],
        now: Optional[float] = None,
    ) -> Optional[SurvivalVerdict]:
        """Scores an edge against a freshly computed statistic.

        `coefficient=None` means the statistic could not be recomputed at all --
        typically the series no longer exists. That is recorded as non-survival
        with a distinct reason, because "the relationship vanished" and "we
        could not look" should not be conflated in the survival rate.
        """
        if not self.redis:
            return None
        try:
            raw = getattr(self.redis, "raw", self.redis)
            blob = await raw.hget(COHORT_KEY, edge_id)
            if not blob:
                return None
            reg = EdgeRegistration(**json.loads(blob.decode() if isinstance(blob, bytes) else blob))
        except Exception as e:
            logger.warning("Could not load edge %s for evaluation: %s", edge_id, e)
            return None

        elapsed_days = ((now or time.time()) - reg.discovered_at) / 86400.0

        if coefficient is None:
            verdict = SurvivalVerdict(
                edge_id=edge_id, method=reg.method, survived=False,
                original_coefficient=reg.coefficient, retest_coefficient=None,
                retest_p_value=None, coefficient_decay=None,
                days_elapsed=round(elapsed_days, 2),
                reason="series_unavailable",
            )
        else:
            strong_enough = abs(coefficient) >= SURVIVAL_MIN_ABS_COEFFICIENT
            significant = p_value is not None and p_value <= SURVIVAL_MAX_P_VALUE
            # Sign flip is a failure even at high magnitude: a relationship that
            # reversed is not the relationship that was discovered.
            same_direction = (coefficient * reg.coefficient) >= 0

            survived = bool(strong_enough and significant and same_direction)
            if not same_direction:
                reason = "sign_reversed"
            elif not strong_enough:
                reason = "decayed_below_threshold"
            elif not significant:
                reason = "no_longer_significant"
            else:
                reason = "replicated"

            decay = (abs(coefficient) / abs(reg.coefficient)) if reg.coefficient else None
            verdict = SurvivalVerdict(
                edge_id=edge_id, method=reg.method, survived=survived,
                original_coefficient=reg.coefficient,
                retest_coefficient=round(coefficient, 4),
                retest_p_value=round(p_value, 6) if p_value is not None else None,
                coefficient_decay=round(decay, 4) if decay is not None else None,
                days_elapsed=round(elapsed_days, 2),
                reason=reason,
            )

        await self._record(verdict)
        return verdict

    async def _record(self, verdict: SurvivalVerdict) -> None:
        """Folds a verdict into the rolling aggregate and retires the edge."""
        if not self.redis:
            return
        try:
            raw = getattr(self.redis, "raw", self.redis)
            pipe = raw.pipeline()
            pipe.hincrby(SUMMARY_KEY, "tested_total", 1)
            pipe.hincrby(SUMMARY_KEY, f"tested:{verdict.method}", 1)
            if verdict.survived:
                pipe.hincrby(SUMMARY_KEY, "survived_total", 1)
                pipe.hincrby(SUMMARY_KEY, f"survived:{verdict.method}", 1)
            pipe.hincrby(SUMMARY_KEY, f"reason:{verdict.reason}", 1)
            # Retire from the cohort: an edge is judged once, so a re-discovered
            # relationship enters as a fresh observation rather than
            # double-counting the original.
            pipe.hdel(COHORT_KEY, verdict.edge_id)
            await pipe.execute()
        except Exception as e:
            logger.warning("Could not record survival verdict: %s", e)

    # ── reporting ────────────────────────────────────────────────────────────

    async def survival_report(self) -> Dict[str, Any]:
        """Survival rate overall and per method, with the cohort still pending.

        Returns rates as None rather than 0.0 when nothing has been tested:
        an untested engine has no survival rate, and reporting 0% would read as
        a catastrophic result rather than an absent measurement.
        """
        empty = {
            "tested": 0, "survived": 0, "survival_rate": None,
            "by_method": {}, "failure_reasons": {}, "pending_retest": 0,
        }
        if not self.redis:
            return empty
        try:
            raw = getattr(self.redis, "raw", self.redis)
            summary = await raw.hgetall(SUMMARY_KEY) or {}
            cohort = await raw.hlen(COHORT_KEY) or 0
        except Exception as e:
            logger.warning("Survival report unavailable: %s", e)
            return empty

        def _get(field: str) -> int:
            for k, v in summary.items():
                key = k.decode() if isinstance(k, bytes) else k
                if key == field:
                    return int(v.decode() if isinstance(v, bytes) else v)
            return 0

        tested, survived = _get("tested_total"), _get("survived_total")

        methods: Dict[str, Dict[str, Any]] = {}
        reasons: Dict[str, int] = {}
        for k, v in summary.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = int(v.decode() if isinstance(v, bytes) else v)
            if key.startswith("tested:"):
                methods.setdefault(key[7:], {})["tested"] = val
            elif key.startswith("survived:"):
                methods.setdefault(key[9:], {})["survived"] = val
            elif key.startswith("reason:"):
                reasons[key[7:]] = val

        for stats in methods.values():
            t, s = stats.get("tested", 0), stats.get("survived", 0)
            stats["survival_rate"] = round(s / t, 4) if t else None

        return {
            "tested": tested,
            "survived": survived,
            "survival_rate": round(survived / tested, 4) if tested else None,
            "by_method": methods,
            "failure_reasons": reasons,
            "pending_retest": int(cohort),
        }
