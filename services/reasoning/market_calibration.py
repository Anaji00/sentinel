"""
services/reasoning/market_calibration.py

Calibration against prediction-market odds (INN-01).

The platform computes conviction scores and grades them with an internal Brier
score. That measures the model against itself: it says whether stated confidence
tracked realized outcomes *within Sentinel's own record*, and cannot say whether
that confidence is competitive with an informed observer.

A liquid prediction market is exactly such an observer -- a probability estimate
produced continuously by people with money at stake, and empirically among the
better-calibrated forecasts available for free. `collector-prediction` has been
ingesting Polymarket and Kalshi odds since the platform was built, and those
numbers have never been compared to Sentinel's own.

Two measurements, requiring different data:

  DIVERGENCE  available now. How far Sentinel's conviction sits from the market's
              implied probability on the same proposition. Needs no outcome, and
              a persistent one-sided divergence is itself a finding: either an
              edge or a bias, and which one is answered by the second measure.

  SKILL       requires settled outcomes. Brier score for Sentinel versus Brier
              for the market on the same resolved questions. A skill score above
              zero means Sentinel beat the market's forecast.

KNOWN GAP: `PredictionMarketData` carries `resolution_date` but no settled
outcome -- the collector records when a market resolves, not what it resolved
to. Skill scoring therefore stays dormant until a resolution feed exists. That
is stated rather than worked around: inferring outcomes from the final traded
price would silently grade the market against a proxy for itself.
"""

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("reasoning.market_calibration")

FORECAST_KEY = "sentinel:calibration:market_forecasts"
RESOLVED_KEY = "sentinel:calibration:market_resolved"

# Reliability-curve bins. Wider than the backtester's three, because
# calibration against a market is interesting precisely at the extremes where a
# coarse binning hides everything.
PROBABILITY_BINS = [
    (0.0, 0.1), (0.1, 0.2), (0.2, 0.3), (0.3, 0.4), (0.4, 0.5),
    (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0),
]

# Divergence beyond this is worth surfacing: the platform and the market
# materially disagree about the same proposition.
MATERIAL_DIVERGENCE = 0.15

FORECAST_TTL_SEC = 180 * 24 * 3600


@dataclass
class PairedForecast:
    """Sentinel's probability and the market's, on one proposition."""
    market_id: str
    question: str
    sentinel_probability: float
    market_probability: float
    recorded_at: float = field(default_factory=time.time)
    resolution_date: Optional[str] = None
    ticker: Optional[str] = None

    @property
    def divergence(self) -> float:
        """Signed: positive when Sentinel is more confident than the market."""
        return self.sentinel_probability - self.market_probability


@dataclass
class ResolvedForecast:
    """A paired forecast with its settled outcome."""
    market_id: str
    question: str
    sentinel_probability: float
    market_probability: float
    outcome: int                 # 1 = resolved YES, 0 = resolved NO
    resolved_at: float = field(default_factory=time.time)

    @property
    def sentinel_brier(self) -> float:
        return (self.sentinel_probability - self.outcome) ** 2

    @property
    def market_brier(self) -> float:
        return (self.market_probability - self.outcome) ** 2


class MarketCalibrationTracker:
    """Pairs Sentinel forecasts with market odds and scores both."""

    def __init__(self, redis_client: Any = None):
        self.redis = redis_client

    # ── recording ────────────────────────────────────────────────────────────

    async def record_forecast(self, forecast: PairedForecast) -> bool:
        """Stores a Sentinel/market probability pair for one proposition.

        Both probabilities must be genuine 0-1 ratios. A conviction score on a
        different scale would be silently graded as a probability and produce a
        meaningless Brier score, so the range is enforced here.
        """
        for label, p in (("sentinel", forecast.sentinel_probability),
                         ("market", forecast.market_probability)):
            if not (0.0 <= p <= 1.0):
                logger.warning(
                    "Rejected forecast for %s: %s probability %.4f is outside [0,1]",
                    forecast.market_id, label, p,
                )
                return False

        if not self.redis:
            return False
        try:
            raw = getattr(self.redis, "raw", self.redis)
            pipe = raw.pipeline()
            pipe.hset(FORECAST_KEY, forecast.market_id, json.dumps(asdict(forecast)))
            pipe.expire(FORECAST_KEY, FORECAST_TTL_SEC)
            await pipe.execute()
            return True
        except Exception as e:
            logger.warning("Could not record paired forecast: %s", e)
            return False

    async def resolve(self, market_id: str, outcome: int) -> Optional[ResolvedForecast]:
        """Grades a stored forecast once its market has settled.

        `outcome` is 1 for YES, 0 for NO. Anything else is rejected rather than
        coerced -- a mis-signed outcome inverts the skill score.
        """
        if outcome not in (0, 1):
            logger.warning("Rejected resolution for %s: outcome must be 0 or 1", market_id)
            return None
        if not self.redis:
            return None
        try:
            raw = getattr(self.redis, "raw", self.redis)
            blob = await raw.hget(FORECAST_KEY, market_id)
            if not blob:
                return None
            data = json.loads(blob.decode() if isinstance(blob, bytes) else blob)
            resolved = ResolvedForecast(
                market_id=data["market_id"],
                question=data["question"],
                sentinel_probability=data["sentinel_probability"],
                market_probability=data["market_probability"],
                outcome=outcome,
            )
            pipe = raw.pipeline()
            pipe.hset(RESOLVED_KEY, market_id, json.dumps(asdict(resolved)))
            pipe.expire(RESOLVED_KEY, FORECAST_TTL_SEC)
            pipe.hdel(FORECAST_KEY, market_id)
            await pipe.execute()
            return resolved
        except Exception as e:
            logger.warning("Could not resolve forecast %s: %s", market_id, e)
            return None

    # ── reporting ────────────────────────────────────────────────────────────

    async def divergence_report(self, limit: int = 25) -> Dict[str, Any]:
        """Where Sentinel and the market most disagree, right now.

        Available without any resolved outcome, which makes it the usable half
        of this module today.
        """
        empty = {"paired_forecasts": 0, "mean_abs_divergence": None,
                 "mean_signed_divergence": None, "material_disagreements": []}
        if not self.redis:
            return empty
        try:
            raw = getattr(self.redis, "raw", self.redis)
            entries = await raw.hgetall(FORECAST_KEY) or {}
        except Exception as e:
            logger.warning("Divergence report unavailable: %s", e)
            return empty

        pairs: List[PairedForecast] = []
        for _, blob in entries.items():
            try:
                pairs.append(PairedForecast(**json.loads(
                    blob.decode() if isinstance(blob, bytes) else blob)))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

        if not pairs:
            return empty

        divergences = [p.divergence for p in pairs]
        material = sorted(
            (p for p in pairs if abs(p.divergence) >= MATERIAL_DIVERGENCE),
            key=lambda p: abs(p.divergence), reverse=True,
        )[:limit]

        return {
            "paired_forecasts": len(pairs),
            "mean_abs_divergence": round(sum(abs(d) for d in divergences) / len(divergences), 4),
            # A persistent positive mean means Sentinel is systematically more
            # confident than the market -- overconfidence until skill says
            # otherwise.
            "mean_signed_divergence": round(sum(divergences) / len(divergences), 4),
            "material_threshold": MATERIAL_DIVERGENCE,
            "material_disagreements": [
                {
                    "market_id": p.market_id,
                    "question": p.question[:140],
                    "sentinel_probability": round(p.sentinel_probability, 4),
                    "market_probability": round(p.market_probability, 4),
                    "divergence": round(p.divergence, 4),
                    "ticker": p.ticker,
                }
                for p in material
            ],
        }

    async def skill_report(self) -> Dict[str, Any]:
        """Brier scores for Sentinel and the market on resolved questions.

        Returns nulls with an explicit reason when nothing has resolved, rather
        than a zero that would read as a measured result.
        """
        empty = {
            "resolved_count": 0,
            "sentinel_brier": None,
            "market_brier": None,
            "skill_score": None,
            "reliability_curve": [],
            "note": "No resolved markets. A resolution feed is required; the "
                    "collector records resolution_date but not the settled outcome.",
        }
        if not self.redis:
            return empty
        try:
            raw = getattr(self.redis, "raw", self.redis)
            entries = await raw.hgetall(RESOLVED_KEY) or {}
        except Exception as e:
            logger.warning("Skill report unavailable: %s", e)
            return empty

        resolved: List[ResolvedForecast] = []
        for _, blob in entries.items():
            try:
                resolved.append(ResolvedForecast(**json.loads(
                    blob.decode() if isinstance(blob, bytes) else blob)))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

        if not resolved:
            return empty

        n = len(resolved)
        s_brier = sum(r.sentinel_brier for r in resolved) / n
        m_brier = sum(r.market_brier for r in resolved) / n

        # Murphy skill score against the market as reference. Positive means
        # Sentinel's forecasts were sharper than the market's; zero means no
        # advantage; negative means the market was better.
        skill = (1.0 - (s_brier / m_brier)) if m_brier > 1e-12 else None

        curve = []
        for lo, hi in PROBABILITY_BINS:
            bucket = [r for r in resolved if lo <= r.sentinel_probability < hi]
            curve.append({
                "bin": f"{int(lo*100)}-{int(hi*100)}%",
                "count": len(bucket),
                # None, not 0.0: an empty bin is an absent measurement.
                "mean_forecast": round(sum(r.sentinel_probability for r in bucket) / len(bucket), 4) if bucket else None,
                "observed_rate": round(sum(r.outcome for r in bucket) / len(bucket), 4) if bucket else None,
            })

        return {
            "resolved_count": n,
            "sentinel_brier": round(s_brier, 6),
            "market_brier": round(m_brier, 6),
            "skill_score": round(skill, 4) if skill is not None else None,
            "interpretation": (
                "positive: Sentinel outperformed the market; "
                "zero: parity; negative: the market forecast better"
            ),
            "reliability_curve": curve,
        }
