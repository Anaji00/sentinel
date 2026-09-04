"""
shared/utils/chokepoints.py

How busy a chokepoint is, relative to how busy it usually is.

The platform watches nine straits and had no measure of traffic through any of
them. It could say a particular vessel went dark; it could not say whether the
Strait of Hormuz was quieter this morning than it has been all month, which is
the question a blockade, a closure or a fleet standing off actually poses.

Two things make that measure awkward, and both are handled here rather than by
the callers:

  Sources differ    AIS delivers a vessel count. Sentinel-1 delivers a share of
                    water surface returning like metal. Neither is convertible
                    into the other, and averaging them would be meaningless.
                    Each source is therefore scored against its own history for
                    its own chokepoint, and only the resulting z-score is
                    comparable.

  Coverage differs  Four of the nine chokepoints have never returned an AIS
                    message, so an AIS count of zero there is a statement about
                    receiver coverage rather than about traffic. A chokepoint
                    with no history for a source is refused rather than reported
                    as quiet.

The output is deliberately a deviation, not a volume. "Forty vessels" means
nothing without knowing that the usual figure is two hundred; "three standard
deviations below normal" is the same sentence in every strait and from either
instrument.
"""

import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

logger = logging.getLogger("shared.chokepoints")

# Observations required before a baseline means anything.
#
# Below this the standard deviation is mostly an artefact of which few readings
# happened to arrive, and a z-score computed from it would be confident nonsense
# -- the same failure the earnings surprise scorer had when it compared the
# first observation of an issuer against itself.
MIN_BASELINE_OBSERVATIONS = 12

# How many observations to keep per chokepoint per source.
BASELINE_WINDOW = 500

# Deviation at which a reading is worth someone's attention. Two sigma either
# way; the direction matters as much as the size, because a strait emptying is
# a different event from one filling.
NOTABLE_SIGMA = 2.0

_BASELINE_KEY = "sentinel:chokepoint:baseline:{source}:{chokepoint}"


@dataclass(frozen=True)
class TrafficReading:
    """One measurement of one chokepoint from one instrument."""

    chokepoint: str
    source: str          # "ais" or "sar"
    value: float
    observed_at: str


@dataclass(frozen=True)
class TrafficAssessment:
    """What a reading means against that chokepoint's own history."""

    reading: TrafficReading
    baseline_mean: float
    baseline_std: float
    observations: int
    z_score: float

    @property
    def is_notable(self) -> bool:
        return abs(self.z_score) >= NOTABLE_SIGMA

    @property
    def direction(self) -> str:
        """Quieter or busier. A strait emptying is not a strait filling."""
        if self.z_score <= -NOTABLE_SIGMA:
            return "quieter_than_usual"
        if self.z_score >= NOTABLE_SIGMA:
            return "busier_than_usual"
        return "normal"

    def as_payload(self) -> dict:
        return {
            "chokepoint": self.reading.chokepoint,
            "source": self.reading.source,
            "value": self.reading.value,
            "baseline_mean": round(self.baseline_mean, 4),
            "baseline_std": round(self.baseline_std, 4),
            "observations": self.observations,
            "z_score": round(self.z_score, 3),
            "direction": self.direction,
            "observed_at": self.reading.observed_at,
        }


def baseline_key(source: str, chokepoint: str) -> str:
    return _BASELINE_KEY.format(
        source=str(source).strip().lower(),
        chokepoint=str(chokepoint).strip().lower().replace(" ", "_"),
    )


def assess(reading: TrafficReading, history: Sequence[float]) -> Optional[TrafficAssessment]:
    """A reading against its own chokepoint's history, or None when it cannot be judged.

    None is a real answer. A chokepoint with no history for this source has not
    been quiet -- it has not been measured, and the two are only distinguishable
    if one of them declines to produce a number.
    """
    values = []
    for item in history or []:
        try:
            v = float(item)
        except (TypeError, ValueError):
            continue
        if v == v and math.isfinite(v):
            values.append(v)

    if len(values) < MIN_BASELINE_OBSERVATIONS:
        return None

    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance)

    if std <= 0:
        # A baseline that never varies cannot rank a new reading. This is the
        # frozen-quote shape: repetition looks like certainty.
        return None

    return TrafficAssessment(
        reading=reading,
        baseline_mean=mean,
        baseline_std=std,
        observations=len(values),
        z_score=(reading.value - mean) / std,
    )


async def record_and_assess(redis_client, reading: TrafficReading) -> Optional[TrafficAssessment]:
    """Scores a reading against stored history, then adds it to that history.

    Read before write, deliberately: scoring a reading against a baseline that
    already contains it pulls the mean toward the very observation being judged,
    which is how a first sighting was made to look ordinary elsewhere in this
    system.
    """
    key = baseline_key(reading.source, reading.chokepoint)
    history: List[float] = []
    try:
        raw = await redis_client.raw.lrange(key, 0, BASELINE_WINDOW - 1)
        history = [float(x) for x in (raw or [])]
    except (TypeError, ValueError):
        history = []
    except Exception as e:
        logger.debug(f"Chokepoint baseline read failed for {key}: {e}")

    assessment = assess(reading, history)

    try:
        await redis_client.raw.lpush(key, reading.value)
        await redis_client.raw.ltrim(key, 0, BASELINE_WINDOW - 1)
        await redis_client.raw.expire(key, 90 * 86400)
    except Exception as e:
        logger.debug(f"Chokepoint baseline write failed for {key}: {e}")

    return assessment
