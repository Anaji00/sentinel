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

_repeats: Dict[str, int] = {}


def repeated(site: str, key: str) -> None:
    """Count a reading dropped as a re-read, and say so as it becomes routine.

    Silence here would be the defect this function exists to fix: a baseline
    that stops growing for a good reason and a baseline that stops growing
    because nothing is being written look identical from outside.
    """
    n = _repeats[site] = _repeats.get(site, 0) + 1
    if n == 1 or n % 25 == 0:
        logger.info(
            "%s: %s reading(s) not added to a baseline because the window "
            "returned the same acquisition as last time (%s).", site, n, key,
        )

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

# Appended to the key when a source states how it was calibrated.
#
# A z-score compares a reading against readings taken the same way. When the
# SAR threshold was corrected -- it had been comparing a decibel number against
# linear backscatter, so every density was ~1.0 -- the corrected readings came
# back around 0.001. Scored against the old history, the first one would have
# been about a thousand sigma below the mean, and this collector would have
# announced that every chokepoint it watches had emptied simultaneously.
#
# So the calibration is part of the identity of the series. Change it and a new
# baseline starts, which is the honest behaviour: there is no history for the
# new measurement, and `assess` already refuses to judge a reading it has no
# history for.
_CALIBRATED_KEY = "sentinel:chokepoint:baseline:{source}@{calibration}:{chokepoint}"

# The latest per-cell radar grid for a chokepoint.
#
# Current state rather than a time series: the question it answers is "where was
# the metal on the most recent pass", and the answer is replaced wholesale each
# time the satellite comes round. It lives here rather than in either caller
# because the collector writes it and the API reads it, and two spellings of one
# key is how this platform lost a watchlist lookup.
GRID_KEY = "sentinel:chokepoint:grid:{chokepoint}"


def grid_key(chokepoint: str) -> str:
    """The grid key for one chokepoint, normalised the way the baseline is."""
    return GRID_KEY.format(
        chokepoint=str(chokepoint).strip().lower().replace(" ", "_")
    )


@dataclass(frozen=True)
class TrafficReading:
    """One measurement of one chokepoint from one instrument."""

    chokepoint: str
    source: str          # "ais" or "sar"
    value: float
    observed_at: str
    # How this number was produced, when the producer can say. Two readings with
    # different calibrations are not the same measurement and must not share a
    # baseline; see _CALIBRATED_KEY.
    calibration: str = ""


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


def baseline_key(source: str, chokepoint: str, calibration: str = "") -> str:
    """The history key for one source's readings of one chokepoint.

    `calibration` names how the number was produced. Sources that do not state
    one keep the original key, so nothing that was already accumulating a
    baseline loses it.
    """
    source = str(source).strip().lower()
    chokepoint = str(chokepoint).strip().lower().replace(" ", "_")
    calibration = str(calibration or "").strip().lower().replace(" ", "_")
    if not calibration:
        return _BASELINE_KEY.format(source=source, chokepoint=chokepoint)
    return _CALIBRATED_KEY.format(
        source=source, calibration=calibration, chokepoint=chokepoint,
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
    key = baseline_key(
        reading.source, reading.chokepoint, getattr(reading, "calibration", ""),
    )
    history: List[float] = []
    try:
        raw = await redis_client.raw.lrange(key, 0, BASELINE_WINDOW - 1)
        history = [float(x) for x in (raw or [])]
    except (TypeError, ValueError):
        history = []
    except Exception as e:
        logger.debug(f"Chokepoint baseline read failed for {key}: {e}")

    assessment = assess(reading, history)

    # One observation per acquisition, not one per sweep.
    #
    # The SAR window is eight days wide and reduced with max-over-time, so a
    # daily sweep re-reads the same acquisitions and returns bit-identical
    # numbers. Appending each of those made a baseline of fifteen observations
    # out of three: measured on this deployment, bab-el-mandeb held 3 distinct
    # values across 15 entries and strait_of_hormuz 4 across 9.
    #
    # It inflates the count past MIN_BASELINE_OBSERVATIONS on a third of the
    # evidence, and it drives the standard deviation toward zero -- so the first
    # reading that genuinely differs scores an enormous z against a series that
    # never moved because nothing new was ever read.
    #
    # Equality is the right test here rather than a timestamp: these are the
    # same pixels through the same computation, so a repeat is exact. Two
    # distinct acquisitions agreeing to sixteen significant figures is not a
    # thing that happens.
    if history and history[0] == reading.value:
        repeated("chokepoints.baseline.same_acquisition", key)
        return assessment

    try:
        await redis_client.raw.lpush(key, reading.value)
        await redis_client.raw.ltrim(key, 0, BASELINE_WINDOW - 1)
        await redis_client.raw.expire(key, 90 * 86400)
    except Exception as e:
        logger.debug(f"Chokepoint baseline write failed for {key}: {e}")

    return assessment
