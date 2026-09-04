"""
services/collector-sar/sar_detection.py

Radar ship detection for the chokepoints AIS cannot see.

Four of nine watched chokepoints have never returned a single AIS message:
Strait of Hormuz, Bab-el-Mandeb, Suez Canal and the Gulf of Guinea. The cause is
not configuration -- the bounding boxes are correct, and widening the Hormuz box
to include Bandar Abbas, Jebel Ali, Abu Dhabi and Fujairah still returned zero
over twelve minutes. AISStream is a free terrestrial aggregator and its coverage
follows volunteer receivers, which the Persian Gulf and West Africa do not have.

Sentinel-1 does not care. It is a radar satellite: a metal hull on flat water is
a bright target against a dark background, day or night, through cloud, and
entirely independent of whether the vessel is transmitting. A ship running dark
is invisible to every AIS source in existence and visible here.

What this is not:

  Real time    Sentinel-1C and Sentinel-1D fly a six-day nominal revisit, so a
               chokepoint is imaged every few days rather than continuously.
               This augments AIS where AIS is blind; it does not replace it
               where AIS works.

  A vessel      Backscatter above a threshold is a target, not an identity.
  count         There is no MMSI, no name and no destination -- and separating
               two adjacent hulls from one large one needs more than a
               threshold. What this reports is how much metal was on the water,
               which is a traffic measure rather than a manifest.

The value is precisely in what AIS cannot offer: an independent count that a
transponder cannot switch off. A drop in AIS traffic through Hormuz with no
matching drop here is a fleet going dark, and that comparison is impossible
with either source alone.
"""

import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("collector.sar")

# The chokepoints AIS has never delivered from. Deliberately not every watch
# zone: imaging somewhere already covered by a live AIS feed spends credits to
# learn less than the feed already says.
BLIND_CHOKEPOINTS: Dict[str, Dict[str, float]] = {
    "Strait of Hormuz": {"west": 55.80, "south": 26.10, "east": 56.90, "north": 27.10},
    "Bab-el-Mandeb":    {"west": 43.20, "south": 12.40, "east": 43.60, "north": 12.80},
    "Suez Canal":       {"west": 32.25, "south": 29.90, "east": 32.65, "north": 31.30},
    "Gulf of Guinea":   {"west": 3.00,  "south": 3.50,  "east": 5.00,  "north": 5.00},
}

# Where a Sentinel-1 VV return stops looking like water.
#
# Calm sea sits well below -20 dB and a ship's hull returns far above it. Zero
# is deliberately conservative: it accepts fewer targets than a keener cut and
# is far less prone to calling a whitecap a tanker. Wind roughens the sea and
# raises the floor, which is the main reason this is a relative measure over
# time rather than an absolute count.
VV_TARGET_THRESHOLD_DB = float(os.getenv("SAR_VV_THRESHOLD_DB", "0.0"))

# How far back to look for an acquisition. The constellation revisit is six
# days nominal, so a shorter window frequently finds nothing at all, which
# would look like an empty strait rather than a satellite that has not passed.
LOOKBACK_DAYS = int(os.getenv("SAR_LOOKBACK_DAYS", "8"))

COLLECTION = "SENTINEL1_GRD"
OPENEO_URL = os.getenv("OPENEO_URL", "https://openeo.dataspace.copernicus.eu")


@dataclass(frozen=True)
class ChokepointReading:
    """One radar look at one chokepoint."""

    chokepoint: str
    observed_on: str
    target_pixels: int
    water_pixels: int
    bbox: Dict[str, float]

    @property
    def target_density(self) -> float:
        """Share of the water surface returning like metal.

        The comparable quantity between passes. Absolute pixel counts move with
        swath geometry and sea state; the ratio moves rather less.
        """
        if self.water_pixels <= 0:
            return 0.0
        return round(self.target_pixels / self.water_pixels, 8)

    def as_event_payload(self) -> dict:
        return {
            "chokepoint": self.chokepoint,
            "observed_on": self.observed_on,
            "target_pixels": self.target_pixels,
            "water_pixels": self.water_pixels,
            "target_density": self.target_density,
            "bbox": self.bbox,
            "instrument": "sentinel-1-sar",
            "method": f"VV backscatter > {VV_TARGET_THRESHOLD_DB} dB",
            # Stated on every reading, because a reader who takes this for a
            # vessel count will draw conclusions it cannot support.
            "is_vessel_count": False,
        }


def credentials() -> Optional[Tuple[str, str]]:
    """CDSE service-account credentials, or None when unconfigured.

    Registered through the Sentinel Hub dashboard's self-service OAuth client
    page. Absent credentials are an ordinary state -- this collector is an
    augmentation, and the platform runs without it.
    """
    client_id = os.getenv("CDSE_CLIENT_ID", "").strip()
    client_secret = os.getenv("CDSE_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        return None
    return client_id, client_secret


def observation_window(today: Optional[date] = None) -> List[str]:
    """The date range to ask for, as openEO expects it."""
    end = today or date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return [start.isoformat(), end.isoformat()]


def build_datacube(connection, bbox: Dict[str, float], window: List[str]):
    """A VV-band radar cube over one chokepoint.

    Kept separate from the counting so the query can be read, and tested,
    without a live connection.
    """
    return connection.load_collection(
        COLLECTION,
        spatial_extent=bbox,
        temporal_extent=window,
        bands=["VV"],
    )


def count_targets(values, threshold_db: float = VV_TARGET_THRESHOLD_DB) -> Tuple[int, int]:
    """(target pixels, water pixels) from a flat sequence of VV values.

    Separated from the openEO call because this is the part with a decision in
    it. Nodata is excluded from both counts rather than being treated as water,
    since a masked pixel is not evidence of an empty sea.
    """
    targets = 0
    water = 0
    for value in values:
        if value is None:
            continue
        try:
            v = float(value)
        except (TypeError, ValueError):
            continue
        if v != v:  # NaN
            continue
        water += 1
        if v > threshold_db:
            targets += 1
    return targets, water
