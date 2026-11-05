"""
shared/utils/vessel_score.py

What one ship position report scores.

A ship's flag and navigation status used to be detector inputs, and also took it
out of the routine band, so a flagged or restricted ship scored high on every
report whatever it was doing. Over 14 days to 4 Nov 2026, 293 of 357 stored ships
reached 0.9, and OSHONE averaged 0.88 over 3,332 reports from one spot. The
detector now sees motion only, and status is handled here: it can raise a report a
little, and it counts in full only when it changes.

The detector's score is a percentile, so ordinary motion alone reaches 0.7 about a
third of the time. That is why every report starts in the routine band.
"""
from __future__ import annotations

from typing import Any, Mapping, Optional

from shared.utils.regions import is_restricted_nav_status, routine_band_score

# Top of the detector's range: only here does a status ship's motion count as odd.
ODD_MOTION_CUT = 0.97
# Odd motion is ordered in this band, below the findings that assert something
# about a ship (spoofed identity 0.82, sailing inland 0.74 and transfers 0.78 can
# sit above it).
ODD_MOTION_FLOOR = 0.60
ODD_MOTION_CEILING = 0.75
# Shares of the headroom above the routine score. Entering a restricted status is
# news, enough to be stored; a name on the list or the watchlist is a standing fact.
ENTERED_RESTRICTED_LIFT = 0.45
LISTED_LIFT = 0.20
# The ship's own MMSI or IMO on the list: more than a matching name, and enough to
# be stored on its own, though a report of an ordinary position still is not news.
IDENTIFIED_LIFT = 0.35
WATCHED_LIFT = 0.15


def entered_restricted(previous: Optional[Mapping[str, Any]], nav_code: Optional[int]) -> bool:
    """Whether this report is the one that put the ship in a restricted status.

    A stored report from before nav_code was saved says nothing either way, so it
    does not count: otherwise every restricted ship would alert once on deploy.
    """
    if not is_restricted_nav_status(nav_code):
        return False
    if not previous:
        return True
    return previous.get("nav_code", nav_code) != nav_code


def position_score(motion: float, region: Optional[str], *, listed: bool = False, identified: bool = False,
                   watched: bool = False, restricted: bool = False, entered: bool = False) -> float:
    """The report's anomaly score from its motion percentile and the ship's status."""
    base = routine_band_score(motion, region)
    if (listed or identified or watched or restricted) and float(motion or 0.0) >= ODD_MOTION_CUT:
        share = (min(1.0, float(motion)) - ODD_MOTION_CUT) / (1.0 - ODD_MOTION_CUT)
        return round(ODD_MOTION_FLOOR + share * (ODD_MOTION_CEILING - ODD_MOTION_FLOOR), 4)
    lift = max(ENTERED_RESTRICTED_LIFT if entered else 0.0, IDENTIFIED_LIFT if identified else 0.0,
               LISTED_LIFT if listed else 0.0, WATCHED_LIFT if watched else 0.0)
    return round(base + (1.0 - base) * lift, 4)


__all__ = ["ENTERED_RESTRICTED_LIFT", "IDENTIFIED_LIFT", "LISTED_LIFT", "ODD_MOTION_CEILING", "ODD_MOTION_CUT", "ODD_MOTION_FLOOR",
           "WATCHED_LIFT", "entered_restricted", "position_score"]
