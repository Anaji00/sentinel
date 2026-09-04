"""
tests/test_sar_collector.py

Radar augmentation for the four chokepoints AIS has never covered.

Neither obvious route would have worked. The Raspberry Pi project widely cited
for Hormuz monitoring polls aisstream.io -- the same free terrestrial feed
already in use, with the Pi as compute rather than a receiver -- and its author
states the mid-strait coverage limit outright. Widening the Hormuz bounding box
to include Bandar Abbas, Jebel Ali, Abu Dhabi and Fujairah returned zero vessels
over twelve minutes, which disposes of the theory that the box was drawn wrong.

Sentinel-1 sidesteps the problem: it detects hulls, not transponders, so a
vessel running dark is visible to it and invisible to every AIS source there is.

Two limits are structural and are stated on every reading rather than left for a
reader to discover. The constellation flies a six-day nominal revisit, so this
is a periodic look and not a feed. And backscatter above a threshold is a
target, not a vessel -- no MMSI, no name, and no way to separate two adjacent
hulls from one large one.
"""

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (str(ROOT), str(ROOT / "services" / "collector-sar")):
    if path not in sys.path:
        sys.path.insert(0, path)

from sar_detection import (  # noqa: E402
    BLIND_CHOKEPOINTS, LOOKBACK_DAYS, ChokepointReading, count_targets,
    credentials, observation_window,
)


def _reading(targets=14, water=20000, name="Strait of Hormuz"):
    return ChokepointReading(name, "2026-09-02", targets, water, BLIND_CHOKEPOINTS[name])


# -- scope: only where AIS is blind --------------------------------------------

def test_it_covers_the_chokepoints_ais_never_reached():
    for name in ("Strait of Hormuz", "Bab-el-Mandeb", "Suez Canal", "Gulf of Guinea"):
        assert name in BLIND_CHOKEPOINTS


def test_it_does_not_image_where_ais_already_works():
    """Imaging a covered strait spends credits to learn less than the live feed
    already says. 10,000 credits a month is not a large budget."""
    for name in ("Taiwan Strait", "Strait of Malacca", "Black Sea"):
        assert name not in BLIND_CHOKEPOINTS


def test_the_hormuz_box_contains_the_strait():
    box = BLIND_CHOKEPOINTS["Strait of Hormuz"]
    assert box["west"] <= 56.25 <= box["east"]
    assert box["south"] <= 26.57 <= box["north"]


# -- the counting decision -----------------------------------------------------

def test_bright_returns_are_counted_as_targets():
    targets, water = count_targets([-25.0, -24.0, 3.5, 12.0])
    assert (targets, water) == (2, 4)


def test_nodata_is_excluded_from_both_counts():
    """A masked pixel is not evidence of empty sea. Counting it as water would
    dilute the density and make a cloudy pass look like a quiet strait."""
    targets, water = count_targets([None, float("nan"), -20.0, 5.0])
    assert (targets, water) == (1, 2)


def test_unparseable_values_do_not_raise():
    assert count_targets(["abc", {}, [], -20.0]) == (0, 1)


def test_an_empty_pass_is_not_an_empty_strait():
    """No acquisition returns no pixels, which the collector must distinguish
    from open water with no ships on it."""
    assert count_targets([]) == (0, 0)


# -- what a reading claims -----------------------------------------------------

def test_density_is_the_comparable_quantity():
    """Absolute pixel counts move with swath geometry and sea state; the ratio
    moves rather less, which is what makes two passes comparable."""
    assert _reading(14, 20000).target_density == 0.0007


def test_no_water_means_no_density_rather_than_a_division_error():
    assert _reading(0, 0).target_density == 0.0


def test_every_reading_denies_being_a_vessel_count():
    """A reader who takes this for a ship count will draw conclusions it cannot
    support, so the payload says so on every record rather than in a document."""
    assert _reading().as_event_payload()["is_vessel_count"] is False


def test_the_payload_states_how_it_was_measured():
    payload = _reading().as_event_payload()
    assert payload["instrument"] == "sentinel-1-sar"
    assert "dB" in payload["method"]


# -- operating without credentials ---------------------------------------------

def test_absent_credentials_are_an_ordinary_state(monkeypatch):
    """This is an augmentation. The platform ran without it before and must
    continue to, rather than the collector crash-looping."""
    monkeypatch.delenv("CDSE_CLIENT_ID", raising=False)
    monkeypatch.delenv("CDSE_CLIENT_SECRET", raising=False)
    assert credentials() is None


def test_blank_credentials_count_as_absent(monkeypatch):
    """An empty value in .env is the shape this arrives in, not a missing key."""
    monkeypatch.setenv("CDSE_CLIENT_ID", "   ")
    monkeypatch.setenv("CDSE_CLIENT_SECRET", "")
    assert credentials() is None


def test_complete_credentials_are_accepted(monkeypatch):
    monkeypatch.setenv("CDSE_CLIENT_ID", "abc")
    monkeypatch.setenv("CDSE_CLIENT_SECRET", "xyz")
    assert credentials() == ("abc", "xyz")


# -- the observation window ----------------------------------------------------

def test_the_window_is_wider_than_the_revisit():
    """A window shorter than the six-day revisit frequently finds nothing,
    which would read as an empty strait rather than a satellite that has not
    passed over yet."""
    assert LOOKBACK_DAYS >= 6


def test_the_window_ends_today():
    start, end = observation_window(date(2026, 9, 2))
    assert end == "2026-09-02"
    assert start == "2026-08-25"
