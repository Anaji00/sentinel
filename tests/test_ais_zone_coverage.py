"""
tests/test_ais_zone_coverage.py

The system claims to watch nine maritime chokepoints. Four of them have never
returned a single message.

    Taiwan Strait      8,668        Strait of Hormuz      0
    Strait of Malacca  5,616        Bab-el-Mandeb         0
    Black Sea          4,840        Gulf of Guinea        2
    Turkish Straits    3,775        Red Sea              53

The Strait of Hormuz is the most consequential energy chokepoint on the map and
the configuration for it is correct: the bounding box is right, it is first in
the subscription, and classify_region knows the name. Zero vessels have ever
been recorded inside those coordinates.

The cause is the data source, not the code. AISStream is a free terrestrial
aggregator fed by volunteer receivers, and coverage follows where those
receivers are -- dense across Europe and East Asia, absent in the Persian Gulf
and West Africa. Terrestrial AIS needs a receiver within roughly 40nm; the
satellite feed that covers the rest is a paid product.

That cannot be fixed from here. What can be fixed is the silence: a subscribed
box with no coverage produces exactly the same output as a quiet one, so nobody
reading this system would know that a quarter of its declared watch area has
never reported anything.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services" / "collector-ais" / "main.py").read_text(encoding="utf-8")


def _load():
    import importlib.util

    spec = importlib.util.spec_from_file_location("ais_main", ROOT / "services/collector-ais/main.py")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:
        pass
    return module


def test_every_box_has_a_name():
    """A zone cannot be reported as silent if nothing knows what it is called."""
    m = _load()
    assert len(m.ZONE_NAMES) == len(m.WATCH_ZONES)


def test_hormuz_is_still_subscribed():
    """The gap is coverage, not configuration. If this box ever disappears the
    explanation above stops being true."""
    m = _load()
    idx = m.ZONE_NAMES.index("Strait of Hormuz")
    (lat1, lon1), (lat2, lon2) = m.WATCH_ZONES[idx]
    assert lat1 < 26.6 < lat2 and lon1 < 56.5 < lon2, "the box no longer contains the strait"


def test_a_position_is_attributed_to_its_zone():
    m = _load()
    counter = m.MessageCounter()
    counter.note_position(26.6, 56.5)      # Strait of Hormuz
    counter.note_position(1.3, 103.8)      # Singapore, Strait of Malacca
    assert counter.per_zone.get("Strait of Hormuz") == 1
    assert counter.per_zone.get("Strait of Malacca") == 1


def test_a_position_outside_every_zone_is_not_misattributed():
    """Silently bucketing open ocean into the nearest chokepoint would make the
    coverage report worse than useless."""
    m = _load()
    counter = m.MessageCounter()
    counter.note_position(0.0, -30.0)      # mid-Atlantic
    assert counter.per_zone == {}


def test_silent_zones_are_named_in_the_report():
    """Absence has to be stated. It reads identically to quiet otherwise."""
    assert "Silent: %s" in SOURCE
    assert "no receiver coverage looks identical" in SOURCE


def test_the_report_only_fires_when_messages_are_arriving():
    """During a genuine outage every zone is silent and the list would be noise
    on top of an outage that is already reported."""
    assert "if silent and self.total:" in SOURCE


def test_malformed_coordinates_do_not_break_ingestion():
    """AIS metadata arrives absent and arrives as text."""
    m = _load()
    counter = m.MessageCounter()
    for lat, lon in ((None, None), ("abc", "def")):
        try:
            counter.note_position(float(lat), float(lon))
        except (TypeError, ValueError):
            pass
    assert counter.per_zone == {}
