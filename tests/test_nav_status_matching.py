"""
tests/test_nav_status_matching.py

A vessel that cannot manoeuvre never raised an anomaly.

Found by tracing maritime messages end to end while the system was live. The
headlines read:

    Tanker 'BANDA' restrictedmanoeuverability in Turkish Straits
    Unknown 'FERTILE' underwayusingengine in Turkish Straits

The status table is CamelCase, and the enricher tested for it with prose:

    any(w in nav_status.lower() for w in
        ("not under command", "restricted", "constrained", "aground"))

Lowercased, "NotUnderCommand" is "notundercommand". The spaced term never
appears in it. Three of the four terms happened to be single words and matched
by luck; the multi-word one silently did not -- so AIS status 2, among the most
serious states the protocol can report, was invisible to the detector.

The label is a display string. The code is the value, and that is what the
detector now reads. The label is also spaced now, because it reaches vessel
headlines and the reasoning prompt, where "restrictedmanoeuverability" is a word
nobody writes and no model has seen.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.regions import (  # noqa: E402
    RESTRICTED_NAV_STATUS_CODES, decode_nav_status, is_restricted_nav_status,
)


def test_not_under_command_is_finally_detected():
    """The defect verbatim. Status 2 is a vessel that cannot manoeuvre."""
    assert is_restricted_nav_status(2)


def test_the_other_restricted_statuses_still_are():
    for code in (3, 4, 6):  # restricted, constrained by draught, aground
        assert is_restricted_nav_status(code)


def test_ordinary_navigation_is_not_an_anomaly():
    for code in (0, 1, 5, 7, 8, 15):  # under way, anchored, moored, fishing...
        assert not is_restricted_nav_status(code)


def test_a_missing_or_malformed_status_is_not_an_anomaly():
    """AIS fields arrive absent and arrive as text. Neither is a vessel in
    distress, and raising on either would be worse than missing one."""
    for value in (None, "", "abc", [], {}):
        assert not is_restricted_nav_status(value)


def test_a_numeric_string_still_resolves():
    """Payload fields arrive as strings often enough to matter."""
    assert is_restricted_nav_status("2")
    assert not is_restricted_nav_status("0")


# -- the label is read by people and by the model ------------------------------

def test_the_label_is_words():
    assert decode_nav_status(3) == "Restricted Manoeuverability"
    assert decode_nav_status(2) == "Not Under Command"
    assert decode_nav_status(0) == "Under Way Using Engine"


def test_an_unknown_code_says_so_rather_than_guessing():
    assert "Unknown" in decode_nav_status(99)


def test_the_detector_no_longer_depends_on_the_label():
    """String-matching a display label is what broke. If the wording changes
    again, detection must not."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "maritime.py").read_text(encoding="utf-8")
    assert "is_restricted_nav_status(nav_code)" in source
    assert 'any(w in nav_status.lower()' not in source


def test_every_restricted_code_has_a_readable_label():
    for code in RESTRICTED_NAV_STATUS_CODES:
        label = decode_nav_status(code)
        assert label and "Unknown" not in label
