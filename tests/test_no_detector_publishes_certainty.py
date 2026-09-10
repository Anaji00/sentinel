"""Nothing in this platform should publish certainty.

The argument is stated repeatedly in this codebase and was applied unevenly:
the most extreme thing seen so far is still only that, and a detector reporting
1.000 leaves nothing to say when something genuinely worse arrives. The
streaming detectors were bounded at 0.995, then the open-interest ramp, the
dark-vessel path, the sanctions floor, and `_notional_score` after a $106.61M
whale transfer scored exactly 1.000.

Three paths were missed, and the live data said so -- 24 hours, by type:

    options_flow   48 at exactly 1.000
    headline        9
    social_signal   2

`_options_premium_score` hard-returned 1.0 at its reference, the after-hours
session divide capped at 1.0, and the news threat branch -- **three lines below
the sanctions floor that was corrected** -- still read `min(1.0, ...)`. It runs
after the sanctions floor and overwrites it, which is why the nine headlines
were all Iran/UN Security Council wire copy matching both.

That is the pattern this audit records more than once: a uniformity pass
converts the sites it was looking at and the identical one beside it survives.
"""
import pathlib
import re

import pytest

from shared.utils.streaming_detectors import FALLBACK_MAX_SCORE

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_the_ceiling_is_below_one():
    assert 0.9 < FALLBACK_MAX_SCORE < 1.0


def test_a_reference_sized_options_premium_does_not_report_certainty():
    from services.enrichment.enrichers.tradfi import _options_premium_score

    for premium in (2_000_000, 5_000_000, 50_000_000):
        assert _options_premium_score(premium) <= FALLBACK_MAX_SCORE, premium


def test_the_options_score_still_orders_by_size():
    """Bounding must not flatten the top of the scale."""
    from services.enrichment.enrichers.tradfi import _options_premium_score

    small = _options_premium_score(120_000)
    mid = _options_premium_score(600_000)
    assert 0.0 < small < mid <= FALLBACK_MAX_SCORE


def test_the_thin_session_divide_cannot_exceed_the_scale():
    src = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
    assert "min(1.0, base_score / _session_depth)" not in src
    assert "min(FALLBACK_MAX_SCORE, base_score / _session_depth)" in src


def test_the_threat_branch_matches_the_sanctions_floor_beside_it():
    """They are three lines apart and only one had been corrected."""
    src = (ROOT / "services" / "enrichment" / "enrichers" / "news.py").read_text(encoding="utf-8")
    body = re.sub(r"#[^\n]*", "", src)
    assert "min(1.0, max(0.80" not in body
    assert "min(FALLBACK_MAX_SCORE, max(0.80" in body
    # And the floor it sits beside is still bounded too.
    assert "min(FALLBACK_MAX_SCORE, max(0.85" in body


@pytest.mark.parametrize(
    "path,needle",
    [
        ("services/enrichment/enrichers/news.py", "min(1.0, max(0.80"),
        ("services/enrichment/enrichers/tradfi.py", "min(1.0, base_score / _session_depth)"),
    ],
)
def test_the_corrected_forms_do_not_come_back(path, needle):
    src = re.sub(r"#[^\n]*", "", (ROOT / path).read_text(encoding="utf-8"))
    assert needle not in src
