"""
tests/test_constant_score_detectors.py

Three detectors emitted one score for every event they ever produced, found by
sweeping the live system during market hours:

    bgp_anomaly      219 events   1 distinct score   0.850
    earnings_report  183 events   1 distinct score   0.300
    flight_anomaly    85 events   1 distinct score   0.800

For comparison, over the same window market_anomaly carried 229 distinct scores
and options_flow 117. A detector whose output never varies ranks nothing, and
every one of these clears the thresholds that decide which handful of events a
capacity-bound host spends an inference on.

Each had a different cause and they are worth separating:

  bgp_anomaly      A TypeError. RIS sends AS paths as integers and
                   ",".join(as_path) raised on the first hop -- thrown after
                   the two node upserts and before the relationship, and caught
                   at debug. Zero ANNOUNCES relationships existed against 2,028
                   AS nodes, so "has this AS announced this prefix before" was
                   always answered no, path_novelty pinned at 1.0, and every
                   event scored 0.70 + 0.30 x (0.5 x 1.0) = 0.850.

  earnings_report  A flat baseline. A report that has not happened has no
                   surprise to measure, but that does not make every upcoming
                   report equally interesting.

  flight_anomaly   A category overwriting a measurement -- `elif is_sanctioned:
                   raw_score = 0.80` -- which is the same defect already fixed
                   in crypto transfers, where a watched counterparty replaced
                   the size signal and 39,262 transfers shared one score.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.anomaly_scorer import lift_score  # noqa: E402


# -- bgp: the AS path type error -----------------------------------------------

def test_an_integer_as_path_no_longer_raises():
    """RIS sends hops as integers. This is the whole defect."""
    as_path = [7725, 1299, 3356]
    assert ",".join(str(hop) for hop in as_path) == "7725,1299,3356"


def test_the_upsert_stringifies_every_hop():
    source = (ROOT / "shared" / "utils" / "streaming_detectors.py").read_text(encoding="utf-8")
    assert 'str(hop) for hop in as_path' in source
    assert '",".join(as_path)' not in source, "the raw join is what raised"


def test_the_upsert_failure_is_no_longer_silent():
    """Debug-level swallowing is what let path novelty stay pinned for the life
    of the detector."""
    source = (ROOT / "shared" / "utils" / "streaming_detectors.py").read_text(encoding="utf-8")
    block = source.split("async def upsert_as_path")[1].split("async def ")[0]
    assert "logger.warning" in block
    assert "logger.debug" not in block


def test_missing_graph_features_are_reported():
    """The defaults claim maximum novelty, which is a fixed score for every
    hijack. Falling back to them is a measurement failure."""
    source = (ROOT / "shared" / "utils" / "streaming_detectors.py").read_text(encoding="utf-8")
    block = source.split("async def extract_features")[1].split("async def ")[0]
    assert "logger.warning" in block


def test_the_pinned_bgp_score_is_reproducible_from_the_defaults():
    """0.850 was not arbitrary: it is exactly what the blend returns when
    novelty is 1.0 and everything else is 0."""
    from services.enrichment.anomaly_scorer import (
        HIJACK_BASE_SCORE, HIJACK_CENTRALITY_WEIGHT,
        HIJACK_NOVELTY_WEIGHT, HIJACK_VELOCITY_WEIGHT,
    )

    base = HIJACK_BASE_SCORE
    contribution = HIJACK_NOVELTY_WEIGHT * 1.0 + HIJACK_CENTRALITY_WEIGHT * 0.0 + HIJACK_VELOCITY_WEIGHT * 0.0
    assert round(base + (1.0 - base) * contribution, 3) == 0.850


# -- earnings: a pre-announcement is not a constant ----------------------------

def test_a_nearer_report_outranks_a_distant_one():
    from services.enrichment.enrichers.tradfi import (
        EARNINGS_LOOKAHEAD_DAYS, PRE_ANNOUNCEMENT_FLOOR, PROXIMITY_WEIGHT,
    )

    tomorrow = PRE_ANNOUNCEMENT_FLOOR + PROXIMITY_WEIGHT * (1.0 - 1 / EARNINGS_LOOKAHEAD_DAYS)
    next_week = PRE_ANNOUNCEMENT_FLOOR + PROXIMITY_WEIGHT * 0.0
    assert tomorrow > next_week


def test_an_upcoming_report_cannot_outrank_a_measured_surprise():
    """A surprise is scored on a z-score against the issuer's own history.
    Something that has not happened must not outrank it."""
    from services.enrichment.enrichers.tradfi import PRE_ANNOUNCEMENT_CEILING

    assert PRE_ANNOUNCEMENT_CEILING < 1.0


def test_the_pre_announcement_band_is_actually_a_band():
    """The defect was one value for 183 events."""
    from services.enrichment.enrichers.tradfi import (
        PRE_ANNOUNCEMENT_CEILING, PRE_ANNOUNCEMENT_FLOOR,
        PROXIMITY_WEIGHT, SURPRISE_VOLATILITY_WEIGHT,
    )

    top = min(PRE_ANNOUNCEMENT_CEILING,
              PRE_ANNOUNCEMENT_FLOOR + PROXIMITY_WEIGHT + SURPRISE_VOLATILITY_WEIGHT)
    assert top - PRE_ANNOUNCEMENT_FLOOR >= 0.2, "the band must be wide enough to rank on"


def test_earnings_boosts_use_headroom_not_addition():
    """Four call sites in this file still added their boosts, in a file whose
    own comments say it was converted to a headroom lift."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
    executable = [
        ln for ln in source.splitlines()
        if "anomaly + w_boost + f_boost" in ln and not ln.lstrip().startswith("#")
    ]
    assert executable == [], f"still adding boosts at {len(executable)} site(s)"


# -- aviation: sanctioned is a reason to look, not a score ---------------------

def test_a_sanctioned_aircraft_is_ranked_by_its_behaviour():
    """The defect verbatim: every sanctioned aircraft scored 0.80, so one
    holding a normal cruise ranked with one manoeuvring off its route."""
    from services.enrichment.enrichers.aviation import SANCTIONED_LIFT_WEIGHT

    calm = lift_score(0.05, SANCTIONED_LIFT_WEIGHT)
    erratic = lift_score(0.85, SANCTIONED_LIFT_WEIGHT)
    assert erratic > calm
    assert round(calm, 3) != round(erratic, 3)


def test_a_sanctions_match_still_lifts_the_floor():
    """Provenance must still matter; it just must not be the whole answer."""
    from services.enrichment.enrichers.aviation import SANCTIONED_LIFT_WEIGHT

    assert lift_score(0.05, SANCTIONED_LIFT_WEIGHT) > 0.05


def test_squawk_codes_keep_their_distinct_meanings():
    """7500 is a hijack and 7600 a radio failure. These are standardised codes
    whose meanings genuinely differ, so they stay distinct floors."""
    floors = {"7500": 1.0, "7700": 0.85, "7600": 0.70}
    assert len(set(floors.values())) == 3


def test_aviation_boosts_use_headroom_not_addition():
    source = (ROOT / "services" / "enrichment" / "enrichers" / "aviation.py").read_text(encoding="utf-8")
    assert "raw_score + w_boost + f_boost" not in source


def test_the_kinematic_score_is_always_measured():
    """It used to be skipped entirely for emergency and sanctioned aircraft,
    which is why those branches could not discriminate.

    The measurement now happens in enrich_batch, in one batch call for the whole
    scan, and is passed into _score_flight -- so the invariant is stronger than
    it was: it is computed for every aircraft before any branch can see a squawk
    or a sanctions flag. This guard was previously written against the singular
    call site inside _score_flight, which no longer exists.
    """
    source = (ROOT / "services" / "enrichment" / "enrichers" / "aviation.py").read_text(encoding="utf-8")

    # Measured for the whole batch, unconditionally, before any scoring branch.
    batch_block = source.split("async def enrich_batch")[1].split("async def ")[0]
    assert "score_kinematic_event_batch" in batch_block, (
        "aviation must score kinematics via the batch API; it is the highest-volume domain"
    )

    # And every branch of the per-aircraft combination uses that measurement.
    block = source.split("async def _score_flight")[1].split("return final_score")[0]
    assert "kinematic" in block.split("if is_emerg")[0], "kinematic must be in scope before branching"
    emerg_branch, _, rest = block.partition("if is_emerg")
    sanctioned_branch = rest.split("else:")[0]
    assert "kinematic" in sanctioned_branch, (
        "the emergency and sanctioned branches must still incorporate the measurement, "
        "not replace it with a category"
    )


# -- bgp: why the score was arithmetically certain -----------------------------
#
# Fixing the AS-path TypeError restored path novelty, and the score stayed at
# 0.850 anyway. Tracing the blend on this deployment explains why:
#
#   betweenness_centrality  permanently 0 -- Neo4j here has no GDS plugin
#                           (SHOW PROCEDURES yields 0 gds.* entries)
#   path_novelty            1.0 for a hijack almost by definition; announcing a
#                           prefix you have never announced is what a hijack is,
#                           and zero repeat announcements existed to prove it
#   velocity                explicitly zeroed for hijacks:
#                               vel = ... if not hijack else 0.0
#
# Three features, two structurally fixed and the third switched off for exactly
# the population that needed telling apart. The output could not have varied.

def test_velocity_is_measured_for_hijacks():
    """The one differentiator this deployment can still compute was the one
    being discarded."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "cyber.py").read_text(encoding="utf-8")
    executable = [
        ln for ln in source.splitlines()
        if "_calculate_velocity(\"bgp\"" in ln and not ln.lstrip().startswith("#")
    ]
    assert executable, "the bgp velocity call went missing"
    assert not any("if not hijack" in ln for ln in executable)


def test_hijack_scores_now_separate_on_velocity():
    """A burst of hijacked announcements from one AS is a different event from
    a single one, and the score has to say so."""
    from services.enrichment.anomaly_scorer import (
        HIJACK_BASE_SCORE, HIJACK_CENTRALITY_WEIGHT,
        HIJACK_NOVELTY_WEIGHT, HIJACK_VELOCITY_WEIGHT,
    )

    def blend(velocity: float) -> float:
        contribution = (
            HIJACK_NOVELTY_WEIGHT * 1.0
            + HIJACK_CENTRALITY_WEIGHT * 0.0
            + HIJACK_VELOCITY_WEIGHT * velocity
        )
        return round(HIJACK_BASE_SCORE + (1.0 - HIJACK_BASE_SCORE) * min(1.0, contribution), 4)

    assert blend(1.0) > blend(0.5) > blend(0.0)


def test_the_announces_edge_is_what_makes_novelty_measurable():
    """Novelty asks whether this AS announced this prefix before, which is
    unanswerable while the edge recording it is never written."""
    source = (ROOT / "shared" / "utils" / "streaming_detectors.py").read_text(encoding="utf-8")
    assert "MERGE (a)-[r:ANNOUNCES]->(p)" in source
    assert "ON CREATE SET r.first_seen" in source


# -- market anomaly: an unbounded statistic mapped without a cliff -------------
#
# Traced live in a single fifteen-minute window:
#
#   QUANT RADAR VOLUME SPIKE | OKE | Z-Score:  5.02  -> 1.00
#   QUANT RADAR VOLUME SPIKE | MA  | Z-Score: 12.76  -> 1.00
#
# min(1.0, z / 5.0) saturates at five sigma, so a spike two and a half times
# more extreme than another scored identically, and 45 of the last half hour's
# market_anomaly events sat on the ceiling. The z-score's entire content is how
# far from ordinary something is, and the mapping was throwing that away exactly
# where it mattered most.

def test_five_and_thirteen_sigma_are_no_longer_the_same_number():
    import math

    from services.enrichment.enrichers.tradfi import Z_SCORE_SCALE

    def base(z: float) -> float:
        return 1.0 - math.exp(-z / Z_SCORE_SCALE)

    assert base(12.76) > base(5.02)
    assert round(base(12.76), 3) != round(base(5.02), 3)


def test_the_curve_is_monotonic_across_the_observed_range():
    import math

    from services.enrichment.enrichers.tradfi import Z_SCORE_SCALE

    observed = [2.0, 3.5, 5.02, 6.49, 9.37, 12.76, 20.0]
    scores = [1.0 - math.exp(-z / Z_SCORE_SCALE) for z in observed]
    assert scores == sorted(scores)


def test_five_sigma_still_clears_the_downstream_threshold():
    """Recalibration must not silently drop significant spikes below the 0.6
    the consumers filter on."""
    import math

    from services.enrichment.enrichers.tradfi import Z_SCORE_SCALE

    assert 1.0 - math.exp(-5.0 / Z_SCORE_SCALE) > 0.6


def test_the_ceiling_is_approached_and_never_reached():
    """There is always a larger spike. A score of exactly 1.0 claims otherwise."""
    import math

    from services.enrichment.enrichers.tradfi import Z_SCORE_SCALE

    assert 1.0 - math.exp(-100.0 / Z_SCORE_SCALE) < 1.0


def test_a_negative_or_zero_z_score_is_safe():
    import math

    from services.enrichment.enrichers.tradfi import Z_SCORE_SCALE

    assert 1.0 - math.exp(-max(0.0, -3.0) / Z_SCORE_SCALE) == 0.0


def test_the_radar_boosts_use_headroom_not_addition():
    source = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
    executable = [
        ln for ln in source.splitlines()
        if "base_score + w_boost + f_boost" in ln and not ln.lstrip().startswith("#")
    ]
    assert executable == []
