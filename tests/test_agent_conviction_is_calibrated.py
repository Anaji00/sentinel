"""An agent claims what its record supports.

Thirty days to 4 October: the wargamer stated 0.89 on average and was right 54%
of the time (Brier 0.346); quant stated 1.00 on downside calls that went 0 for 8
(Brier 0.463). A coin flip scores 0.25. Bulletins, alerts and the consensus
engine all read the stated number.
"""

from services.agents.prediction_scoring import (
    CALIBRATION_MIN_RESOLVED, AgentScorecard, calibrated_conviction,
)


def _card(made, correct):
    return AgentScorecard(agent_name="a", predictions_made=made, predictions_correct=correct)


def test_a_short_record_does_not_cap():
    assert calibrated_conviction(0.95, _card(CALIBRATION_MIN_RESOLVED - 1, 2)) == 0.95


def test_overconfidence_is_capped_at_the_record():
    assert calibrated_conviction(0.89, _card(104, 55)) == round(56 / 106, 4)
    assert calibrated_conviction(1.0, _card(40, 16)) == round(17 / 42, 4)


def test_modesty_is_left_alone():
    assert calibrated_conviction(0.4, _card(104, 55)) == 0.4


def test_garbage_passes_through_untouched():
    assert calibrated_conviction(None, _card(104, 55)) is None


def test_the_cap_beats_the_stated_number_on_the_wargamers_own_record():
    """90 calls at 0.89 that hit 60%, 7 at 1.00 that hit 29% -- from the table."""
    calls = [(0.89, True)] * 54 + [(0.89, False)] * 36 + [(1.0, True)] * 2 + [(1.0, False)] * 5
    card = _card(104, 55)

    def brier(conv):
        return sum((conv(c) - (1.0 if ok else 0.0)) ** 2 for c, ok in calls) / len(calls)

    stated = brier(lambda c: c)
    capped = brier(lambda c: calibrated_conviction(c, card))
    assert stated > 0.25 > capped


def test_both_choke_points_apply_it():
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1] / "services" / "agents"
    scoring = (root / "prediction_scoring.py").read_text(encoding="utf-8")
    body = scoring[scoring.index("async def record_prediction"):scoring.index("pred = AgentPrediction(")]
    assert "calibrated_conviction(conviction, await self.get_scorecard())" in body
    base = (root / "base.py").read_text(encoding="utf-8")
    bulletin = base[base.index("async def publish_bulletin"):base.index("offer_focus(")]
    assert "calibrated_conviction(conviction, await self.get_scorecard())" in bulletin
