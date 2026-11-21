"""Rule-based regime labels for a stock that moved against its peers (shared/utils/regime_packet.py)."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import regime_packet as rp  # noqa: E402

FULL = dict(adverse_8k_items=[], catalyst_8k_items=[], earnings_surprise=0.0, news_articles=0)


def test_a_residual_under_the_bar_or_unmeasured_abstains() -> None:
    assert rp.classify(rp.Packet(residual_z=1.2, **FULL)).label == rp.CROWDED_NO_EDGE
    v = rp.classify(rp.Packet())
    assert v.label == rp.CROWDED_NO_EDGE and v.missing == ["residual_z"]


def test_a_drop_explained_by_an_adverse_filing_or_a_bad_surprise_is_a_trap_and_vetoes_the_buy() -> None:
    v = rp.classify(rp.Packet(residual_z=-3.0, adverse_8k_items=["4.02"], catalyst_8k_items=[], earnings_surprise=0.0, news_articles=2))
    assert v.label == rp.FUNDAMENTAL_IMPAIRMENT_TRAP and v.action == "veto_buy" and "4.02" in v.reasons[0]
    v2 = rp.classify(rp.Packet(residual_z=-2.6, **{**FULL, "earnings_surprise": -0.12}))
    assert v2.label == rp.FUNDAMENTAL_IMPAIRMENT_TRAP and "-12.0%" in v2.reasons[0]
    assert rp.classify(rp.Packet(residual_z=3.0, **{**FULL, "adverse_8k_items": ["4.02"]})).label != rp.FUNDAMENTAL_IMPAIRMENT_TRAP     # a rise is not a trap


def test_a_catalyst_in_the_direction_of_the_move_is_drift_either_way() -> None:
    up = rp.classify(rp.Packet(residual_z=2.8, **{**FULL, "earnings_surprise": 0.09}))
    assert up.label == rp.CONFIRMED_CATALYST_DRIFT and up.action == "go_long"
    down = rp.classify(rp.Packet(residual_z=-2.8, **{**FULL, "earnings_surprise": -0.0}, ))
    assert down.label != rp.CONFIRMED_CATALYST_DRIFT                       # no surprise: not drift
    contract = rp.classify(rp.Packet(residual_z=2.4, **{**FULL, "catalyst_8k_items": ["1.01"]}))
    assert contract.label == rp.CONFIRMED_CATALYST_DRIFT and "1.01" in contract.reasons[0]


def test_no_catalyst_with_everything_measured_is_a_fade_and_the_side_follows_the_move() -> None:
    sell = rp.classify(rp.Packet(residual_z=2.9, **FULL))
    buy = rp.classify(rp.Packet(residual_z=-2.9, **FULL))
    assert (sell.label, sell.action) == (rp.UNEXPLAINED_DISLOCATION, "fade_sell") and (buy.label, buy.action) == (rp.UNEXPLAINED_DISLOCATION, "fade_buy")


def test_missing_catalyst_evidence_never_becomes_no_catalyst() -> None:
    v = rp.classify(rp.Packet(residual_z=2.9, adverse_8k_items=[], earnings_surprise=None, news_articles=None))
    assert v.label == rp.CROWDED_NO_EDGE and v.basis == "insufficient_evidence" and v.missing == ["earnings_surprise", "news_articles"]


def test_known_thesis_killers_and_conflicts_abstain() -> None:
    assert rp.classify(rp.Packet(residual_z=2.9, **{**FULL, "insider_net": 0.8})).label == rp.CROWDED_NO_EDGE      # insiders buying a stock we would fade
    assert rp.classify(rp.Packet(residual_z=-2.9, **{**FULL, "insider_net": -0.8})).label == rp.CROWDED_NO_EDGE   # insiders selling into the drop
    assert rp.classify(rp.Packet(residual_z=2.9, **{**FULL, "options_tilt": 0.9})).label == rp.CROWDED_NO_EDGE    # flow agrees with the move
    assert rp.classify(rp.Packet(residual_z=-2.9, **{**FULL, "days_to_cover": 11.0})).label == rp.CROWDED_NO_EDGE
    killer = rp.classify(rp.Packet(residual_z=2.9, **{**FULL, "iv_percentile": 97.0, "earnings_within_days": 1}))
    assert killer.label == rp.CROWDED_NO_EDGE and "implied volatility" in killer.reasons[0]
    assert rp.classify(rp.Packet(residual_z=2.9, **{**FULL, "news_articles": 5})).label == rp.CROWDED_NO_EDGE


def test_the_verdict_serialises_for_an_alert_annotation() -> None:
    d = rp.classify(rp.Packet(residual_z=2.9, **FULL)).as_dict()
    # Deliberate update (layer audit 18 Nov, finding 4): two display keys were added beside the unchanged ones.
    assert set(d) == {"label", "action", "reasons", "missing", "basis", "action_supported", "action_label"} and d["label"] in rp.LABELS


def test_directional_actions_are_labelled_unsupported_without_changing_the_action() -> None:
    from shared.utils import regime_packet as rp
    for a in ("fade_buy", "fade_sell", "go_long", "go_short"):
        f = rp.action_fields(a)
        assert f["action_supported"] is False and "not established" in f["action_label"]
    assert rp.action_fields("abstain") == {"action_supported": True, "action_label": "abstain"}
    assert rp.action_fields("veto_buy")["action_supported"] is True
    v = rp.Verdict(rp.UNEXPLAINED_DISLOCATION, "fade_buy", ["x"]).as_dict()
    assert v["action"] == "fade_buy" and v["action_supported"] is False
