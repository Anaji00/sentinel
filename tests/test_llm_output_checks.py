from shared.utils.llm_output_checks import calls_itself_artifact, check_scenario, confidence_mismatch, round_triple, stated_confidence


def test_the_stated_confidence_is_read_and_compared_with_the_field():
    assert stated_confidence("Overall confidence 60% given thin evidence.") == 60
    assert stated_confidence("overall confidence: 85 %") == 85 and stated_confidence("no number here") is None
    assert confidence_mismatch("Overall confidence 60%", 85) == (60, 85)
    assert confidence_mismatch("Overall confidence 62%", 60) is None            # within five points
    assert confidence_mismatch("nothing stated", 60) is None and confidence_mismatch("Overall confidence 60%", None) is None


def test_round_triples_are_flagged_and_irregular_splits_are_not():
    assert round_triple([60, 30, 10]) == "60/30/10" and round_triple([10, 60, 30]) == "60/30/10"
    assert round_triple([0.6, 0.3, 0.1]) == "60/30/10"
    assert round_triple([57, 31, 12]) is None and round_triple([60, 40]) is None and round_triple(["x", 1, 2]) is None
    assert round_triple([1, 1, 1]) is None                                       # integers 1 are not multiples of five


def test_artifact_language_and_the_combined_flags():
    assert calls_itself_artifact("Algorithmic Spurious Cointegration Artifact (70%)") and calls_itself_artifact("likely a data artifact")
    assert not calls_itself_artifact("Supply disruption raises margins for refiners")
    flags = check_scenario("Overall confidence 60%. A spurious link.", 85, [60, 30, 10])
    assert flags == {"confidence_mismatch": (60, 85), "round_triple": "60/30/10", "artifact": True}
