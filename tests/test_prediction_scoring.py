

def test_a_prediction_names_its_source_correlation_and_the_caller_can_override():
    """99% of resolved predictions had no correlation_id, so no replay could say which correlation produced a call (13 Nov 2026)."""
    from services.agents.prediction_scoring import CURRENT_CORRELATION_ID, with_correlation_id
    assert with_correlation_id({"a": 1}, "c-1") == {"a": 1, "correlation_id": "c-1"}
    assert with_correlation_id({"correlation_id": "mine"}, "c-1")["correlation_id"] == "mine"
    assert with_correlation_id(None, None) == {} and with_correlation_id({"a": 1}, "") == {"a": 1}
    assert CURRENT_CORRELATION_ID.get() is None
