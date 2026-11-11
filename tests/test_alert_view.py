"""Divergence situation and brief reach the dashboard; nothing else from metrics_summary does."""
import json

from shared.utils.alert_view import divergence_view

SITUATION = {"stage": "CONFIRMED", "number": 1, "sessions": 2, "peak_z": 3.2, "last_z": 3.0, "first_session": "2026-11-09", "last_session": "2026-11-10", "news": False}
BRIEF = {"what_happened": "X moved", "catalyst": "none", "peers": {"reference": "industry", "members": ["A +1.0%"], "returns_known": True},
         "evidence": "e", "positioning": "not measured: nothing joined", "watch": ["w"], "limits": ["l"]}


def test_a_divergence_alert_yields_its_situation_and_brief_only():
    out = divergence_view({"gap": 0.05, "return": 0.04, "situation": SITUATION, "brief": BRIEF, "domain_count": 3})
    assert out == {"situation": SITUATION, "brief": BRIEF}
    assert "gap" not in out and "domain_count" not in out


def test_horizon_and_the_three_measurements_are_carried_through_and_only_when_valid():
    out = divergence_view({"situation": SITUATION, "horizon": "5d", "breadth": 0.62, "beta": 1.1, "relative_volume": None})
    assert out["horizon"] == "5d" and out["breadth"] == 0.62 and out["beta"] == 1.1
    assert "relative_volume" not in out, "a 5-session row has no relative volume: absent, not zero"
    assert "horizon" not in divergence_view({"situation": SITUATION, "horizon": "decade"})
    assert "beta" not in divergence_view({"situation": SITUATION, "beta": True})
    assert "beta" not in divergence_view({"situation": SITUATION, "beta": float("nan")})
    assert "gap" not in divergence_view({"horizon": "session", "gap": 0.05})


def test_a_json_string_from_the_database_is_read():
    assert divergence_view(json.dumps({"situation": SITUATION}))["situation"]["stage"] == "CONFIRMED"
    assert divergence_view(json.dumps({"brief": BRIEF}).encode()) == {"brief": BRIEF}


def test_other_rules_and_unreadable_values_yield_nothing():
    assert divergence_view({"domain_count": 2}) == {}
    assert divergence_view(None) == {}
    assert divergence_view("not json") == {}
    assert divergence_view([1, 2]) == {}
    assert divergence_view({"situation": {"stage": "MADE_UP"}}) == {}
    assert divergence_view({"brief": {}}) == {}
