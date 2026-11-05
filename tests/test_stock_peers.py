"""Related stocks: what the nightly peer tables can say, and nothing they cannot."""
from datetime import date
from pathlib import Path

import pytest

from shared.utils import stock_peers as sp
from shared.utils import sympathy

ROOT = Path(__file__).resolve().parents[1]
TODAY = date(2026, 11, 5)


def _sym(peer, corr, as_of=TODAY, n=120):
    return {"peer": peer, "corr": corr, "n": n, "as_of": as_of}


def _txt(peer, sim, as_of=TODAY):
    return {"peer": peer, "sim": sim, "as_of": as_of}


def test_the_window_is_the_one_the_job_measures() -> None:
    assert sp.WINDOW_SESSIONS == sympathy.WINDOW


@pytest.mark.parametrize("raw, want", [("nvda", "NVDA"), (" BRK.B ", "BRK.B"), ("a-b", "A-B"),
                                       ("", None), (None, None), ("TOOLONGTICKER", None), ("A B", None),
                                       ("../x", None), ("$X", None)])
def test_a_ticker_is_checked_before_it_reaches_a_query_or_a_cache_key(raw, want) -> None:
    assert sp.clean_ticker(raw) == want


def test_closeness_reads_as_words_at_each_band() -> None:
    assert sp.closeness_words(0.94) == "moves almost in step"
    assert sp.closeness_words(0.60) == "moves closely with it"
    assert sp.closeness_words(0.45) == "often moves with it"
    assert sp.closeness_words(0.35) == "loosely moves with it"


def test_peers_are_strongest_first_and_capped_and_exclude_the_stock() -> None:
    rows = [_sym(f"P{i}", 0.35 + i * 0.01) for i in range(12)] + [_sym("NVDA", 1.0)]
    out = sp.build_related("NVDA", rows, [], {"P11": "Eleven Inc"}, TODAY, TODAY)
    got = [p["ticker"] for p in out["moves_with"]]
    assert got[0] == "P11" and len(got) == sp.MAX_MOVES_WITH and "NVDA" not in got
    assert out["moves_with"][0]["name"] == "Eleven Inc"
    assert out["moves_with"][1]["name"] == "P10", "no name falls back to the ticker"
    assert out["moves_with"][0]["sessions"] == 120


def test_similar_business_is_its_own_list() -> None:
    out = sp.build_related("NVDA", [_sym("TSM", 0.54)], [_txt("SMCI", 0.22), _txt("AKAM", 0.21)], {}, TODAY, TODAY)
    assert [p["ticker"] for p in out["similar_business"]] == ["SMCI", "AKAM"]
    assert [p["ticker"] for p in out["moves_with"]] == ["TSM"]
    assert out["empty_reason"] is None


def test_nothing_found_says_why_without_pretending_to_know() -> None:
    assert sp.build_related("ZZZZ", [], [], {}, TODAY, TODAY)["empty_reason"] == "none_found"
    assert sp.build_related("ZZZZ", [], [], {}, None, TODAY)["empty_reason"] == "not_computed"


def test_old_data_is_marked_stale_and_dated() -> None:
    fresh = sp.build_related("X", [_sym("Y", 0.5, as_of=date(2026, 11, 3))], [], {}, TODAY, TODAY)
    old = sp.build_related("X", [_sym("Y", 0.5, as_of=date(2026, 10, 25))], [], {}, TODAY, TODAY)
    assert not fresh["stale"] and old["stale"]
    assert old["as_of"] == date(2026, 10, 25)


def test_it_never_claims_what_is_not_stored() -> None:
    out = sp.build_related("X", [_sym("Y", 0.5)], [_txt("Z", 0.2)], {}, TODAY, TODAY)
    assert set(out["moves_with"][0]) == {"ticker", "name", "correlation", "sessions", "closeness"}
    assert "forecast" in out["note"].lower()
    for forbidden in ("lag", "direction", "hit_rate", "days_same_direction"):
        assert forbidden not in out and forbidden not in out["moves_with"][0]


def test_the_route_is_registered_authenticated_and_cached() -> None:
    main = (ROOT / "services/api_gateway/routes/main.py").read_text(encoding="utf-8")
    assert "app.include_router(stock_peers.router)" in main
    route = (ROOT / "services/api_gateway/routes/stock_peers.py").read_text(encoding="utf-8")
    assert "require_role(Role.VIEWER)" in route and "cached_json(" in route
    assert "clean_ticker(ticker)" in route, "the ticker is validated before it is used"
