"""The bull and bear case: grounded in facts the platform holds, two-sided or not shown, never advice."""
from pathlib import Path

from shared.utils import thesis as th

ROOT = Path(__file__).resolve().parents[1]

LINES = th.facts(
    "NVDA", 0.82, 21, ["its price has risen faster than most stocks"],
    {"the last session": 0.012, "the last 5 sessions": None, "the last month": -0.05},
    [("2026-11-04", "Analysts raise targets after strong chip demand", 0.8)],
    [("2026-11-05", "Large block trade of $2.3M")],
)


def _view(bull, bear, change="A weak earnings report would shift this."):
    return {"bull": bull, "bear": bear, "would_change_it": change}


def _pt(text, *cited):
    return {"point": text, "cited": list(cited)}


def test_facts_are_numbered_plain_sentences_and_skip_what_is_unknown() -> None:
    assert LINES[0].startswith("F1: Our stock model ranks NVDA at 82 out of 100")
    assert any("changed +1.2% over the last session" in x for x in LINES)
    assert not any("5 sessions" in x for x in LINES), "an unknown move is left out, not guessed"
    assert any("the wording is positive" in x for x in LINES)
    assert [x.split(":")[0] for x in LINES] == [f"F{i + 1}" for i in range(len(LINES))]


def test_a_grounded_two_sided_answer_is_kept_with_the_facts_each_point_rests_on() -> None:
    out = th.validate(_view([_pt("The model ranks it near the top.", "F1")], [_pt("It fell over the last month.", "F4")]), LINES)
    assert out is not None
    assert out["bull"][0]["point"] == "The model ranks it near the top."
    assert out["bull"][0]["sources"][0]["id"] == "F1"
    assert "ranks NVDA" in out["bull"][0]["sources"][0]["text"] and not out["bull"][0]["sources"][0]["text"].startswith("F1")
    assert out["would_change_it"].startswith("A weak earnings")


def test_a_point_citing_nothing_or_a_fact_that_does_not_exist_is_dropped() -> None:
    n = len(LINES)
    out = th.validate(_view(
        [_pt("Invented reason.", f"F{n + 3}"), _pt("No citation at all."), _pt("Real one.", "F2")],
        [_pt("Bear reason.", "F1")],
    ), LINES)
    assert out is not None and [p["point"] for p in out["bull"]] == ["Real one."]


def test_one_sided_means_nothing_is_shown() -> None:
    assert th.validate(_view([_pt("Only the upside.", "F1")], []), LINES) is None
    assert th.validate(_view([], [_pt("Only the downside.", "F1")]), LINES) is None
    assert th.validate(_view([_pt("Up.", "F99")], [_pt("Down.", "F1")]), LINES) is None


def test_advice_and_predictions_are_not_evidence() -> None:
    out = th.validate(_view(
        [_pt("You should buy this now.", "F1"), _pt("It will double by spring.", "F1"), _pt("Ranked highly by the model.", "F1")],
        [_pt("Fell last month.", "F4")],
        change="Set a price target of 200.",
    ), LINES)
    assert out is not None and [p["point"] for p in out["bull"]] == ["Ranked highly by the model."]
    assert out["would_change_it"] == ""


def test_fact_ids_written_into_the_sentence_are_removed_from_what_is_shown() -> None:
    out = th.validate(_view([_pt("The model ranks it highly (F1).")], [_pt("It fell (F4).")]), LINES)
    assert out is not None
    assert out["bull"][0]["point"] == "The model ranks it highly."
    assert out["bear"][0]["point"] == "It fell."
    assert out["bull"][0]["sources"][0]["id"] == "F1", "the id left the sentence but still counts as the citation"


def test_at_most_four_points_a_side_and_no_repeats() -> None:
    bull = [_pt(f"Reason number {i}.", "F1") for i in range(7)] + [_pt("Reason number 0.", "F2")]
    out = th.validate(_view(bull, [_pt("Down.", "F1")]), LINES)
    assert out is not None and len(out["bull"]) == th.MAX_POINTS


def test_the_prompt_forbids_what_the_validator_drops() -> None:
    system, user = th.prompt("NVDA", LINES)
    assert "Use only the numbered facts" in system and "do not tell anyone to buy or sell" in system.lower().replace(", and", "")
    assert "F1:" in user and "NVDA" in user


def test_the_route_states_its_cap_and_admission_and_is_registered() -> None:
    route = (ROOT / "services/api_gateway/routes/stock_thesis.py").read_text(encoding="utf-8")
    for needle in ("require_role(Role.VIEWER)", "InferenceBudget(", 'lane="thesis"', "try_acquire()", "RATE_PER_HOUR",
                   "th.CACHE_TTL_SEC", "clean_ticker(t", "fallback_model=None"):
        assert needle in route, needle
    assert "max_retries=2" in route, "malformed output is retried; a timeout is not"
    main = (ROOT / "services/api_gateway/routes/main.py").read_text(encoding="utf-8")
    assert "app.include_router(stock_thesis.router)" in main
    assert th.RATE_PER_HOUR <= 10 and th.CACHE_TTL_SEC >= 3600


def test_nothing_runs_until_asked() -> None:
    panel = (ROOT / "frontend/src/components/ThesisPanel.tsx").read_text(encoding="utf-8")
    assert "apiClient.post" in panel and "onClick={() => void ask()}" in panel
    assert "useEffect" not in panel, "opening the drawer must never start a model call"
