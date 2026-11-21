"""Parent request, 20 Nov: the stock page's caches are warmed in the background for followed and most-traded names."""
import asyncio
import pathlib

from shared.utils import warm_set as ws

ROOT = pathlib.Path(__file__).resolve().parents[1]
clean = lambda t: t.strip().upper() if t.strip().isalpha() and len(t.strip()) <= 5 else None      # noqa: E731


def test_followed_names_come_first_interleaved_then_the_most_traded_without_repeats() -> None:
    out = ws.choose([["aaa", "bbb", "ccc"], ["ddd", "aaa"]], ["bbb", "eee", "123", None, "fff"], clean)
    assert out == ["AAA", "DDD", "BBB", "CCC", "EEE", "FFF"]


def test_the_set_is_capped_and_an_empty_input_is_an_empty_set() -> None:
    many = [[f"{chr(65 + i)}{chr(65 + j)}" for j in range(26)] for i in range(10)]
    assert len(ws.choose(many, [], clean, cap=40)) == 40 and ws.choose([], [], clean) == []


def test_a_pass_goes_one_name_at_a_time_counts_failures_and_never_stops(monkeypatch) -> None:
    from services.api_gateway import warmup as w

    seen = []

    async def fake_evidence(t, db=None, redis=None, user=None):
        seen.append(t)
        if t == "BBB":
            raise RuntimeError("source down")

    async def fake_names(db):
        return ["AAA", "BBB", "CCC"]

    monkeypatch.setattr(w, "evidence", fake_evidence)
    monkeypatch.setattr(w, "names_to_warm", fake_names)
    monkeypatch.setattr(w, "PAUSE_BETWEEN_NAMES_SEC", 0.0)
    assert asyncio.run(w.warm_once(object(), object())) == {"names": 3, "warmed": 2, "failed": 1} and seen == ["AAA", "BBB", "CCC"]


def test_the_gateway_starts_and_cancels_the_loop_and_no_model_is_called() -> None:
    main = (ROOT / "services/api_gateway/routes/main.py").read_text(encoding="utf-8")
    assert "warm_loop(app.state.db, app.state.redis)" in main and "warm_task.cancel()" in main
    src = (ROOT / "services/api_gateway/warmup.py").read_text(encoding="utf-8").lower()
    assert "ollama" not in src and "llm" not in src and "infer" not in src.replace("no model call", "")
