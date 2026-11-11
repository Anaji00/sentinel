"""The wargamer on a hosted model: own producer id, hard daily cap, no local fallback (services/agents/adversarial_wargamer.py)."""

import asyncio
import logging
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import adversarial_wargamer as aw  # noqa: E402
from services.agents import base  # noqa: E402


class Raw:
    def __init__(self, broken: bool = False) -> None:
        self.d: dict = {}
        self.broken = broken

    async def incr(self, k):
        if self.broken:
            raise ConnectionError("down")
        self.d[k] = self.d.get(k, 0) + 1
        return self.d[k]

    async def expire(self, k, s):
        return True


class R:
    def __init__(self, raw) -> None:
        self.raw = raw


def _agent(raw, model="groq/qwen/qwen3.8-27b"):
    a = aw.AdversarialWargamerAgent.__new__(aw.AdversarialWargamerAgent)
    a.redis, a.name, a.model, a.logger = R(raw), aw.WARGAMER_V2_NAME, model, logging.getLogger("t")
    return a


def test_the_cap_stops_the_151st_wargame_of_the_day(monkeypatch) -> None:
    monkeypatch.setenv("WARGAMER_DAILY_CAP", "3")
    a = _agent(Raw())
    assert [asyncio.run(a._within_daily_cap()) for _ in range(5)] == [True, True, True, False, False]


def test_the_default_cap_is_150_and_a_bad_value_falls_back_to_it(monkeypatch) -> None:
    monkeypatch.delenv("WARGAMER_DAILY_CAP", raising=False)
    assert aw.daily_cap() == 150
    monkeypatch.setenv("WARGAMER_DAILY_CAP", "lots")
    assert aw.daily_cap() == 150


def test_a_cap_that_cannot_be_read_stops_the_call_instead_of_letting_it_through() -> None:
    assert asyncio.run(_agent(Raw(broken=True))._within_daily_cap()) is False


def test_the_hosted_producer_has_its_own_id_and_the_local_one_stays_cut() -> None:
    from shared.utils.llm_policy import agent_disabled
    assert aw.WARGAMER_V2_NAME != "adversarial_wargamer" and ":" not in aw.WARGAMER_V2_NAME
    assert agent_disabled("adversarial_wargamer") is True and agent_disabled(aw.WARGAMER_V2_NAME) is False
    assert base.hosted_share(aw.WARGAMER_V2_NAME) == 8


def test_main_builds_it_only_for_a_hosted_model_with_no_local_fallback() -> None:
    src = (ROOT / "services/agents/main.py").read_text(encoding="utf-8")
    block = src[src.index("adversarial_wargamer_v2 = build_agent("):src.index("edge_validator_agent = build_agent(")]
    assert "agent_name=WARGAMER_V2_NAME" in block and "fallback_model=None" in block and "if is_hosted(wargamer_model) else None" in block
    assert 'WARGAMER_HOSTED_MODEL' in src and "agent_tier_map.append((WARGAMER_V2_NAME" in src


def test_the_hosted_variant_skips_the_local_slot_peek_and_the_local_one_keeps_it() -> None:
    src = (ROOT / "services/agents/adversarial_wargamer.py").read_text(encoding="utf-8")
    assert 'if hosted_llm.is_hosted(getattr(self, "model", "") or ""):' in src and "elif not await self.capacity_or_defer(message):" in src
    assert src.index("_is_worth_simulating(message)") < src.index("_within_daily_cap()")


def test_the_example_env_leaves_it_off() -> None:
    env = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "WARGAMER_HOSTED_MODEL=\n" in env and "WARGAMER_DAILY_CAP=150" in env
