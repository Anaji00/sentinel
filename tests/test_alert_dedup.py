"""One alert per stock, rule family and session unless a later one outranks it (shared/utils/alert_dedup.py)."""

import asyncio
import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import alert_dedup as ad  # noqa: E402


class FakeRedis:
    def __init__(self) -> None:
        self.d: dict = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.d:
            return None
        self.d[key] = value
        return True

    async def get(self, key):
        return self.d.get(key)


T = datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)


def test_copies_of_one_title_share_a_family_and_other_titles_do_not() -> None:
    a = ad.rule_family("Macro Energy Price Headline to Equity Block Convergence")
    assert a == ad.rule_family("macro energy price headline to equity block convergence")
    assert a != ad.rule_family("Options Print Convergence")
    assert ad.rule_family(None) == "unnamed"


def test_the_session_is_the_new_york_date() -> None:
    assert ad.session_date(datetime(2026, 11, 10, 2, 0, tzinfo=timezone.utc)) == "2026-11-09"     # 21:00 the evening before in New York
    assert ad.session_date(datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)) == "2026-11-10"
    assert ad.session_date(datetime(2026, 11, 10, 15, 0)) == "2026-11-10"                          # naive is read as UTC


def test_a_repeat_is_blocked_but_an_escalation_and_a_new_stock_or_session_pass() -> None:
    r = FakeRedis()
    run = lambda *a: asyncio.run(ad.admit(r, *a))  # noqa: E731
    assert run("NVDA", "Oil Price Spike", T, "ALERT") is True
    assert run("NVDA", "Oil Price Spike", T, "ALERT") is False
    assert run("NVDA", "Oil Price Spike", T, "WATCH") is False
    assert run("NVDA", "Oil Price Spike", T, "ELEVATED") is True            # escalation
    assert run("NVDA", "Oil Price Spike", T, "ELEVATED") is False
    assert run("AMD", "Oil Price Spike", T, "ALERT") is True                # another stock
    assert run("NVDA", "Oil Price Spike", datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc), "ALERT") is True   # next session


def test_a_garbled_stored_value_lets_the_alert_through() -> None:
    r = FakeRedis()
    r.d[ad.dedup_key("X", "Rule", T)] = b"junk"
    assert asyncio.run(ad.admit(r, "X", "Rule", T, "ALERT")) is True


def test_the_correlation_engine_uses_it_for_dynamic_rules_and_counts_the_drop() -> None:
    src = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert "alert_dedup.admit(" in src and "correlation.dynamic_rule_session_dedup" in src
    assert src.index("alert_dedup.admit(") < src.index("await store.save_correlation(c)", src.index("alert_dedup.admit("))
