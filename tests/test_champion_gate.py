"""A refit replaces the live news model only when it is validated and not distinguishably worse (shared/utils/champion_gate.py)."""

import asyncio
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.correlation import news_model_job as job  # noqa: E402
from shared.utils import news_model as nm  # noqa: E402
from shared.utils.champion_gate import challenger_may_replace  # noqa: E402


def test_unvalidated_or_much_weaker_challengers_do_not_replace_a_champion() -> None:
    ok, why = challenger_may_replace(0.040, 4.0, 0.030, False)
    assert not ok and "gate" in why
    assert challenger_may_replace(0.040, 4.0, None, True)[0] is False
    assert challenger_may_replace(0.040, 4.0, float("nan"), True)[0] is False
    ok, why = challenger_may_replace(0.040, 4.0, 0.020, True)                 # champion se 0.010: 0.020 is two se below
    assert not ok and "below" in why


def test_an_equal_better_or_noise_level_challenger_replaces() -> None:
    assert challenger_may_replace(0.040, 4.0, 0.040, True)[0]
    assert challenger_may_replace(0.040, 4.0, 0.055, True)[0]
    assert challenger_may_replace(0.040, 4.0, 0.031, True)[0]                  # 0.009 below, inside one se of 0.010
    assert challenger_may_replace(None, None, 0.02, True) == (True, "no champion IC on record")


def test_a_champion_with_no_usable_t_gets_no_tolerance() -> None:
    assert challenger_may_replace(0.040, None, 0.039, True)[0] is False
    assert challenger_may_replace(0.040, 0.0, 0.039, True)[0] is False
    assert challenger_may_replace(0.040, None, 0.041, True)[0] is True


class _Raw:
    def __init__(self, stored):
        self.store = {nm.NEWS_MODEL_KEY: json.dumps(stored)} if stored else {}

    async def get(self, k):
        return self.store.get(k)

    async def set(self, k, v):
        self.store[k] = v


def _run(monkeypatch, stored, new_report):
    async def fake_build(_):
        import numpy as np
        return np.zeros((600, 768), dtype="float32"), np.zeros(600, dtype="float32"), np.arange(600), {"samples": 600}

    async def fake_to_thread(fn, *a, **k):
        return new_report if fn is nm.walk_forward else [0.1] * 768

    monkeypatch.setattr(job, "build_samples", fake_build)
    monkeypatch.setattr(job.asyncio, "to_thread", fake_to_thread)

    class _R:
        pass
    r = _Raw(stored)
    r.raw = r
    asyncio.run(job.run_once(None, r))
    return json.loads(r.store[nm.NEWS_MODEL_KEY])


CHAMP = {"validated": True, "weights": [0.1] * 768, "mean_ic": 0.04, "t": 4.0, "generated_at": "2026-11-01T00:00:00+00:00"}


def test_the_job_keeps_a_validated_champion_against_a_failing_report(monkeypatch) -> None:
    out = _run(monkeypatch, CHAMP, {"days": 40, "mean_ic": -0.01, "t": -1.0, "signed_bps": 0, "validated": False})
    assert out["validated"] and out["weights"] and out["challenger"]["held_over"] and out["challenger"]["validated"] is False


def test_the_job_replaces_a_champion_with_a_validated_equal_and_stores_a_first_one(monkeypatch) -> None:
    out = _run(monkeypatch, CHAMP, {"days": 40, "mean_ic": 0.05, "t": 5.0, "signed_bps": 10, "validated": True})
    assert out["mean_ic"] == 0.05 and "challenger" not in out
    first = _run(monkeypatch, None, {"days": 40, "mean_ic": 0.03, "t": 3.0, "signed_bps": 5, "validated": True})
    assert first["validated"] and first["weights"]


def test_the_embedding_refit_asks_the_same_gate() -> None:
    src = (ROOT / "services/agents/news_model_refit.py").read_text(encoding="utf-8")
    assert "challenger_may_replace(cur.get(\"oos_mean_ic\")" in src and "held_over" in src
