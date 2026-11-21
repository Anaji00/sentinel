"""Review 18 Nov, F3 and F4.

F3: a thin factor run (fewer scores than MIN_SCORES_TO_REBUILD) is archived but its report must not replace the live report, whose as_of every freshness
guard reads; a full run publishes both. F4: a correlation event whose processing raises is counted with swallowed(), not lost silently."""
import asyncio
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import factor_engine as fe  # noqa: E402
from shared.utils.auto_trade import MIN_SCORES_TO_REBUILD  # noqa: E402
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY  # noqa: E402


class Raw:
    def __init__(self) -> None:
        self.d: dict = {}

    async def get(self, k):
        return self.d.get(k)

    async def set(self, k, v, ex=None, nx=False):
        self.d[k] = v
        return True


class Db:
    async def execute(self, *a, **k):
        return None

    async def query(self, *a, **k):
        return []


def _engine(monkeypatch, n_scores: int, as_of: str):
    eng = fe.FactorEngine(Db(), Raw())

    async def panel():
        return object()

    async def incumbents():
        return {}

    async def noop(*a, **k):
        return None

    result = {"as_of": as_of, "universe": 9000, "tradable": 4000, "trading_days": 900, "primary_horizon": "monthly",
              "scores": {f"T{i}": 0.1 for i in range(n_scores)}, "horizons": {}}
    monkeypatch.setattr(eng, "_panel", panel)
    monkeypatch.setattr(eng, "_incumbents", incumbents)
    for name in ("_equity_regime", "_return_clusters", "_portfolio_lab", "_keep_and_grade"):
        monkeypatch.setattr(eng, name, noop)
    monkeypatch.setattr(fe.fm, "run_panel", lambda *a, **k: dict(result))
    monkeypatch.setattr(fe.fm, "tradable_subset", lambda p: p)
    return eng


def test_a_thin_run_leaves_the_live_report_and_scores_alone(monkeypatch) -> None:
    eng = _engine(monkeypatch, 94, "2026-11-12")
    old = json.dumps({"as_of": "2026-11-06"})
    eng.redis.d[FACTOR_REPORT_KEY] = old
    out = asyncio.run(eng.run_once())
    assert out.get("scores_withheld") == 1
    assert eng.redis.d[FACTOR_REPORT_KEY] == old
    assert FACTOR_SCORES_KEY not in eng.redis.d


def test_a_full_run_publishes_the_report_with_its_scores(monkeypatch) -> None:
    eng = _engine(monkeypatch, MIN_SCORES_TO_REBUILD + 5, "2026-11-12")
    eng.redis.d[FACTOR_REPORT_KEY] = json.dumps({"as_of": "2026-11-06"})
    asyncio.run(eng.run_once())
    assert json.loads(eng.redis.d[FACTOR_REPORT_KEY])["as_of"] == "2026-11-12"
    assert FACTOR_SCORES_KEY in eng.redis.d


def test_the_correlation_event_handler_counts_a_failed_event() -> None:
    src = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    i = src.index("Failed to process correlation for event")
    assert 'swallowed("correlation.event"' in src[i:i + 700]
