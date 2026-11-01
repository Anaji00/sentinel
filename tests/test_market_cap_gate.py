"""Calls only on companies of at least $1bn (30 Oct 2026): the best-known cap
table, and the gates that read it."""
import asyncio
import pathlib

from shared.utils import market_cap as mc

ROOT = pathlib.Path(__file__).resolve().parents[1]


class _Raw:
    def __init__(self, table):
        self.table = table

    async def hgetall(self, key):
        return self.table

    async def hget(self, key, field):
        return self.table.get(field)


def test_the_larger_estimate_wins_and_unknown_is_not_large():
    assert mc.best_cap(0.78e9, 1170e9) == 1170e9                 # BRK.B: one share class in the SEC count
    assert mc.best_cap(None, 2e9) == 2e9 and mc.best_cap(3e9, None) == 3e9
    assert mc.best_cap(None, None) is None
    rows = mc.cap_rows({"AAA": 2e9, "BBB": 5e8}, {"BBB": 1.5e9, "CCC": None}, ["AAA", "BBB", "CCC"])
    assert rows == {"AAA": 2e9, "BBB": 1.5e9}


def test_large_caps_and_is_large_cap():
    raw = _Raw({"AAA": "2e9", "BBB": "5e8"})
    assert asyncio.run(mc.large_caps(raw)) == {"AAA"}
    assert asyncio.run(mc.is_large_cap(raw, "aaa")) is True
    assert asyncio.run(mc.is_large_cap(raw, "BBB")) is False
    assert asyncio.run(mc.is_large_cap(raw, "ZZZ")) is False      # unknown is not large
    assert asyncio.run(mc.large_caps(_Raw({}))) is None           # no table is not "everything qualifies"


def test_every_call_path_is_gated():
    src = {p: (ROOT / p).read_text(encoding="utf-8") for p in (
        "services/agents/prediction_scoring.py", "services/agents/analyst_agent.py",
        "services/agents/stock_correlation_agent.py", "services/agents/sympathy_job.py",
        "services/agents/auto_trader.py")}
    assert "below_min_market_cap" in src["services/agents/prediction_scoring.py"]
    assert "is_large_cap(self.redis, ticker)" in src["services/agents/analyst_agent.py"]
    assert "is_large_cap(self.redis, ticker)" in src["services/agents/stock_correlation_agent.py"]
    assert "mc.large_caps(self.raw)" in src["services/agents/sympathy_job.py"]
    assert "mc.large_caps(self.raw)" in src["services/agents/auto_trader.py"]


def test_a_readable_event_id_maps_to_the_uuid_the_events_writer_stores():
    """A correlation triggered by shortvol_NXPI_20261030_e3a1ae failed its insert
    (30 Oct 2026); both writers now share one mapping, so the ids still join."""
    import uuid
    from services.enrichment.db_writer import _as_uuid
    from shared.utils.event_identity import as_uuid
    rid = "shortvol_NXPI_20261030_e3a1ae"
    assert as_uuid(rid) == _as_uuid(rid) == str(uuid.uuid5(uuid.NAMESPACE_DNS, rid))
    real = str(uuid.uuid4())
    assert as_uuid(real) == real
