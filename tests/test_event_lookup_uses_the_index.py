"""
tests/test_event_lookup_uses_the_index.py

The reasoning tier's event lookups cast the indexed column to text.

`WHERE event_id::text = ANY($1::text[])` cannot use the index, so each lookup
scanned the whole 5.8 GB events hypertable: 34.9s for three ids against 2.2s
uncast, past the client's timeout. The failure logged no message, every
scenario was generated without its supporting events, and the entity pruning
that reads those events then stripped the hypotheses bare. On 1 October the
reasoning service started 8 syntheses, discarded 7 as indiscriminable and saved
none, while holding a model slot for 31 of every 40 minutes.
"""

import asyncio
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

GOOD = "e7a87a0a-9c98-4f6f-a96b-f76a7a29605f"


def test_no_reasoning_lookup_casts_the_event_id():
    for name in ("context_builder.py", "scenario_generator.py"):
        src = (ROOT / "services" / "reasoning" / name).read_text(encoding="utf-8")
        code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
        assert "event_id::text = ANY" not in code, name
        assert "event_id = ANY($1::uuid[])" in code, name


def test_the_context_builder_passes_uuids_and_skips_junk():
    from services.reasoning.context_builder import ContextBuilder
    cb = ContextBuilder.__new__(ContextBuilder)
    cb._db = MagicMock()
    cb._db.query = AsyncMock(return_value=[])
    cb._serialize_row = lambda r: r
    asyncio.run(cb._fetch_events([GOOD, "not-a-uuid", None]))
    sql, ids = cb._db.query.await_args.args
    assert ids == [uuid.UUID(GOOD)]


def test_only_junk_ids_make_no_query():
    from services.reasoning.context_builder import ContextBuilder
    cb = ContextBuilder.__new__(ContextBuilder)
    cb._db = MagicMock()
    cb._db.query = AsyncMock(return_value=[])
    assert asyncio.run(cb._fetch_events(["junk"])) == []
    cb._db.query.assert_not_awaited()


def test_the_generator_passes_uuids():
    from services.reasoning.scenario_generator import ScenarioGenerator
    gen = ScenarioGenerator.__new__(ScenarioGenerator)
    gen.db = MagicMock()
    gen.db.query = AsyncMock(return_value=[])
    asyncio.run(gen._fetch_events([GOOD, "bad"]))
    assert gen.db.query.await_args.args[1] == [uuid.UUID(GOOD)]


def test_scenario_sources_are_looked_up_by_id_not_by_join():
    """The join matched events on a three-way OR and scanned the hypertable
    once per resolved scenario."""
    from services.reasoning.scenario_tracker import ScenarioTracker
    a, b, c = (uuid.uuid4() for _ in range(3))
    t = ScenarioTracker.__new__(ScenarioTracker)
    t._db = MagicMock()
    t._db.query = AsyncMock(side_effect=[
        [{"trigger_event_id": a, "c_ids": [b, a], "s_ids": [c, None]}],
        [{"source": "finnhub_equities"}, {"source": "sec_form4"}],
    ])
    out = asyncio.run(t._get_scenario_event_sources(str(uuid.uuid4())))
    assert out == ["finnhub_equities", "sec_form4"]
    second_sql, ids = t._db.query.await_args_list[1].args
    assert "event_id = ANY($1::uuid[])" in second_sql
    assert set(ids) == {a, b, c}


def test_a_scenario_with_no_events_makes_one_query():
    from services.reasoning.scenario_tracker import ScenarioTracker
    t = ScenarioTracker.__new__(ScenarioTracker)
    t._db = MagicMock()
    t._db.query = AsyncMock(return_value=[{"trigger_event_id": None, "c_ids": None, "s_ids": []}])
    assert asyncio.run(t._get_scenario_event_sources(str(uuid.uuid4()))) == []
    assert t._db.query.await_count == 1
