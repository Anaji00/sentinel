"""The instrument for the conversion ratio has to work.

`scripts/measure_intelligence_flow.py` exists because nothing in the platform
states what fraction of collected events reaches a model. Measured twice on
2026-09-21: 1,484/min and 1,377/min collected, 0.3/min inferences completed --
one per roughly 4,500-5,000 events. Two of this audit's findings were stages
sitting at zero for days underneath a number nobody was dividing.

The first version of the tool ran one Kafka call per topic. It took 221
seconds and returned no Kafka stages at all -- a tool written to make a
missing measurement visible, silently reporting nothing for most of the
pipeline. `GetOffsetShell` with no `--topic` lists every partition in one call.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "measure_intelligence_flow", ROOT / "scripts/measure_intelligence_flow.py"
)
flow = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(flow)


# ── topic names are read, never typed ────────────────────────────────────────


def test_topics_come_from_the_shared_constant():
    from shared.kafka import Topics

    names = flow._topics()
    assert names["ENRICHED_EVENTS"] == Topics.ENRICHED_EVENTS
    assert names["CORRELATIONS"] == Topics.CORRELATIONS


def test_the_non_topics_are_excluded():
    """ALL_RAW is a list and heartbeats travel through Redis."""
    assert "ALL_RAW" not in flow._topics()
    assert "SYSTEM_HEARTBEAT" not in flow._topics()


# ── the parsing that returned nothing ────────────────────────────────────────


def test_offsets_are_summed_across_partitions(monkeypatch):
    monkeypatch.setattr(flow, "_sh", lambda *_: (
        "enriched.events:0:100\n"
        "enriched.events:1:250\n"
        "enriched.events:2:50\n"
        "sentinel.correlations:0:7\n"
    ))
    counts = flow._kafka_offsets("x")
    assert counts["enriched.events"] == 400
    assert counts["sentinel.correlations"] == 7


def test_topics_the_platform_does_not_own_are_ignored(monkeypatch):
    """__consumer_offsets is the largest topic on the broker."""
    monkeypatch.setattr(flow, "_sh", lambda *_: (
        "__consumer_offsets:3:73242691\nenriched.events:0:5\n"
    ))
    counts = flow._kafka_offsets("x")
    assert counts == {"enriched.events": 5}


def test_malformed_lines_do_not_abort_the_snapshot(monkeypatch):
    monkeypatch.setattr(flow, "_sh", lambda *_: (
        "garbage\n\nenriched.events:0:9\nenriched.events:1:oops\n"
    ))
    assert flow._kafka_offsets("x") == {"enriched.events": 9}


def test_an_unreachable_broker_returns_nothing_rather_than_raising(monkeypatch):
    monkeypatch.setattr(flow, "_sh", lambda *_: "")
    assert flow._kafka_offsets("x") == {}


# ── the report ───────────────────────────────────────────────────────────────


def _snap(ts, **kw):
    d = {"_ts": ts}
    d.update(kw)
    return d


def test_the_ratio_is_reported(capsys):
    from shared.kafka import Topics

    a = _snap(0, **{"PG:events": 0, Topics.ENRICHED_EVENTS: 0,
                    Topics.CORRELATIONS: 0, "PG:tel_think": 0, "PG:tel_done": 0})
    b = _snap(600, **{"PG:events": 10000, Topics.ENRICHED_EVENTS: 2000,
                      Topics.CORRELATIONS: 20, "PG:tel_think": 2, "PG:tel_done": 2})
    flow.report(a, b)
    out = capsys.readouterr().out
    assert "one inference per 5,000 collected events" in out


def test_zero_inference_is_stated_not_divided_by(capsys):
    """Four agents doing no work at all is how this audit started."""
    from shared.kafka import Topics

    a = _snap(0, **{"PG:events": 0, Topics.ENRICHED_EVENTS: 0,
                    Topics.CORRELATIONS: 0, "PG:tel_think": 0, "PG:tel_done": 0})
    b = _snap(600, **{"PG:events": 10000, Topics.ENRICHED_EVENTS: 2000,
                      Topics.CORRELATIONS: 20, "PG:tel_think": 2, "PG:tel_done": 0})
    flow.report(a, b)
    assert "NO INFERENCE COMPLETED" in capsys.readouterr().out


def test_two_snapshots_at_the_same_instant_are_refused(capsys):
    flow.report(_snap(5), _snap(5))
    assert "not apart in time" in capsys.readouterr().out
