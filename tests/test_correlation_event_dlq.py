"""Review F4 (21 Nov 2026): a correlation failure on one event was counted but the batch still committed, so the event was lost. It is now also parked in the DLQ
(replayable once the cause is fixed); a failing DLQ send is counted, never raised, so one bad event still cannot abort the batch."""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_a_failed_event_is_counted_and_parked_in_the_dlq():
    src = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    i = src.index('swallowed("correlation.event", e, logger')
    block = src[i:i + 700]
    assert "producer.send(Topics.DLQ" in block and "event.model_dump(mode=\"json\")" in block
    assert 'swallowed("correlation.event_dlq"' in block and block.index("producer.send") < block.index("return")
