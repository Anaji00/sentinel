"""
tests/test_wiring_and_stall_detection.py

Two open items, and what closing them actually showed.

WIRING. This audit found the same defect four times, one instance at a time:
`release()` freeing the inference slot and never called; `try_acquire(score=...)`
deciding admission from a parameter nothing read; RadarAgent, the one agent that
reliably won a slot, being the only one not wired to an output; and
`get_entity_centrality()`, which weights cluster severity by graph centrality and
has no callers, so that weighting does not happen anywhere.

None were broken. Each was written, several were tested, and none ran -- which is
harder to see than a bug, because everything that does run keeps working and
missing behaviour reports no failure.

`scripts/find_unreached_code.py` now finds them in one pass: 45 of 836 functions
are never referenced. Its output is a lead list rather than a defect list, and
the first run proved why -- three consecutive candidates were false alarms:

  * install_redaction() has no callers, but RedactingFilter is attached directly
    in shared/utils/logging.py, so redaction works.
  * fetch_and_sync_ofac_sdn_list() has no callers, but _ofac_sync_loop()
    duplicates its body and runs at enrichment startup. The live automaton holds
    39,085 keywords, not the 31-keyword seed list.
  * get_entity_centrality() also looked broken for querying {id: ...} on a graph
    keyed by name -- until the graph showed both properties on 146,970 of
    146,993 nodes.

So the tests below pin the wirings this audit actually established, rather than
asserting that the sweep returns nothing.

OBSERVABILITY. Ollama wedged for four hours and every check said fine. The
container healthcheck runs `ollama list`, which answers from the model registry
without touching generation. The circuit breaker did not fire either: it counts
client timeouts, and the agents were starved at the time, so almost nothing was
being sent to time out -- twelve hours of logs, zero timeout lines, through four
hours of a dead server.

The obvious fix is worse than the bug. With OLLAMA_NUM_PARALLEL=1 a probe queues
behind real work: `ollama run` with a two-word prompt took over 400s under
ordinary load, so a healthcheck built on generation would fail while the server
was merely busy and restart it mid-inference, in a loop. The detector therefore
lives in the client, which already sees every request and every response and
needs no extra load to notice that one is going out and the other is not.
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _source(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


# -- the wirings this audit established ----------------------------------------

@pytest.mark.parametrize(
    "rel,needle,why",
    [
        ("services/agents/base.py", "self._inference_budget.finish()",
         "the slot is never shortened after an inference completes"),
        ("services/agents/base.py", "score=_message_score(message)",
         "admission is decided by arrival time again"),
        ("services/agents/base.py", "owner=getattr(self, \"name\", None)",
         "the budget cannot tell callers apart, so it cannot be fair"),
        ("services/agents/radar_agent.py", "publish_bulletin",
         "the one agent that wins slots is unwired from the output again"),
        ("shared/utils/ollama.py", "self._note_request()",
         "the stall detector is defined and never called"),
    ],
)
def test_the_call_still_exists(rel, needle, why):
    assert needle in _source(rel), why


def test_the_sweep_tool_exists_and_explains_its_own_output():
    """A list of 45 names with no guidance is how a false alarm becomes a
    commit. Three of the first run's candidates were working code."""
    source = _source("scripts/find_unreached_code.py")
    assert "lead list, not a defect list" in source
    assert "install_redaction" in source
    assert "_ofac_sync_loop" in source


def test_decorated_functions_are_excluded_by_default():
    """Routes, validators and fixtures are called by their decorator. Including
    them turned 45 real candidates into 142 mostly-routes."""
    source = _source("scripts/find_unreached_code.py")
    assert "if node.decorator_list and not include_decorated:" in source


def test_string_literals_count_as_references():
    """getattr(obj, "name") and registry lookups are uses that no call node
    records."""
    source = _source("scripts/find_unreached_code.py")
    assert "literals.get(name, 0) == 0" in source


# -- the stall detector --------------------------------------------------------

def _client():
    from shared.utils.ollama import OllamaClient

    return OllamaClient.__new__(OllamaClient)


def _fresh(monkeypatch, stall_after=0.0):
    import shared.utils.ollama as mod

    client = _client()
    client._last_completion = None
    client._requests_since_completion = 0
    client._stall_reported = False
    monkeypatch.setattr(mod, "STALL_AFTER_SEC", stall_after)
    return client


def _silent_for(client, seconds: float) -> None:
    """Backdate the last completion instead of waiting for the clock.

    Windows resolves time.monotonic() to about 15ms, so two consecutive calls
    return the same value and a test that leans on real elapsed time compares
    0.0 against a 0.0 threshold and quietly proves nothing.
    """
    import time as _time

    client._last_completion = _time.monotonic() - seconds


def test_a_single_slow_request_is_not_a_stall(monkeypatch, caplog):
    """One inference in flight is a queue. This host routinely takes 90-150s per
    call, and reporting that as a failure would be noise on every busy minute."""
    client = _fresh(monkeypatch, stall_after=900.0)
    client._note_request()                       # first ever: starts the clock
    with caplog.at_level("ERROR"):
        client._note_request()                   # second, moments later
    assert client._requests_since_completion == 2
    assert "OLLAMA STALLED" not in caplog.text, "ordinary queueing reported as a stall"


def test_requests_going_out_with_nothing_coming_back_is_reported(monkeypatch, caplog):
    client = _fresh(monkeypatch, stall_after=900.0)
    client._note_request()
    client._note_completion()                    # establishes a real completion
    _silent_for(client, 1200)                    # 20 minutes with no answer
    client._note_request()
    with caplog.at_level("ERROR"):
        client._note_request()

    assert client._stall_reported
    assert "OLLAMA STALLED" in caplog.text


def test_a_stall_is_reported_once_not_per_request(monkeypatch, caplog):
    """A wedged server is asked repeatedly; one line per attempt would bury the
    logs it is meant to make legible."""
    client = _fresh(monkeypatch, stall_after=900.0)
    client._note_request()
    client._note_completion()
    _silent_for(client, 1200)
    with caplog.at_level("ERROR"):
        for _ in range(6):
            client._note_request()

    assert caplog.text.count("OLLAMA STALLED") == 1


def test_recovery_is_reported_and_rearms(monkeypatch, caplog):
    client = _fresh(monkeypatch, stall_after=900.0)
    client._note_request()
    client._note_completion()
    _silent_for(client, 1200)
    client._note_request()
    client._note_request()
    assert client._stall_reported

    with caplog.at_level("WARNING"):
        client._note_completion()
    assert "answered again" in caplog.text
    assert not client._stall_reported
    assert client._requests_since_completion == 0


def test_a_healthy_client_never_reports(monkeypatch, caplog):
    client = _fresh(monkeypatch, stall_after=900.0)
    with caplog.at_level("ERROR"):
        for _ in range(10):
            client._note_request()
            client._note_completion()
    assert "OLLAMA STALLED" not in caplog.text


def test_a_long_window_does_not_fire_on_ordinary_latency(monkeypatch, caplog):
    """The default is 900s precisely so a normal 150s inference never trips it."""
    client = _fresh(monkeypatch, stall_after=900.0)
    client._note_request()
    client._note_completion()
    with caplog.at_level("ERROR"):
        client._note_request()
        client._note_request()
    assert "OLLAMA STALLED" not in caplog.text


def test_the_detector_does_not_generate_anything():
    """The reason it is here and not in the healthcheck: a generation probe
    queues behind real work on a single-slot server and would restart it."""
    source = _source("shared/utils/ollama.py")
    start = source.index("def _note_request")
    body = source[start:source.index("def _note_completion")]
    for forbidden in ("_session.post", "api/generate", "ollama run"):
        assert forbidden not in body


# -- the accounting gap --------------------------------------------------------

def test_the_inference_services_publish_their_metrics():
    """`ollama_calls_total` was incremented in these services since they were
    written and never left the process.

    bind_redis() -- the function that makes a process's counters visible -- had
    exactly one caller, a collector-specific helper. So only the collectors
    published, and the services doing all the inference did not participate in
    the cross-process aggregation the metrics module exists to provide.

    The cost was not a missing dashboard. It made "how much model time does each
    service consume" unanswerable from inside, which left parsing Ollama's
    access log by container IP as the only route -- and Docker reassigns those
    on restart, so the attribution was wrong in a way that took two corrections
    to notice.
    """
    for module in ("services/agents/main.py", "services/reasoning/main.py"):
        source = _source(module)
        assert "bind_redis" in source, f"{module} still does not publish its metrics"


def test_each_inference_service_publishes_under_its_own_name():
    """Metrics are keyed sentinel:metrics:{service}. Two tiers sharing a name
    would overwrite each other, which is worse than no attribution because it
    looks like attribution."""
    compose = _source("docker-compose.yml")
    for name in ("SENTINEL_SERVICE=agents-fast", "SENTINEL_SERVICE=agents-heavy",
                 "SENTINEL_SERVICE=reasoning"):
        assert name in compose


def test_the_client_labels_calls_with_the_real_service():
    """SERVICE_NAME is set nowhere in this deployment, so every client labelled
    its calls "default" and the gateway summed three services into one series --
    discarding the attribution at the last step."""
    source = _source("shared/utils/ollama.py")
    assert 'os.getenv("SENTINEL_SERVICE")' in source
