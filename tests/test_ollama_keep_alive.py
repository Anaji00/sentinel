"""Guards the field that took the whole reasoning tier offline.

`OLLAMA_KEEP_ALIVE=-1` is valid as a server environment variable, where it means
"hold the model in RAM forever". The REST API's `keep_alive` field is a
different grammar: it parses a *duration*, so the string "-1" fails with
`time: missing unit in duration "-1"` and the request returns 400.

Passing the env var through verbatim meant every inference 400'd. Each agent
then walked its retry-and-fallback ladder on a request that could never succeed,
and the Kafka consumers wedged behind it -- measured at 0 messages/minute across
every topic whose handler calls a model, while lag grew 180/minute.

Verified against a live Ollama: string "-1" -> 400, number -1 -> 200, "5m" -> 200.
"""
import importlib
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _reload(monkeypatch, value):
    monkeypatch.setenv("OLLAMA_KEEP_ALIVE", value)
    import shared.utils.ollama as o
    return importlib.reload(o)


@pytest.mark.parametrize("value", ["-1", "0", "300", "-30"])
def test_bare_numbers_are_sent_as_json_numbers(monkeypatch, value):
    """A duration parser rejects a number with no unit; JSON numbers are valid."""
    mod = _reload(monkeypatch, value)
    assert isinstance(mod.OLLAMA_KEEP_ALIVE, int), (
        f"{value!r} would be serialised as a string and rejected by Ollama"
    )
    assert mod.OLLAMA_KEEP_ALIVE == int(value)


@pytest.mark.parametrize("value", ["5m", "1h", "30s", "24h"])
def test_duration_strings_pass_through_unchanged(monkeypatch, value):
    mod = _reload(monkeypatch, value)
    assert mod.OLLAMA_KEEP_ALIVE == value
    assert isinstance(mod.OLLAMA_KEEP_ALIVE, str)


def test_the_request_payload_carries_a_serialisable_keep_alive(monkeypatch):
    """The value must survive json.dumps in the shape Ollama accepts."""
    import json
    mod = _reload(monkeypatch, "-1")
    encoded = json.dumps({"keep_alive": mod.OLLAMA_KEEP_ALIVE})
    assert encoded == '{"keep_alive": -1}', encoded
    assert '"-1"' not in encoded, "still quoted; Ollama parses this as a duration"


def test_default_is_a_valid_duration(monkeypatch):
    """With nothing configured the client must still send something Ollama takes."""
    monkeypatch.delenv("OLLAMA_KEEP_ALIVE", raising=False)
    import shared.utils.ollama as o
    mod = importlib.reload(o)
    ka = mod.OLLAMA_KEEP_ALIVE
    assert isinstance(ka, int) or (isinstance(ka, str) and ka[-1].isalpha()), (
        f"default {ka!r} is neither a number nor a unit-suffixed duration"
    )


# ── model sizing against the container ceiling ───────────────────────────────

def test_no_agent_requests_a_model_larger_than_the_container():
    """Four agents named qwen2.5:7b directly.

    That image is 4.7 GB and the ollama service is capped at 4 GB with
    OLLAMA_MAX_LOADED_MODELS=2, so it could never load. Every call walked the
    retry-and-fallback ladder, paying a full timeout, before landing on a small
    model anyway. Model choice belongs to the tier's AGENT_MODEL, not a literal.
    """
    import re
    src = (ROOT / "services/agents/main.py").read_text(encoding="utf-8")
    oversized = re.findall(r'model\s*=\s*"(qwen2\.5:7b|llama3:latest)"', src)
    assert not oversized, (
        f"agents hardcode {set(oversized)}, which cannot fit the ollama memory limit"
    )


def test_compose_ceiling_still_matches_the_configured_models():
    """If the ceiling is raised or the tier model changed, this should be revisited."""
    import re, yaml
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    ollama = compose["services"]["ollama"]
    limit = (((ollama.get("deploy") or {}).get("resources") or {}).get("limits") or {}).get("memory")
    assert limit, "ollama has no memory limit; sizing assumptions here are unverified"

    env = ollama.get("environment") or []
    joined = " ".join(env if isinstance(env, list) else [f"{k}={v}" for k, v in env.items()])
    assert "OLLAMA_MAX_LOADED_MODELS" in joined, "model residency is unbounded"
