"""The model configuration has to agree with itself.

Three separate places state how many models this deployment runs and which
ones, and all three had drifted apart:

  * docker-compose.yml argued at length for one resident model and set the
    value to 2, so the thread-barrier collapse it describes stayed possible
    for as long as it was documented as fixed.
  * services/agents/main.py said the compose file gave agents-heavy
    qwen2.5:3b. It gave it qwen2.5:1.5b.
  * OllamaClient's fallback ladder chose from every pulled tag, so a single
    failure could reach llama3:latest or qwen2.5:7b -- 4.7 GB images against
    a 4.5 GB container limit, unloadable, and therefore a guaranteed 600 s
    timeout producing nothing.

None of that is visible at runtime: an agent whose inference times out looks
like an agent doing nothing, which is what an idle agent also looks like.
These tests read the configuration rather than the comments.
"""
import pathlib

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def _env(service: dict) -> dict:
    out = {}
    for item in service.get("environment") or []:
        k, _, v = str(item).partition("=")
        out[k] = v
    return out


def test_ollama_restarts_like_everything_that_depends_on_it(compose):
    """It exited 255 and stayed down while both agent tiers stayed up."""
    ollama = compose["services"]["ollama"]
    assert ollama.get("restart") == "always", (
        "ollama has no restart policy but is the sole dependency of agents-fast "
        "and agents-heavy, which restart forever into a backend that is gone."
    )


def test_one_model_slot_because_two_runners_do_not_fit_six_cores(compose):
    slots = int(_env(compose["services"]["ollama"])["OLLAMA_MAX_LOADED_MODELS"])
    cpus = float(compose["services"]["ollama"]["deploy"]["resources"]["limits"]["cpus"])
    assert slots == 1, (
        f"{slots} model slots against a {cpus}-core quota. Each resident model is "
        "a runner started with NumThreads matching the quota, so two of them "
        "oversubscribe the cores and llama.cpp's busy-wait barrier collapses "
        "rather than degrades."
    )


def test_both_agent_tiers_name_the_same_model(compose):
    """One slot only works if nothing asks for a second model."""
    fast, heavy = (_env(compose["services"][n]) for n in ("agents-fast", "agents-heavy"))
    names = {
        "agents-fast.AGENT_MODEL": fast["AGENT_MODEL"],
        "agents-fast.OLLAMA_FALLBACK_MODEL": fast["OLLAMA_FALLBACK_MODEL"],
        "agents-heavy.AGENT_MODEL": heavy["AGENT_MODEL"],
        "agents-heavy.OLLAMA_FALLBACK_MODEL": heavy["OLLAMA_FALLBACK_MODEL"],
    }
    assert len(set(names.values())) == 1, (
        f"the tiers name more than one model: {names}. With one slot, any "
        "request for a second evicts the first and pays the reload."
    )


def test_every_configured_model_is_one_the_client_will_load(compose):
    from shared.utils.ollama import OLLAMA_ALLOWED_MODELS

    configured = set()
    for name in ("agents-fast", "agents-heavy"):
        env = _env(compose["services"][name])
        configured.update(v for k, v in env.items() if k.endswith("MODEL"))
    unlistable = configured - set(OLLAMA_ALLOWED_MODELS)
    assert not unlistable, (
        f"{sorted(unlistable)} is configured but not in OLLAMA_ALLOWED_MODELS, so "
        "the fallback ladder would refuse to route back to it."
    )


def test_the_allowlist_excludes_models_too_large_for_the_container(compose):
    """4.7 GB of weights will not load under a 4.5 GB limit, ever."""
    from shared.utils.ollama import OLLAMA_ALLOWED_MODELS

    too_big = {"llama3:latest", "qwen2.5:7b"}
    assert not (too_big & set(OLLAMA_ALLOWED_MODELS)), (
        f"{sorted(too_big & set(OLLAMA_ALLOWED_MODELS))} exceeds the ollama "
        "container's memory limit. Reaching one costs a full client timeout and "
        "returns nothing."
    )


def test_fallback_selection_honours_the_allowlist():
    """The filter, not the comment, is what keeps an unloadable model out."""
    from shared.utils.ollama import _permitted

    pulled = ["llama3:latest", "qwen2.5:7b", "qwen2.5:3b", "gemma:2b", "qwen2.5:1.5b"]
    assert _permitted(pulled) == ["qwen2.5:3b", "gemma:2b", "qwen2.5:1.5b"]


def test_an_empty_allowlist_means_no_opinion_not_no_models(monkeypatch):
    """Clearing the env must not stop every inference."""
    import importlib

    import shared.utils.ollama as mod

    monkeypatch.setenv("OLLAMA_ALLOWED_MODELS", "")
    reloaded = importlib.reload(mod)
    try:
        assert reloaded._permitted(["anything:latest"]) == ["anything:latest"]
    finally:
        monkeypatch.delenv("OLLAMA_ALLOWED_MODELS", raising=False)
        importlib.reload(mod)


def test_the_default_model_is_one_that_fits():
    from shared.utils.ollama import DEFAULT_MODEL, OLLAMA_ALLOWED_MODELS, OLLAMA_MODEL

    assert OLLAMA_MODEL in OLLAMA_ALLOWED_MODELS, (
        f"OLLAMA_MODEL={OLLAMA_MODEL} is not loadable here. It defaulted to "
        "qwen2.5:7b, which cannot fit the container, so any caller falling "
        "through to the default timed out instead of answering."
    )
    assert DEFAULT_MODEL in OLLAMA_ALLOWED_MODELS
