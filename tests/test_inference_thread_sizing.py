"""
tests/test_inference_thread_sizing.py

llama.cpp sized its thread pool to hardware the container may not use.

Measured on this deployment: `ps -o nlwp` showed the ollama runner with 16
threads while the ollama container's cgroup quota was 6 cores (`cpus: '6.0'`).
nproc and os.cpu_count() both report the host's 12 CPUs; neither sees the
quota. Sixteen threads then contended for six cores' worth of scheduling, which
appears as throughput below the cap -- ollama sat at 383% of an available 600%
-- rather than as any error.

The count is configured, not detected, and that distinction is the point. The
natural implementation reads the cgroup quota, but this code runs in the
*client*: agents-heavy is capped at 2.5 cores, so quota detection returned 2 and
would have told a 6-core Ollama to use two threads -- slower than changing
nothing. Only the deployment knows the server's allocation.
"""

import sys
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.ollama import _inference_threads  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_cache():
    _inference_threads.cache_clear()
    yield
    _inference_threads.cache_clear()


def test_unset_means_ollama_keeps_its_own_default():
    """Sending nothing is the safe default; a wrong number is worse than none."""
    with mock.patch.dict("os.environ", {}, clear=True):
        assert _inference_threads() is None


def test_an_explicit_setting_is_used():
    with mock.patch.dict("os.environ", {"OLLAMA_NUM_THREAD": "6"}):
        assert _inference_threads() == 6


def test_the_clients_own_cpu_quota_is_never_consulted():
    """The bug this guards: agents-heavy has 2.5 cores, ollama has 6.

    Detecting the quota here would cap a 6-core model server at two threads.
    """
    with mock.patch.dict("os.environ", {}, clear=True),          mock.patch("os.cpu_count", return_value=2):
        assert _inference_threads() is None


def test_a_nonsense_value_is_ignored_rather_than_crashing():
    with mock.patch.dict("os.environ", {"OLLAMA_NUM_THREAD": "not-a-number"}):
        assert _inference_threads() is None


def test_a_zero_or_negative_thread_count_is_refused():
    for bad in ("0", "-4"):
        _inference_threads.cache_clear()
        with mock.patch.dict("os.environ", {"OLLAMA_NUM_THREAD": bad}):
            assert _inference_threads() is None, f"{bad} would stall inference"


def test_the_option_reaches_the_payload_only_when_configured():
    src = (ROOT / "shared/utils/ollama.py").read_text(encoding="utf-8")
    assert 'payload["options"]["num_thread"] = _threads' in src
    assert "if _threads is not None:" in src


def test_the_deployment_states_a_value_matching_the_server():
    """A setting nothing sets changes nothing."""
    env = (ROOT / ".env").read_text(encoding="utf-8")
    assert "OLLAMA_NUM_THREAD=6" in env
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "cpus: '6.0'" in compose, "ollama's cpu limit moved; the thread count must follow"
