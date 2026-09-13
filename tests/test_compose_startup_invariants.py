"""Things about docker-compose.yml that are only discovered by starting it.

The platform came up with 23 of its 30 containers working and the rest either
stopped or restarting, and none of the four causes were visible in a code
review:

  * `prometheus`, `grafana` and `kafka-ui` sit behind the `obs` profile, so a
    plain `docker compose up` never selects them. The README told the reader to
    run exactly that command.
  * `neo4j` left a pid file in its writable layer when it was killed rather than
    shut down, so every subsequent start printed "Neo4j is already running
    (pid:8)" and exited.
  * `ingress` (nginx) and `frontend` (node) were given the shared Python
    healthcheck, which cannot run in either image, so both reported unhealthy
    permanently while serving every request.
  * the stateful services had Docker's default ten-second stop grace, which is
    shorter than any of them takes to shut down cleanly -- and an unclean stop
    is what left the pid file in the first place.

These assertions read the compose file. They need no Docker.
"""
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker-compose.yml"

# The shared probe runs `python /app/scripts/healthcheck.py`, which exists only
# in images built from the repository root Dockerfile.
PYTHON_PROBE = "/app/scripts/healthcheck.py"

# Services built from a base image with no Python and no /app.
NON_PYTHON_SERVICES = {"ingress", "frontend"}

# Stateful services whose shutdown does real work: flushing log segments,
# writing a checkpoint, closing store files.
STATEFUL_SERVICES = {"neo4j", "kafka", "timescaledb"}


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def services(compose):
    return compose["services"]


def _probe_text(service: dict) -> str:
    test = (service.get("healthcheck") or {}).get("test")
    if test is None:
        return ""
    return " ".join(test) if isinstance(test, list) else str(test)


def test_no_non_python_image_runs_the_python_healthcheck(services):
    offenders = sorted(
        name for name in NON_PYTHON_SERVICES
        if PYTHON_PROBE in _probe_text(services.get(name, {}))
    )
    assert not offenders, (
        f"{offenders} are not built from the Python image. The probe fails with "
        "'exec: python: executable file not found', which Docker records as an "
        "unhealthy container rather than as a broken probe."
    )


def test_every_service_with_a_python_probe_is_built_from_the_python_image(services):
    for name, service in services.items():
        if PYTHON_PROBE not in _probe_text(service):
            continue
        build = service.get("build")
        context = build.get("context") if isinstance(build, dict) else build
        assert context in (".", None), (
            f"{name} runs the shared Python healthcheck but is built from "
            f"{context!r}, not the repository root image that contains it."
        )


def test_neo4j_clears_its_pid_file_before_starting(services):
    entrypoint = services["neo4j"].get("entrypoint")
    assert entrypoint, (
        "neo4j has no entrypoint override. Without one the pid file left by an "
        "unclean stop survives in the writable layer, and every restart exits "
        "with 'Neo4j is already running'."
    )
    text = " ".join(entrypoint) if isinstance(entrypoint, list) else str(entrypoint)
    assert ".pid" in text and "rm " in text
    # The image's own entrypoint must still run: this override replaces the
    # startup sequence otherwise, including the apoc install and the auth setup.
    assert "docker-entrypoint.sh" in text


@pytest.mark.parametrize("name", sorted(STATEFUL_SERVICES))
def test_stateful_services_get_time_to_shut_down(services, name):
    grace = services[name].get("stop_grace_period")
    assert grace, (
        f"{name} uses Docker's 10s default before SIGKILL. It needs longer than "
        "that to shut down cleanly, and being killed part-way is what caused "
        "the restart loop this guards against."
    )


def test_every_container_has_a_restart_policy(services):
    # Excludes the one-shot services: a migrator that restarts is a migrator
    # that runs forever.
    one_shot = {"migrator", "integration-tests"}
    missing = sorted(
        name for name, svc in services.items()
        if name not in one_shot and not svc.get("restart")
    )
    assert not missing, f"No restart policy on: {missing}"


def test_the_readme_does_not_tell_the_reader_to_run_a_profileless_up():
    """The command that starts two thirds of the platform and looks like a failure."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for line in readme.splitlines():
        stripped = line.strip()
        if not stripped.startswith("docker compose up"):
            continue
        assert "--profile" in stripped, (
            f"README line {stripped!r} starts only the profile-less services. "
            "Name a profile, or point the reader at the Makefile."
        )


def test_profiled_services_are_reachable_through_the_makefile():
    """Every profile has a target, or it can only be started by hand."""
    compose_doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    profiles = {
        profile
        for svc in compose_doc["services"].values()
        for profile in (svc.get("profiles") or [])
    }
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    # `test` is CI-only and is invoked by the CI workflow, not by a person.
    unreachable = sorted(
        p for p in profiles - {"test"} if f"--profile {p}" not in makefile
    )
    assert not unreachable, f"Profiles with no Makefile target: {unreachable}"
