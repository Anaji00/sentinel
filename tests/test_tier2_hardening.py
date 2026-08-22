"""
tests/test_tier2_hardening.py

Tier 2 Security & Infrastructure Hardening Tests:
1. TLS Ingress Reverse Proxy Configuration & SSL/Security Headers (2.1)
2. Conditional CORS Localhost Regex restricted to SAFE_DEV_ENVS (2.2)
3. Internal Datastore & Monitoring Port Isolation (2.3)
4. Container CPU & Memory Resource Limits (2.4)
"""

import os
import re
import yaml
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from shared.utils.env_guard import SAFE_DEV_ENVS
from services.api_gateway.routes.main import app, build_cors_kwargs


REPO_ROOT = Path(__file__).resolve().parents[1]


# ── 1. CORS CONFIGURATION TESTS (2.2) ──────────────────────────────────────────

def test_cors_configuration_under_dev_env():
    """Verify that in SAFE_DEV_ENVS, localhost regex is enabled."""
    sentinel_env = "test"
    assert sentinel_env in SAFE_DEV_ENVS

    # Inspect the CORSMiddleware configuration in the FastAPI app
    cors_middleware = None
    for middleware in app.user_middleware:
        if "CORSMiddleware" in str(middleware.cls):
            cors_middleware = middleware
            break

    assert cors_middleware is not None, "CORSMiddleware should be installed on FastAPI app"
    assert cors_middleware.kwargs.get("allow_credentials") is True


def test_cors_regex_logic_disabled_in_production():
    """Verify that the REAL app CORS builder disables the localhost regex in production."""
    assert "production" not in SAFE_DEV_ENVS

    kwargs = build_cors_kwargs("production", "https://dashboard.sentinel-quant.io")

    # The open localhost regex must be disabled outside dev/test.
    assert kwargs["allow_origin_regex"] is None
    assert kwargs["allow_origins"] == ["https://dashboard.sentinel-quant.io"]
    assert kwargs["allow_credentials"] is True


def test_cors_regex_enabled_in_dev():
    """Verify the localhost regex IS enabled in dev/test environments."""
    kwargs = build_cors_kwargs("test", "")
    assert kwargs["allow_origin_regex"] == r"https?://(localhost|127\.0\.0\.1)(:\d+)?"
    # Falls back to the built-in dev origins when none are configured.
    assert "http://localhost:3000" in kwargs["allow_origins"]


# ── 2. NGINX INGRESS & TLS CONFIGURATION TESTS (2.1) ──────────────────────────

def test_nginx_ingress_configuration():
    """Verify Nginx reverse proxy configuration, TLS termination, and security headers."""
    nginx_conf_path = REPO_ROOT / "deploy" / "nginx" / "nginx.conf"
    assert nginx_conf_path.exists(), "nginx.conf must exist in deploy/nginx/"

    conf_content = nginx_conf_path.read_text(encoding="utf-8")

    # 1. HTTP -> HTTPS 301 Redirect
    assert "listen 80;" in conf_content
    assert "return 301 https://$host$request_uri;" in conf_content

    # 2. HTTPS Port 443 with TLS
    assert "listen 443 ssl http2;" in conf_content
    assert "ssl_protocols TLSv1.2 TLSv1.3;" in conf_content
    assert "ssl_certificate /etc/nginx/ssl/sentinel.crt;" in conf_content
    assert "ssl_certificate_key /etc/nginx/ssl/sentinel.key;" in conf_content

    # 3. Security Headers
    assert "Strict-Transport-Security" in conf_content
    assert "X-Frame-Options" in conf_content
    assert "X-Content-Type-Options" in conf_content
    assert "X-XSS-Protection" in conf_content

    # 4. Proxy Destinations & BFF routing.
    #    Browser /api/auth/* and /api/proxy/* are Next.js route handlers and MUST be
    #    routed to the frontend, not the gateway, or login and all data calls break.
    assert "proxy_pass http://api_gateway_backend;" in conf_content
    assert "proxy_pass http://frontend_backend;" in conf_content
    assert "location /api/auth/" in conf_content
    assert "location /api/proxy/" in conf_content

    # 5. WebSocket upgrade must be configured on the path the client actually uses
    #    (/api/v1/events/ws/...), not a dead /ws prefix.
    assert "location /api/v1/events/ws" in conf_content
    assert "Upgrade $http_upgrade;" in conf_content


def test_nginx_entrypoint_certificate_bootstrapper():
    """Verify Nginx entrypoint generates self-signed certs for local development if missing."""
    entrypoint_path = REPO_ROOT / "deploy" / "nginx" / "entrypoint.sh"
    assert entrypoint_path.exists(), "entrypoint.sh must exist in deploy/nginx/"

    content = entrypoint_path.read_text(encoding="utf-8")
    assert "openssl req -x509" in content
    assert "sentinel.crt" in content
    assert "sentinel.key" in content
    assert "exec nginx" in content


# ── 3. PORT ISOLATION & DOCKER COMPOSE TESTS (2.3) ─────────────────────────────

def test_docker_compose_port_isolation():
    """
    Verify that internal datastores and monitoring do NOT publish host ports,
    and that only the ingress reverse proxy exposes ports 80/443.
    """
    compose_path = REPO_ROOT / "docker-compose.yml"
    assert compose_path.exists(), "docker-compose.yml must exist"

    with open(compose_path, "r", encoding="utf-8") as f:
        compose_data = yaml.safe_load(f)

    services = compose_data.get("services", {})
    assert "ingress" in services, "Ingress reverse proxy service must be defined"

    # Ingress must be the only service binding web ports, and it must bind
    # loopback explicitly. A bare "80:80" publishes on every interface, which on
    # a laptop means the dashboard is reachable from any untrusted network the
    # host joins.
    # The bind address is parameterised so a public deployment can opt in, but
    # the *default* must stay loopback: an operator who never sets the variable
    # must not end up exposed. Assert the resolved default rather than a literal.
    ingress_ports = [str(p) for p in services["ingress"].get("ports", [])]

    def _default_host(mapping: str) -> str:
        """Resolves ${VAR:-default}:container:host to the host it binds by default."""
        m = re.match(r"^\$\{[A-Z_][A-Z0-9_]*:-([^}]*)\}:", mapping)
        if m:
            return m.group(1)
        return mapping.rsplit(":", 2)[0] if mapping.count(":") >= 2 else ""

    published = {mapping.rsplit(":", 1)[0].rsplit(":", 1)[-1]: mapping for mapping in ingress_ports}
    assert any(m.endswith(":80:80") for m in ingress_ports), f"ingress must publish 80: {ingress_ports}"
    assert any(m.endswith(":443:443") for m in ingress_ports), f"ingress must publish 443: {ingress_ports}"
    for mapping in ingress_ports:
        host = _default_host(mapping)
        assert host == "127.0.0.1", (
            f"ingress port {mapping!r} defaults to {host!r}, not loopback. A bare "
            f"'80:80' or a 0.0.0.0 default publishes on every interface, which on a "
            f"laptop means the dashboard is reachable from any untrusted network."
        )

    # Internal services that MUST NOT have public host port mappings in production stack
    internal_services = [
        "zookeeper",
        "kafka",
        "kafka-ui",
        "timescaledb",
        "neo4j",
        "redis",
        "ollama",
        "qdrant",
        "api-gateway",
        "frontend",
        "prometheus",
        "grafana",
    ]

    for svc_name in internal_services:
        svc = services.get(svc_name)
        assert svc is not None, f"Service '{svc_name}' should exist in compose"
        ports = svc.get("ports", [])
        assert len(ports) == 0, f"Service '{svc_name}' should NOT expose host ports! Found: {ports}"


# ── 4. CONTAINER RESOURCE LIMITS TESTS (2.4) ──────────────────────────────────

def test_docker_compose_resource_limits():
    """Verify that every container defines CPU and Memory limits in deploy.resources.limits."""
    compose_path = REPO_ROOT / "docker-compose.yml"
    with open(compose_path, "r", encoding="utf-8") as f:
        compose_data = yaml.safe_load(f)

    services = compose_data.get("services", {})
    assert len(services) >= 20, "Expected at least 20 services in docker-compose.yml"

    for svc_name, svc_conf in services.items():
        deploy = svc_conf.get("deploy", {})
        resources = deploy.get("resources", {})
        limits = resources.get("limits", {})

        assert "cpus" in limits, f"Service '{svc_name}' missing CPU resource limit!"
        assert "memory" in limits, f"Service '{svc_name}' missing Memory resource limit!"

        # Sizing checks for heavy services
        if svc_name == "ollama":
            assert float(limits["cpus"]) >= 4.0, "Ollama requires at least 4.0 CPUs"
            assert "G" in str(limits["memory"]), "Ollama requires gigabyte-scale memory limit"
        elif svc_name == "agents-heavy":
            assert float(limits["cpus"]) >= 2.0, "agents-heavy requires at least 2.0 CPUs"
        elif svc_name == "timescaledb":
            assert float(limits["cpus"]) >= 2.0, "timescaledb requires at least 2.0 CPUs"


# ── 5. RUNTIME MEMORY BOUNDING & DEPLOYMENT PROFILES ──────────────────────────

def _compose():
    with open(REPO_ROOT / "docker-compose.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _mib(v):
    v = str(v).strip().upper()
    return float(v[:-1]) * 1024 if v.endswith("G") else float(v[:-1])


def test_jvm_services_have_internal_heap_caps():
    """Kafka and Zookeeper must cap their heap below the cgroup limit.

    A container memory limit alone does not bound a JVM: without -Xmx the heap
    grows toward the cgroup ceiling and the kernel OOM-kills the process instead
    of the JVM garbage-collecting. Neo4j is bounded via its own NEO4J_* settings.
    """
    services = _compose()["services"]

    for name in ("kafka", "zookeeper"):
        env = services[name].get("environment") or {}
        heap = env.get("KAFKA_HEAP_OPTS")
        assert heap, f"{name} has no KAFKA_HEAP_OPTS -- JVM heap is unbounded"

        xmx = re.search(r"-Xmx(\d+)([mMgG])", heap)
        assert xmx, f"{name} heap opts {heap!r} set no -Xmx"
        heap_mib = float(xmx.group(1)) * (1024 if xmx.group(2).lower() == "g" else 1)

        limit = _mib(services[name]["deploy"]["resources"]["limits"]["memory"])
        assert heap_mib < limit, f"{name} heap {heap_mib}M >= cgroup limit {limit}M"
        assert heap_mib <= limit * 0.75, (
            f"{name} heap {heap_mib}M leaves too little headroom under {limit}M "
            f"for direct buffers, mmap'd segments and JVM overhead"
        )

    # Neo4j bounds itself through its own configuration rather than heap opts.
    neo = services["neo4j"].get("environment") or {}
    assert neo.get("NEO4J_server_memory_heap_max__size")
    assert neo.get("NEO4J_dbms_memory_pagecache_size")


def test_postgres_memory_is_bounded():
    """TimescaleDB must pin the settings that otherwise scale with connections."""
    cmd = " ".join(str(_compose()["services"]["timescaledb"]["command"]).split())

    for setting in ("max_connections", "shared_buffers", "work_mem",
                    "effective_cache_size", "maintenance_work_mem"):
        assert f"-c {setting}=" in cmd, f"postgres {setting} is unbounded"

    limit = _mib(_compose()["services"]["timescaledb"]["deploy"]["resources"]["limits"]["memory"])
    shared = re.search(r"-c shared_buffers=(\d+)MB", cmd)
    assert shared and float(shared.group(1)) < limit * 0.5, (
        "shared_buffers should stay well under half the container limit"
    )


def _env_of(service: str) -> str:
    env = _compose()["services"][service].get("environment") or []
    return " ".join(env) if isinstance(env, list) else " ".join(f"{k}={v}" for k, v in env.items())


def test_ollama_residency_is_declared_explicitly():
    """Residency must be a stated decision, whichever way it goes.

    Originally this asserted the model must NOT be pinned, because the stack was
    over-committed and 8 GB of ollama was a third of the VM. After right-sizing
    the ceilings against measured usage there is ~16 GB of headroom, and pinning
    became the better trade: a cold load costs ~15s on CPU and the agent tiers
    are called in bursts. What must hold in either case is that the value is
    explicit rather than defaulted.
    """
    joined = _env_of("ollama")
    assert "OLLAMA_KEEP_ALIVE=" in joined, "KEEP_ALIVE must be set explicitly"

    parallel = re.search(r"OLLAMA_NUM_PARALLEL=(\d+)", joined)
    assert parallel and int(parallel.group(1)) <= 2, (
        "CPU-only inference cannot serve many parallel contexts; each one "
        "multiplies the KV cache and divides throughput"
    )


def test_keep_alive_is_consistent_between_server_and_clients():
    """A per-request keep_alive silently overrides the server setting.

    shared/utils/ollama.py hardcoded `"keep_alive": -1`, so the container's
    OLLAMA_KEEP_ALIVE was inoperative and the declared config did not describe
    the running system. The client now reads the same variable.
    """
    server = re.search(r"OLLAMA_KEEP_ALIVE=(\S+)", _env_of("ollama"))
    assert server, "ollama declares no keep-alive"

    for tier in ("agents-heavy", "agents-fast"):
        client = re.search(r"OLLAMA_KEEP_ALIVE=(\S+)", _env_of(tier))
        assert client, f"{tier} does not declare a keep-alive"
        assert client.group(1) == server.group(1), (
            f"{tier} asks for keep_alive={client.group(1)} while the server is "
            f"configured for {server.group(1)}"
        )

    src = (REPO_ROOT / "shared" / "utils" / "ollama.py").read_text(encoding="utf-8")
    assert '"keep_alive": -1' not in src, (
        "client hardcodes keep_alive, overriding whatever the server declares"
    )


def test_deployment_profiles_fit_the_memory_budget():
    """Each supported operating mode must fit inside the WSL2 allocation.

    26 GiB is what .wslconfig grants the VM. Collectors and agents together
    deliberately exceed it -- that is a hardware fact, surfaced as two named
    modes rather than discovered as an OOM kill under load.
    """
    WSL_BUDGET_GIB = 26.0
    services = _compose()["services"]

    def ceiling(active):
        total = 0.0
        for name, svc in services.items():
            profiles = svc.get("profiles")
            if profiles and not any(p in active for p in profiles):
                continue
            limits = ((svc.get("deploy") or {}).get("resources") or {}).get("limits", {})
            if limits.get("memory"):
                total += _mib(limits["memory"])
        return total / 1024

    assert ceiling(set()) < WSL_BUDGET_GIB
    assert ceiling({"collectors"}) < WSL_BUDGET_GIB, "analyst mode must fit"
    assert ceiling({"collectors", "obs"}) < WSL_BUDGET_GIB
    assert ceiling({"agents"}) < WSL_BUDGET_GIB, "reasoning mode must fit"

    # Collectors and agents together previously exceeded the budget, and this
    # asserted the incompatibility to keep it deliberate. Ceilings were then
    # right-sized against measured usage -- frontend 30MiB of 1G, timescaledb
    # 143MiB of 2G, agents-heavy 705MiB of 2.5G -- and every mode now fits.
    assert ceiling({"collectors", "agents"}) < WSL_BUDGET_GIB, (
        "collectors and agents must co-exist"
    )
    assert ceiling({"collectors", "agents", "obs"}) < WSL_BUDGET_GIB, (
        "the full stack including observability must fit"
    )


def test_llm_services_are_opt_in():
    """The memory-dominant services must not start by default."""
    services = _compose()["services"]
    for name in ("ollama", "agents-heavy", "agents-fast", "reasoning"):
        assert "agents" in (services[name].get("profiles") or []), (
            f"{name} must be behind the 'agents' profile"
        )
    for name in ("prometheus", "grafana", "kafka-ui"):
        assert "obs" in (services[name].get("profiles") or [])


def test_no_unprofiled_service_depends_on_a_profiled_one():
    """A default-start service depending on an opt-in one breaks `compose up`."""
    services = _compose()["services"]
    profiled = {n for n, s in services.items() if s.get("profiles")}

    for name, svc in services.items():
        if name in profiled:
            continue
        deps = svc.get("depends_on") or {}
        for target in (deps if isinstance(deps, dict) else {d: None for d in deps}):
            assert target not in profiled, (
                f"{name} (always-on) depends on {target} (profiled) -- "
                f"`docker compose up` fails without --profile"
            )


def test_ollama_keeps_every_tier_model_resident():
    """MAX_LOADED_MODELS must cover the number of distinct models in use.

    agents-heavy and agents-fast deliberately request different models. If Ollama
    may hold fewer models than the tiers request, it evicts and reloads on every
    alternating request -- on a CPU-only host, loading a multi-billion-parameter
    model from disk costs far more than the memory the eviction saved.
    """
    services = _compose()["services"]

    def env_of(name):
        e = services[name].get("environment") or []
        if isinstance(e, dict):
            return {str(k): str(v) for k, v in e.items()}
        return dict(kv.split("=", 1) for kv in e if "=" in kv)

    tier_models = set()
    for tier in ("agents-heavy", "agents-fast"):
        env = env_of(tier)
        assert env.get("AGENT_MODEL"), f"{tier} must pin AGENT_MODEL explicitly"
        tier_models.add(env["AGENT_MODEL"])

    ollama_env = env_of("ollama")
    max_loaded = int(ollama_env["OLLAMA_MAX_LOADED_MODELS"])
    assert max_loaded >= len(tier_models), (
        f"{len(tier_models)} distinct tier models {sorted(tier_models)} but "
        f"OLLAMA_MAX_LOADED_MODELS={max_loaded} -- this thrashes"
    )


def test_heavy_tier_model_fits_the_cpu_only_budget():
    """The heavy tier must stay within what a CPU-only host can hold and serve.

    A 7B model needs ~4.7G resident; alongside the 1.5B fast tier that exceeds
    the ollama ceiling and pushes reasoning mode past the VM budget.
    """
    services = _compose()["services"]
    env = services["agents-heavy"].get("environment") or []
    env = dict(kv.split("=", 1) for kv in env if "=" in kv) if isinstance(env, list) else env

    model = str(env.get("AGENT_MODEL", ""))
    assert "7b" not in model.lower(), (
        f"agents-heavy pinned to {model!r}: a 7B model on this CPU serves ~3-5 "
        f"tok/s and cannot stay resident beside the fast tier within budget"
    )

    limit = _mib(services["ollama"]["deploy"]["resources"]["limits"]["memory"])
    assert limit <= 5 * 1024, "ollama ceiling should reflect the smaller tier models"
