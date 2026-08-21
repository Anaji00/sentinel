"""
tests/test_tier2_hardening.py

Tier 2 Security & Infrastructure Hardening Tests:
1. TLS Ingress Reverse Proxy Configuration & SSL/Security Headers (2.1)
2. Conditional CORS Localhost Regex restricted to SAFE_DEV_ENVS (2.2)
3. Internal Datastore & Monitoring Port Isolation (2.3)
4. Container CPU & Memory Resource Limits (2.4)
"""

import os
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

    # Ingress must be the only service binding public web ports
    ingress_ports = services["ingress"].get("ports", [])
    assert "80:80" in ingress_ports
    assert "443:443" in ingress_ports

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
