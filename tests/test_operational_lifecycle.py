"""
tests/test_operational_lifecycle.py

Guards the operational surface: a fresh clone must be able to reach a running
system, and the durability tooling must cover every store that holds state.

These assert on repository artifacts rather than a live stack, so they run in
CI without Docker.
"""

import os
import re
import stat
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


# ── ENVIRONMENT TEMPLATE ─────────────────────────────────────────────────────

def test_env_example_exists():
    """Without a template, a fresh clone cannot start a fail-closed stack."""
    assert (REPO / ".env.example").exists(), (
        "no .env.example: the stack requires ~10 vars and refuses to boot without them"
    )


def test_env_example_covers_every_hard_required_var():
    """Every ${VAR:?} in compose aborts startup; all must be documented."""
    compose = _read("docker-compose.yml")
    required = set(re.findall(r"\$\{([A-Z0-9_]+):\?", compose))
    documented = set(re.findall(r"^([A-Z0-9_]+)=", _read(".env.example"), re.M))

    missing = required - documented
    assert not missing, f".env.example omits hard-required vars: {sorted(missing)}"


def test_env_example_covers_code_level_required_secrets():
    """Secrets resolved with required=True raise at import if unset."""
    documented = set(re.findall(r"^([A-Z0-9_]+)=", _read(".env.example"), re.M))
    for var in ("API_GATEWAY_KEY", "SESSION_SECRET", "ADMIN_PASSWORD"):
        assert var in documented, f".env.example must document {var}"


def test_env_example_ships_no_real_secrets():
    """The template must carry empty slots, never working credentials."""
    for line in _read(".env.example").splitlines():
        if not re.match(r"^[A-Z0-9_]+=", line):
            continue
        key, _, value = line.partition("=")
        if any(t in key for t in ("PASSWORD", "SECRET", "KEY")):
            assert not value.strip() or "CHANGEME" in value, (
                f"{key} ships a value in .env.example; secrets must be blank"
            )


def test_env_example_is_not_committed_as_dotenv():
    """.env itself must never be tracked."""
    gitignore = _read(".gitignore") if (REPO / ".gitignore").exists() else ""
    assert re.search(r"^\.env$", gitignore, re.M) or ".env" in gitignore, (
        ".env must be gitignored"
    )


# ── OPERATIONAL SCRIPTS ──────────────────────────────────────────────────────

REQUIRED_SCRIPTS = ("preflight.sh", "health.sh", "backup.sh", "restore.sh")


@pytest.mark.parametrize("name", REQUIRED_SCRIPTS)
def test_operational_script_exists_and_is_executable(name):
    path = REPO / "scripts" / name
    assert path.exists(), f"scripts/{name} missing"
    if os.name != "nt":  # Windows does not carry the exec bit
        assert path.stat().st_mode & stat.S_IXUSR, f"scripts/{name} not executable"


@pytest.mark.parametrize("name", REQUIRED_SCRIPTS)
def test_operational_scripts_fail_fast(name):
    """`set -e`/`set -u` so a failed step does not silently continue."""
    head = _read(f"scripts/{name}").split("\n", 12)[:12]
    assert any(l.startswith("set -") for l in head), f"scripts/{name} sets no shell flags"


def test_backup_covers_every_stateful_store():
    """A store with a volume but no backup path is unrecoverable data."""
    compose = yaml.safe_load(_read("docker-compose.yml"))
    stateful = {
        name for name, svc in compose["services"].items()
        if any(":" in str(v) and not str(v).startswith("./") for v in (svc.get("volumes") or []))
    }
    backup = _read("scripts/backup.sh")

    # Qdrant is a derived index (re-embeddable); the rest hold primary state.
    for store in ("timescaledb", "neo4j", "redis"):
        if store in stateful:
            assert store in backup, f"backup.sh does not cover {store}"


def test_backup_isolates_the_audit_ledger():
    """The compliance record gets its own restorable copy."""
    backup = _read("scripts/backup.sh")
    assert "audit_ledger" in backup, "audit ledger has no dedicated backup path"


def test_restore_verifies_integrity_before_overwriting():
    """Restoring a corrupt dump over good data is worse than not restoring."""
    restore = _read("scripts/restore.sh")
    assert "sha256sum -c" in restore, "restore.sh does not verify checksums"
    assert "refusing to restore" in restore.lower(), "restore.sh does not abort on mismatch"


def test_restore_requires_explicit_confirmation():
    restore = _read("scripts/restore.sh")
    assert "read -r" in restore, "restore.sh proceeds without confirmation"


# ── PREFLIGHT COVERAGE ───────────────────────────────────────────────────────

def test_preflight_checks_every_required_secret():
    preflight = _read("scripts/preflight.sh")
    for var in ("API_GATEWAY_KEY", "SESSION_SECRET", "POSTGRES_PASSWORD",
                "NEO4J_PASSWORD", "REDIS_PASSWORD", "GRAFANA_ADMIN_PASSWORD"):
        assert var in preflight, f"preflight.sh does not validate {var}"


def test_preflight_blocks_accidental_live_trading():
    """A live broker setting must be a deliberate, loud choice."""
    preflight = _read("scripts/preflight.sh")
    assert "alpaca_live" in preflight, "preflight does not detect live trading config"
    assert "Real money" in preflight or "REAL MONEY" in preflight.upper()


def test_preflight_parses_variable_names_containing_digits():
    """NEO4J_USER contains a digit; a [A-Z_]+ pattern silently skips it."""
    preflight = _read("scripts/preflight.sh")
    loader = re.search(r"grep -E '(\^\[[^']+\]\+=)'", preflight)
    assert loader, "preflight has no env loader pattern"
    assert "0-9" in loader.group(1), (
        "env loader pattern excludes digits; NEO4J_* vars would be skipped"
    )


# ── MAKEFILE ─────────────────────────────────────────────────────────────────

def test_makefile_exposes_the_core_lifecycle():
    mk = _read("Makefile")
    for target in ("setup", "preflight", "analyst", "reasoning",
                   "health", "backup", "restore", "verify"):
        assert re.search(rf"^{target}:", mk, re.M), f"Makefile lacks '{target}' target"


def test_destructive_target_requires_confirmation():
    """`make nuke` destroys every volume; it must not run bare."""
    mk = _read("Makefile")
    assert "CONFIRM" in mk, "nuke target has no confirmation guard"


def test_startup_targets_run_preflight_first():
    """Starting without validation reproduces the failure preflight prevents."""
    mk = _read("Makefile")
    assert re.search(r"^analyst: preflight", mk, re.M), "analyst does not gate on preflight"
    assert re.search(r"^reasoning: preflight", mk, re.M), "reasoning does not gate on preflight"


# ── CI PIPELINE ──────────────────────────────────────────────────────────────

def test_ci_workflow_exists():
    assert (REPO / ".github" / "workflows" / "verify.yml").exists(), (
        "no CI pipeline: nothing runs the verification gates on push"
    )


def test_ci_runs_the_same_gates_as_make_verify():
    """A pipeline checking something different produces unreproducible failures."""
    wf = yaml.safe_load(_read(".github/workflows/verify.yml"))
    body = _read(".github/workflows/verify.yml")

    assert set(wf["jobs"]) >= {"backend", "frontend", "compose"}
    assert "pytest tests/" in body
    assert "tsc --noEmit" in body
    assert "vitest run" in body
    assert "docker compose" in body and "config --quiet" in body


def test_ci_supplies_fail_closed_secrets():
    """The stack refuses to import without them; CI must provide throwaways."""
    body = _read(".github/workflows/verify.yml")
    for var in ("SENTINEL_ENV", "SESSION_SECRET", "API_GATEWAY_KEY"):
        assert var in body, f"CI does not set {var}; imports will fail"
    assert "SENTINEL_ENV: test" in body


def test_ci_builds_the_frontend():
    """next.config.ts sets no ignore flags, so a type error fails the image build."""
    assert "npm run build" in _read(".github/workflows/verify.yml")


def test_ci_security_scans_are_present_and_non_blocking():
    """A gate that fails on day one gets disabled on day two."""
    wf = yaml.safe_load(_read(".github/workflows/verify.yml"))
    sec = wf["jobs"]["security"]
    assert sec.get("continue-on-error") is True
    body = _read(".github/workflows/verify.yml")
    for tool in ("pip-audit", "bandit", "gitleaks"):
        assert tool in body, f"CI lacks a {tool} scan"


# ── FIRST-RUN BOOTSTRAP ──────────────────────────────────────────────────────

def test_bootstrap_script_exists():
    assert (REPO / "scripts" / "bootstrap.py").exists()


def test_bootstrap_fetches_real_data_and_invents_none():
    """The seed it replaces fabricated events; this must not."""
    src = _read("scripts/bootstrap.py")
    assert "guarded_get" in src, "bootstrap bypasses the circuit-breaker client"
    assert "finance.yahoo.com" in src, "bootstrap does not fetch from a real upstream"

    import re as _re
    # No random generation, and no invented price constants.
    assert not _re.search(r"\brandom\b|np\.random|synthetic", src), (
        "bootstrap generates data instead of fetching it"
    )
    # A gap bar is skipped, never forward-filled.
    assert "if None in (o, h, l, c)" in src


def test_bootstrap_warms_the_cache_the_agents_actually_read():
    """A database-only backfill leaves the quant engine as cold as before."""
    src = _read("scripts/bootstrap.py")
    assert "candle_cache_key" in src
    assert "warm_candle_cache" in src


def test_bootstrap_is_idempotent():
    """Re-running must extend coverage, not duplicate it."""
    assert "ON CONFLICT (ticker, time) DO UPDATE" in _read("scripts/bootstrap.py")


def test_fabricating_seed_is_disclosed():
    """It writes invented events into the same table as real ingestion."""
    src = _read("scripts/seed_live_pipeline.py")
    assert "DEMONSTRATION FIXTURE" in src
    assert "FABRICATED" in src.upper()
    # And it points at the honest alternative.
    assert "bootstrap.py" in src


def test_makefile_exposes_bootstrap():
    assert re.search(r"^bootstrap:", _read("Makefile"), re.M)


# ── LOG REDACTION ────────────────────────────────────────────────────────────

def test_redaction_filter_is_installed_centrally():
    """Auditing every call site decays; a handler filter covers new ones too."""
    src = _read("shared/utils/logging.py")
    assert "RedactingFilter" in src
    assert "addFilter" in src


def test_redaction_reuses_the_shared_sensitivity_definition():
    """A parallel list drifted immediately -- it missed API_GATEWAY_KEY."""
    src = _read("shared/utils/log_redaction.py")
    assert "from shared.utils.secrets import is_sensitive_key" in src


def test_redaction_covers_message_args_and_exceptions():
    """The realistic leak is a DSN inside an exception, not a logged key."""
    src = _read("shared/utils/log_redaction.py")
    assert "record.args" in src, "args bypass a message-only redaction"
    assert "record.exc_info" in src, "exception text is where DSNs leak"


def test_redaction_never_suppresses_a_log_record():
    """Losing the line is worse than an unredacted one."""
    src = _read("shared/utils/log_redaction.py")
    filter_body = src[src.index("def filter("):]
    assert "return True" in filter_body


# ── INTEGRATIONS SURFACE ─────────────────────────────────────────────────────

def test_integrations_endpoint_is_registered():
    import os
    os.environ.setdefault("SENTINEL_ENV", "test")
    os.environ.setdefault("SESSION_SECRET", "t" * 16)
    os.environ.setdefault("API_GATEWAY_KEY", "t" * 16)
    from services.api_gateway.routes.main import app

    paths = {r.path for r in app.routes if hasattr(r, "path")}
    assert "/api/v1/integrations" in paths
    assert "/api/v1/integrations/execution" in paths


def test_integrations_never_echo_secret_material():
    """Status only: no value, no prefix, no length that narrows a guess."""
    src = _read("services/api_gateway/routes/integrations.py")
    # Presence is computed from emptiness, and the value never enters the payload.
    assert "os.getenv(v)" in src
    for leak in ('"value"', "[:4]", "len(os.getenv", "mask_secret"):
        assert leak not in src, f"integrations endpoint may expose secret material via {leak}"


def test_integrations_do_not_accept_credentials():
    """Storing user brokerage keys would make the app their custodian."""
    src = _read("services/api_gateway/routes/integrations.py")
    assert "@router.post" not in src, "integrations exposes a credential-writing route"
    assert "@router.put" not in src


def test_execution_endpoint_states_the_venue_unambiguously():
    """The gap between simulator and real capital is one env var."""
    src = _read("services/api_gateway/routes/integrations.py")
    assert "is_live_capital" in src
    assert "alpaca_live" in src
    assert "real capital" in src.lower()


def test_frontend_integrations_panel_collects_no_keys():
    """The browser must never see or send a credential."""
    tsx = _read("frontend/src/components/IntegrationsPanel.tsx")
    assert "<input" not in tsx, "integrations panel contains an input field"
    assert "type=\"password\"" not in tsx
    # It names the variables to set, without ever handling their values.
    assert "missing_env_vars" in tsx
