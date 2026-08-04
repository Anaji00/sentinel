"""
tests/test_env_guard.py

Unit tests for the fail-closed credential resolution utility.
"""
import os
import pytest
from unittest.mock import patch


def _import_module():
    """Lazy import to avoid module-level side effects from shared.utils."""
    from shared.utils.env_guard import resolve_env_var, SAFE_DEV_ENVS
    return resolve_env_var, SAFE_DEV_ENVS


class TestResolveEnvVar:
    """Tests for resolve_env_var()."""

    # ── Happy path: env var is set ──────────────────────────────────────

    @patch.dict(os.environ, {"MY_SECRET": "real-value", "SENTINEL_ENV": "production"})
    def test_returns_explicit_env_var_regardless_of_sentinel_env(self):
        resolve_env_var, _ = _import_module()
        assert resolve_env_var("MY_SECRET", "fallback") == "real-value"

    @patch.dict(os.environ, {"MY_SECRET": "real-value"}, clear=False)
    def test_returns_explicit_env_var_when_sentinel_env_unset(self):
        resolve_env_var, _ = _import_module()
        env = os.environ.copy()
        env.pop("SENTINEL_ENV", None)
        with patch.dict(os.environ, env, clear=True):
            # MY_SECRET is set, so it should return even without SENTINEL_ENV
            os.environ["MY_SECRET"] = "real-value"
            assert resolve_env_var("MY_SECRET", "fallback") == "real-value"

    # ── Dev fallback: SENTINEL_ENV in safe whitelist ────────────────────

    @patch.dict(os.environ, {"SENTINEL_ENV": "dev"}, clear=False)
    def test_returns_fallback_when_sentinel_env_is_dev(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        assert resolve_env_var("MY_SECRET", "dev-fallback") == "dev-fallback"

    @patch.dict(os.environ, {"SENTINEL_ENV": "development"}, clear=False)
    def test_returns_fallback_when_sentinel_env_is_development(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        assert resolve_env_var("MY_SECRET", "dev-fallback") == "dev-fallback"

    @patch.dict(os.environ, {"SENTINEL_ENV": "local"}, clear=False)
    def test_returns_fallback_when_sentinel_env_is_local(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        assert resolve_env_var("MY_SECRET", "dev-fallback") == "dev-fallback"

    @patch.dict(os.environ, {"SENTINEL_ENV": "test"}, clear=False)
    def test_returns_fallback_when_sentinel_env_is_test(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        assert resolve_env_var("MY_SECRET", "dev-fallback") == "dev-fallback"

    @patch.dict(os.environ, {"SENTINEL_ENV": "testing"}, clear=False)
    def test_returns_fallback_when_sentinel_env_is_testing(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        assert resolve_env_var("MY_SECRET", "dev-fallback") == "dev-fallback"

    @patch.dict(os.environ, {"SENTINEL_ENV": "TEST"}, clear=False)
    def test_case_insensitive_sentinel_env(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        assert resolve_env_var("MY_SECRET", "dev-fallback") == "dev-fallback"

    # ── Fail-closed: unknown / missing / misspelled SENTINEL_ENV ───────

    @patch.dict(os.environ, {"SENTINEL_ENV": "prod"}, clear=False)
    def test_raises_when_sentinel_env_is_prod(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
            resolve_env_var("MY_SECRET", "dev-fallback")

    @patch.dict(os.environ, {"SENTINEL_ENV": "production"}, clear=False)
    def test_raises_when_sentinel_env_is_production(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
            resolve_env_var("MY_SECRET", "dev-fallback")

    @patch.dict(os.environ, {"SENTINEL_ENV": "staging"}, clear=False)
    def test_raises_when_sentinel_env_is_staging(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
            resolve_env_var("MY_SECRET", "dev-fallback")

    @patch.dict(os.environ, {"SENTINEL_ENV": "prdo"}, clear=False)
    def test_raises_when_sentinel_env_is_misspelled(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
            resolve_env_var("MY_SECRET", "dev-fallback")

    @patch.dict(os.environ, {"SENTINEL_ENV": ""}, clear=False)
    def test_raises_when_sentinel_env_is_empty(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
            resolve_env_var("MY_SECRET", "dev-fallback")

    def test_raises_when_sentinel_env_is_unset(self):
        resolve_env_var, _ = _import_module()
        env = os.environ.copy()
        env.pop("SENTINEL_ENV", None)
        env.pop("MY_SECRET", None)
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
                resolve_env_var("MY_SECRET", "dev-fallback")

    @patch.dict(os.environ, {"SENTINEL_ENV": "prod_us_east"}, clear=False)
    def test_raises_for_unknown_env_variant(self):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
            resolve_env_var("MY_SECRET", "dev-fallback")

    # ── warn_on_fallback flag ──────────────────────────────────────────

    @patch.dict(os.environ, {"SENTINEL_ENV": "dev"}, clear=False)
    def test_warn_on_fallback_logs_warning(self, caplog):
        resolve_env_var, _ = _import_module()
        os.environ.pop("MY_SECRET", None)
        import logging
        with caplog.at_level(logging.WARNING, logger="shared.utils.env_guard"):
            result = resolve_env_var("MY_SECRET", "dev-fallback", warn_on_fallback=True)
        assert result == "dev-fallback"
        assert "SECURITY WARNING" in caplog.text

    # ── SAFE_DEV_ENVS completeness ─────────────────────────────────────

    def test_safe_dev_envs_contains_expected_values(self):
        _, SAFE_DEV_ENVS = _import_module()
        assert SAFE_DEV_ENVS == {"dev", "development", "local", "test", "testing"}
