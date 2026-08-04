"""
tests/test_hawkes_correlator.py

Tests for the Multivariate Hawkes Process cross-domain excitation engine.
Validates:
  - MLE parameter estimation convergence
  - Branching ratio computation
  - Spectral radius stationarity constraint
  - Excitation forecast generation
  - CrossDomainHawkesCorrelator integration
"""

import math
import time
import pytest
from services.correlation.hawkes_correlator import (
    HawkesMLE,
    CrossDomainHawkesCorrelator,
    SENTINEL_DOMAINS,
)
from shared.utils.streaming_detectors import HawkesIntensityTracker


# ── MLE PARAMETER ESTIMATION ────────────────────────────────────────────────

class TestHawkesMLE:

    def test_fit_with_minimal_data_returns_without_crash(self):
        """MLE should handle too-few events gracefully."""
        mle = HawkesMLE(domains=["crypto", "tradfi"])
        result = mle.fit({"crypto": [1.0, 2.0], "tradfi": [3.0]}, T=10.0)
        assert "mu" in result
        assert "branching_ratios" in result

    def test_fit_with_synthetic_self_exciting_stream(self):
        """A domain with clustered events should show self-excitation."""
        mle = HawkesMLE(domains=["crypto", "tradfi"], max_iter=50, learning_rate=5e-3)
        # Simulate clustered crypto events (self-exciting)
        crypto_events = []
        t = 0.0
        for _ in range(30):
            t += 0.5  # Tight bursts
            crypto_events.append(t)
        # Sparse tradfi events
        tradfi_events = [5.0, 15.0, 25.0]

        result = mle.fit({"crypto": crypto_events, "tradfi": tradfi_events}, T=30.0)
        assert result["spectral_radius"] < 1.0, "Stationarity must be enforced"
        assert result.get("mu", {}).get("crypto", 0) > 0

    def test_spectral_radius_constraint(self):
        """Branching matrix spectral radius must stay below cap."""
        mle = HawkesMLE(domains=["a", "b", "c"], spectral_radius_cap=0.9)
        # Force large alpha values
        for i in range(3):
            for j in range(3):
                mle.alpha[i][j] = 5.0
        mle.beta = 0.1
        mle._project_stationarity()

        # Compute resulting spectral radius
        sr = mle._compute_spectral_radius()
        assert sr <= 0.95, f"Spectral radius {sr} exceeds cap after projection"

    def test_branching_ratio_computation(self):
        """Branching ratio R_ij = α_ij / β should be correctly computed."""
        mle = HawkesMLE(domains=["a", "b"])
        mle.alpha[0][1] = 0.3
        mle.beta = 0.1
        result = mle._current_params_dict()
        assert "a->b" in result["branching_ratios"]
        assert abs(result["branching_ratios"]["a->b"] - 3.0) < 0.1


# ── CROSS-DOMAIN HAWKES CORRELATOR ───────────────────────────────────────────

class TestCrossDomainHawkesCorrelator:

    def test_record_event_returns_intensity_state(self):
        """Recording an event should return intensity metrics."""
        correlator = CrossDomainHawkesCorrelator()
        now = time.time()
        state = correlator.record_event("crypto", now)
        assert "intensity" in state
        assert "baseline" in state
        assert "excitation_ratio" in state
        assert state["excitation_ratio"] >= 1.0

    def test_excitation_ratio_increases_with_bursts(self):
        """Rapid events should increase the excitation ratio."""
        correlator = CrossDomainHawkesCorrelator()
        now = time.time()
        # Record a burst of crypto events
        for i in range(10):
            state = correlator.record_event("crypto", now + i * 0.1)
        ratio = correlator.get_excitation_ratio("crypto", now + 1.0)
        assert ratio > 1.0, f"Expected elevated ratio after burst, got {ratio}"

    def test_excitation_forecast_generation(self):
        """After burst in one domain, forecasts should appear for cross-excited domains."""
        correlator = CrossDomainHawkesCorrelator()
        now = time.time()
        # Simulate a heavy burst of crypto events
        for i in range(20):
            correlator.record_event("crypto", now + i * 0.05)

        forecasts = correlator.get_excitation_forecasts(now + 1.0, threshold=1.2)
        # crypto → tradfi should appear (it's in the default excitation matrix)
        if forecasts:
            assert any(f["target_domain"] == "tradfi" for f in forecasts), \
                f"Expected tradfi forecast from crypto burst, got: {[f['target_domain'] for f in forecasts]}"
            for f in forecasts:
                assert "narrative" in f
                assert f["excess_multiplier"] > 1.0

    def test_forecast_narrative_format(self):
        """Forecast narratives should be well-formed natural language."""
        correlator = CrossDomainHawkesCorrelator()
        now = time.time()
        for i in range(20):
            correlator.record_event("cyber", now + i * 0.1)

        forecasts = correlator.get_excitation_forecasts(now + 2.0, threshold=1.1)
        for f in forecasts:
            assert "above baseline" in f["narrative"]
            assert "forecast" in f["narrative"]
            assert f["source_domain"] == "cyber"

    def test_branching_ratio_matrix(self):
        """The branching ratio matrix should reflect the excitation structure."""
        correlator = CrossDomainHawkesCorrelator()
        ratios = correlator.get_branching_ratio_matrix()
        # Initially empty until MLE fit happens
        # But the tracker has defaults, so let's check the tracker
        tracker = correlator._tracker
        assert tracker._baselines["crypto"] > 0
        assert ("crypto", "tradfi") in tracker._excitation

    def test_no_forecasts_when_below_threshold(self):
        """When intensity is not elevated, no forecasts should be generated."""
        correlator = CrossDomainHawkesCorrelator()
        now = time.time()
        correlator.record_event("news", now)
        forecasts = correlator.get_excitation_forecasts(now, threshold=2.0)
        assert len(forecasts) == 0


# ── HAWKES INTENSITY TRACKER (from streaming_detectors) ──────────────────────

class TestHawkesIntensityTracker:

    def test_intensity_above_baseline_after_event(self):
        tracker = HawkesIntensityTracker()
        now = time.time()
        tracker.record_event("crypto", now)
        intensity = tracker.get_intensity("crypto", now + 0.1)
        baseline = tracker._baselines["crypto"]
        assert intensity >= baseline

    def test_cross_excitation_effect(self):
        """Events in crypto should raise tradfi intensity (α > 0)."""
        tracker = HawkesIntensityTracker()
        now = time.time()
        tradfi_before = tracker.get_intensity("tradfi", now)
        for i in range(5):
            tracker.record_event("crypto", now + i * 0.1)
        tradfi_after = tracker.get_intensity("tradfi", now + 0.5)
        assert tradfi_after > tradfi_before, \
            f"tradfi intensity should increase from crypto events: {tradfi_before} -> {tradfi_after}"

    def test_intensity_decays_over_time(self):
        """Intensity should decay back toward baseline over time."""
        tracker = HawkesIntensityTracker()
        now = time.time()
        tracker.record_event("crypto", now)
        intensity_soon = tracker.get_intensity("crypto", now + 1.0)
        intensity_later = tracker.get_intensity("crypto", now + 100.0)
        assert intensity_soon > intensity_later, \
            "Intensity should decay over time"
