"""
services/correlation/hawkes_correlator.py

MULTIVARIATE HAWKES PROCESS — Cross-Domain Excitation Engine
=============================================================
Extends the HawkesIntensityTracker (shared/utils/streaming_detectors.py) with:

1. MLE-based parameter estimation from historical event timestamps
2. Branching ratio matrix computation (R_ij = α_ij / β)
3. Human-readable excitation forecast generation
4. Periodic batch refitting against TimescaleDB/Redis history

The HawkesIntensityTracker in streaming_detectors.py handles real-time
intensity tracking with fixed priors. This module adds the statistical
backbone: fitting those priors from data, computing formal branching ratios,
and generating the "news before the news" excitation forecasts.

Theory:
    λ_j(t) = μ_j + Σ_i Σ_{t_k < t} α_ij * exp(-β * (t - t_k))

    Branching ratio R_ij = α_ij / β
    When R_ij > 0 for pair (i, j), anomalies in domain i measurably
    raise near-term event intensity in domain j.

    Spectral radius ρ(R) < 1 guarantees stationarity (process won't
    explode). We enforce this during fitting.
"""

import json
import logging
import math
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("correlation.hawkes")

# All Sentinel domains
SENTINEL_DOMAINS = [
    "maritime", "aviation", "tradfi", "crypto",
    "macro", "cyber", "news", "prediction",
]


class HawkesMLE:
    """
    Maximum Likelihood Estimator for a multivariate exponential Hawkes process.

    Given historical event timestamps grouped by domain, estimates:
        μ_j     — baseline intensity per domain
        α_ij    — cross-excitation magnitude from domain i → j
        β       — shared exponential decay rate

    Uses projected gradient descent on the log-likelihood with a
    spectral-radius constraint (ρ(branching_matrix) < 1) for stationarity.
    """

    def __init__(
        self,
        domains: Optional[List[str]] = None,
        learning_rate: float = 1e-3,
        max_iter: int = 200,
        convergence_tol: float = 1e-6,
        spectral_radius_cap: float = 0.95,
    ):
        self.domains = domains or SENTINEL_DOMAINS
        self.D = len(self.domains)
        self.domain_idx = {d: i for i, d in enumerate(self.domains)}
        self.lr = learning_rate
        self.max_iter = max_iter
        self.tol = convergence_tol
        self.spectral_radius_cap = spectral_radius_cap

        # Parameters: μ (D,), α (D x D), β (scalar)
        self.mu: List[float] = [0.01] * self.D
        self.alpha: List[List[float]] = [[0.01] * self.D for _ in range(self.D)]
        self.beta: float = 0.1

    def fit(self, event_streams: Dict[str, List[float]], T: Optional[float] = None) -> Dict[str, Any]:
        """
        Fit Hawkes parameters via MLE with gradient ascent.

        Args:
            event_streams: {domain_name: [sorted unix timestamps]}
            T: observation window end time (default: max timestamp across all streams)

        Returns:
            {
                "mu": {domain: baseline_intensity},
                "alpha": {(src, tgt): excitation_coeff},
                "beta": decay_rate,
                "branching_ratios": {(src, tgt): ratio},
                "spectral_radius": float,
                "log_likelihood": float,
                "iterations": int,
            }
        """
        # Validate and index the streams
        streams: List[List[float]] = [[] for _ in range(self.D)]
        for domain, timestamps in event_streams.items():
            if domain in self.domain_idx:
                streams[self.domain_idx[domain]] = sorted(timestamps)

        if T is None:
            all_ts = [t for s in streams for t in s]
            T = max(all_ts) if all_ts else time.time()

        total_events = sum(len(s) for s in streams)
        if total_events < 10:
            logger.warning(f"Hawkes MLE: only {total_events} events — too few for reliable estimation")
            return self._current_params_dict()

        # Initialize parameters from empirical rates
        for j in range(self.D):
            n_j = len(streams[j])
            self.mu[j] = max(1e-6, n_j / max(1.0, T - (min(streams[j]) if streams[j] else T)))

        prev_ll = -float('inf')

        for iteration in range(self.max_iter):
            # E-step: compute recursive kernel sums R_ij(k) for each event
            # and log-likelihood
            ll = 0.0
            grad_mu = [0.0] * self.D
            grad_alpha = [[0.0] * self.D for _ in range(self.D)]
            grad_beta = 0.0

            import bisect
            cutoff_window = 5.0 / max(1e-6, self.beta)

            for j in range(self.D):
                stream_j = streams[j]
                n_j = len(stream_j)
                if n_j == 0:
                    continue

                # Integral term: -μ_j * T
                ll -= self.mu[j] * T
                grad_mu[j] -= T

                for k_idx, t_k in enumerate(stream_j):
                    # Compute λ_j(t_k) for log-likelihood
                    lam = self.mu[j]
                    
                    # Pre-calculate active historical event slices per domain using binary search
                    active_slices = {}
                    for i in range(self.D):
                        stream_i = streams[i]
                        if not stream_i:
                            continue
                        idx_end = bisect.bisect_left(stream_i, t_k)
                        idx_start = bisect.bisect_left(stream_i, t_k - cutoff_window)
                        if idx_start < idx_end:
                            active_slices[i] = stream_i[idx_start:idx_end]

                    for i, prev_ts_list in active_slices.items():
                        alpha_ij = self.alpha[i][j]
                        if alpha_ij <= 0:
                            continue
                        for t_prev in prev_ts_list:
                            dt = t_k - t_prev
                            kernel = math.exp(-self.beta * dt)
                            lam += alpha_ij * kernel

                    if lam > 1e-10:
                        ll += math.log(lam)
                        inv_lam = 1.0 / lam

                        # Gradient of log(λ_j(t_k)) w.r.t. μ_j
                        grad_mu[j] += inv_lam

                        # Gradient w.r.t. α_ij and β
                        for i, prev_ts_list in active_slices.items():
                            alpha_ij = self.alpha[i][j]
                            for t_prev in prev_ts_list:
                                dt = t_k - t_prev
                                kernel = math.exp(-self.beta * dt)
                                grad_alpha[i][j] += inv_lam * kernel
                                if alpha_ij > 0:
                                    grad_beta -= inv_lam * alpha_ij * dt * kernel

                # Integral of excitation terms: -Σ_i α_ij/β * Σ_k (1 - exp(-β(T - t_k^i)))
                for i in range(self.D):
                    alpha_ij = self.alpha[i][j]
                    if alpha_ij <= 0:
                        continue
                    integral_sum = 0.0
                    for t_prev in streams[i]:
                        integral_sum += (1.0 - math.exp(-self.beta * (T - t_prev)))
                    ll -= alpha_ij / max(1e-10, self.beta) * integral_sum

            # Gradient step
            for j in range(self.D):
                self.mu[j] = max(1e-8, self.mu[j] + self.lr * grad_mu[j])
            for i in range(self.D):
                for j in range(self.D):
                    self.alpha[i][j] = max(0.0, self.alpha[i][j] + self.lr * grad_alpha[i][j])
            self.beta = max(1e-4, self.beta + self.lr * grad_beta)

            # Project: enforce spectral radius < cap
            self._project_stationarity()

            # Check convergence
            if abs(ll - prev_ll) < self.tol:
                logger.info(f"Hawkes MLE converged at iteration {iteration + 1}, LL={ll:.4f}")
                break
            prev_ll = ll

        result = self._current_params_dict()
        result["log_likelihood"] = round(ll, 4)
        result["iterations"] = iteration + 1
        return result

    def _project_stationarity(self):
        """Project α matrix so spectral radius of branching matrix < cap."""
        # Branching matrix R_ij = α_ij / β
        if self.beta < 1e-10:
            return
        # Simple Frobenius norm bound as spectral radius proxy
        # (for small D, this is conservative but cheap)
        frob_sq = sum(
            (self.alpha[i][j] / self.beta) ** 2
            for i in range(self.D)
            for j in range(self.D)
        )
        frob = math.sqrt(frob_sq)
        if frob > self.spectral_radius_cap:
            scale = self.spectral_radius_cap / frob
            for i in range(self.D):
                for j in range(self.D):
                    self.alpha[i][j] *= scale

    def _current_params_dict(self) -> Dict[str, Any]:
        """Package current parameters into a result dict."""
        mu_dict = {self.domains[j]: round(self.mu[j], 6) for j in range(self.D)}
        alpha_dict = {}
        branching = {}
        for i in range(self.D):
            for j in range(self.D):
                if self.alpha[i][j] > 1e-8:
                    key = f"{self.domains[i]}->{self.domains[j]}"
                    alpha_dict[key] = round(self.alpha[i][j], 6)
                    branching[key] = round(self.alpha[i][j] / max(1e-10, self.beta), 4)

        spectral = self._compute_spectral_radius()

        return {
            "mu": mu_dict,
            "alpha": alpha_dict,
            "beta": round(self.beta, 6),
            "branching_ratios": branching,
            "spectral_radius": round(spectral, 4),
        }

    def _compute_spectral_radius(self) -> float:
        """Compute spectral radius of the branching matrix via power iteration."""
        if self.beta < 1e-10:
            return 0.0
        # Build branching matrix
        R = [[self.alpha[i][j] / self.beta for j in range(self.D)] for i in range(self.D)]
        # Power iteration for dominant eigenvalue
        v = [1.0 / self.D] * self.D
        for _ in range(100):
            w = [sum(R[i][j] * v[j] for j in range(self.D)) for i in range(self.D)]
            norm = math.sqrt(sum(x * x for x in w))
            if norm < 1e-15:
                return 0.0
            v = [x / norm for x in w]
        # Rayleigh quotient
        Rv = [sum(R[i][j] * v[j] for j in range(self.D)) for i in range(self.D)]
        eigenvalue = sum(Rv[i] * v[i] for i in range(self.D))
        return abs(eigenvalue)


class CrossDomainHawkesCorrelator:
    """
    Production Hawkes correlation layer for the Sentinel correlation engine.

    Wraps:
    - Real-time intensity tracking (via HawkesIntensityTracker from streaming_detectors)
    - Periodic MLE refitting from historical event logs
    - Excitation forecast generation ("news before the news")
    - Branching ratio surface for all domain pairs

    Usage in correlation/main.py:
        hawkes = CrossDomainHawkesCorrelator(redis_client)
        await hawkes.initialize()
        ...
        # On each event:
        state = hawkes.record_event(domain, timestamp)
        forecasts = hawkes.get_excitation_forecasts(timestamp)
    """

    # Refit interval: 6 hours (in seconds)
    REFIT_INTERVAL = 6 * 3600
    # History window for fitting: 7 days
    FIT_WINDOW_HOURS = 168
    # Minimum events required to trigger MLE refit
    MIN_EVENTS_FOR_FIT = 50

    def __init__(self, redis_client=None, db_client=None):
        self._redis = redis_client
        self._db = db_client
        self._last_refit: float = 0.0

        # Import the real-time tracker
        from shared.utils.streaming_detectors import HawkesIntensityTracker
        self._tracker = HawkesIntensityTracker()
        self._mle = HawkesMLE()

        # Cached branching ratios and fit result
        self._fit_result: Optional[Dict[str, Any]] = None
        self._branching_ratios: Dict[str, float] = {}

    async def initialize(self):
        """Load last fitted parameters from Redis if available."""
        if not self._redis:
            return
        try:
            cached = await self._redis.raw.get("sentinel:hawkes:fitted_params")
            if cached:
                params = json.loads(cached)
                self._fit_result = params
                self._branching_ratios = params.get("branching_ratios", {})
                self._apply_params_to_tracker(params)
                logger.info(f"Loaded cached Hawkes parameters (spectral radius: {params.get('spectral_radius', '?')})")
        except Exception as e:
            logger.warning(f"Failed to load cached Hawkes params: {e}")

    def record_event(self, domain: str, timestamp: float) -> Dict[str, Any]:
        """
        Record an event arrival and return intensity state.
        Delegates to the streaming HawkesIntensityTracker.
        """
        return self._tracker.record_event(domain, timestamp)

    def get_excitation_ratio(self, domain: str, timestamp: float) -> float:
        """Get intensity / baseline for a domain. >1.0 = excited state."""
        return self._tracker.get_excitation_ratio(domain, timestamp)

    def get_excitation_forecasts(
        self,
        timestamp: float,
        threshold: float = 1.5,
        forecast_hours: int = 24,
    ) -> List[Dict[str, Any]]:
        """
        Generate excitation forecasts for all domain pairs where current
        intensity significantly exceeds baseline.

        Returns list of forecast dicts:
        [
            {
                "source_domain": "cyber",
                "target_domain": "tradfi",
                "excitation_ratio": 3.2,
                "branching_ratio": 0.45,
                "forecast_hours": 24,
                "narrative": "Given current elevated cyber-domain intensity, ..."
            },
            ...
        ]
        """
        forecasts = []
        now = timestamp

        for source in SENTINEL_DOMAINS:
            source_ratio = self._tracker.get_excitation_ratio(source, now)
            if source_ratio < threshold:
                continue

            for target in SENTINEL_DOMAINS:
                if target == source:
                    continue

                # Check if there's a meaningful cross-excitation path
                br_key = f"{source}->{target}"
                branching = self._branching_ratios.get(br_key, 0.0)

                # Also check current target intensity
                target_ratio = self._tracker.get_excitation_ratio(target, now)

                # Only forecast if branching ratio is non-trivial
                if branching < 0.05:
                    continue

                # Compute expected excess intensity multiplier
                # The expected number of secondary events per primary is the branching ratio
                # With source_ratio elevated, the expected excess is branching * (source_ratio - 1)
                excess_multiplier = 1.0 + branching * (source_ratio - 1.0)

                if excess_multiplier < 1.1:
                    continue

                narrative = (
                    f"Given current elevated {source}-domain intensity "
                    f"({source_ratio:.1f}x baseline), {target}-domain anomaly intensity "
                    f"is forecast {excess_multiplier:.1f}x above baseline "
                    f"over the next {forecast_hours} hours "
                    f"(branching ratio {source}→{target}: {branching:.2f})."
                )

                forecasts.append({
                    "source_domain": source,
                    "target_domain": target,
                    "source_excitation_ratio": round(source_ratio, 3),
                    "target_current_ratio": round(target_ratio, 3),
                    "excess_multiplier": round(excess_multiplier, 3),
                    "branching_ratio": round(branching, 4),
                    "forecast_hours": forecast_hours,
                    "narrative": narrative,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                })

        # Sort by excess multiplier descending
        forecasts.sort(key=lambda f: f["excess_multiplier"], reverse=True)
        return forecasts

    async def maybe_refit(self, force: bool = False) -> Optional[Dict[str, Any]]:
        """
        Periodically refit Hawkes parameters from historical event data.
        Called from the correlation engine's main loop or a scheduled task.

        Returns fit result if refitting occurred, None otherwise.
        """
        now = time.time()
        if not force and (now - self._last_refit) < self.REFIT_INTERVAL:
            return None

        self._last_refit = now

        try:
            event_streams = await self._load_historical_events()
            total = sum(len(v) for v in event_streams.values())
            if total < self.MIN_EVENTS_FOR_FIT:
                logger.info(f"Hawkes refit skipped: only {total} events (need {self.MIN_EVENTS_FOR_FIT})")
                return None

            logger.info(f"Hawkes MLE refit starting with {total} events across {len(event_streams)} domains")
            result = self._mle.fit(event_streams)
            self._fit_result = result
            self._branching_ratios = result.get("branching_ratios", {})

            # Apply fitted params to the real-time tracker
            self._apply_params_to_tracker(result)

            # Cache to Redis
            if self._redis:
                await self._redis.raw.set(
                    "sentinel:hawkes:fitted_params",
                    json.dumps(result),
                    ex=86400,  # 24h TTL
                )
                # Also publish branching ratios for dashboard consumption
                await self._redis.raw.set(
                    "sentinel:hawkes:branching_ratios",
                    json.dumps(self._branching_ratios),
                    ex=86400,
                )

            logger.info(
                f"Hawkes MLE refit complete: spectral_radius={result.get('spectral_radius', '?')}, "
                f"non-zero branching pairs={len(self._branching_ratios)}"
            )
            return result

        except Exception as e:
            logger.error(f"Hawkes MLE refit failed: {e}", exc_info=True)
            return None

    async def _load_historical_events(self) -> Dict[str, List[float]]:
        """
        Load event timestamps from Redis event cache or TimescaleDB.
        Groups by top-level domain.
        """
        streams: Dict[str, List[float]] = {d: [] for d in SENTINEL_DOMAINS}

        try:
            # Try Redis sorted set first (fast path)
            if self._redis:
                cutoff = time.time() - (self.FIT_WINDOW_HOURS * 3600)
                raw_results = await self._redis.raw.zrange(
                    "events:recent_window",
                    "+inf",
                    cutoff,
                    desc=True,
                    byscore=True,
                )
                for raw in raw_results:
                    try:
                        e = json.loads(raw)
                        domain = e.get("domain", e.get("type", "").split("_")[0])
                        if domain in streams:
                            # Extract timestamp from the event
                            ts = e.get("timestamp") or e.get("occurred_at")
                            if isinstance(ts, str):
                                ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                            elif isinstance(ts, (int, float)):
                                pass
                            else:
                                continue
                            streams[domain].append(float(ts))
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue

        except Exception as e:
            logger.warning(f"Redis event history load failed, falling back to TimescaleDB: {e}")

        # Fallback: TimescaleDB if Redis didn't yield enough data
        total = sum(len(v) for v in streams.values())
        if total < self.MIN_EVENTS_FOR_FIT and self._db:
            try:
                rows = await self._db.query(
                    """
                    SELECT type AS event_type, occurred_at
                    FROM events
                    WHERE occurred_at > NOW() - ($1::integer * INTERVAL '1 hour')
                    ORDER BY occurred_at ASC
                    """,
                    int(self.FIT_WINDOW_HOURS)
                )

                streams = {d: [] for d in SENTINEL_DOMAINS}
                for row in rows:
                    domain = str(row.get("event_type", "")).split("_")[0]
                    if domain in streams:
                        ts = row.get("occurred_at")
                        if isinstance(ts, str):
                            try:
                                ts_dt = datetime.fromisoformat(ts)
                                streams[domain].append(ts_dt.timestamp())
                            except Exception:
                                pass
                        elif hasattr(ts, "timestamp"):
                            streams[domain].append(ts.timestamp())
            except Exception as db_err:
                logger.error(f"TimescaleDB event history load failed: {db_err}")

        # Sort each stream
        for d in streams:
            streams[d].sort()

        return streams

    def _apply_params_to_tracker(self, params: Dict[str, Any]):
        """Apply fitted MLE parameters to the real-time HawkesIntensityTracker."""
        try:
            if "mu" in params:
                for domain, mu_val in params["mu"].items():
                    if domain in self._tracker._baselines:
                        self._tracker._baselines[domain] = mu_val

            if "beta" in params:
                self._tracker._decay = params["beta"]

            if "alpha" in params:
                new_excitation = {}
                for key, val in params["alpha"].items():
                    parts = key.split("->")
                    if len(parts) == 2:
                        new_excitation[(parts[0], parts[1])] = val
                if new_excitation:
                    self._tracker._excitation = new_excitation

        except Exception as e:
            logger.warning(f"Failed to apply fitted params to tracker: {e}")

    def get_branching_ratio_matrix(self) -> Dict[str, float]:
        """Return the current branching ratio matrix as {src->tgt: ratio}."""
        return dict(self._branching_ratios)

    def get_fit_summary(self) -> Optional[Dict[str, Any]]:
        """Return the last fit result summary."""
        return self._fit_result
