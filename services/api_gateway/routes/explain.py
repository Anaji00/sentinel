"""
services/api_gateway/routes/explain.py

Explainability & Model Card Surface (§B.2).
Provides computation audit trails, factor attribution waterfall, step-by-step
score adjustments, data provenance, and model stability metadata for any alert or signal.
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils.feature_flags import FeatureFlagManager
from shared.utils.quant_calc import compute_ta_indicators, kelly_criterion

logger = logging.getLogger("api-gateway.explain")

# Identity of the scorer that actually runs. Anomaly scoring migrated to
# streaming RRCF (shared/utils/streaming_detectors.py); the batch
# IsolationForest ONNX export was retired, and naming it here would misdescribe
# the model to anyone auditing a signal.
ACTIVE_MODEL_NAME = "RRCF-StreamingAnomaly-v1"
ACTIVE_MODEL_FAMILY = (
    "Robust Random Cut Forest (online) + Welford streaming variance + Hawkes contagion"
)
# Below this many settled predictions a win rate is noise, not an estimate.
MIN_SETTLED_FOR_WIN_RATE = 20
FEATURE_SCHEMA_VERSION = "v2.6"
FEATURES_USED = (
    "price_return_zscore",
    "realized_volatility_ewma",
    "order_flow_imbalance",
    "kyle_lambda",
    "amihud_illiquidity",
    "parkinson_high_low_vol",
)

# Published by services/telemetry-worker/drift_scheduler.py.
DRIFT_REPORT_KEY = "sentinel:ml:drift_report"

# PSI convention: < 0.10 stable, 0.10-0.25 slight, >= 0.25 significant.
PSI_SLIGHT = 0.10
PSI_SIGNIFICANT = 0.25


async def _read_drift_status(redis: Any) -> Dict[str, Any]:
    """Returns measured drift, or an explicit unknown state.

    The scheduler publishes under a TTL, so an absent key means drift is not
    currently being evaluated. Reporting STABLE in that case would assert model
    health that nothing has checked.
    """
    unknown = {
        "psi_score": None,
        "drift_state": "UNKNOWN",
        "last_evaluated": None,
        "detail": "No drift evaluation published; the drift scheduler may not be running.",
    }
    if not redis:
        return unknown
    try:
        raw = await redis.raw.get(DRIFT_REPORT_KEY)
        if not raw:
            return unknown
        report = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception as e:
        logger.warning(f"Could not read drift report: {e}")
        return unknown

    psi = report.get("psi")
    if psi is None:
        return unknown
    if report.get("status") == "initializing":
        return {
            "psi_score": None,
            "drift_state": "INITIALIZING",
            "last_evaluated": report.get("timestamp"),
            "detail": "Insufficient baseline history to evaluate drift.",
        }

    psi = float(psi)
    state = "STABLE" if psi < PSI_SLIGHT else "SLIGHT_DRIFT" if psi < PSI_SIGNIFICANT else "SIGNIFICANT_DRIFT"
    return {
        "psi_score": round(psi, 4),
        "drift_state": state,
        "psi_threshold": PSI_SIGNIFICANT,
        "baseline_count": report.get("baseline_count"),
        "current_count": report.get("current_count"),
        "last_evaluated": report.get("timestamp"),
    }


router = APIRouter(prefix="/api/v1/explain", tags=["Model Explainability & Audit Trail"])


@router.get("/event/{event_id}")
async def explain_event_alert(
    event_id: str,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """
    Returns the complete mathematical factor attribution waterfall, score adjustments,
    data provenance, and model card for a specific event alert.
    """
    # Measured, not asserted: reported as processing_latency_ms below.
    _t0 = time.perf_counter()

    event = None
    looked = False
    if db:
        try:
            rows = await db.query("SELECT * FROM events WHERE event_id = $1 LIMIT 1", event_id)
            looked = True
            if rows:
                event = dict(rows[0])
        except Exception as e:
            logger.debug("DB event lookup failed for %s: %s", event_id, e)

    # No invented event.
    #
    # This branch used to build one: an NVDA "high-frequency volume and
    # volatility anomaly" at 0.88, priced at 128.50 against a 127.80 VWAP, with
    # a realized volatility, an order-flow imbalance, a Kyle lambda and an
    # Amihud illiquidity -- and then explained it, in full, under whatever event
    # id had been asked about. On the one endpoint whose entire purpose is to
    # show how a number was arrived at, a wrong id returned a confident
    # explanation of an event that never happened.
    #
    # "Not found" and "could not look" are different answers, and only one of
    # them is about the event: a 404 for a database that never answered would
    # tell the caller the event does not exist, which nobody here knows.
    if not event:
        if not looked:
            raise HTTPException(
                status_code=503,
                detail="The events store is unavailable; nothing could be read.",
            )
        raise HTTPException(
            status_code=404,
            detail=f"No event {event_id}. Nothing to explain.",
        )

    anomaly_score = float(event.get("anomaly_score", 0.0) or 0.0)
    raw_fin = event.get("financial_data") or {}
    fin_data = json.loads(raw_fin) if isinstance(raw_fin, str) else raw_fin
    if not isinstance(fin_data, dict):
        fin_data = {}

    raw_breakdown = event.get("anomaly_breakdown") or {}
    breakdown = json.loads(raw_breakdown) if isinstance(raw_breakdown, str) else raw_breakdown
    if not isinstance(breakdown, dict):
        breakdown = {}

    # The dimensions the scorer actually measured, and their share of what it
    # measured. Not a weighted model: this platform has no linear composite
    # with published coefficients, and the 0.40 / 0.30 / 0.20 / 0.10 weights
    # printed here previously did not come from one. Neither did the inputs --
    # a missing volatility z-score defaulted to 2.4, a missing realized
    # volatility to 0.35, a missing order-flow imbalance to 0.5, and the
    # cross-domain Hawkes factor was the literal 35.0 on every event ever
    # explained, while an actual Hawkes correlator runs in this deployment.
    #
    # An empty waterfall is the correct output for an event whose score carries
    # no breakdown, and most do not: `anomaly_breakdown` is populated by the
    # tradfi path today. Showing nothing says so. Showing four bars does not.
    _DIMENSIONS = (
        ("volatility_z_score", "Volatility Z-Score"),
        ("volume_z_score", "Volume Z-Score"),
        ("spatial_score", "Spatial Dispersion"),
        ("temporal_score", "Temporal Clustering"),
        ("cross_domain_correlation_score", "Cross-Domain Correlation"),
    )
    measured = []
    for key, label in _DIMENSIONS:
        value = breakdown.get(key)
        if value is None:
            continue
        measured.append((key, label, abs(float(value))))
    total_measured = sum(v for _, _, v in measured)
    factor_attribution = [
        {
            "factor_key": key,
            "label": label,
            "raw_subscore": round(value, 4),
            # Share of the measured dimensions, stated as that and nothing
            # more. Null when every dimension measured zero, because a share
            # of nothing is not zero percent, it is undefined.
            "contribution_pct": (
                round((value / total_measured) * 100.0, 1) if total_measured > 0 else None
            ),
        }
        for key, label, value in measured
    ]

    # How the score actually moved, from the steps the scorer recorded.
    #
    # `NormalizedEvent.score_adjustments` is a real ordered list of
    # (reason, delta) and reaches the table as of migration 0024. What stood
    # here instead was four fixed steps -- "Base IsolationForest Anomaly
    # Score", "Watchlist Prior Alignment Boost +0.10", "3+ correlated prints
    # within rolling 60-second window", "Sigmoid normalization" -- with their
    # deltas computed as fractions of the final score, so the arithmetic always
    # reconciled and the reasons were asserted of every event regardless of
    # whether the entity was watched, whether anything clustered, or whether an
    # IsolationForest had run at all. It had not: the scorer is streaming RRCF,
    # as this handler's own model card says twenty lines below.
    raw_steps = event.get("score_adjustments") or []
    if isinstance(raw_steps, str):
        try:
            raw_steps = json.loads(raw_steps)
        except (ValueError, TypeError):
            raw_steps = []
    score_adjustments = []
    if isinstance(raw_steps, list) and raw_steps:
        # The recorded deltas are the tail of the derivation; the base is
        # whatever the score was before the first of them.
        total_delta = sum(float(st.get("delta") or 0.0) for st in raw_steps if isinstance(st, dict))
        running = round(anomaly_score - total_delta, 6)
        score_adjustments.append({
            "step": 1,
            "action": "Base score",
            "score_before": 0.0,
            "delta": running,
            "score_after": running,
            "reason": f"{ACTIVE_MODEL_NAME} ({ACTIVE_MODEL_FAMILY}) before recorded adjustments",
        })
        for idx, st in enumerate(raw_steps, start=2):
            if not isinstance(st, dict):
                continue
            delta = float(st.get("delta") or 0.0)
            before = running
            running = round(before + delta, 6)
            score_adjustments.append({
                "step": idx,
                "action": st.get("reason") or "adjustment",
                "score_before": before,
                "delta": round(delta, 6),
                "score_after": running,
                "reason": st.get("reason") or "",
            })

    # Data source provenance. Values that are not measured are reported as null
    # rather than as a plausible constant -- an audit trail that invents its own
    # latency and quality figures cannot be used to audit anything.
    provenance = {
        "source_collector": event.get("source"),
        "event_id": event_id,
        "ingest_timestamp": event.get("occurred_at"),
        # Deterministic content hash. Python's builtin hash() is randomized per
        # process, so the previous construction produced a different "hash" for
        # the same event on every restart.
        "payload_hash": "sha256:" + hashlib.sha256(
            f"{event_id}|{event.get('occurred_at')}".encode("utf-8")
        ).hexdigest(),
        "processing_latency_ms": round((time.perf_counter() - _t0) * 1000.0, 2),
        "data_quality_score": None,
    }

    # Model Card Metadata (§B.2). Identity reflects the scorer that actually
    # runs -- streaming RRCF, not the retired batch IsolationForest export.
    model_card = {
        "model_name": ACTIVE_MODEL_NAME,
        "model_family": ACTIVE_MODEL_FAMILY,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "features_used": list(FEATURES_USED),
        "training_window": "Online / streaming -- no batch training window",
        "model_drift_status": await _read_drift_status(redis),
    }

    return {
        "event_id": event_id,
        "entity": event.get("primary_entity_name") or event.get("primary_entity_id") or "UNKNOWN",
        "event_type": event.get("type", "unknown"),
        "overall_anomaly_score": anomaly_score,
        "is_significant": anomaly_score >= 0.70,
        "factor_attribution": factor_attribution,
        "score_adjustments": score_adjustments,
        # What backed the number. A 0.4 from a warm-up curve and a 0.4 from a
        # full percentile window are not the same claim, and an explainability
        # surface that cannot say which is not explaining anything.
        "score_basis": breakdown.get("coverage_basis"),
        "score_coverage_fraction": breakdown.get("coverage_fraction"),
        "provenance": provenance,
        "model_card": model_card,
        "market_microstructure": fin_data if isinstance(fin_data, dict) else {},
    }


async def _observed_win_rate(db, ticker: str) -> Optional[float]:
    """This ticker's settled prediction win rate, or None if nothing has settled.

    None is a real answer here and a placeholder is not: a Kelly fraction built
    on an invented win rate sizes a position on a number nobody measured.
    """
    try:
        rows = await db.query(
            "SELECT count(*) FILTER (WHERE outcome_correct)::float AS wins, "
            "       count(*)::float AS settled "
            "FROM agent_predictions "
            "WHERE ticker = $1 AND outcome_correct IS NOT NULL",
            ticker,
        )
    except Exception as e:
        logger.debug("Win-rate lookup failed for %s: %s", ticker, e)
        return None
    if not rows:
        return None
    settled = float(rows[0].get("settled") or 0.0)
    if settled < MIN_SETTLED_FOR_WIN_RATE:
        return None
    return float(rows[0].get("wins") or 0.0) / settled


async def _graph_precheck(redis, ticker: str) -> Dict[str, Any]:
    """Reference data for the ticker as the platform actually holds it.

    The literals here named a sector, three index memberships and a two-name
    supply chain for every signal, whatever the instrument.
    """
    empty = {
        "sector": None,
        "indices": [],
        "supply_chain_dependencies": [],
        "empirical_correlations": [],
        "note": "No reference data cached for this ticker.",
    }
    if redis is None:
        return empty
    try:
        blob = await redis.raw.get(f"sentinel:refdata:{ticker}")
    except Exception as e:
        logger.debug("Reference data lookup failed for %s: %s", ticker, e)
        return empty
    if not blob:
        return empty
    try:
        ref = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8"))
    except (ValueError, AttributeError, UnicodeDecodeError):
        return empty
    return {
        "sector": ref.get("sector"),
        "industry": ref.get("industry"),
        "indices": ref.get("index_membership") or [],
        "supply_chain_dependencies": ref.get("suppliers") or [],
        "empirical_correlations": ref.get("correlations") or [],
    }


async def _feature_flag_status(redis) -> Dict[str, Any]:
    """The flags as the flag store answers them, not as they were typed here.

    These were three string literals -- "ENABLED", "ENABLED", "NORMAL" -- so a
    signal produced while a flag was killed still reported the flag enabled, on
    the endpoint an auditor would consult to find out.
    """
    names = ("covered_calls", "granger_causality")
    if redis is None:
        return {n: "UNKNOWN" for n in names}
    try:
        manager = FeatureFlagManager(redis)
        return {n: ("ENABLED" if await manager.is_enabled(n) else "DISABLED") for n in names}
    except Exception as e:
        logger.debug("Feature flag lookup failed: %s", e)
        return {n: "UNKNOWN" for n in names}


@router.get("/signal/{signal_id}")
async def explain_trading_signal(
    signal_id: str,
    redis=Depends(get_redis_optional),
    db=Depends(get_db_optional),
):
    """Explain a trading signal from the bars it was computed on.

    This returned a fixture. Every field was a literal -- price 128.50, ATR
    3.20, stop 123.70, RSI 58.4, an "empirical_win_rate_W" of 0.62, a sector of
    Information Technology and a supply chain of TSM and ASML -- returned
    unchanged for every signal_id, on an endpoint whose whole purpose is to
    show the arithmetic behind a recommendation. An explainability surface that
    invents its evidence is worse than none: it survives exactly the audit it
    exists to support.

    Everything below is now derived from `tradfi_bars` for the ticker, and the
    endpoint 404s when there is no history rather than filling the gap.
    """
    ticker = "NVDA"
    if "_" in signal_id:
        parts = signal_id.split("_")
        for part in parts:
            if part.upper() not in ("SIGNAL", "TRADE", "REC", "ADVICE", "STRATEGY"):
                ticker = part.upper()
                break

    if db is None:
        raise HTTPException(
            status_code=503,
            detail="Signal explanation needs the bar history; the database is unavailable.",
        )

    # 250 bars: enough for the 200-period SMA the alignment check needs.
    try:
        rows = await db.query(
            "SELECT time, open, high, low, close, volume FROM tradfi_bars "
            "WHERE ticker = $1 ORDER BY time DESC LIMIT 250",
            ticker,
        )
    except Exception as e:
        logger.warning("Bar history lookup failed for %s: %s", ticker, e)
        raise HTTPException(status_code=503, detail="Bar history is temporarily unavailable.")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No bar history for {ticker}, so there is nothing to explain. "
                "This endpoint previously answered with a worked example for a "
                "different instrument."
            ),
        )

    rows = list(reversed(rows))                       # oldest first
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]
    ta = compute_ta_indicators(closes, highs, lows)

    current_price = closes[-1]
    atr = float(ta["atr"])
    stop_distance = 1.5 * atr
    stop_loss = current_price - stop_distance
    reward_risk = 2.0
    target_price = current_price + (stop_distance * reward_risk)

    # Kelly from this agent's own recorded outcomes, or nothing.
    #
    # The 0.62 that sat here was labelled "empirical" and had never been
    # measured. A win rate the platform has not observed is not an input to a
    # position size.
    win_rate = await _observed_win_rate(db, ticker)
    if win_rate is None:
        kelly = {
            "empirical_win_rate_W": None,
            "payoff_ratio_R": reward_risk,
            "raw_kelly_pct": None,
            "half_kelly_clamped_pct": None,
            "note": (
                "No settled predictions for this ticker yet, so no win rate has "
                "been observed and no Kelly fraction is defined. Sizing must not "
                "use a placeholder."
            ),
        }
    else:
        raw_kelly = kelly_criterion(win_rate, reward_risk)
        kelly = {
            "empirical_win_rate_W": round(win_rate, 4),
            "payoff_ratio_R": reward_risk,
            "raw_kelly_pct": round(raw_kelly * 100.0, 2),
            "half_kelly_clamped_pct": round(min(max(raw_kelly / 2.0, 0.0), 0.25) * 100.0, 2),
        }

    sma_200 = ta.get("sma_200")
    explanation = {
        "signal_id": signal_id,
        "ticker": ticker,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": "Empirical Half-Kelly ATR Momentum",
        "bars_used": len(rows),
        "bar_range": {
            "from": rows[0]["time"].isoformat() if hasattr(rows[0]["time"], "isoformat") else str(rows[0]["time"]),
            "to": rows[-1]["time"].isoformat() if hasattr(rows[-1]["time"], "isoformat") else str(rows[-1]["time"]),
        },
        "deterministic_math_audit": {
            "current_price": round(current_price, 4),
            "atr_14": round(atr, 4),
            "stop_distance": round(stop_distance, 4),
            "stop_formula": "Entry - (1.5 * ATR)",
            "calculated_stop_loss": round(stop_loss, 4),
            "target_multiplier": reward_risk,
            "target_formula": "Entry + (1.5 * ATR * RiskRewardRatio)",
            "calculated_target_price": round(target_price, 4),
            "half_kelly_inputs": kelly,
        },
        "technical_indicator_inputs": {
            "rsi_14": ta.get("rsi"),
            "ema_12": ta.get("ema_12"),
            "ema_26": ta.get("ema_26"),
            "ma_alignment": ta.get("ma_alignment"),
            "sma_200": sma_200,
            "dist_sma_200_pct": ta.get("dist_sma_200_pct"),
            "fib_support_0_618": (ta.get("fib_levels") or {}).get("0.618"),
        },
        "graph_topology_precheck": await _graph_precheck(redis, ticker),
        "feature_flag_status": await _feature_flag_status(redis),
    }
    return explanation
