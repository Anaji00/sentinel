"""
services/api_gateway/routes/portfolio.py

Real Portfolio & Position Tracking API with Broker Abstraction.
Computes real-time portfolio risk (VaR 95%, CVaR 99%, Beta), exposure, and trade execution.
"""

import json
import math
import logging
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from shared.broker import (
    get_broker,
    BrokerInterface,
    OrderSide,
    OrderType,
    OrderStatus,
    LiveTradingNotArmed,
)
from shared.utils.feature_flags import FeatureFlagManager
from shared.utils.rbac import require_permission, require_role, Role
from shared.utils.audit_ledger import AuditLedger, AuditLedgerUnavailable
from shared.utils.quiet_failures import swallowed
from services.api_gateway.dependencies import get_redis_optional, get_db_optional

logger = logging.getLogger("api-gateway.portfolio")

router = APIRouter(prefix="/api/v1/portfolio", tags=["Portfolio & Execution"])


class OrderSubmissionRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10)
    qty: float = Field(..., gt=0.0)
    side: OrderSide
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = Field(default=None, gt=0.0)
    stop_price: Optional[float] = Field(default=None, gt=0.0)
    client_order_id: Optional[str] = None


@router.get("/account", dependencies=[Depends(require_role(Role.VIEWER))])
async def get_account_summary(redis = Depends(get_redis_optional)):
    """Fetch active account summary, portfolio valuation, and buying power."""
    broker = get_broker(redis_client=redis)
    summary = await broker.get_account()
    return summary.model_dump()


@router.get("/positions", dependencies=[Depends(require_role(Role.VIEWER))])
async def get_portfolio_positions(redis = Depends(get_redis_optional)):
    """Fetch all open positions with real-time unrealized P&L and market values."""
    broker = get_broker(redis_client=redis)
    positions = await broker.get_positions()
    
    total_market_val = sum(p.market_value for p in positions)
    total_unrealized_pl = sum(p.unrealized_pl for p in positions)

    return {
        "count": len(positions),
        "total_market_value": round(total_market_val, 2),
        "total_unrealized_pl": round(total_unrealized_pl, 2),
        "positions": [p.model_dump() for p in positions],
    }


@router.post("/orders", dependencies=[Depends(require_role(Role.ADMIN))])
async def submit_trade_order(
    req: OrderSubmissionRequest,
    request: Request,
    redis = Depends(get_redis_optional),
    db = Depends(get_db_optional),
):
    """
    Submit a trade execution order through the active broker adapter.

    Order entry requires ADMIN: session cookies resolve to ANALYST by default, so
    gating here at ANALYST made every authenticated session an order-entry
    principal. Also honours the platform master kill switch and records a durable
    audit entry before anything reaches the venue.
    """
    actor = getattr(request.state, "identity", "trader")
    ledger = AuditLedger(redis_client=redis, db_client=db)

    # Emergency Halt must stop order flow, not just signal generation.
    flags = FeatureFlagManager(redis_client=redis)
    if not await flags.is_enabled("order_execution"):
        logger.warning(f"Order rejected — execution halted by kill switch (actor={actor}).")
        raise HTTPException(
            status_code=423,
            detail="Order rejected: trade execution is currently halted by the "
                   "platform kill switch.",
        )

    try:
        broker = get_broker(redis_client=redis)
    except LiveTradingNotArmed as e:
        logger.error(f"Order rejected — live venue not armed: {e}")
        raise HTTPException(
            status_code=503,
            detail="Order rejected: a live trading venue is configured but not "
                   "armed. No order was placed.",
        )

    # Record intent BEFORE execution. If the ledger cannot durably record the
    # order, the order must not be placed — an executed-but-unaudited trade is
    # not a recoverable state, whereas a rejected order is.
    try:
        await ledger.record_entry(
            actor=actor,
            action=f"SUBMIT_{req.side.value}_ORDER",
            resource_type="ORDER",
            resource_id=req.client_order_id or f"{req.symbol.upper()}:{req.qty}",
            details={
                "symbol": req.symbol.upper(),
                "qty": req.qty,
                "side": req.side.value,
                "order_type": req.order_type.value,
                "limit_price": req.limit_price,
                "stop_price": req.stop_price,
                "phase": "intent",
            },
        )
    except AuditLedgerUnavailable as e:
        logger.error(f"Refusing order execution — audit ledger unavailable: {e}")
        raise HTTPException(
            status_code=503,
            detail="Order rejected: the audit ledger is unavailable, so this "
                   "execution cannot be recorded. No order was placed.",
        )

    try:
        order = await broker.submit_order(
            symbol=req.symbol,
            qty=req.qty,
            side=req.side,
            order_type=req.order_type,
            limit_price=req.limit_price,
            stop_price=req.stop_price,
            client_order_id=req.client_order_id,
        )
    except Exception as e:
        logger.error(f"Order submission failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Order execution error: {str(e)}")

    # Record the outcome. The trade has already executed at this point, so a
    # ledger failure here is logged loudly but cannot un-place the order.
    try:
        await ledger.record_entry(
            actor=actor,
            action=f"EXECUTE_{req.side.value}_ORDER",
            resource_type="ORDER",
            resource_id=order.order_id,
            details={
                "symbol": req.symbol.upper(),
                "qty": req.qty,
                "side": req.side.value,
                "order_type": req.order_type.value,
                "status": order.status.value,
                "filled_avg_price": order.filled_avg_price,
                "phase": "execution",
            },
        )
    except AuditLedgerUnavailable as e:
        logger.critical(
            "EXECUTED ORDER %s IS UNAUDITED — ledger write failed after execution: %s",
            order.order_id, e,
        )

    return order.model_dump()


@router.delete(
    "/orders/{order_id}",
    # Cancelling an order reaches the broker exactly as placing one does, and
    # the permission table grants write:brokers to ADMIN alone. This required
    # ANALYST while POST /orders required ADMIN -- two routes into the same
    # venue with two different answers, because nothing read the table.
    dependencies=[Depends(require_permission("write:brokers"))],
)
async def cancel_trade_order(
    order_id: str,
    request: Request,
    redis = Depends(get_redis_optional),
    db = Depends(get_db_optional),
):
    """Cancel an active pending order."""
    broker = get_broker(redis_client=redis)
    success = await broker.cancel_order(order_id)
    if not success:
        raise HTTPException(status_code=400, detail=f"Could not cancel order '{order_id}'.")

    actor = getattr(request.state, "identity", "trader")
    ledger = AuditLedger(redis_client=redis, db_client=db)
    await ledger.record_entry(
        actor=actor,
        action="CANCEL_ORDER",
        resource_type="ORDER",
        resource_id=order_id,
        details={"status": "CANCELLED"},
    )

    return {"status": "CANCELLED", "order_id": order_id}


from shared.models.provenance import ProvenanceEnvelope, ProvenanceSourceType

# Trading days in a year, for turning the platform's annualised realised
# volatility measurement into the one-day figure a 1-day VaR needs.
TRADING_DAYS_PER_YEAR = 252


def _expected_shortfall_multiplier(z: float, alpha: float) -> float:
    """phi(z) / (1 - alpha): the Gaussian expected-shortfall multiplier.

    This is the formula /methodology has always published for CVaR --
    `\\frac{\\phi(Z_{\\alpha})}{1 - \\alpha} \\cdot \\sigma_P \\cdot V_P` -- and it is
    not the one that ran. The code used `z_99 * daily_vol * 1.25`, an ad-hoc
    multiplier of 2.908 where the published formula gives 2.665, so the
    endpoint documenting the platform's risk mathematics described a
    computation the platform did not perform.

    Derived rather than written down as 2.665, so the two cannot drift again.
    """
    pdf = math.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)
    return pdf / (1.0 - alpha)

# What the volatility was before it was measured.
#
# 0.018 sat in the body of this function under the comment "Standard
# assumptions for broad equity portfolio volatility", and the response wrapped
# it in a ProvenanceEnvelope declaring COMPUTED_DETERMINISTIC,
# is_synthetic=False and model_name="deterministic_risk_engine". Every VaR and
# CVaR this endpoint has returned was that constant times a weight norm, on a
# platform that measures realised volatility hourly.
#
# Kept only as the disclosed fallback for when nothing has been measured, and
# the provenance now says so when it is used.
ASSUMED_DAILY_VOL = 0.018


async def _measured_daily_volatility(redis):
    """(daily vol, annualised percent or None). Measured where it is measured.

    `sentinel:macro:realised_vol` holds annualised realised volatility as a
    percentage, computed by the enrichment service from QQQ or SPY one-minute
    bars and refreshed hourly. It is the only volatility measurement a gateway
    request can reach: `_compute_ewma_volatility` writes a per-entity variance
    key and has no callers anywhere in the tree, so that key does not exist.
    """
    if redis is None:
        return ASSUMED_DAILY_VOL, None
    try:
        from shared.utils.volatility import REALISED_VOL_KEY

        raw = await redis.raw.get(REALISED_VOL_KEY)
        if raw is None:
            return ASSUMED_DAILY_VOL, None
        annualised_pct = float(raw)
        if annualised_pct <= 0.0:
            return ASSUMED_DAILY_VOL, None
        return (annualised_pct / 100.0) / math.sqrt(TRADING_DAYS_PER_YEAR), annualised_pct
    except Exception as _exc:
        swallowed("api_gateway.routes.portfolio._measured_daily_volatility", _exc, logger)
        return ASSUMED_DAILY_VOL, None


async def _sector_exposure(redis, positions, total_equity: float) -> Dict[str, float]:
    """Share of equity by sector, from the reference data already cached.

    The docstring below has always promised a "Sector Concentration
    Breakdown", and the populated branch returned no `sector_exposure` key at
    all -- only the no-positions branch did, as an empty object. A client
    reading the field got it when there was nothing to report and lost it the
    moment there was something.
    """
    exposure: Dict[str, float] = {}
    # The key convention the enrichment service writes under. Imported rather
    # than retyped, so a rename there breaks here loudly instead of returning
    # UNKNOWN for every position.
    from services.enrichment.ref_data import REFDATA_PREFIX

    for position in positions:
        sector = "UNKNOWN"
        if redis is not None:
            try:
                cached = await redis.raw.get(f"{REFDATA_PREFIX}{position.symbol.upper()}")
                if cached:
                    payload = json.loads(
                        cached.decode() if isinstance(cached, bytes) else cached
                    )
                    sector = (payload.get("sector") or "").strip() or "UNKNOWN"
            except Exception as _exc:
                swallowed("api_gateway.routes.portfolio._sector_exposure", _exc, logger)
        share = abs(position.market_value) / total_equity if total_equity else 0.0
        exposure[sector] = round(exposure.get(sector, 0.0) + share, 4)
    return exposure


@router.get("/risk", dependencies=[Depends(require_role(Role.VIEWER))])
async def get_portfolio_risk_metrics(redis = Depends(get_redis_optional)):
    """
    Computes real-time portfolio risk metrics across open positions:
    - 1-Day 95% Parametric Value at Risk (VaR)
    - 1-Day 99% Conditional Value at Risk (CVaR / Expected Shortfall)
    - Sector Concentration Breakdown

    Portfolio beta is not among them. It was reported as a flat 1.05 beside the
    computed figures, on a platform that holds no per-position return series
    against SPY at this layer and so cannot regress one. The field is still
    returned, as null, because a caller asking for beta should be told it is
    unavailable rather than handed a constant.
    """
    broker = get_broker(redis_client=redis)
    positions = await broker.get_positions()
    account = await broker.get_account()

    total_equity = max(1.0, account.portfolio_value)
    if not positions:
        return {
            "portfolio_value": total_equity,
            "positions_count": 0,
            "var_95_daily_usd": 0.0,
            "var_95_daily_pct": 0.0,
            "cvar_99_daily_usd": 0.0,
            "cvar_99_daily_pct": 0.0,
            "portfolio_beta": None,
            "diversification_score": 1.0,
            "sector_exposure": {},
            "provenance": ProvenanceEnvelope(
                source_type=ProvenanceSourceType.DISCLOSED_PLACEHOLDER,
                methodology="No active positions; risk parameters in baseline state",
                data_inputs=["account:cash"],
            ).model_dump(),
        }

    daily_vol, annualised_pct = await _measured_daily_volatility(redis)
    # 95% 1-tail Z = 1.6449, 99% 1-tail Z = 2.3263
    z_95 = 1.6449
    z_99 = 2.3263
    es_99 = _expected_shortfall_multiplier(z_99, 0.99)

    # Calculate parametric VaR based on position concentration
    weights = [p.market_value / total_equity for p in positions]
    herfindahl = sum(w ** 2 for w in weights)
    diversification_score = round(max(0.0, min(1.0, 1.0 - math.sqrt(herfindahl) * 0.5)), 3)

    # Parametric VaR
    var_95_pct = round(z_95 * daily_vol * math.sqrt(herfindahl), 4) * 100.0
    cvar_99_pct = round(es_99 * daily_vol * math.sqrt(herfindahl), 4) * 100.0

    var_95_usd = round((var_95_pct / 100.0) * total_equity, 2)
    cvar_99_usd = round((cvar_99_pct / 100.0) * total_equity, 2)

    measured = annualised_pct is not None
    methodology = (
        "1-day parametric VaR: z * sigma_daily * sqrt(sum(w^2)) * equity. "
        "CVaR: phi(z)/(1-alpha) * sigma_daily * sqrt(sum(w^2)) * equity, the "
        "Gaussian expected shortfall -- the formula /methodology publishes, "
        "which the previous code replaced with an ad-hoc 1.25 multiplier. "
        "sqrt(sum(w^2)) is the L2 norm of position weights, which equals the "
        "portfolio volatility only if the positions are uncorrelated and share "
        "one volatility -- an assumption that understates risk for a long "
        "equity book, and one the previous methodology string did not state. "
    )
    methodology += (
        f"sigma_daily from measured realised volatility of {annualised_pct:.2f}% "
        f"annualised (QQQ/SPY 1m bars, refreshed hourly) over sqrt(252)."
        if measured
        else f"sigma_daily is the disclosed assumption {ASSUMED_DAILY_VOL}; no "
             f"realised-volatility measurement was available."
    )

    return {
        "portfolio_value": round(total_equity, 2),
        "positions_count": len(positions),
        "var_95_daily_usd": var_95_usd,
        "var_95_daily_pct": round(var_95_pct, 2),
        "cvar_99_daily_usd": cvar_99_usd,
        "cvar_99_daily_pct": round(cvar_99_pct, 2),
        # Null, not 1.05. See the note on this endpoint.
        "portfolio_beta": None,
        "daily_volatility_used": round(daily_vol, 6),
        "daily_volatility_is_measured": measured,
        "diversification_score": diversification_score,
        "concentration_hhi": round(herfindahl, 4),
        "sector_exposure": await _sector_exposure(redis, positions, total_equity),
        "provenance": ProvenanceEnvelope(
            # A figure resting on an unmeasured constant is not a deterministic
            # computation, whatever the arithmetic around it looks like.
            source_type=(
                ProvenanceSourceType.COMPUTED_DETERMINISTIC
                if measured
                else ProvenanceSourceType.DISCLOSED_PLACEHOLDER
            ),
            methodology=methodology,
            data_inputs=(
                [f"position:{p.symbol}:{p.qty}" for p in positions]
                + (["redis:sentinel:macro:realised_vol"] if measured else [])
            ),
            is_synthetic=not measured,
            model_name="deterministic_risk_engine",
        ).model_dump(),
    }
