"""
services/api-gateway/routes/scenarios.py

This file defines the API endpoints (routes) for the 'Intelligence' section of our application.
It allows front-end dashboards or external systems to retrieve data about AI-generated scenarios 
and raw correlation clusters from the TimescaleDB database.
"""

import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from services.api_gateway.dependencies import (
    get_db,
    get_db_optional,
    get_redis_client,
    require_pro,
)
from shared.utils.rbac import require_role, Role
from pydantic import BaseModel
import json

from shared.broker.base import (
    BrokerInterface,
    OrderSide,
    OrderType,
    OrderStatus,
)
from shared.broker import get_broker
from shared.broker.paper import PaperBroker
from shared.broker.alpaca import AlpacaBroker
from shared.utils.serialization import to_dto
from shared.utils.audit_ledger import AuditLedger, AuditLedgerUnavailable
from shared.utils.feature_flags import FeatureFlagManager

# Client-facing shape of a correlation record. Keeps the response contract
# independent of the `correlations` table layout.
CORRELATION_FIELDS = (
    "correlation_id",
    "rule_name",
    "alert_tier",
    "detected_at",
    "description",
    "tags",
    "scenario",
    "evidence_trail",
)

logger = logging.getLogger("api-gateway.scenarios")
router = APIRouter(prefix="/api/v1", tags=["Intelligence"])

# Generated scenario briefs are LLM output: the one surface with real marginal
# cost per request, which is what makes it the natural paid boundary. The
# analyst platform below it stays free.
@router.get("/scenarios", dependencies=[Depends(require_pro("scenarios"))])
async def get_active_scenarios(
    limit: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None, description="e.g., HYPOTHESIS, CONFIRMED, UNDER_REVISE, DENIED"),
    db = Depends(get_db)
):
    """Fetch the latest AI-generated geopolitical scenarios with optional status filtering."""
    try:
        query = "SELECT * FROM scenarios"
        params = []
        
        if status and status.lower() != "all":
            params.append(status.strip())
            # `lower()`, not `toLower()`. The latter is Cypher: this query runs
            # against PostgreSQL, which has no such function, so every request
            # naming a status raised `function tolower(text) does not exist`,
            # was caught by the handler below and returned as a 500. The feed's
            # HYPOTHESIS / CONFIRMED / UNDER_REVISE / DENIED tabs therefore
            # rendered "NO ACTIVE SCENARIOS FOUND" every time, whatever the
            # table held; only the unfiltered "all" tab ever worked.
            query += f" WHERE lower(status) = lower(${len(params)})"
        params.append(limit)
        query += f" ORDER BY created_at DESC LIMIT ${len(params)}"
        
        return await db.query(query, *params)
    except Exception as e:
        logger.error(f"Error fetching scenarios: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")
    
@router.get("/correlations")
async def get_correlations(
    limit: int = Query(50, le=500),
    min_tier: int = Query(1, description="Minimum alert tier (1=WATCH, 2=ALERT, 3=INTEL)"),
    db = Depends(get_db),
    redis = Depends(get_redis_client)
):
    """Fetch raw correlation clusters before AI scenario generation, including dynamic LLM stock correlations & sympathy movers."""
    results = []
    
    # 1. Fetch dynamic StockCorrelationAgent discoveries from Redis
    try:
        if hasattr(redis, "raw"):
            # SCAN, and stop at the number actually used.
            #
            # This called KEYS -- blocking the single-threaded server across the
            # whole keyspace -- and then used `keys[:20]`. The other n-20 were
            # fetched from the server and discarded, on an HTTP route any
            # caller can hit.
            keys = []
            async for k in redis.raw.scan_iter(
                match="sentinel:correlations:stock:*", count=100
            ):
                keys.append(k)
                if len(keys) >= 20:
                    break
            if keys:
                raw_vals = await redis.raw.mget(keys)
                for rv in raw_vals:
                    if rv:
                        try:
                            c_item = json.loads(rv)
                            ass = c_item.get("assessment", {})
                            macro = ass.get("macro_asset", "MACRO")
                            equity = ass.get("equity_ticker", "EQUITY")
                            ctype = ass.get("correlation_type", "STOCK_CORRELATION")
                            chan = ass.get("transmission_channel", "")
                            rat = ass.get("agentic_rationale", "")
                            sym_movers = ass.get("sympathy_movers", [])

                            sym_text = ""
                            if sym_movers:
                                sym_str_list = [f"{sm.get('ticker')} ({sm.get('relationship', 'SYMPATHY')})" for sm in sym_movers]
                                sym_text = f" | Sympathy Movers: {', '.join(sym_str_list)}"

                            # Whether any relationship was measured, carried
                            # through rather than dropped.
                            #
                            # The agent stamps `covariance_measured` on every
                            # assessment so that "a reader, a consumer and the
                            # graph writer all see the same thing". This route
                            # dropped it, so a model's hypothesis about NQ=F and
                            # MSFT -- written when the graph held no measured
                            # link between them -- was listed here as a
                            # correlation beside the measured ones, in the same
                            # shape. It also invented a conviction of 0.8 when
                            # none was given, and ignored `min_tier`.
                            measured = bool(ass.get("covariance_measured"))
                            tier = 2 if ass.get("impact_severity") in ("MODERATE", "SEVERE") else 1
                            if tier < min_tier:
                                continue
                            conviction = ass.get("conviction")
                            results.append({
                                "correlation_id": f"corr_stock_{macro}_{equity}",
                                "rule_name": (
                                    f"StockCorrelationAgent: {macro} / {equity} ({ctype})"
                                    + ("" if measured else " -- hypothesis, no measured relationship")
                                ),
                                "covariance_measured": measured,
                                "detected_covariance": ass.get("detected_covariance") if measured else None,
                                "alert_tier": tier,
                                "detected_at": c_item.get("created_at") or ass.get("detected_at"),
                                "description": f"{rat} Transmitting via {chan}.{sym_text}",
                                "tags": ["STOCK_CORRELATION", macro, equity, "LLM_AGENTIC"]
                                        + ([] if measured else ["UNMEASURED"]),
                                "evidence_trail": [
                                    {
                                        "agent_name": "StockCorrelationAgent",
                                        "direction": ctype,
                                        "conviction": conviction,
                                        "score": conviction,
                                        "weight": 1.5,
                                    }
                                ]
                            })
                        except Exception as parse_err:
                            logger.debug(f"Redis stock correlation parse bypass: {parse_err}")
    except Exception as re:
        logger.debug(f"Redis stock correlations fetch bypass: {re}")

    # 2. Query TimescaleDB correlations table
    try:
        rows = await db.query("""
            SELECT correlation_id, rule_name, alert_tier, detected_at, description, tags, scenario 
            FROM correlations 
            WHERE alert_tier >= $1 
            ORDER BY detected_at DESC LIMIT $2
        """, min_tier, limit)

        for r in rows:
            item = dict(r)
            scen = item.get("scenario")
            if isinstance(scen, str):
                try:
                    scen = json.loads(scen)
                except Exception:
                    scen = {}
            elif not isinstance(scen, dict):
                scen = {}

            item["evidence_trail"] = item.get("evidence_trail") or scen.get("evidence_trail") or []

            # Explicit field allowlist rather than spreading the row: the query
            # selects from `correlations`, and a schema change there would
            # otherwise silently start shipping new columns to the client.
            results.append(to_dto(item, fields=CORRELATION_FIELDS))

        return results[:limit]
    except Exception as e:
        logger.error(f"Failed to fetch correlations: {e}")
        return results[:limit]
        
def no_financial_advice(reason: str) -> dict:
    """What /financial/advice says when no advisor has written a brief.

    This returned a fixed trade plan -- NVDA BUY at 219.16, target 242, stop
    208.50, conviction 0.88, a 6.8% Kelly allocation, Sharpe 2.45 -- stamped
    with the current time. Nothing in the platform writes the advice key, so
    every operator always saw that plan, beside an Execute button, while the
    signal engine reported that no family had earned a trade. A recommendation
    that exists only as a literal is the noise a trading system must not raise.
    """
    from datetime import datetime, timezone
    return {
        "agent": "FinancialAdvisorAgent",
        "available": False,
        "reason": reason,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "brief": None,
    }


@router.get("/financial/advice")
async def get_financial_advice(redis = Depends(get_redis_client)):
    """Fetch the latest AI Financial Advisor advice and portfolio sizing recommendations."""
    try:
        raw_advice = await redis.raw.get("sentinel:financial:advice:latest")
        if not raw_advice:
            return no_financial_advice("No advisor brief has been produced.")
        return json.loads(raw_advice)
    except Exception as e:
        logger.error(f"Failed to fetch financial advice: {e}")
        return no_financial_advice("The advice store could not be read.")

def get_execution_broker(redis_client=None) -> BrokerInterface:
    """Returns the live broker only when execution is explicitly enabled.

    Credentials alone must not route orders to a real venue. The same Alpaca key
    is used for market data -- the radar collector needs it for the tradable
    universe -- so selecting the live broker merely because a key exists meant
    configuring a data feed silently switched order execution from simulation to
    a real brokerage account. Live execution now requires ENABLE_LIVE_BROKER.

    The simulated case delegates to `shared.broker.get_broker`, which holds one
    paper book per process.

    It used to `return PaperBroker(initial_cash=100_000.0, ...)` -- a new book
    on every request. `shared.broker` grew a `_PAPER_BOOK` singleton precisely
    to stop that happening, and this second factory kept building its own, so
    the defect survived on the only order path any UI component calls.

    Measured on the deployment before the change: an order for 10 AAPL returned
    EXECUTIVE_FILLED with a bracket, and `GET /portfolio/positions` returned
    zero positions with cash unchanged at 100,000. The operator was told a trade
    had executed and nothing anywhere had moved.
    """
    live_enabled = os.getenv("ENABLE_LIVE_BROKER", "").strip().lower() in ("1", "true", "yes", "on")
    alpaca_key = os.getenv("ALPACA_API_KEY")
    alpaca_secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET")
    if live_enabled and alpaca_key and alpaca_secret:
        # Paper unless explicitly turned off, so enabling execution cannot reach
        # a funded account by omission.
        paper = os.getenv("ALPACA_PAPER", "true").strip().lower() not in ("0", "false", "no", "off")
        logger.warning(
            "Live broker execution ENABLED (paper=%s) -- orders route to Alpaca, not the simulator.",
            paper,
        )
        return AlpacaBroker(api_key=alpaca_key, secret_key=alpaca_secret, paper=paper)
    return get_broker(broker_type="paper", redis_client=redis_client)


class OrderExecutionRequest(BaseModel):
    ticker: str
    action: str
    order_type: Optional[str] = "Limit"
    entry_price: float
    target_price: float
    stop_loss: float
    position_size_usd: float
    kelly_allocation_pct: float


@router.post(
    "/trading/orders/execute",
    # Every other financial router gates on a role -- portfolio 5/5,
    # watchlists 4/4, filings 4/4, flags 4/4 -- and this, the only route that
    # reaches a broker, did not. An API key valid for reading a chart was
    # valid for placing a trade.
    #
    # ADMIN, matching `POST /portfolio/orders`, and for the reason already
    # written there: session cookies resolve to ANALYST by default, so gating
    # order entry at ANALYST makes every authenticated session an order-entry
    # principal. Two routes into the same venue answering differently is the
    # pattern this codebase has now found three times.
    dependencies=[Depends(require_role(Role.ADMIN))],
)
async def execute_trade_order(
    order: OrderExecutionRequest,
    request: Request,
    redis = Depends(get_redis_client),
    db = Depends(get_db_optional),
):
    """
    Paper Trading & Broker Execution Bridge.
    Routes order execution directly through the shared BrokerInterface (PaperBroker or AlpacaBroker).

    Carries the two guards its sibling route has always had and this one never
    did: the platform kill switch, and a durable audit entry recorded before
    anything reaches the venue.
    """
    actor = getattr(request.state, "identity", "trader")
    ledger = AuditLedger(redis_client=redis, db_client=db)

    # The kill switch stops order flow, or it stops nothing.
    #
    # `POST /portfolio/orders` refuses when `order_execution` is disabled, and
    # this route -- the one a person can actually reach from the UI -- did not
    # look at the flag at all. An emergency halt left the only reachable order
    # path wide open.
    flags = FeatureFlagManager(redis_client=redis)
    if not await flags.is_enabled("order_execution"):
        logger.warning("Order rejected -- execution halted by kill switch (actor=%s).", actor)
        raise HTTPException(
            status_code=423,
            detail="Order rejected: trade execution is currently halted by the "
                   "platform kill switch.",
        )

    broker = get_execution_broker(redis_client=redis)
    side = OrderSide.BUY if order.action.upper() == "BUY" else OrderSide.SELL
    order_type = OrderType.LIMIT if (order.order_type or "").upper() == "LIMIT" else OrderType.MARKET

    # An entry price is required, not defaulted.
    #
    # This read `max(0.01, order.entry_price)`, written to avoid a division by
    # zero. A zero entry price therefore divided the position by a penny: a
    # $2,000 Kelly-sized position became an order for 200,000 shares, and the
    # `shares < 1` check below is satisfied more comfortably the worse the
    # input gets. The quant engine does publish zero entry prices, so this was
    # reachable from the UI with one click.
    if not math.isfinite(order.entry_price) or order.entry_price <= 0:
        raise HTTPException(
            status_code=400,
            detail=(
                f"entry_price must be a positive number; got {order.entry_price}. "
                f"No order was submitted for {order.ticker}."
            ),
        )
    if not math.isfinite(order.position_size_usd) or order.position_size_usd <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"position_size_usd must be positive; got {order.position_size_usd}.",
        )

    # Whole shares: a protected order is submitted as a bracket, and brackets
    # are rejected on fractional quantities. Rounding to 2dp produced orders the
    # venue would not accept.
    shares = float(int(order.position_size_usd / order.entry_price))
    if shares < 1:
        raise HTTPException(
            status_code=400,
            detail=(
                f"position_size_usd {order.position_size_usd} buys less than one share of "
                f"{order.ticker} at {order.entry_price}. Increase the position size."
            ),
        )
    max_risk = round(shares * abs(order.entry_price - order.stop_loss), 2)
    max_reward = round(shares * abs(order.target_price - order.entry_price), 2)
    client_oid = f"sentinel_order_{order.ticker.lower()}_{int(time.time())}"

    # Intent, recorded durably BEFORE execution.
    #
    # The interface tells every operator that "every order submission ... is
    # immutably recorded in a SHA-256 hash-chained ledger". This route wrote
    # nothing, and it is the only order path the UI has -- so that sentence was
    # false for every trade a person could place. An executed-but-unaudited
    # trade is not a recoverable state; a rejected order is.
    try:
        await ledger.record_entry(
            actor=actor,
            action=f"SUBMIT_{side.value}_ORDER",
            resource_type="ORDER",
            resource_id=client_oid,
            details={
                "symbol": order.ticker.upper(),
                "shares": shares,
                "side": side.value,
                "order_type": order_type.value,
                "entry_price": order.entry_price,
                "stop_loss": order.stop_loss,
                "target_price": order.target_price,
                "position_size_usd": order.position_size_usd,
                "kelly_allocation_pct": order.kelly_allocation_pct,
                "phase": "intent",
            },
        )
    except AuditLedgerUnavailable as e:
        logger.error("Refusing order execution -- audit ledger unavailable: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Order rejected: the audit ledger is unavailable, so this "
                   "execution cannot be recorded. No order was placed.",
        )

    try:
        executed_order = await broker.submit_order(
            symbol=order.ticker,
            qty=shares,
            side=side,
            order_type=order_type,
            limit_price=order.entry_price if order_type == OrderType.LIMIT else None,
            stop_price=order.stop_loss,
            # The profit target was previously echoed back in the response but
            # never sent to the broker, so a filled position had no exit.
            take_profit_price=order.target_price,
            estimated_market_price=order.entry_price,
            client_order_id=client_oid,
        )

        fill_price = executed_order.filled_avg_price or order.entry_price
        broker_label = "Alpaca Broker API v2" if isinstance(broker, AlpacaBroker) else "Sentinel PaperBroker"

        # The outcome. The trade has already happened by now, so a ledger
        # failure here is logged loudly and cannot un-place the order.
        try:
            await ledger.record_entry(
                actor=actor,
                action=f"EXECUTE_{side.value}_ORDER",
                resource_type="ORDER",
                resource_id=executed_order.order_id or client_oid,
                details={
                    "symbol": executed_order.symbol,
                    "shares": executed_order.filled_qty or executed_order.qty,
                    "side": side.value,
                    "status": executed_order.status.value,
                    "filled_avg_price": executed_order.filled_avg_price,
                    "bracket_submitted": executed_order.is_bracket,
                    "phase": "execution",
                },
            )
        except AuditLedgerUnavailable as e:
            logger.critical(
                "EXECUTED ORDER %s IS UNAUDITED -- ledger write failed after execution: %s",
                executed_order.order_id, e,
            )

        return {
            "status": "EXECUTIVE_FILLED" if executed_order.status == OrderStatus.FILLED else "SUBMITTED",
            "order_id": executed_order.order_id or executed_order.client_order_id,
            "ticker": executed_order.symbol,
            "action": executed_order.side.value.upper(),
            "units": executed_order.filled_qty or executed_order.qty,
            "notional_usd": round(fill_price * shares, 2),
            "entry_price": fill_price,
            # Report what the broker actually accepted, not what was requested.
            "target_price": executed_order.take_profit_price,
            "stop_loss": executed_order.stop_price,
            "bracket_submitted": executed_order.is_bracket,
            "max_risk_usd": max_risk,
            "max_reward_usd": max_reward,
            "kelly_pct": order.kelly_allocation_pct,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "broker": broker_label,
        }
    except Exception as e:
        logger.error(f"Broker order execution failed for {order.ticker}: {e}")
        raise HTTPException(status_code=400, detail=f"Broker execution failed: {str(e)}")