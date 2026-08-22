import json
import logging
from fastapi import APIRouter, Depends, Query
from services.api_gateway.dependencies import get_redis_client, require_pro
from shared.utils.heartbeat import get_all_heartbeats_status

logger = logging.getLogger("api-gateway.agents")

# Heartbeat components the LLM swarm publishes under, one per compose tier.
AGENT_TIER_COMPONENTS = ["agents-heavy", "agents-fast"]

router = APIRouter(prefix="/api/v1/agents", tags=["Agentic Intelligence"])

# The agent swarm itself is the paid tier.
@router.get("/processes", dependencies=[Depends(require_pro("reasoning"))])
async def get_agent_processes(redis = Depends(get_redis_client)):
    """Report the LLM swarm's real composition, liveness, and recent decisions.

    Every field is derived from runtime state. Where a signal is unavailable the
    response says so explicitly rather than substituting a plausible-looking
    default -- an operator cannot act on a status that is always "RUNNING".
    """
    # Roster and liveness come from the heartbeats each tier publishes, which
    # carry the models and agent names that process is actually running.
    hb = await get_all_heartbeats_status(redis, custom_components=AGENT_TIER_COMPONENTS)
    tier_status = hb.get("components", {})

    agents = []
    for component in AGENT_TIER_COMPONENTS:
        entry = tier_status.get(component, {})
        status = entry.get("status", "OFFLINE")
        meta = entry.get("metadata") or {}
        tier = component.replace("agents-", "")

        for agent_name in sorted(meta.get("agents") or []):
            agents.append({
                "name": agent_name,
                "tier": tier,
                "group_id": f"agent-{agent_name.replace('_', '-')}",
                "model": meta.get("model") or None,
                "fallback_model": meta.get("fallback_model") or None,
                "status": status,
                "last_seen": entry.get("last_seen"),
                "heartbeat_age_seconds": entry.get("age_seconds"),
            })

        if not meta.get("agents"):
            # The tier is not reporting a roster. Say which tier and why rather
            # than omitting it, so a stopped profile is distinguishable from a
            # crashed one.
            agents.append({
                "name": None,
                "tier": tier,
                "group_id": None,
                "model": None,
                "fallback_model": None,
                "status": status,
                "last_seen": entry.get("last_seen"),
                "heartbeat_age_seconds": entry.get("age_seconds"),
                "detail": (
                    "Tier is not deployed (run with --profile agents)"
                    if status in ("NOT_DEPLOYED", "OFFLINE")
                    else "Tier is running but has not published an agent roster"
                ),
            })

    running = [a for a in agents if a["status"] == "HEALTHY" and a["name"]]

    # Recent decisions, if the swarm has published any.
    decisions = []
    decisions_available = False
    if redis:
        try:
            cursor = 0
            keys = []
            while True:
                cursor, scan_keys = await redis.raw.scan(cursor=cursor, match="sentinel:agent:decision:*", count=50)
                if scan_keys:
                    keys.extend(scan_keys)
                if cursor == 0 or len(keys) >= 15:
                    break
            decisions_available = True
            if keys:
                values = await redis.raw.mget(keys[:15])
                for val in values:
                    if val:
                        try:
                            decisions.append(json.loads(val if isinstance(val, str) else val.decode("utf-8")))
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"Error fetching agent decisions from Redis: {e}")

    decisions.sort(key=lambda d: str(d.get("timestamp") or ""), reverse=True)

    return {
        "active_agents_count": len(running),
        "reporting_tiers": {
            c: tier_status.get(c, {}).get("status", "OFFLINE") for c in AGENT_TIER_COMPONENTS
        },
        "agents": agents,
        "recent_decisions_count": len(decisions),
        "recent_decisions": decisions,
        # Distinguishes "the swarm made no decisions" from "we could not ask".
        "decisions_source": (
            "runtime" if decisions_available else "unavailable"
        ),
        "data_provenance": "live_measurement",
    }
