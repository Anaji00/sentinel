"""Background warm-up of the stock page's caches (parent request, 20 Nov 2026).

Every WARM_INTERVAL_SEC, one name at a time, it calls the evidence route (which fills the snapshot cache, 300 s, and the evidence cache) for the chosen set
(shared/utils/warm_set.choose): followed stocks of every account, then the top names by dollar volume. Each name is bounded by NAME_TIMEOUT_SEC, a failure is counted
and the pass goes on, and the loop pauses between names so it never competes with a viewer. Stored data only; no model call. Started by the gateway's lifespan.
"""
import asyncio
import json
import logging
from typing import Any, List

from services.api_gateway.routes.evidence import evidence
from shared.utils import warm_set
from shared.utils.quiet_failures import swallowed
from shared.utils.stock_peers import clean_ticker

logger = logging.getLogger("api.warmup")

START_DELAY_SEC = 20.0
WARM_INTERVAL_SEC = 240.0       # a little under the snapshot's 300 s cache, so it is rarely cold
NAME_TIMEOUT_SEC = 25.0
PAUSE_BETWEEN_NAMES_SEC = 1.0

_FOLLOWED_SQL = "SELECT prefs->'tickers' AS tickers FROM user_preferences WHERE jsonb_typeof(prefs->'tickers') = 'array'"
_TOP_SQL = """
    SELECT ticker FROM daily_bars WHERE day = (SELECT max(day) FROM daily_bars WHERE ticker = 'SPY') AND close > 0 AND volume > 0
    ORDER BY close * volume DESC LIMIT $1
"""


async def names_to_warm(db: Any) -> List[str]:
    followed: List[Any] = []
    top: List[Any] = []
    try:
        followed = [r["tickers"] if isinstance(r["tickers"], list) else json.loads(r["tickers"]) for r in await db.query(_FOLLOWED_SQL) or []]
    except Exception as exc:
        swallowed("api_gateway.warmup.followed", exc, logger)
    try:
        top = [r["ticker"] for r in await db.query(_TOP_SQL, warm_set.TOP_BY_DOLLAR_VOLUME) or []]
    except Exception as exc:
        swallowed("api_gateway.warmup.top", exc, logger)
    return warm_set.choose(followed, top, clean_ticker)


async def warm_once(db: Any, redis: Any) -> dict:
    """One pass over the set; returns {names, warmed, failed}."""
    names = await names_to_warm(db)
    warmed = failed = 0
    for t in names:
        try:
            await asyncio.wait_for(evidence(t, db=db, redis=redis, user=None), NAME_TIMEOUT_SEC)
            warmed += 1
        except Exception as exc:
            failed += 1
            swallowed("api_gateway.warmup.name", exc, logger, detail=t)
        await asyncio.sleep(PAUSE_BETWEEN_NAMES_SEC)
    return {"names": len(names), "warmed": warmed, "failed": failed}


async def warm_loop(db: Any, redis: Any) -> None:
    await asyncio.sleep(START_DELAY_SEC)
    while True:
        try:
            logger.info("Stock page warm-up: %s", await warm_once(db, redis))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            swallowed("api_gateway.warmup.pass", exc, logger)
        await asyncio.sleep(WARM_INTERVAL_SEC)


__all__ = ["warm_loop", "warm_once", "names_to_warm"]
