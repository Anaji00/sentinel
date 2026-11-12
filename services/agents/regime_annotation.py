"""Builds the numbers packet for a divergence alert from the database and returns its rule-based regime label (shared/utils/regime_packet.py).

Annotation only: the label is written into the alert's metrics_summary and tags and changes no order. No model calls. A lookup that fails
leaves that input unmeasured, and the classifier then says so rather than claiming no catalyst.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from shared.utils import regime_packet as rp
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.regime_annotation")

_FILINGS_SQL = ("SELECT filing_data->'items' AS items FROM events WHERE type = 'filing' "
                "AND upper(coalesce(filing_data->>'ticker', primary_entity_id)) = $1 "
                "AND created_at BETWEEN $2::timestamptz - INTERVAL '5 days' AND $2::timestamptz")
_REACTIONS_SQL = ("SELECT headline FROM events WHERE type = 'earnings_reaction' AND primary_entity_id = $1 "
                  "AND created_at BETWEEN $2::timestamptz - INTERVAL '5 days' AND $2::timestamptz")
_INSIDERS_SQL = ("SELECT tags FROM events WHERE type = 'insider_cluster' "
                 "AND upper(coalesce(financial_data->>'ticker', primary_entity_id)) = $1 "
                 "AND created_at > $2::timestamptz - INTERVAL '14 days' AND created_at <= $2")
_SHORT_SQL = "SELECT days_to_cover FROM short_interest WHERE upper(ticker) = $1 ORDER BY settlement_date DESC LIMIT 1"
_PERCENT = re.compile(r"([+-]\d+(?:\.\d+)?)%")


def _json(v: Any) -> Any:
    return v if isinstance(v, (dict, list)) else json.loads(v or "null")


def insider_net(tag_lists: List[Any]) -> Optional[float]:
    """+1 for clusters that were buying, -1 for selling, 0 for a mix; None when no cluster was seen."""
    if not tag_lists:
        return None
    tags = " ".join(str(x) for tl in tag_lists for x in (tl or []))
    buy, sell = "buy" in tags, "sell" in tags
    return 1.0 if buy and not sell else -1.0 if sell and not buy else 0.0


def reaction_from_headline(headline: str) -> Optional[float]:
    m = _PERCENT.search(headline or "")
    return float(m.group(1)) / 100.0 if m else None


async def build_packet(db: Any, ticker: str, at: datetime, z: float, news_articles: Optional[int]) -> rp.Packet:
    t = ticker.upper()
    pk = rp.Packet(residual_z=float(z), news_articles=news_articles)
    try:
        items = {str(i) for r in await db.query(_FILINGS_SQL, t, at) or [] for i in (_json(r["items"]) or [])}
        pk.adverse_8k_items = sorted(i for i in items if i in rp.ADVERSE_8K_ITEMS)
        pk.catalyst_8k_items = sorted(i for i in items if i in rp.CATALYST_8K_ITEMS)
    except Exception as exc:
        swallowed("agents.regime_annotation.filings", exc, logger, detail=t)
    try:
        moves = [m for m in (reaction_from_headline(r["headline"]) for r in await db.query(_REACTIONS_SQL, t, at) or []) if m is not None]
        pk.earnings_surprise = max(moves, key=abs) if moves else 0.0
    except Exception as exc:
        swallowed("agents.regime_annotation.reactions", exc, logger, detail=t)
    try:
        pk.insider_net = insider_net([r["tags"] for r in await db.query(_INSIDERS_SQL, t, at) or []])
    except Exception as exc:
        swallowed("agents.regime_annotation.insiders", exc, logger, detail=t)
    try:
        row = await db.query_one(_SHORT_SQL, t)
        pk.days_to_cover = float(row["days_to_cover"]) if row and row.get("days_to_cover") is not None else None
    except Exception as exc:
        swallowed("agents.regime_annotation.short_interest", exc, logger, detail=t)
    return pk


async def annotate(db: Any, ticker: str, at: datetime, z: float, news_articles: Optional[int]) -> Optional[Dict[str, object]]:
    """The regime verdict as a plain dictionary for the alert, or None if it could not be built."""
    try:
        return rp.classify(await build_packet(db, ticker, at, z, news_articles)).as_dict()
    except Exception as exc:
        swallowed("agents.regime_annotation.annotate", exc, logger, detail=ticker)
        return None
