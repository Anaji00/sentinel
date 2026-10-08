"""
shared/utils/asset_universe.py

What each listed symbol is: an operating company's stock, a fund, or something
else (warrants, units, rights, preferreds, notes), and whether it can be
shorted -- from Alpaca's asset list, cached in Redis for a day.

The cross-sectional model ranks stocks. Run across every liquid listed symbol
on 8 October it put leveraged and inverse ETFs (MUU, GDXD, QBTX) at both ends
of its rankings: a 2x fund's decay and leverage look like momentum and
volatility to a model built for companies. And its short side filled with
distressed microcaps that Alpaca marks unshortable. Both are screened here.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

ASSET_META_KEY = "sentinel:equities:asset_meta"
ASSET_META_TTL_SEC = 2 * 86400
ALPACA_ASSET_URLS = (
    "https://paper-api.alpaca.markets/v2/assets?status=active&asset_class=us_equity",
    "https://api.alpaca.markets/v2/assets?status=active&asset_class=us_equity",
)

_FUND = re.compile(
    r"\b(ETFs?|ETNs?|Exchange[- ]Traded|Fund|Funds|Leveraged|Inverse|Index|[1-5]X|-[1-5]x)\b|\bShares Trust\b"
    r"|\bPhysical\b.*\b(Shares|Trust)\b|\bDaily\b.*\b(Bull|Bear)\b",
    re.IGNORECASE,
)
# A company's own share class, stated outright, outranks fund words in its name.
_STOCK = re.compile(r"\b(Common Stock|Ordinary Shares|American Depositary)\b", re.IGNORECASE)
_OTHER = re.compile(
    r"\b(Warrants?|Units?|Rights?|Preferred|Notes?|Debentures?|Subordinated|Perpetual|Depositary Shares,? Each)\b",
    re.IGNORECASE,
)


def classify(name: str) -> str:
    """"stock", "fund" or "other" from an Alpaca asset name."""
    n = str(name or "")
    if not n:
        return "other"
    if _OTHER.search(n):
        return "other"
    if _STOCK.search(n):
        return "stock"
    if _FUND.search(n):
        return "fund"
    return "stock"


def compact(assets: Any) -> Dict[str, list]:
    """Alpaca asset records -> {symbol: [kind, shortable, easy_to_borrow, name]}."""
    out: Dict[str, list] = {}
    for a in assets or []:
        sym = str(a.get("symbol") or "").upper()
        if not sym:
            continue
        name = str(a.get("name") or "")
        out[sym] = [classify(name), bool(a.get("shortable")), bool(a.get("easy_to_borrow")), name[:80]]
    return out


async def load_asset_meta(raw: Any) -> Dict[str, list]:
    blob = await raw.get(ASSET_META_KEY)
    if not blob:
        return {}
    try:
        return json.loads(blob if isinstance(blob, str) else blob.decode("utf-8"))
    except (ValueError, AttributeError):
        return {}


async def refresh_asset_meta(raw: Any, session: Any, headers: Optional[Dict[str, str]]) -> int:
    """Fetch the asset list and cache it; returns symbols cached (0 on failure)."""
    if not headers:
        return 0
    for url in ALPACA_ASSET_URLS:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                continue
            meta = compact(await resp.json())
        if len(meta) > 1000:
            await raw.set(ASSET_META_KEY, json.dumps(meta), ex=ASSET_META_TTL_SEC)
            return len(meta)
    return 0


__all__ = ["ASSET_META_KEY", "classify", "compact", "load_asset_meta", "refresh_asset_meta"]
