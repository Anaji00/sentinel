"""Where each hosted free-tier model stands against its limits, read from the counters shared/utils/hosted_llm.py keeps.

`view` is pure (counts in, one plain dictionary out); `snapshot` reads Redis. Read only: nothing is reserved or changed.
"""
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Mapping, Optional

from shared.utils import hosted_llm as hl

NEAR_LIMIT_SHARE = 0.8


def view(step: str, q: hl.Quota, got: Mapping[str, int], cooling_seconds: int, key_configured: bool) -> Dict[str, Any]:
    """One step's usage; a limit of 0 in the config means the provider sets none, shown as null. `got` holds rm, rd, tm, td (this minute and today) already adjusted for the sliding minute."""
    provider, model = hl.split_step(step)
    pairs = [(got.get("rm", 0), q.rpm), (got.get("rd", 0), q.rpd), (got.get("tm", 0), q.tpm), (got.get("td", 0), q.tpd)]
    shares = [used / limit for used, limit in pairs if limit]
    share = max(shares) if shares else 0.0
    if not key_configured:
        status = "no key set"
    elif cooling_seconds > 0:
        status = "cooling down"
    elif any(limit and used >= limit for used, limit in pairs):
        status = "at limit"
    elif share >= NEAR_LIMIT_SHARE:
        status = "near limit"
    else:
        status = "ok"
    return {
        "model": model, "provider": provider, "status": status, "cooling_down_seconds": max(0, int(cooling_seconds)),
        "requests_this_minute": got.get("rm", 0), "requests_per_minute_limit": q.rpm or None,
        "requests_today": got.get("rd", 0), "requests_per_day_limit": q.rpd or None,
        "tokens_this_minute": got.get("tm", 0), "tokens_per_minute_limit": q.tpm or None,
        "tokens_today": got.get("td", 0), "tokens_per_day_limit": q.tpd or None,
        "share_of_limit_used": round(share, 3),
    }


async def snapshot(raw: Any, now: Optional[float] = None) -> List[Dict[str, Any]]:
    """Every step with a configured quota, most used first. A step whose counters cannot be read is left out."""
    now = time.time() if now is None else now
    quotas = hl.parse_quotas(os.getenv("LLM_QUOTAS", ""))
    w, pm = hl.windows(now), hl.prev_minute(now)
    out: List[Dict[str, Any]] = []
    for step, q in quotas.items():
        keys = {"rm": hl.BUCKET_KEY.format(step=step, unit="r", window=w["m"]), "rd": hl.BUCKET_KEY.format(step=step, unit="r", window=w["d"]),
                "tm": hl.BUCKET_KEY.format(step=step, unit="t", window=w["m"]), "td": hl.BUCKET_KEY.format(step=step, unit="t", window=w["d"]),
                "prm": hl.BUCKET_KEY.format(step=step, unit="r", window=pm), "ptm": hl.BUCKET_KEY.format(step=step, unit="t", window=pm)}
        try:
            vals = await raw.mget(list(keys.values()))
            got = {k: int(v or 0) for k, v in zip(keys, vals)}
            ttl = int(await raw.ttl(hl.COOLDOWN_KEY.format(step=step)))
        except Exception as exc:
            hl.swallowed("utils.model_quota.read", exc, hl.logger, detail=step)
            continue
        used = hl.sliding({k: got[k] for k in ("rm", "rd", "tm", "td")}, {"rm": got["prm"], "tm": got["ptm"]}, now)
        provider, _ = hl.split_step(step)
        env_name = hl.PROVIDERS.get(provider, ("", ""))[1]
        out.append(view(step, q, used, ttl, bool(env_name and os.getenv(env_name, "").strip())))
    return sorted(out, key=lambda r: -r["share_of_limit_used"])


__all__ = ["NEAR_LIMIT_SHARE", "snapshot", "view"]
