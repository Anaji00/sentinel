"""Shadow: regime labels for the last 14 days of divergence alerts, with what followed. Read-only; writes nothing and changes no order.
Run inside a service container: python /tmp/ad.py"""

import asyncio
import collections
import json
import re
from datetime import datetime

from shared.db import get_timescale
from shared.utils import regime_packet as rp


def dt(v):
    return v if isinstance(v, datetime) else datetime.fromisoformat(str(v).replace('Z', '+00:00'))


def js(v):
    return v if isinstance(v, (dict, list)) else json.loads(v or "{}")


async def main() -> None:
    db = await get_timescale()
    alerts = await db.query(
        "SELECT primary_entity_id AS t, detected_at, metrics_summary FROM correlations WHERE rule_id LIKE 'DIVERGENCE%' AND detected_at > NOW() - INTERVAL '14 days' ORDER BY detected_at")
    out = []
    for a in alerts or []:
        t, at, ms = a["t"], dt(a["detected_at"]), js(a["metrics_summary"])
        z = ms.get("z")
        if z is None or not t:
            continue
        items = await db.query(
            "SELECT filing_data->'items' AS items FROM events WHERE type = 'filing' AND upper(coalesce(filing_data->>'ticker', primary_entity_id)) = $1 "
            "AND created_at BETWEEN $2::timestamptz - INTERVAL '5 days' AND $2::timestamptz", t, at)
        seen = sorted({str(i) for r in items or [] for i in (js(r["items"]) if r["items"] else [])})
        er = await db.query(
            "SELECT headline FROM events WHERE type = 'earnings_reaction' AND primary_entity_id = $1 AND created_at BETWEEN $2::timestamptz - INTERVAL '5 days' AND $2::timestamptz", t, at)
        ear = 0.0
        for r in er or []:
            m = re.search(r"([+-]\d+(?:\.\d+)?)%", str(r["headline"] or ""))
            ear = float(m.group(1)) / 100.0 if m else ear
        ins = await db.query(
            "SELECT tags FROM events WHERE type = 'insider_cluster' AND upper(coalesce(financial_data->>'ticker', primary_entity_id)) = $1 AND created_at > $2::timestamptz - INTERVAL '14 days' AND created_at <= $2", t, at)
        net = None
        if ins:
            tags = " ".join(str(x) for r in ins for x in (r["tags"] or []))
            net = 1.0 if "buy" in tags and "sell" not in tags else -1.0 if "sell" in tags and "buy" not in tags else 0.0
        dtc = await db.query_one("SELECT days_to_cover FROM short_interest WHERE upper(ticker) = $1 ORDER BY settlement_date DESC LIMIT 1", t)
        pk = rp.Packet(residual_z=float(z), adverse_8k_items=[i for i in seen if i in rp.ADVERSE_8K_ITEMS], catalyst_8k_items=[i for i in seen if i in rp.CATALYST_8K_ITEMS],
                       earnings_surprise=ear, news_articles=ms.get("news_articles"), insider_net=net,
                       days_to_cover=float(dtc["days_to_cover"]) if dtc and dtc["days_to_cover"] is not None else None)
        v = rp.classify(pk)
        out.append((t, at, float(z), v))
    n = len(out)
    print("divergence alerts labelled:", n)
    c = collections.Counter(v.label for *_, v in out)
    print("labels:", dict(c))
    print("basis:", dict(collections.Counter(v.basis for *_, v in out)), "| actions:", dict(collections.Counter(v.action for *_, v in out)))
    miss = collections.Counter(m for *_, v in out for m in v.missing)
    print("missing inputs:", dict(miss))
    for t, at, z, v in out[-8:]:
        print(f"  {t:6s} {str(at)[:16]} z={z:+.1f} -> {v.label} ({v.action}): {'; '.join(v.reasons)[:110]}")


asyncio.run(main())
