"""Read-only. How many daily_bars rows in the factor panel's window are flat carry-forward (open=high=low=close, or volume 0),
overall, among model-scored names (Redis factor_scores monthly) and among the stored B1 book. Run:
docker exec -i -w /app sentinel-api_gateway python - < scripts/audit_layer_flatbars.py"""
import asyncio, json, sys
sys.path.insert(0, "/app")
from shared.db import get_redis, get_timescale
from shared.utils.signal_store import factor_scores_key

Q = """SELECT count(*) n, count(*) FILTER (WHERE open=high AND high=low AND low=close) flat,
 count(*) FILTER (WHERE coalesce(volume,0)=0) zero_vol,
 count(*) FILTER (WHERE (open=high AND high=low AND low=close) OR coalesce(volume,0)=0) either
 FROM daily_bars WHERE day > CURRENT_DATE - $1::int AND close > 0 AND ticker = ANY($2::text[])"""
QALL = Q.replace("AND ticker = ANY($2::text[])", "AND $2::text[] IS NOT NULL")

async def main() -> None:
    r = await get_redis(); raw = getattr(r, "raw", r); db = await get_timescale()
    v = await raw.get(factor_scores_key("monthly"))
    if v is None:
        v = await raw.hgetall(factor_scores_key("monthly")) if False else None
    scored = []
    if v:
        d = json.loads(v if isinstance(v, str) else v.decode())
        scored = list((d.get("scores") or d).keys()) if isinstance(d, dict) else []
    print("model-scored names:", len(scored), "sample", scored[:5])
    book = await raw.get("sentinel:auto_trade:book")
    for days in (21, 252):
        for lab, tk in (("ALL", []), ("SCORED", scored)):
            if lab == "SCORED" and not scored: continue
            row = (await db.query(Q if lab == "SCORED" else QALL, days, tk))[0]
            n = int(row["n"]); print(f"{days}d {lab}: rows {n}, flat {int(row['flat'])/n:.2%}, zero-vol {int(row['zero_vol'])/n:.2%}, either {int(row['either'])/n:.2%}")
    if book:
        b = json.loads(book if isinstance(book, str) else book.decode())
        names = [x if isinstance(x, str) else (x.get("ticker") or x.get("t")) for k in ("long", "short") for x in (b.get(k) or [])]
        names = [n for n in names if n]
        row = (await db.query(Q, 252, names))[0]
        n = int(row["n"]) or 1
        print(f"B1 book {len(names)} names, 252d rows {n}, flat {int(row['flat'])/n:.2%}, zero-vol {int(row['zero_vol'])/n:.2%}; in scored set: {sum(1 for x in names if x in scored)}")
asyncio.run(main())
