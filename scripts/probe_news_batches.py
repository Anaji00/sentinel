"""What the news-scoring batches really cost Groq, and what Groq says when they are refused: three real batches back to back,
as one pass of the job sends them. Run inside agents-heavy (needs the database and the Groq key; prints no secrets)."""

import asyncio
import json
import os
import time
import urllib.error
import urllib.request

from shared.db import get_timescale
from shared.utils import hosted_llm as hl
from shared.utils import llm_news as ln

model = os.getenv("NEWS_LLM_MODEL", "")
prov, name = hl.split_step(model)


def call(body: dict) -> tuple:
    req = urllib.request.Request(hl.PROVIDERS[prov][0], data=json.dumps(body).encode(), headers={
        "Authorization": "Bearer " + os.getenv("GROQ_API_KEY", ""), "Content-Type": "application/json", "User-Agent": "sentinel-probe/1.0"})
    t0 = time.monotonic()
    try:
        r = urllib.request.urlopen(req, timeout=90)
        return r.status, r.headers, json.loads(r.read()), time.monotonic() - t0
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers, exc.read().decode()[:700], time.monotonic() - t0


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT n.symbols[1] AS ticker, n.headline FROM stock_news n WHERE cardinality(n.symbols) = 1 ORDER BY n.created_at DESC LIMIT $1", ln.BATCH * 3) or []
    print("model", model, "| unscored rows fetched", len(rows), "| batch", ln.BATCH)
    for k in range(3):
        batch = rows[k * ln.BATCH:(k + 1) * ln.BATCH]
        if not batch:
            break
        prompt = ln.prompt([(str(r["ticker"]), str(r["headline"])) for r in batch])
        body = hl.request_body(name, prompt, 0.0, ln.Scores.model_json_schema(), 1024, ln.SYSTEM)
        est = hl.estimate_tokens(ln.SYSTEM + prompt, 1024)
        status, headers, payload, secs = call(body)
        line = {"request": k + 1, "status": status, "seconds": round(secs, 1), "our_estimate_tokens": est}
        if status == 200:
            u = payload.get("usage", {})
            line.update(prompt_tokens=u.get("prompt_tokens"), completion_tokens=u.get("completion_tokens"), total=u.get("total_tokens"))
        else:
            line["retry_after"] = headers.get("retry-after")
            line["body"] = payload
        line["remaining_tokens"] = headers.get("x-ratelimit-remaining-tokens")
        line["reset_tokens"] = headers.get("x-ratelimit-reset-tokens")
        print(json.dumps(line))


asyncio.run(main())
