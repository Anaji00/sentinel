"""Groq's real rate limits for the news-scoring model, from the headers of one tiny request, next to what we configured.
Prints no secrets. Run inside a service container (factor-model runs the news job)."""

import json
import os
import urllib.error
import urllib.request

from shared.utils import hosted_llm as hl

model = os.getenv("NEWS_LLM_MODEL", "")
print("NEWS_LLM_MODEL:", model)
print("configured quota for it:", hl.parse_quotas(os.getenv("LLM_QUOTAS", "")).get(model))
print("fallbacks:", hl.fallback_chain(model, "llm_news") if hasattr(hl, "fallback_chain") else "n/a")
prov, name = hl.split_step(model)
if prov != "groq":
    raise SystemExit("not a Groq model; nothing to probe")
body = json.dumps({"model": name, "messages": [{"role": "user", "content": "Reply with the word ok."}], "max_tokens": int(os.getenv("PROBE_MAX_TOKENS", "32"))}).encode()
req = urllib.request.Request(hl.PROVIDERS["groq"][0], data=body, headers={
    "Authorization": "Bearer " + os.getenv("GROQ_API_KEY", ""), "Content-Type": "application/json", "User-Agent": "sentinel-probe/1.0"})
try:
    resp = urllib.request.urlopen(req, timeout=40)
    status, headers, text = resp.status, resp.headers, resp.read().decode()[:300]
except urllib.error.HTTPError as exc:
    status, headers, text = exc.code, exc.headers, exc.read().decode()[:600]
print("status:", status)
for k, v in headers.items():
    if k.lower().startswith(("x-ratelimit", "retry-after")):
        print(" ", k, "=", v)
if status != 200:
    print("body:", text)
