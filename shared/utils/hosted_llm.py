"""
shared/utils/hosted_llm.py

Free hosted inference in front of the local Ollama, behind the same client.

Groq and Google AI Studio both serve an OpenAI-compatible chat endpoint, so one
backend covers both. A route maps a local model to an ordered list of hosted
ones; OllamaClient._call_ollama tries the route first and uses the local model
only when every hosted step is unavailable:

    LLM_HOSTED_MODE=on
    LLM_ROUTES=qwen2.5:7b=groq/openai/gpt-oss-120b;qwen2.5:3b=groq/openai/gpt-oss-20b,gemini/gemini-3.5-flash-lite

Free tiers are quota-gated, not paid per token, so each hosted model has a
client-side bucket (requests a minute and a day, tokens a minute and a day,
0 = unlimited) checked before sending:

    LLM_QUOTAS=groq/openai/gpt-oss-20b=30,1000,8000,200000;...

A full bucket or a 429 is overload. Neither is retried: the step is skipped and,
on a 429, cooled down for the provider's Retry-After, and the next step runs.
Keys live in .env only (GROQ_API_KEY, GEMINI_API_KEY); a provider without one is
skipped. A model named as "provider/model" is hosted-only (the analyst's shadow).

Pure parsing and request shaping up top; the network call at the bottom.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp

logger = logging.getLogger("sentinel.hosted_llm")

PROVIDERS: Dict[str, Tuple[str, str]] = {
    # name: (chat completions URL, env var holding the key)
    "groq": ("https://api.groq.com/openai/v1/chat/completions", "GROQ_API_KEY"),
    "gemini": ("https://generativelanguage.googleapis.com/v1beta/openai/chat/completions", "GEMINI_API_KEY"),
}
CHARS_PER_TOKEN = 4
HOSTED_TIMEOUT = aiohttp.ClientTimeout(total=float(os.getenv("LLM_HOSTED_TIMEOUT_SEC", "60")))
DEFAULT_MAX_TOKENS = 1024
COOLDOWN_KEY = "sentinel:hosted_llm:cooldown:{step}"
BUCKET_KEY = "sentinel:hosted_llm:{step}:{unit}:{window}"


@dataclass(frozen=True)
class Quota:
    rpm: int
    rpd: int
    tpm: int
    tpd: int


def parse_routes(spec: str) -> Dict[str, List[str]]:
    """'local=prov/model,prov/model;local2=...' -> {local: [steps]}."""
    out: Dict[str, List[str]] = {}
    for part in (spec or "").split(";"):
        local, _, steps = part.partition("=")
        chain = [s.strip() for s in steps.split(",") if is_hosted(s.strip())]
        if local.strip() and chain:
            out[local.strip()] = chain
    return out


def parse_quotas(spec: str) -> Dict[str, Quota]:
    """'prov/model=rpm,rpd,tpm,tpd;...' -> {step: Quota}. Malformed entries are skipped."""
    out: Dict[str, Quota] = {}
    for part in (spec or "").split(";"):
        step, _, nums = part.partition("=")
        vals = [v.strip() for v in nums.split(",")]
        if step.strip() and len(vals) == 4 and all(v.isdigit() for v in vals):
            out[step.strip()] = Quota(*(int(v) for v in vals))
    return out


def is_hosted(model: Optional[str]) -> bool:
    """A step names a known provider before its first slash."""
    prov, sep, name = (model or "").partition("/")
    return bool(sep and name and prov in PROVIDERS)


def split_step(step: str) -> Tuple[str, str]:
    prov, _, name = step.partition("/")
    return prov, name


def estimate_tokens(prompt: str, max_tokens: int) -> int:
    return len(prompt) // CHARS_PER_TOKEN + max_tokens


def windows(now: float) -> Dict[str, str]:
    """The minute and UTC-day a request is counted in."""
    t = datetime.fromtimestamp(now, timezone.utc)
    return {"m": t.strftime("%Y%m%d%H%M"), "d": t.strftime("%Y%m%d")}


def over_quota(used: Dict[str, int], q: Quota, tokens: int) -> Optional[str]:
    """The limit this request would break, or None. `used` holds rm, rd, tm, td."""
    for name, lim, cur, add in (("rpm", q.rpm, used.get("rm", 0), 1), ("rpd", q.rpd, used.get("rd", 0), 1),
                                ("tpm", q.tpm, used.get("tm", 0), tokens), ("tpd", q.tpd, used.get("td", 0), tokens)):
        if lim and cur + add > lim:
            return name
    return None


def request_body(model: str, prompt: str, temperature: float, fmt: Optional[Union[str, dict]],
                 max_tokens: int) -> Dict[str, Any]:
    """The chat request. A JSON-schema `format` becomes json_object mode plus the
    schema stated in the prompt: the local path enforces the shape with a grammar
    and drops the prose schema, which these endpoints cannot replace."""
    if isinstance(fmt, dict):
        prompt = (f"{prompt}\n\nJSON SCHEMA:\n{json.dumps(fmt, separators=(',', ':'))}\n"
                  "Return ONLY a raw JSON object conforming to this schema.")
    body: Dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if fmt:
        body["response_format"] = {"type": "json_object"}
    if "gpt-oss" in model or model.startswith("gemini-3"):
        # Both reason by default, and reasoning is counted against the same
        # token quota as the answer (measured: 30s for a five-token reply).
        body["reasoning_effort"] = "low"
    return body


def usage_as_ollama(resp: Dict[str, Any], elapsed_sec: float) -> Dict[str, Any]:
    """The response's usage in the fields the usage meter reads."""
    u = resp.get("usage") or {}
    choices = resp.get("choices") or []
    finish = (choices[0] or {}).get("finish_reason") if choices else None
    return {"prompt_eval_count": int(u.get("prompt_tokens") or 0),
            "eval_count": int(u.get("completion_tokens") or 0),
            "total_duration": int(elapsed_sec * 1e9),
            # Ollama's name for it, so the caller's truncation handling applies:
            # a reasoning model can spend max_tokens before its JSON closes.
            "done_reason": "length" if finish == "length" else "stop"}


def reply_text(resp: Dict[str, Any]) -> Optional[str]:
    choices = resp.get("choices") or []
    msg = (choices[0] or {}).get("message") or {} if choices else {}
    content = msg.get("content")
    return content if isinstance(content, str) and content.strip() else None


def mode() -> str:
    return os.getenv("LLM_HOSTED_MODE", "off").strip().lower()


def route_for(local_model: str) -> List[str]:
    """The hosted steps to try for a local model, in order; empty when off."""
    if is_hosted(local_model):
        return [local_model]
    if mode() != "on":
        return []
    return parse_routes(os.getenv("LLM_ROUTES", "")).get(local_model, [])


# ── I/O ──────────────────────────────────────────────────────────────────────

_local_counts: Dict[str, int] = {}
_local_cooldown: Dict[str, float] = {}


async def _reserve(raw: Any, step: str, q: Optional[Quota], tokens: int) -> Optional[str]:
    """Counts this request against the step's buckets, unless it would overflow
    one; returns the limit it would break. Redis when the process has it, so the
    containers share one quota; in-process otherwise."""
    if q is None:
        return None
    w = windows(time.time())
    keys = {"rm": BUCKET_KEY.format(step=step, unit="r", window=w["m"]),
            "rd": BUCKET_KEY.format(step=step, unit="r", window=w["d"]),
            "tm": BUCKET_KEY.format(step=step, unit="t", window=w["m"]),
            "td": BUCKET_KEY.format(step=step, unit="t", window=w["d"])}
    adds = {"rm": 1, "rd": 1, "tm": tokens, "td": tokens}
    if raw is None:
        # Windows that have passed are dropped, or a long-lived process would
        # keep four keys for every minute it ever called a hosted model.
        live = (w["m"], w["d"])
        for stale in [k for k in _local_counts if not k.endswith(live)]:
            del _local_counts[stale]
        used = {k: _local_counts.get(v, 0) for k, v in keys.items()}
        broken = over_quota(used, q, tokens)
        if broken:
            return broken
        for k, v in keys.items():
            _local_counts[v] = _local_counts.get(v, 0) + adds[k]
        return None
    # Increment first, then check, and give it back if it overflowed: a read
    # followed by a separate increment let containers reserving in the same
    # moment all pass on the same count and overrun the shared quota.
    async with raw.pipeline() as pipe:
        for k, v in keys.items():
            pipe.incrby(v, adds[k])
            pipe.expire(v, 120 if k.endswith("m") else 90000)
        res = await pipe.execute()
    after = {k: int(res[2 * i]) for i, k in enumerate(keys)}
    before = {k: after[k] - adds[k] for k in keys}
    broken = over_quota(before, q, tokens)
    if broken:
        async with raw.pipeline() as pipe:
            for k, v in keys.items():
                pipe.decrby(v, adds[k])
            await pipe.execute()
    return broken


async def _cooling(raw: Any, step: str) -> bool:
    if raw is None:
        return _local_cooldown.get(step, 0.0) > time.time()
    return bool(await raw.exists(COOLDOWN_KEY.format(step=step)))


async def _cool(raw: Any, step: str, seconds: int) -> None:
    seconds = max(5, min(seconds, 3600))
    if raw is None:
        _local_cooldown[step] = time.time() + seconds
    else:
        await raw.set(COOLDOWN_KEY.format(step=step), "1", ex=seconds)


async def has_capacity(raw: Any, local_model: str, tokens: int = 1000, reserve: float = 0.0) -> bool:
    """Whether some hosted step on this model's route could take a call now:
    a key, no cooldown, and room in its buckets. Read only, nothing reserved.
    Lets callers skip the local CPU budget for work that will not run locally.
    `reserve` is the share of each limit a lower-priority caller must leave
    for others: at 0.3 it sees room only below 70% of every limit."""
    quotas = parse_quotas(os.getenv("LLM_QUOTAS", ""))
    for step in route_for(local_model):
        prov, _ = split_step(step)
        if not os.getenv(PROVIDERS[prov][1], "").strip():
            continue
        try:
            if await _cooling(raw, step):
                continue
            q = quotas.get(step)
            if q is None:
                return True
            w = windows(time.time())
            keys = {"rm": BUCKET_KEY.format(step=step, unit="r", window=w["m"]),
                    "rd": BUCKET_KEY.format(step=step, unit="r", window=w["d"]),
                    "tm": BUCKET_KEY.format(step=step, unit="t", window=w["m"]),
                    "td": BUCKET_KEY.format(step=step, unit="t", window=w["d"])}
            if raw is None:
                used = {k: _local_counts.get(v, 0) for k, v in keys.items()}
            else:
                used = {k: int(v or 0) for k, v in zip(keys, await raw.mget(list(keys.values())))}
            share = max(0.0, 1.0 - float(reserve))
            q_avail = Quota(*(int(v * share) if v else 0 for v in (q.rpm, q.rpd, q.tpm, q.tpd))) if reserve else q
            if over_quota(used, q_avail, tokens) is None:
                return True
        except Exception as exc:
            logger.debug("Hosted capacity check for %s failed: %s", step, exc)
    return False


async def complete(session: Optional[aiohttp.ClientSession], raw: Any, local_model: str, prompt: str,
                   temperature: float, fmt: Optional[Union[str, dict]],
                   max_tokens: Optional[int]) -> Optional[Tuple[str, str, Dict[str, Any]]]:
    """(text, step, usage) from the first hosted step that answers, or None when
    the route is empty or every step is unavailable (no key, cooling, over
    quota, 429, error). Never retries a step."""
    steps = route_for(local_model)
    if not steps:
        return None
    quotas = parse_quotas(os.getenv("LLM_QUOTAS", ""))
    mt = int(max_tokens or DEFAULT_MAX_TOKENS)
    own = session is None
    sess = session or aiohttp.ClientSession()
    try:
        for step in steps:
            prov, model = split_step(step)
            url, key_env = PROVIDERS[prov]
            key = os.getenv(key_env, "").strip()
            if not key:
                continue
            try:
                if await _cooling(raw, step):
                    continue
                broken = await _reserve(raw, step, quotas.get(step), estimate_tokens(prompt, mt))
                if broken:
                    logger.info("Hosted %s skipped: %s quota would be exceeded", step, broken)
                    continue
                t0 = time.monotonic()
                async with sess.post(url, json=request_body(model, prompt, temperature, fmt, mt),
                                     headers={"Authorization": f"Bearer {key}"}, timeout=HOSTED_TIMEOUT) as resp:
                    if resp.status == 429:
                        retry = resp.headers.get("retry-after", "60")
                        await _cool(raw, step, int(float(retry)) if retry.replace(".", "", 1).isdigit() else 60)
                        logger.warning("Hosted %s rate-limited (429); cooling down", step)
                        continue
                    if resp.status != 200:
                        logger.warning("Hosted %s HTTP %s: %s", step, resp.status, (await resp.text())[:200])
                        continue
                    data = await resp.json(content_type=None)
                text = reply_text(data)
                if text is None:
                    logger.warning("Hosted %s returned no content", step)
                    continue
                return text, step, usage_as_ollama(data, time.monotonic() - t0)
            except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                logger.warning("Hosted %s failed: %s", step, exc)
                continue
        return None
    finally:
        if own:
            await sess.close()


__all__ = ["PROVIDERS", "Quota", "complete", "estimate_tokens", "is_hosted", "mode", "over_quota", "parse_quotas",
           "parse_routes", "reply_text", "request_body", "route_for", "usage_as_ollama", "windows"]
