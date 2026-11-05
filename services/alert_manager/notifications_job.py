"""
services/alert_manager/notifications_job.py

Fills each person's inbox with what changed on the stocks they follow, and
sends the optional daily email. The decisions are in shared/utils/notifications.py;
this is the I/O around them. No model is called.

Every few minutes: compare the validated calls, the higher-confidence verdicts
and the latest 13F flows with the last run's, read new filings, and address what
changed to each follower.

The first run for each source only records where things stand, and an empty
source is left alone (a restart empties them while they rebuild). So neither a
fresh start nor a restart can tell anyone that everything changed. Rows are
unique per person and event, so a repeated run tells nobody twice, and one
person's failure is counted without stopping the next person's.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from shared.utils import mailer
from shared.utils import notifications as nt
from shared.utils.ownership import CHANGES_KEY
from shared.utils.quiet_failures import swallowed
from shared.utils.signal_store import RECOMMENDATIONS_KEY, SIGNAL_MATRIX_INDEX_KEY

logger = logging.getLogger("alert-manager.notifications")

INTERVAL_SEC = 300
STATE_KEY = "sentinel:notifications:state"
DIGEST_HOUR_UTC = 13  # about 9 am US Eastern, before the open

_FOLLOWERS_SQL = "SELECT owner, prefs FROM user_preferences"
_FILINGS_SQL = """
    SELECT event_id, type, primary_entity_id, summary, created_at FROM events
    WHERE type = ANY($1::text[]) AND primary_entity_id = ANY($2::text[])
      AND occurred_at > $3::timestamptz - INTERVAL '7 days' AND created_at > $3::timestamptz
    ORDER BY created_at
"""
_TODAY_SQL = "SELECT count(*) AS n FROM notifications WHERE owner = $1 AND created_at::date = CURRENT_DATE"
_INSERT_SQL = """
    INSERT INTO notifications (owner, kind, ticker, title, body, url, dedupe_key)
    VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (owner, dedupe_key) DO NOTHING RETURNING id
"""
_UNSENT_SQL = """
    SELECT id, title, body FROM notifications
    WHERE owner = $1 AND emailed_at IS NULL AND read_at IS NULL AND created_at > NOW() - INTERVAL '2 days'
    ORDER BY created_at
"""
_MARK_SENT_SQL = "UPDATE notifications SET emailed_at = NOW() WHERE owner = $1 AND id = ANY($2::bigint[])"


async def _json(raw: Any, key: str) -> Any:
    blob = await raw.get(key)
    return json.loads(blob) if blob else None


async def _latest_flows(raw: Any) -> Dict[str, Dict[str, Any]]:
    """Institutional flows in the most recent quarter with any 13F changes."""
    periods: List[str] = []
    async for key in raw.scan_iter(CHANGES_KEY.format(period="*"), count=100):
        k = key.decode() if isinstance(key, bytes) else key
        periods.append(k.rsplit(":", 1)[1])
    if not periods:
        return {}
    period = max(periods)
    blob = await raw.hgetall(CHANGES_KEY.format(period=period)) or {}
    per_filer = {(f.decode() if isinstance(f, bytes) else f): json.loads(v) for f, v in blob.items()}
    return nt.holder_flows(per_filer, period)


def _followers(rows: List[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        prefs = r["prefs"]
        prefs = json.loads(prefs) if isinstance(prefs, str) else dict(prefs or {})
        out[r["owner"]] = {"tickers": prefs.get("tickers") or [], "notify": prefs.get("notify") or {}}
    return out


async def collect(raw: Any, db: Any, state: Dict[str, Any], tickers: List[str], now: datetime) -> List[nt.Notice]:
    """Everything new since the last run, updating `state` as it goes."""
    day = now.date().isoformat()
    notices: List[nt.Notice] = []

    calls = nt.call_snapshot(await _json(raw, RECOMMENDATIONS_KEY))
    if calls:
        if "calls" in state:
            notices += nt.call_notices(state["calls"], calls, day)
        state["calls"] = calls

    index = await _json(raw, SIGNAL_MATRIX_INDEX_KEY)
    verdicts = nt.verdict_snapshot((index or {}).get("board"))
    if verdicts:
        if "verdicts" in state:
            notices += nt.verdict_notices(state["verdicts"], verdicts, day)
        state["verdicts"] = verdicts

    flows = await _latest_flows(raw)
    if flows:
        if "flows" in state:
            notices += nt.holder_notices(state["flows"], flows)
        # Kept for every quarter seen, so an old flow cannot come back as new.
        state["flows"] = {**state.get("flows", {}), **{k: v["flow"] for k, v in flows.items()}}

    since = state.get("filings_since")
    if since and tickers:
        rows = await db.query(_FILINGS_SQL, list(nt.FILING_TYPES), tickers, datetime.fromisoformat(since))
        notices += [n for n in (nt.event_notice(r) for r in rows or []) if n]
    state["filings_since"] = now.isoformat()
    return notices


async def deliver(db: Any, followers: Mapping[str, Mapping[str, Any]], notices: List[nt.Notice], day: str) -> int:
    """Commits each person's rows. Returns how many were written."""
    written = 0
    for owner, who in followers.items():
        try:
            today = await db.query_one(_TODAY_SQL, owner)
            mine = nt.address(notices, who["tickers"], who["notify"], int((today or {}).get("n") or 0), day)
            for n in mine:
                # A row only comes back if it was new: a repeat is not "written".
                if await db.query_one(_INSERT_SQL, owner, n.kind, n.ticker or None, n.title, n.body, n.url, n.dedupe_key):
                    written += 1
        except Exception as e:
            swallowed("alert_manager.notifications.deliver", e, logger)
    return written


async def send_digests(db: Any, followers: Mapping[str, Mapping[str, Any]]) -> int:
    """One email per person who asked for it. A failed send stays unsent, for
    tomorrow's attempt, and the caller runs this once a day."""
    site = (os.getenv("PUBLIC_BASE_URL") or "").rstrip("/")
    sent = 0
    for owner, who in followers.items():
        # An API-key or development identity is not an address.
        if not who["notify"].get("email_digest") or "@" not in owner:
            continue
        try:
            rows = await db.query(_UNSENT_SQL, owner)
            if not rows:
                continue
            subject, body = nt.digest(rows, site)
            if await mailer.send_email(owner, subject, body):
                await db.execute(_MARK_SENT_SQL, owner, [r["id"] for r in rows])
                sent += 1
        except Exception as e:
            swallowed("alert_manager.notifications.digest", e, logger)
    return sent


async def run_once(raw: Any, db: Any, now: Optional[datetime] = None) -> int:
    now = now or datetime.now(timezone.utc)
    state: Dict[str, Any] = await _json(raw, STATE_KEY) or {}
    followers = _followers(await db.query(_FOLLOWERS_SQL) or [])
    tickers = sorted({str(t).upper() for f in followers.values() for t in f["tickers"]})
    notices = await collect(raw, db, state, tickers, now)
    written = await deliver(db, followers, notices, now.date().isoformat())
    day = now.date().isoformat()
    if now.hour >= DIGEST_HOUR_UTC and state.get("digest_day") != day:
        state["digest_day"] = day
        await send_digests(db, followers)
    # Saved after delivery: a crash before this repeats the run, and the unique
    # key keeps that from telling anyone twice.
    await raw.set(STATE_KEY, json.dumps(state))
    if written:
        logger.info("Notifications: %d written from %d change(s)", written, len(notices))
    return written


async def run_forever(redis_client: Any, db: Any) -> None:
    raw = getattr(redis_client, "raw", redis_client)
    while True:
        try:
            await run_once(raw, db)
        except Exception as e:
            swallowed("alert_manager.notifications.run", e, logger)
        await asyncio.sleep(INTERVAL_SEC)
