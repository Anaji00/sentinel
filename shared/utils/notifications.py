"""
shared/utils/notifications.py

What is worth telling someone about the stocks they follow, and how much of it.

A person could follow a stock and never hear about it: alerts reached only the
operator's Telegram. These are the pure parts of the inbox that changes that.
The job in services/alert_manager/notifications_job.py does the I/O.

  call     the model has a new call on a stock, or reversed one
  signal   the signal board's evidence reached higher confidence, or flipped
  filing   a filing on the stock (Schedule 13D/13G and the like)
  holders  several large institutions moved the same way last quarter

Tier and grading gates apply here as to any alert: calls only from a horizon
whose model passed validation, verdicts only at higher confidence. No model is
called, so this costs no inference. Nothing here is advice or an order.

Missing data is not "no filter": an empty or absent snapshot means the source
has not been rebuilt (a restart empties it), never that every call went away,
so the caller keeps the previous snapshot and reports nothing.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from shared.utils.ownership import aggregate_changes

KINDS: Tuple[str, ...] = ("call", "signal", "filing", "holders")
FILING_TYPES: Tuple[str, ...] = ("filing",)

# Every source agrees past about ten or twenty a day people stop reading. The
# excess becomes one summary row, so nothing is silently lost.
DAILY_CAP = 20
BODY_MAX = 280

HORIZON_WORDS: Dict[str, str] = {"daily": "next day", "weekly": "next week", "monthly": "next month", "events": "after big events"}


@dataclass(frozen=True)
class Notice:
    """One thing that happened to one stock, before it is addressed to anyone."""

    kind: str
    ticker: str
    title: str
    body: str
    url: str
    dedupe_key: str


def stock_url(ticker: str) -> str:
    """Opens the stock drawer on the Calls page."""
    return f"/calls?t={ticker}"


def call_snapshot(recommendations: Optional[Mapping[str, Any]]) -> Dict[str, int]:
    """{"horizon:TICKER": direction} for validated horizons only.

    An idea from a horizon that has not passed validation is research, not
    evidence, and is not worth interrupting anyone for.
    """
    out: Dict[str, int] = {}
    for name, horizon in ((recommendations or {}).get("horizons") or {}).items():
        if not isinstance(horizon, Mapping) or not horizon.get("validated"):
            continue
        for idea in horizon.get("ideas") or []:
            ticker = str(idea.get("ticker") or "").upper()
            direction = idea.get("direction")
            if ticker and direction in (1, -1):
                out[f"{name}:{ticker}"] = int(direction)
    return out


def call_notices(previous: Mapping[str, int], current: Mapping[str, int], day: str) -> List[Notice]:
    """New and reversed calls. A call simply ending is not news: ideas rotate."""
    out: List[Notice] = []
    for key, direction in current.items():
        before = previous.get(key)
        if before == direction:
            continue
        horizon, ticker = key.split(":", 1)
        words = HORIZON_WORDS.get(horizon, horizon)
        action = "buy" if direction > 0 else "short"
        if before is None:
            title = f"New call on {ticker}: {action}"
            body = f"The model now has a {action} call on {ticker} for the {words}."
        else:
            title = f"Call reversed on {ticker}: now {action}"
            body = f"The model reversed its {words} call on {ticker}, and it is now a {action}."
        out.append(Notice("call", ticker, title, body, stock_url(ticker), f"call:{key}:{direction}:{day}"))
    return out


def verdict_snapshot(board: Optional[Iterable[Mapping[str, Any]]]) -> Dict[str, int]:
    """{TICKER: lean} for stocks whose evidence leans one way at higher confidence."""
    out: Dict[str, int] = {}
    for row in board or []:
        if row.get("confidence") != "higher":
            continue
        ticker = str(row.get("ticker") or "").upper()
        net = row.get("net") or 0
        if ticker and net:
            out[ticker] = 1 if net > 0 else -1
    return out


def verdict_notices(previous: Mapping[str, int], current: Mapping[str, int], day: str) -> List[Notice]:
    out: List[Notice] = []
    for ticker, lean in current.items():
        before = previous.get(ticker)
        if before == lean:
            continue
        word = "bullish" if lean > 0 else "bearish"
        title = (f"Evidence on {ticker} now leans {word}" if before is None
                 else f"Evidence on {ticker} flipped to {word}")
        body = f"The signals on {ticker} lean {word} at higher confidence. A lean, not a forecast."
        out.append(Notice("signal", ticker, title, body, stock_url(ticker), f"signal:{ticker}:{lean}:{day}"))
    return out


def holder_flows(per_filer: Mapping[str, Mapping[str, Any]], period: str) -> Dict[str, Dict[str, Any]]:
    """{"period:TICKER": {flow, names}} where large institutions moved one way
    in a quarter's 13Fs, by the same rule the ownership collector uses."""
    out: Dict[str, Dict[str, Any]] = {}
    for ticker, row in aggregate_changes(per_filer).items():
        if row.get("flow"):
            names = row["buyers"] if row["flow"] > 0 else row["sellers"]
            out[f"{period}:{str(ticker).upper()}"] = {"flow": row["flow"], "names": sorted(names)}
    return out


def holder_notices(previous: Mapping[str, Any], current: Mapping[str, Mapping[str, Any]]) -> List[Notice]:
    out: List[Notice] = []
    for key, row in current.items():
        if key in previous:
            continue
        period, ticker = key.split(":", 1)
        buying = row["flow"] > 0
        names = list(row["names"])
        who = ", ".join(names[:3]) + (f" and {len(names) - 3} more" if len(names) > 3 else "")
        verb = "added to or opened positions in" if buying else "cut or exited"
        title = f"Institutions are {'buying' if buying else 'selling'} {ticker}"
        body = f"{who} {verb} {ticker} in filings for the quarter ending {period}. 13F data lags by up to 45 days."
        out.append(Notice("holders", ticker, title, body, stock_url(ticker), f"holders:{key}"))
    return out


def event_notice(row: Mapping[str, Any]) -> Optional[Notice]:
    """A filing, from a row of the events table."""
    ticker = str(row.get("primary_entity_id") or "").upper()
    event_id = row.get("event_id")
    if not ticker or not event_id or row.get("type") not in FILING_TYPES:
        return None
    summary = " ".join(str(row.get("summary") or "").split())
    if not summary:
        return None
    body = summary if len(summary) <= BODY_MAX else summary[: BODY_MAX - 1].rstrip() + "…"
    return Notice("filing", ticker, f"New filing on {ticker}", body, stock_url(ticker), f"event:{event_id}")


def wanted_kinds(notify: Optional[Mapping[str, Any]]) -> Set[str]:
    """The kinds a person has left on; every kind is on until turned off."""
    prefs = notify or {}
    return {k for k in KINDS if prefs.get(k, True)}


def address(
    notices: Sequence[Notice],
    tickers: Iterable[str],
    notify: Optional[Mapping[str, Any]],
    already_today: int,
    day: str,
    cap: int = DAILY_CAP,
) -> List[Notice]:
    """The notices for one person: stocks they follow, kinds they want, under
    their daily cap. Calls come first when there are too many; the excess
    becomes one summary row."""
    follows = {str(t).upper() for t in tickers}
    kinds = wanted_kinds(notify)
    mine = [n for n in notices if n.ticker in follows and n.kind in kinds]
    mine.sort(key=lambda n: KINDS.index(n.kind))
    room = max(0, cap - already_today)
    if len(mine) <= room:
        return mine
    kept = mine[: max(0, room - 1)]
    extra = len(mine) - len(kept)
    return kept + [Notice(
        "summary", "", f"{extra} more updates on stocks you follow",
        "A busy day, so these were rolled up. Each stock's page has the detail.",
        "/calls", f"overflow:{day}",
    )]


def digest(rows: Sequence[Mapping[str, Any]], site_url: str = "") -> Tuple[str, str]:
    """The (subject, plain-text body) of a daily email from unsent inbox rows."""
    count = len(rows)
    subject = f"Sentinel: {count} update{'' if count == 1 else 's'} on stocks you follow"
    lines = [subject, ""]
    for r in rows:
        lines.append(f"- {r.get('title')}")
        if r.get("body"):
            lines.append(f"  {r.get('body')}")
    lines += ["", f"Open your inbox: {site_url or 'your Sentinel dashboard'}",
              "Turn this email off in the bell menu. Paper trading only; not financial advice."]
    return subject, "\n".join(lines)
