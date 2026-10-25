"""
shared/utils/filing_extract.py

The small LLM pointed at extraction, not prediction. Pure parts; the filing
extractor (services/agents/filing_extractor.py) fetches the documents and
calls the model.

Filings are where the platform's proven edge is (8-K equity sales and delisting
notices), and its LLM calls were right 45% of the time when asked to predict
prices. Asked instead to read a document and say what it states, a 3B model
is on firmer ground, and what it extracts becomes an event family the reaction
study judges like any other:

  guidance   from an earnings release (8-K Item 2.02, exhibit 99): did the
             company raise, lower, maintain or initiate its outlook? Only
             passages that mention guidance reach the model, so a release
             without any costs nothing.
  offering   from an unregistered equity sale (Item 3.02): how many shares,
             read by pattern, no model needed, and their value against the
             company's market value.
"""
from __future__ import annotations

import html
import re
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

EXTRACT_SOURCE = "filing_extract"
GUIDANCE_WORDS = re.compile(r"\b(guidance|outlook|forecast|expects?|anticipates?|raises?|raised|lowers?|lowered|"
                            r"reaffirm\w*|reiterat\w*|full[- ]year|fiscal (?:year )?20\d\d)\b", re.I)
_STRONG = re.compile(r"\b(guidance|outlook)\b", re.I)
MAX_PASSAGE_CHARS = 2400
_SHARES = re.compile(r"(?<![\d.])(\d{1,3}(?:,\d{3})+|\d{4,})\s+(?:shares|ordinary shares)\b", re.I)
LARGE_OFFERING_SHARE = 0.05        # 5% of market value


class Guidance(BaseModel):
    direction: Literal["raised", "lowered", "maintained", "initiated", "none"] = Field(
        description="What the release does to the company's own forward guidance")
    evidence: str = Field(default="", description="The sentence that says so, quoted")


GUIDANCE_SYSTEM = (
    "You read company earnings press releases. Answer only from the text given. "
    "Say whether the company RAISED, LOWERED, MAINTAINED (reaffirmed) or INITIATED its own forward guidance "
    "(revenue, earnings or similar outlook for a coming period), or NONE if the text gives no forward guidance. "
    "Analyst estimates and past results are not guidance. Quote the sentence that decides it."
)


def text_of(document: str) -> str:
    """Plain text from an EDGAR HTML document."""
    t = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", document or "")
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def guidance_passages(text: str, max_chars: int = MAX_PASSAGE_CHARS) -> str:
    """The sentences that talk about guidance, in order, within `max_chars`;
    empty when none mentions guidance or outlook outright."""
    if not _STRONG.search(text or ""):
        return ""
    out: List[str] = []
    size = 0
    for s in re.split(r"(?<=[.!?])\s+", text):
        if GUIDANCE_WORDS.search(s) and 30 <= len(s) <= 600:
            if size + len(s) > max_chars:
                break
            out.append(s)
            size += len(s)
    return " ".join(out)


def offered_shares(text: str) -> Optional[int]:
    """The largest share count stated near the Item 3.02 disclosure."""
    i = (text or "").lower().find("3.02")
    window = text[i:i + 4000] if i >= 0 else (text or "")[:4000]
    counts = [int(m.group(1).replace(",", "")) for m in _SHARES.finditer(window)]
    return max(counts) if counts else None


_RELEASE_NAME = re.compile(r"ex-?99|earnings|release|press|pr\d", re.I)


def pick_documents(names: List[str], accession: str) -> Dict[str, Optional[str]]:
    """{'release': the earnings-release exhibit, 'body': the 8-K itself} from a
    filing directory's file names. The directory also holds the index pages,
    the full submission text and rendered R files, none of them documents."""
    acc = accession.replace("-", "")
    docs = [n for n in names if n.lower().endswith((".htm", ".html")) and not n.startswith(("R", acc[:10]))
            and accession not in n and "index" not in n.lower()]
    candidates = [n for n in docs if _RELEASE_NAME.search(n) and not re.search(r"presentat|slides|deck", n, re.I)]
    release = next((n for n in candidates if re.search(r"ex-?99|release|press", n, re.I)), None) or         next(iter(candidates), None)
    body = next((n for n in docs if n != release and not _RELEASE_NAME.search(n)
                 and not re.search(r"presentation|supplement|ex-?\d", n, re.I)), None)
    return {"release": release, "body": body}


def offering_tags(shares: Optional[int], price: Optional[float], market_cap: Optional[float]) -> List[str]:
    if not shares or not price or not market_cap or market_cap <= 0:
        return []
    return ["offering_large" if shares * price / market_cap >= LARGE_OFFERING_SHARE else "offering_small"]


__all__ = ["EXTRACT_SOURCE", "GUIDANCE_SYSTEM", "Guidance", "LARGE_OFFERING_SHARE", "guidance_passages",
           "offered_shares", "offering_tags", "pick_documents", "text_of"]
