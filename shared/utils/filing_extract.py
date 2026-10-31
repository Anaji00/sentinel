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
from typing import Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field

EXTRACT_SOURCE = "filing_extract"
GUIDANCE_WORDS = re.compile(r"\b(guidance|outlook|forecast|expects?|anticipates?|raises?|raised|lowers?|lowered|"
                            r"reaffirm\w*|reiterat\w*|full[- ]year|fiscal (?:year )?20\d\d)\b", re.I)
_STRONG = re.compile(r"\b(guidance|outlook)\b", re.I)
MAX_PASSAGE_CHARS = 2400
_SHARES = re.compile(r"(?<![\d.])(\d{1,3}(?:,\d{3})+|\d{4,})\s+(?:shares|ordinary shares)\b", re.I)
LARGE_OFFERING_SHARE = 0.05        # 5% of market value


class Guidance(BaseModel):
    # The quote first, so the verdict is read off a sentence already written.
    evidence: str = Field(default="", description="The sentence that decides it, quoted verbatim")
    direction: Literal["raised", "lowered", "maintained", "initiated", "none"] = Field(
        description="What the release does to the company's own forward guidance")


_RAISE = re.compile(r"\b(rais\w*|increas\w*|boost\w*|lift\w*|upgrad\w*|improv\w* (its|our|the) (outlook|guidance)|"
                    r"(higher|above) (than )?(its |our |the )?(prior|previous|earlier)|updat\w* .{0,40}to a range of)\b", re.I)
_LOWER = re.compile(r"\b(lower\w*|reduc\w*|cut|cuts|decreas\w*|downgrad\w*|trim\w*|withdr\w*|suspend\w*|"
                    r"(lower|below) (than )?(its |our |the )?(prior|previous|earlier))\b", re.I)


def direction_supported(direction: str, quote: str) -> bool:
    """A raise or a cut only when the quoted sentence says one: on 30 Oct 2026
    a second model agreed with 55% of guidance readings (kappa 0.39), and the
    misses were "raised" for "we expect fourth quarter earnings of $2.81" and
    for "we exceeded the high end of our outlook" -- a stated outlook and a
    past result. Other directions need no change word."""
    if direction == "raised":
        return bool(_RAISE.search(quote or ""))
    if direction == "lowered":
        return bool(_LOWER.search(quote or ""))
    return True


def grounded(g: Guidance, passage: str) -> bool:
    """A guidance change must quote a sentence that is in the passage
    (whitespace-insensitive); "none" needs no quote."""
    if g.direction == "none":
        return True
    q = " ".join(g.evidence.split()).lower().strip(" .\"'")
    return len(q) >= 20 and q in " ".join(passage.split()).lower()


GUIDANCE_SYSTEM = (
    "You read company earnings press releases. Answer only from the text given. "
    "Say whether the company RAISED, LOWERED, MAINTAINED (reaffirmed) or INITIATED its own forward guidance "
    "(revenue, earnings or similar outlook for a coming period), or NONE if the text gives no forward guidance. "
    "Analyst estimates and past results are not guidance. First quote, word for word, the sentence that decides "
    "it, then give the direction."
)


def text_of(document: str) -> str:
    """Plain text from an EDGAR HTML document. A table row becomes its own
    sentence with its cells joined by " | ": flattened into one run of words, a
    customer-share or guidance table lost which number belonged to which row."""
    t = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", document or "")
    t = re.sub(r"(?i)</t[dh]\s*>", " | ", t)
    t = re.sub(r"(?i)</tr\s*>", ". ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", html.unescape(t))
    t = re.sub(r"\s*(\|\s*)+\.", ".", t)                      # a row's trailing separator
    t = re.sub(r"(\.\s*){2,}", ". ", t)                       # empty rows
    return t.strip()


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


_CLEAR = (
    ("raised", re.compile(r"\b(rais(es|ed|ing)|increas(es|ed|ing)|boost(s|ed)?)\b[^.]{0,60}\b(guidance|outlook|forecast)\b",
                          re.I)),
    ("lowered", re.compile(r"\b(lower(s|ed|ing)?|reduc(es|ed|ing)|cut(s|ting)?)\b[^.]{0,60}\b(guidance|outlook|forecast)\b",
                           re.I)),
    ("maintained", re.compile(r"\b(reaffirm(s|ed|ing)?|reiterat(es|ed|ing)|maintain(s|ed|ing)?)\b[^.]{0,60}"
                              r"\b(guidance|outlook|forecast)\b", re.I)),
)
_MONEY = r"\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(billion|million|bn|mm|m|b)?"
_FROM_TO = re.compile(r"\bto\s+(?:a\s+range\s+of\s+)?" + _MONEY + r"(?:\s*(?:to|-|–)\s*" + _MONEY + r")?"
                      r"[^.]{0,40}?\bfrom\s+(?:a\s+range\s+of\s+|the\s+prior\s+|its\s+prior\s+|previous\s+)?"
                      + _MONEY + r"(?:\s*(?:to|-|–)\s*" + _MONEY + r")?", re.I)


def clear_direction(passage: str) -> Optional[Tuple[str, str]]:
    """(direction, sentence) when one sentence says outright that guidance was
    raised, lowered or reaffirmed: read by pattern, no model call. None when no
    sentence does, or sentences disagree."""
    found: Dict[str, str] = {}
    for s in re.split(r"(?<=[.!?])\s+", passage or ""):
        for d, rx in _CLEAR:
            if rx.search(s) and d not in found:
                found[d] = s.strip()
    return next(iter(found.items())) if len(found) == 1 else None


def _amount(num: str, unit: Optional[str]) -> float:
    v = float(num.replace(",", ""))
    u = (unit or "").lower()
    return v * (1e9 if u in ("billion", "bn", "b") else 1e6 if u in ("million", "mm", "m") else 1.0)


def change_pct(quote: str) -> Optional[float]:
    """The guided figure's change, from "to X (to Y) ... from A (to B)" in one
    sentence, range midpoints compared; None without both figures."""
    m = _FROM_TO.search(quote or "")
    if not m:
        return None
    g = m.groups()
    new_lo = _amount(g[0], g[1])
    new_hi = _amount(g[2], g[3] or g[1]) if g[2] else new_lo
    old_lo = _amount(g[4], g[5] or g[1])
    old_hi = _amount(g[6], g[7] or g[5] or g[1]) if g[6] else old_lo
    old = (old_lo + old_hi) / 2
    return round(((new_lo + new_hi) / 2) / old - 1.0, 4) if old > 0 else None


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


__all__ = ["EXTRACT_SOURCE", "GUIDANCE_SYSTEM", "Guidance", "LARGE_OFFERING_SHARE", "direction_supported", "grounded", "guidance_passages",
           "offered_shares", "offering_tags", "pick_documents", "text_of"]
