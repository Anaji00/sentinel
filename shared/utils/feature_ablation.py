"""
shared/utils/feature_ablation.py

Pure helpers for the registered feature trials C3 (ablate llm_news_30 and ev_comment_letter) and B1 (score analyst price-target headlines
with a rule, not a model call). See "Upgrade pre-registrations - 12 Nov", areas B and C. Nothing here touches the live model, the news job
or the database: headlines and scores in, scores, feature rows and verdict numbers out.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from shared.utils import llm_news as ln

# Written down before the run (B1). Case-insensitive; "..." in the registration is any text between the two parts.
RAISE_RE = re.compile(r"\b(?:raises|lifts|boosts)\b.*\bprice\s+target", re.IGNORECASE)
LOWER_RE = re.compile(r"\b(?:lowers|cuts)\b.*\bprice\s+target", re.IGNORECASE)
UPGRADE_RE = re.compile(r"\bupgrades\b", re.IGNORECASE)
DOWNGRADE_RE = re.compile(r"\bdowngrades\b", re.IGNORECASE)
CLASSES = ("raise_pt", "lower_pt", "upgrade", "downgrade")

BAR_C3_COST = 0.03          # C3: removal must cost at least this much net Sharpe ...
BAR_B1_TOLERANCE = 0.02     # B1: the rule may be worse by at most this much ...
BAR_CALENDARS = 6           # ... on at least this many of the 8 calendars
BAR_B1_CALL_CUT = 0.20      # B1: and it must cut model calls by at least this share


def classes_matched(headline: str) -> List[str]:
    """Every registered pattern the headline matches, in CLASSES order."""
    text = headline or ""
    hits = {"raise_pt": RAISE_RE.search(text), "lower_pt": LOWER_RE.search(text),
            "upgrade": UPGRADE_RE.search(text), "downgrade": DOWNGRADE_RE.search(text)}
    return [c for c in CLASSES if hits[c]]


_SIGN = {"raise_pt": 1, "lower_pt": -1, "upgrade": 1, "downgrade": -1}


def rule_class(headline: str) -> Optional[str]:
    """The headline's class when exactly one sign is implied; None when nothing matches or the patterns disagree
    ("Upgrades ... Lowers Price Target": left to the model, the registration's 'no other change')."""
    hit = classes_matched(headline)
    return hit[0] if hit and len({_SIGN[c] for c in hit}) == 1 else None


def rule_score(headline: str) -> Optional[int]:
    """+1 or -1 for a matching headline, None for any other (its model score stands)."""
    c = rule_class(headline)
    return _SIGN[c] if c else None


def rescored(rows: Iterable[Tuple[str, datetime, str, int]]) -> List[Tuple[str, datetime, int]]:
    """(ticker, created_at, headline, model score) -> version 2 (ticker, created_at, score): the rule's score where it matches,
    the stored score untouched elsewhere."""
    out: List[Tuple[str, datetime, int]] = []
    for t, at, headline, score in rows:
        r = rule_score(headline)
        out.append((t, at, int(score) if r is None else r))
    return out


def rebuild_feature(scores: Iterable[Tuple[str, datetime, int]], day: date) -> Dict[str, float]:
    """{ticker: llm_news_30} on `day`, built exactly as the job builds it (llm_news.feature_rows)."""
    return {t: v for t, _d, _f, v in ln.feature_rows(scores, day)}


def agreement(rows: Iterable[Tuple[str, datetime, str, int]]) -> Dict[str, Dict[str, float]]:
    """Per matched class: how many headlines, and the share of stored model scores equal to the rule's sign, zero, or opposite to it."""
    acc: Dict[str, List[int]] = {c: [] for c in CLASSES}
    for _t, _at, headline, score in rows:
        c = rule_class(headline)
        if c:
            acc[c].append(int(score) * _SIGN[c])
    out: Dict[str, Dict[str, float]] = {}
    for c, v in acc.items():
        n = len(v)
        out[c] = {"n": n, "agree": sum(x == 1 for x in v) / n if n else float("nan"),
                  "zero": sum(x == 0 for x in v) / n if n else float("nan"),
                  "opposite": sum(x == -1 for x in v) / n if n else float("nan")}
    return out


def matched_share(headlines: Sequence[str]) -> Tuple[int, int, float]:
    """(matched, total, share) over the headlines the news job would score."""
    n = len(headlines)
    m = sum(1 for h in headlines if rule_score(h) is not None)
    return m, n, (m / n if n else float("nan"))


def ablate(columns: Sequence[str], name: str) -> List[str]:
    """The feature list without exactly `name`. A name that is not in the list raises: an ablation of a column the model never had
    is a no-op, and the caller must say so instead of reporting a null result."""
    if name not in columns:
        raise KeyError(f"{name} is not a column of the feature list")
    return [c for c in columns if c != name]


def is_model_input(name: str, columns: Sequence[str]) -> bool:
    return name in columns


def paired_diffs(variant: Mapping[int, float], base: Mapping[int, float]) -> Dict[int, float]:
    """{offset: variant - base} over calendars where both Sharpes exist and are finite."""
    out: Dict[int, float] = {}
    for o, v in variant.items():
        b = base.get(o)
        if b is not None and v == v and b == b:
            out[o] = float(v) - float(b)
    return out


def c3_verdict(ablated_minus_full: Mapping[int, float], n_calendars: int = 8) -> Dict[str, object]:
    """Keep the calls only if removal costs at least BAR_C3_COST (ablated - full <= -0.03) on at least BAR_CALENDARS of the calendars."""
    need = min(BAR_CALENDARS, n_calendars)
    costly = sum(1 for d in ablated_minus_full.values() if d <= -BAR_C3_COST)
    keep = len(ablated_minus_full) >= need and costly >= need
    return {"calendars": len(ablated_minus_full), "costly": costly, "need": need, "keep_calls": bool(keep),
            "mean_diff": (sum(ablated_minus_full.values()) / len(ablated_minus_full)) if ablated_minus_full else float("nan")}


def b1_verdict(rule_minus_model: Mapping[int, float], call_cut: float, n_calendars: int = 8) -> Dict[str, object]:
    """Adopt only if the rule is not worse by more than BAR_B1_TOLERANCE on at least BAR_CALENDARS calendars AND the call cut is at least
    BAR_B1_CALL_CUT."""
    need = min(BAR_CALENDARS, n_calendars)
    ok = sum(1 for d in rule_minus_model.values() if d >= -BAR_B1_TOLERANCE)
    sharpe_ok = len(rule_minus_model) >= need and ok >= need
    cut_ok = call_cut == call_cut and call_cut >= BAR_B1_CALL_CUT
    return {"calendars": len(rule_minus_model), "not_worse": ok, "need": need, "sharpe_ok": bool(sharpe_ok), "call_cut": call_cut,
            "cut_ok": bool(cut_ok), "adopt": bool(sharpe_ok and cut_ok)}


__all__ = ["BAR_B1_CALL_CUT", "BAR_B1_TOLERANCE", "BAR_C3_COST", "BAR_CALENDARS", "CLASSES", "DOWNGRADE_RE", "LOWER_RE", "RAISE_RE",
           "UPGRADE_RE", "ablate", "agreement", "b1_verdict", "c3_verdict", "classes_matched", "is_model_input", "matched_share",
           "paired_diffs", "rebuild_feature", "rescored", "rule_class", "rule_score"]
