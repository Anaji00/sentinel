"""Which stocks have independent evidence pointing the same way, and how good is it?

Every factor the platform watches -- option sweeps, blocks, insider clusters,
13F flows, mapped headlines, zone escalation, volume anomalies -- arrived as a
separate event, and nothing asked the trader's question across them. The
signal engine already knows how to read each event as (family, instrument,
direction) and already grades every family against forward returns; it used
both only to decide pass or fail. This puts them together per instrument.

Weighting is by the family's own record in the reaction study, not by taste:

  proven        1.0   passed every gate at some horizon
  unstudied     0.25  not yet measurable (too few independent units, no bars)
  no edge       0.0   measured, and did not beat costs or chance -- shown, not counted

A family counts once however many events it produced, so one story repeated by
twenty outlets is one factor. And families that read the same underlying source
count once together: a dark vessel and a ship-to-ship transfer in the Taiwan
zone are one observation of shipping there, not two independent votes, and
"zone escalation" and "sector news" from one headline are one headline. Live
on 5 October the first build ranked SOXX short on exactly that pair.
Price-directed families (volume spikes, blocks) carry no prior direction; they
mark attention, not a side.

This is a ranking of evidence, not a trade. A stance built only from unstudied
families says so.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

PROVEN_WEIGHT = 1.0
UNSTUDIED_WEIGHT = 0.25
NO_EDGE_WEIGHT = 0.0

# Event types the evidence layer reads; kept equal to the signal engine's list
# (a test holds them together).
EVIDENCE_EVENT_TYPES = [
    "headline", "social_signal", "vessel_dark", "vessel_sts", "supply_chain_metric",
    "filing", "regulatory_event", "insider_cluster", "earnings_surprise", "options_flow",
    "market_anomaly", "equity_block", "thirteen_f",
]

# The same admission the signal engine applies: radar anomalies only, and
# option prints with real premium.
EVIDENCE_EVENTS_SQL = """
    SELECT event_id::text AS event_id, type, source, created_at, occurred_at, headline,
           summary, tags, sentiment, primary_entity_id, financial_data, region
    FROM events
    WHERE created_at > $1 AND created_at <= $2
      AND type = ANY($3::text[])
      AND NOT (type = 'market_anomaly' AND source NOT IN ('alpaca_quant_radar', 'finra_short_volume'))
      AND NOT (type = 'options_flow'
               AND COALESCE((financial_data->>'premium_usd')::float, 0) < 50000)
"""


def family_record(cells: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """The best-evidenced study cell per family: a proven horizon if any, else
    the one with the most independent units."""
    best: Dict[str, Dict[str, Any]] = {}
    for c in cells:
        fam = c.get("family")
        if not fam:
            continue
        cur = best.get(fam)
        rank = (c.get("verdict") == "proven", int(c.get("units") or 0))
        if cur is None or rank > (cur.get("verdict") == "proven", int(cur.get("units") or 0)):
            best[fam] = c
    return best


def status_of(record: Optional[Dict[str, Any]]) -> str:
    if not record:
        return "unstudied"
    v = record.get("verdict")
    if v == "proven":
        return "proven"
    if v == "no_edge":
        return "no_edge"
    return "unstudied"


WEIGHTS = {"proven": PROVEN_WEIGHT, "unstudied": UNSTUDIED_WEIGHT, "no_edge": NO_EDGE_WEIGHT}

# Families that read one underlying source; agreement is counted per source.
SOURCE_OF = {
    "dark_vessel_in_zone": "shipping", "tanker_sts_in_zone": "shipping",
    "radar_no_ais": "shipping", "radar_traffic_drop": "shipping",
    "supply_chain_metric": "shipping",
    "zone_escalation": "news", "zone_deescalation": "news",
    "options_call_sweep": "options", "options_put_sweep": "options",
    "volume_spike": "tape", "block_trade": "tape",
    "institutional_accumulation": "13f", "institutional_distribution": "13f",
    "insider_cluster": "insiders",
    "contract_award": "contracts",
    "short_volume_spike": "shorts",
}
_SOURCE_PREFIXES = (
    ("sector_news_", "news"), ("country_news_", "news"), ("company_news_", "news"),
    ("earnings_surprise", "earnings"),
    ("8k_", "filings"),
)


def source_of(family: str) -> str:
    if family in SOURCE_OF:
        return SOURCE_OF[family]
    for prefix, src in _SOURCE_PREFIXES:
        if family.startswith(prefix):
            return src
    return family


@dataclass
class Factor:
    family: str
    source: str
    direction: int
    events: int
    last_seen: str
    status: str
    weight: float
    study: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Dossier:
    instrument: str
    factors: List[Factor]
    score: float
    stance: str
    agreeing: int
    opposing: int
    attention: int
    proven: int
    summary: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "instrument": self.instrument,
            "score": self.score,
            "stance": self.stance,
            "agreeing_families": self.agreeing,
            "opposing_families": self.opposing,
            "attention_families": self.attention,
            "proven_families": self.proven,
            "summary": self.summary,
            "factors": [f.__dict__ for f in self.factors],
        }


def _iso(v: Any) -> str:
    return v.isoformat() if isinstance(v, datetime) else str(v)


def build_dossiers(observations: Iterable[Any], records: Dict[str, Dict[str, Any]]) -> Dict[str, Dossier]:
    """Group observations by instrument and family, then weigh each family once."""
    grouped: Dict[str, Dict[str, List[Any]]] = {}
    for o in observations:
        grouped.setdefault(o.instrument.upper(), {}).setdefault(o.family, []).append(o)

    out: Dict[str, Dossier] = {}
    for inst, fams in grouped.items():
        factors: List[Factor] = []
        for fam, obs in fams.items():
            dirs = [o.direction for o in obs if o.direction]
            net = sum(dirs)
            direction = 1 if net > 0 else -1 if net < 0 else 0
            rec = records.get(fam)
            status = status_of(rec)
            study = {}
            if rec:
                study = {k: rec.get(k) for k in ("verdict", "horizon", "units", "hit_rate", "mean_bps", "reason") if rec.get(k) is not None}
            factors.append(Factor(
                family=fam, source=source_of(fam), direction=direction, events=len(obs),
                last_seen=_iso(max(o.known_at for o in obs)),
                status=status, weight=WEIGHTS[status], study=study,
            ))
        # One vote per source: its weighted net direction, at the weight of its
        # best-evidenced family.
        sources: Dict[str, List[Factor]] = {}
        for f in factors:
            sources.setdefault(f.source, []).append(f)
        votes: List[tuple] = []
        for src, fs in sources.items():
            net = sum(f.weight * f.direction for f in fs)
            w = max((f.weight for f in fs if f.direction), default=0.0)
            d = 1 if net > 0 else -1 if net < 0 else 0
            votes.append((src, d, w))
        score = round(sum(d * w for _s, d, w in votes), 3)
        directional = [(src, d) for src, d, w in votes if d and w > 0]
        side = 1 if score > 0 else -1 if score < 0 else 0
        agreeing = sum(1 for _s, d in directional if d == side) if side else 0
        opposing = sum(1 for _s, d in directional if side and d == -side)
        attention = sum(1 for src, fs in sources.items() if all(f.direction == 0 for f in fs))
        proven = sum(1 for f in factors if f.status == "proven")
        if side == 0:
            stance = "attention" if attention or directional else "none"
        elif opposing:
            stance = "mixed"
        else:
            stance = "long" if side > 0 else "short"
        basis = "proven evidence" if proven and any(f.status == "proven" and f.direction == side for f in factors) else "unproven evidence only"
        names = ", ".join(sorted(f.family for f in factors))
        summary = (
            f"{inst}: {stance} on {agreeing} agreeing independent source{'' if agreeing == 1 else 's'}"
            f"{f', {opposing} opposing' if opposing else ''}"
            f"{f', {attention} attention' if attention else ''} -- {basis}. Families: {names}."
        )
        factors.sort(key=lambda f: (-f.weight, -abs(f.direction), f.family))
        out[inst] = Dossier(inst, factors, score, stance, agreeing, opposing, attention, proven, summary)
    return out


def convergence(dossiers: Dict[str, Dossier], min_agreeing: int = 2, limit: int = 25) -> List[Dossier]:
    """Instruments where at least `min_agreeing` independent directional families
    agree, strongest evidence first."""
    picked = [d for d in dossiers.values() if d.agreeing >= min_agreeing]
    picked.sort(key=lambda d: (d.proven, abs(d.score), d.agreeing, -d.opposing, d.attention), reverse=True)
    return picked[:limit]


__all__ = [
    "Dossier", "EVIDENCE_EVENT_TYPES", "EVIDENCE_EVENTS_SQL", "Factor",
    "SOURCE_OF", "build_dossiers", "convergence", "family_record", "source_of", "status_of",
]
