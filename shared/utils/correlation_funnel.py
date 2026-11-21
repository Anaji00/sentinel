"""What each correlation rule family produces: counts by alert tier, share of all correlations, and how many distinct rules and entities sit behind it, so the list of families
that only ever reach WATCH (and could stop feeding agents) is data, not memory. Rows are per (rule name, tier) from SQL; families are grouped with producer_grades.rule_family.
Report only: nothing here changes which correlations are produced or who reads them (registered change C1, reports/Upgrade pre-registrations - 12 Nov.md). Pure; no I/O.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Mapping, Sequence

from shared.utils.producer_grades import rule_family

# The database's integer alert_tier (shared/models/events.py ALERT_TIER_DB_VALUE): 1 is WATCH, which the alert manager stops (services/alert_manager/main.py:172-174).
TIER_NAMES = {"1": "WATCH", "2": "ALERT", "3": "ELEVATED", "4": "INTELLIGENCE", "5": "CRITICAL"}


def build(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """rows: {rule_name, alert_tier, n, entities}. Returns {total, tier_totals, families:[{family, correlations, share, tier1 (WATCH), tier2 (ALERT), tier3 (ELEVATED), rules, entities_max_per_rule}]}."""
    fam: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"tiers": defaultdict(int), "rules": set(), "ents": 0})
    tier_totals: Dict[int, int] = defaultdict(int)
    total = 0
    for r in rows:
        n, tier = int(r.get("n") or 0), int(r.get("alert_tier") or 0)
        f = fam[rule_family(str(r.get("rule_name") or ""))]
        f["tiers"][tier] += n
        f["rules"].add(str(r.get("rule_name")))
        f["ents"] = max(f["ents"], int(r.get("entities") or 0))
        tier_totals[tier] += n
        total += n
    out: List[Dict[str, Any]] = []
    for name, f in fam.items():
        c = sum(f["tiers"].values())
        out.append({"family": name, "correlations": c, "share": round(c / total, 4) if total else None,
                    "tier1": f["tiers"].get(1, 0), "tier2": f["tiers"].get(2, 0), "tier3": f["tiers"].get(3, 0),
                    "rules": len(f["rules"]), "entities_max_per_rule": f["ents"]})
    out.sort(key=lambda x: -x["correlations"])
    return {"total": total, "tier_names": TIER_NAMES, "tier_totals": {str(k): v for k, v in sorted(tier_totals.items())}, "families": out}


__all__ = ["build"]
