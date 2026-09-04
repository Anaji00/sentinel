"""
services/reasoning/signal_attribution.py

Which of this platform's signals actually predict anything.

The system computes a large feature set -- order-flow imbalance, Kyle's lambda,
volume z-scores, sanctions hits, graph centrality, Hawkes intensity ratios,
corroboration, first-story novelty, cross-domain breadth -- and it has outcome
labels for a growing share of what it publishes. It has never connected the two.

There are 57 hand-set weights and thresholds in the tree. Every one was chosen
by judgement, and judgement is a reasonable way to *start*. None has ever been
checked against an outcome, which means nobody can say which of them is carrying
the system and which is noise wearing a constant. That is the gap this closes:
not by tuning the weights automatically, but by measuring each signal's lift
over the base rate so a person can see which deserve their number.

Deliberately measurement and not optimisation
---------------------------------------------
This reports; it does not rewrite constants. Automatic tuning against a corpus
this size would overfit immediately, and a weight that silently moved would make
every past decision unreproducible. What it produces is evidence: for each
signal, how often the outcome was right when the signal was present, how often
when it was absent, and how much of that difference could be chance.

Reading the output
------------------
`lift` is the difference in success rate between events where the signal was
present and events where it was not. A lift near zero means the signal is not
distinguishing anything, whatever weight it carries. A negative lift means it is
pointing the wrong way, which is worth knowing and impossible to see today.

`support` is how many resolved outcomes the estimate rests on. It is reported
beside every figure, because a lift of 0.4 on nine samples and a lift of 0.04 on
nine thousand are opposite findings and look similar in a table.
"""

import logging
import math
from typing import Any, Dict, List, Optional

logger = logging.getLogger("reasoning.signal_attribution")

# Resolved outcomes required before a signal's lift is reported at all. Below
# this the interval is wider than the effect could possibly be.
MIN_SUPPORT = 30

# And within that, the minimum on each side of the split. A signal present in
# three of four hundred outcomes tells you nothing about its presence.
MIN_SUPPORT_PER_ARM = 10

# How far the corpus is looked back over. Long enough to accumulate outcomes,
# short enough that a signal whose behaviour changed is not averaged with its
# own history.
LOOKBACK_DAYS = 90

# Signals worth attributing, and how to read each from a stored event.
#
# Written down rather than discovered, because a scan over event JSON would
# find hundreds of fields and attribute noise to most of them. These are the
# ones the platform actually spends a weight on.
ATTRIBUTED_SIGNALS: Dict[str, Dict[str, Any]] = {
    "high_anomaly": {
        "sql": "e.anomaly_score >= 0.8",
        "describes": "the detector scored this in its top band",
    },
    "cross_domain": {
        "sql": "(c.metrics_summary->>'domain_count')::int > 1",
        "describes": "the correlation genuinely spanned domains",
    },
    "broad_support": {
        "sql": "coalesce(array_length(c.supporting_event_ids, 1), 0) >= 5",
        "describes": "five or more supporting events",
    },
    "corroborated": {
        "sql": "e.corroboration IS NOT NULL AND (e.corroboration->>'independent_sources')::int > 1",
        "describes": "more than one independent source carried the claim",
    },
    "sanctioned_entity": {
        "sql": "'sanctioned' = ANY(e.tags)",
        "describes": "a sanctions match was involved",
    },
    "watched_entity": {
        "sql": "'watched' = ANY(e.tags) OR 'watched_wallet_transfer' = ANY(e.tags)",
        "describes": "the entity was on a watchlist",
    },
    "reliable_source": {
        "sql": "e.source_reliability >= 0.9",
        "describes": "the feed behind it is structurally reliable",
    },
    "high_confidence": {
        "sql": "c.confidence_score >= 0.7",
        "describes": "the correlation layer was confident",
    },
    "critical_tier": {
        "sql": "c.alert_tier = 'CRITICAL'",
        "describes": "filed at the top severity",
    },
    "options_flow_present": {
        "sql": "'options_flow' = ANY(c.tags)",
        "describes": "options activity was part of the evidence",
    },
}


def _wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple:
    """A confidence interval that behaves at small n and at rates near 0 and 1.

    The normal approximation gives intervals that extend past 1.0 and collapses
    to zero width when every outcome went one way, which is exactly the regime
    a young corpus lives in. Wilson does neither.
    """
    if n <= 0:
        return (0.0, 1.0)
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return (max(0.0, centre - margin), min(1.0, centre + margin))


async def attribute_signals(db_client, lookback_days: int = LOOKBACK_DAYS) -> Dict[str, Any]:
    """Measures each signal's lift over the base rate, from resolved outcomes.

    Joins scenarios (which carry the outcome) to the correlation that produced
    them and the event that triggered it, so a signal recorded at enrichment
    time can be scored against what eventually happened.
    """
    if not db_client:
        return {"available": False, "reason": "no database client"}

    try:
        base_row = await db_client.query(
            f"""
            SELECT count(*) AS n,
                   count(*) FILTER (WHERE s.status = 'confirmed') AS confirmed
            FROM scenarios s
            WHERE s.status IN ('confirmed', 'denied')
              AND s.created_at > NOW() - INTERVAL '{int(lookback_days)} days'
            """
        )
    except Exception as e:
        logger.debug("Signal attribution base query failed: %s", e)
        return {"available": False, "reason": str(e)}

    total = int((base_row or [{}])[0].get("n") or 0)
    confirmed = int((base_row or [{}])[0].get("confirmed") or 0)

    if total < MIN_SUPPORT:
        # Said plainly rather than reported as zeros. A table of null lifts and
        # a table of measured-zero lifts look identical and mean opposite
        # things.
        return {
            "available": False,
            "reason": (
                f"{total} resolved outcomes in {lookback_days} days; "
                f"{MIN_SUPPORT} needed before a lift means anything"
            ),
            "resolved_outcomes": total,
        }

    base_rate = confirmed / float(total)
    results: List[Dict[str, Any]] = []

    for name, spec in ATTRIBUTED_SIGNALS.items():
        try:
            rows = await db_client.query(
                f"""
                SELECT
                    count(*) FILTER (WHERE {spec['sql']})                              AS present_n,
                    count(*) FILTER (WHERE {spec['sql']} AND s.status = 'confirmed')   AS present_ok,
                    count(*) FILTER (WHERE NOT ({spec['sql']}))                        AS absent_n,
                    count(*) FILTER (WHERE NOT ({spec['sql']}) AND s.status = 'confirmed') AS absent_ok
                FROM scenarios s
                JOIN correlations c ON s.correlation_id = c.correlation_id
                LEFT JOIN events e ON e.event_id = c.trigger_event_id
                WHERE s.status IN ('confirmed', 'denied')
                  AND s.created_at > NOW() - INTERVAL '{int(lookback_days)} days'
                """
            )
        except Exception as e:
            # One malformed predicate must not lose the whole report.
            logger.debug("Signal %s could not be attributed: %s", name, e)
            results.append({
                "signal": name,
                "describes": spec["describes"],
                "measurable": False,
                "reason": str(e)[:200],
            })
            continue

        r = (rows or [{}])[0]
        p_n = int(r.get("present_n") or 0)
        p_ok = int(r.get("present_ok") or 0)
        a_n = int(r.get("absent_n") or 0)
        a_ok = int(r.get("absent_ok") or 0)

        if p_n < MIN_SUPPORT_PER_ARM or a_n < MIN_SUPPORT_PER_ARM:
            results.append({
                "signal": name,
                "describes": spec["describes"],
                "measurable": False,
                "reason": (
                    f"present in {p_n}, absent in {a_n}; "
                    f"{MIN_SUPPORT_PER_ARM} needed on each side"
                ),
                "support": p_n + a_n,
            })
            continue

        p_rate = p_ok / float(p_n)
        a_rate = a_ok / float(a_n)
        lo_p, hi_p = _wilson_interval(p_ok, p_n)
        lo_a, hi_a = _wilson_interval(a_ok, a_n)

        results.append({
            "signal": name,
            "describes": spec["describes"],
            "measurable": True,
            "rate_when_present": round(p_rate, 4),
            "rate_when_absent": round(a_rate, 4),
            "lift": round(p_rate - a_rate, 4),
            "support": p_n + a_n,
            "support_present": p_n,
            "support_absent": a_n,
            # Non-overlapping intervals is a weak test and an honest one: it
            # will not call a difference real that a reader could not also see.
            "intervals_disjoint": bool(lo_p > hi_a or lo_a > hi_p),
            "interval_present": [round(lo_p, 4), round(hi_p, 4)],
            "interval_absent": [round(lo_a, 4), round(hi_a, 4)],
        })

    measurable = [r for r in results if r.get("measurable")]
    measurable.sort(key=lambda r: -abs(r["lift"]))

    return {
        "available": True,
        "lookback_days": lookback_days,
        "resolved_outcomes": total,
        "base_rate": round(base_rate, 4),
        "signals": measurable + [r for r in results if not r.get("measurable")],
        "carrying": [r["signal"] for r in measurable
                     if r["lift"] > 0.05 and r["intervals_disjoint"]],
        "inverted": [r["signal"] for r in measurable
                     if r["lift"] < -0.05 and r["intervals_disjoint"]],
        "not_distinguishing": [r["signal"] for r in measurable
                               if abs(r["lift"]) <= 0.05],
    }
