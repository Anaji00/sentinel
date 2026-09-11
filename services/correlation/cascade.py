"""
services/correlation/cascade.py

GEOPOLITICAL FLASHPOINT CASCADE ENGINE
======================================
Tracks sliding 1-hour multi-domain event windows across Cyber, Maritime, Aviation,
TradFi, Crypto, and News Headlines to detect co-occurring compound anomalies.

When 3+ distinct domains exhibit co-occurring anomalies in the same region or
entity cluster within a 1-hour window, emits a composite CorrelationCluster
with an elevated Flashpoint Index.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from shared.models import CorrelationCluster, AlertTier, NormalizedEvent
from shared.models.events import resolve_event_domain

logger = logging.getLogger("correlation.cascade")

# The index this engine computes, expressed as the cluster's confidence.
#
# Every cascade published `confidence_score` unset, so all 963 clusters in a
# measured 24 hours carried the model default of 0.85 -- one distinct value
# across the engine's entire output, while the flashpoint index it computes
# ranged 52.3 to 100.0 and was written only into the prose of `description`.
# Nothing downstream could rank on it and `_tier_supported_by` never saw it.
CASCADE_CONF_CEILING = 0.95

# A single-domain storm is not a cascade by this engine's own definition --
# nothing cascaded, one domain fired repeatedly -- so it cannot claim a
# cascade's confidence. The same reasoning already bars it from CRITICAL.
CASCADE_SINGLE_DOMAIN_FACTOR = 0.70


def _cascade_confidence(flashpoint_index: float, is_multi_domain: bool) -> float:
    """The flashpoint index, carried as a bounded confidence.

    A lead, never a verdict: bounded below 1.0 for the same reason every other
    score in this platform is.
    """
    conf = max(0.0, min(100.0, float(flashpoint_index or 0.0))) / 100.0
    if not is_multi_domain:
        conf *= CASCADE_SINGLE_DOMAIN_FACTOR
    return round(max(0.05, min(CASCADE_CONF_CEILING, conf)), 4)


def _coverage_of(event: NormalizedEvent) -> Optional[float]:
    """The coverage the enricher recorded for this event's score, if any."""
    breakdown = getattr(event, "anomaly_breakdown", None)
    fraction = getattr(breakdown, "coverage_fraction", None) if breakdown else None
    if fraction is None:
        return None
    try:
        return max(0.0, min(1.0, float(fraction)))
    except (TypeError, ValueError):
        return None


def _extract_event_context(event: NormalizedEvent) -> dict:
    """Extract rich context from a NormalizedEvent for the sliding window."""
    pe = event.primary_entity
    entity_name = (pe.name if pe and pe.name else None) or (pe.id if pe else None) or "Unknown"
    entity_type = (pe.type.value if pe and hasattr(pe.type, "value") else str(pe.type) if pe else "unknown")
    event_type = event.type.value if hasattr(event.type, "value") else str(event.type)
    # The canonical domain, not the event type.
    #
    # `domain` here held `event.type.value`, so `domains_present` was a set of
    # event TYPES and every count over it counted types. Two maritime types in
    # one window -- vessel_position and vessel_dark in the Black Sea -- read as
    # two domains, cleared the `len(domains_present) >= 2` cross-domain gate,
    # and were published as a cascade at up to CRITICAL. The flashpoint index's
    # `len(domains_present) * 25.0` term counted them the same way.
    #
    # This is the defect already recorded for the Hawkes tracker, where
    # `.split("_")[0]` minted 29 pseudo-domains against the 8 real ones. The
    # repair there was `resolve_event_domain`; it never reached here.
    domain = resolve_event_domain(event)

    # Build a meaningful headline from available fields, never falling back to bare UUID
    headline = getattr(event, "headline", None) or getattr(event, "summary", None)
    if not headline:
        # Construct a descriptive fallback from entity + event type
        type_label = event_type.replace("_", " ").title()
        headline = f"{type_label}: {entity_name}"
        if event.region:
            headline += f" ({event.region})"

    return {
        "entity_name": str(entity_name),
        "entity_type": entity_type,
        "entity_id": pe.id if pe else "unknown",
        "domain": domain,
        # Kept alongside the domain, because the two answer different
        # questions: the domain decides whether anything cascaded, the type
        # decides what to call the cluster. Collapsing them into one field is
        # what let a single-domain window claim to be cross-domain.
        "event_type": event_type,
        "headline": str(headline)[:200],
        "event_id": str(event.event_id),
        "score": float(getattr(event, "anomaly_score", 0.0) or 0.0),
        # What backed that score, where the enricher said. None when the path
        # does not report it -- absent and zero are different claims.
        "coverage": _coverage_of(event),
        "region": getattr(event, "region", None),
        "summary": str(getattr(event, "summary", "") or "")[:200],
    }


# Domains where a cluster is a claim about the world: someone did something,
# somewhere. A co-occurrence that includes one of these is what "geopolitical"
# was ever meant to describe.
# Canonical domains, as `resolve_event_domain` returns them. This held event
# types -- "vessel_position", "bgp_anomaly" -- mixed with three names ("osint",
# "sanctions", "cyber_incident") that are neither a type nor a domain and could
# never match anything.
_WORLD_DOMAINS = frozenset({
    "news", "maritime", "aviation", "cyber", "macro",
})


# What a single-domain cluster is called, keyed on the domain that actually
# fired. Matched on prefix, because the event vocabulary is
# "<subject>_<observation>" -- vessel_dark, flight_anomaly, bgp_anomaly.
_SINGLE_DOMAIN_LABELS = (
    ("vessel", "maritime_activity_cluster", "Maritime Activity Cluster"),
    ("flight", "aviation_activity_cluster", "Aviation Activity Cluster"),
    ("bgp", "network_activity_cluster", "Network Activity Cluster"),
    ("cyber", "cyber_activity_cluster", "Cyber Activity Cluster"),
    # Ordered before the bare "crypto" prefix: an on-chain transfer is a
    # movement of coins between addresses, not a price event. Labelling a
    # cluster of wallet transfers a "Market Anomaly" says something about
    # markets that the evidence never touched.
    ("crypto_transfer", "onchain_activity_cluster", "On-Chain Activity Cluster"),
    ("crypto_whale", "onchain_activity_cluster", "On-Chain Activity Cluster"),
    ("crypto", "market_anomaly_cluster", "Market Anomaly Cluster"),
    ("market", "market_anomaly_cluster", "Market Anomaly Cluster"),
    ("equity", "market_anomaly_cluster", "Market Anomaly Cluster"),
    ("tradfi", "market_anomaly_cluster", "Market Anomaly Cluster"),
    ("prediction", "market_anomaly_cluster", "Market Anomaly Cluster"),
    ("news", "reporting_cluster", "Reporting Cluster"),
    ("headline", "reporting_cluster", "Reporting Cluster"),
)


def _single_domain_label(domain: str) -> tuple:
    """Names a one-domain cluster after the domain that produced it.

    The first version of this function called every single-domain cluster a
    "Market Anomaly Cluster", because market anomalies were the case being
    fixed. That is the same error it replaced, one step along: four vessel_dark
    events in the Suez Canal were published as a market anomaly, which is no
    more true than calling them a geopolitical cascade. A cluster is named after
    what fired, or it is not named at all.
    """
    lowered = (domain or "").strip().lower()
    for prefix, slug, label in _SINGLE_DOMAIN_LABELS:
        if lowered.startswith(prefix):
            return slug, label
    subject = lowered.split("_")[0] or "signal"
    return f"{subject}_activity_cluster", f"{subject.title()} Activity Cluster"


def _looks_like_identifier(key: str) -> bool:
    """True for a key that is a machine identifier rather than a place name.

    Regions ("black sea", "strait of malacca") read better title-cased; wallet
    addresses, tickers and instrument ids must be shown exactly as written or
    they stop being the thing they identify.
    """
    text = (key or "").strip()
    if not text:
        return False
    if text.lower().startswith("0x"):
        return True
    # No spaces and carrying digits or punctuation: a symbol, not a place.
    return " " not in text and any(c.isdigit() or c in "=-_:." for c in text)


def _classify_cluster(
    domains_present: Set[str], types_present: Set[str], is_multi_domain: bool
) -> tuple:
    """What kind of cluster this is, from what actually fired.

    This was the string "Geopolitical Cascade", unconditionally, for every
    branch and every domain. It reached the operator ("GEOPOLITICAL CASCADE
    DETECTED in 'ethusdt'"), the cluster's rule_name, its description and its
    tags -- and then the scenario generator, where a 1.5B model reading
    "Geopolitical Cascade" above a crypto ticker produced assessments like
    "Geopolitical Cascade Alert in 'Adausdt'". The prompt now carries a
    paragraph telling the model not to believe the detector's name, which is a
    workaround for this function not existing.

    Four correlated ETHUSDT candle anomalies are a market microstructure
    cluster. They are not a geopolitical event, and calling them one costs the
    word its meaning for the cases that are.
    """
    if not is_multi_domain:
        # Named after what actually fired, not after the case that happened to
        # motivate this function.
        #
        # Keyed on the event TYPE, because that is the vocabulary
        # _SINGLE_DOMAIN_LABELS matches on -- "crypto_transfer" and
        # "crypto_trade" are one domain and two very different clusters, and
        # the domain alone cannot tell a wallet movement from a price move.
        return _single_domain_label(next(iter(sorted(types_present)), ""))
    if domains_present & _WORLD_DOMAINS:
        return "geopolitical_cascade", "Geopolitical Cascade"
    return "cross_asset_cascade", "Cross-Asset Cascade"


class GeopoliticalCascadeEngine:
    """
    Sliding-window multi-domain compound event cascade detector.
    Tracks cross-domain co-occurrence across Cyber, Maritime, Aviation,
    Financial Markets, and News Headlines.
    """

    def __init__(self, window_seconds: int = 3600, cooldown_seconds: int = 900, hawkes_tracker=None):
        self.window_seconds = window_seconds
        self.cooldown_seconds = cooldown_seconds
        self.hawkes_tracker = hawkes_tracker
        # Buffer: {region_key: [(timestamp, context_dict)]}
        self._sliding_window: Dict[str, List[tuple]] = {}
        # Cooldown map: {region_key: last_triggered_timestamp}
        self._last_trigger: Dict[str, float] = {}

    def ingest_event(self, event: NormalizedEvent) -> Optional[CorrelationCluster]:
        """
        Ingests a normalized event, prunes expired window entries,
        and checks for multi-domain cascade triggers.
        """
        # Filter out routine position telemetry (vessels & flights with anomaly < 0.30)
        # to prevent chokepoint position flooding from entering the sliding window.
        domain = event.type.value if hasattr(event.type, "value") else str(event.type)
        score = float(getattr(event, "anomaly_score", 0.0) or 0.0)
        if domain in ("vessel_position", "flight_position") and score < 0.30:
            return None

        now = time.time()
        ctx = _extract_event_context(event)
        key = (getattr(event, "region", None) or ctx["entity_id"] or "global").lower()

        # Deduplication Cooldown check: prevent duplicate cascades for the same key within cooldown window
        last_fired = self._last_trigger.get(key, 0)
        if now - last_fired < self.cooldown_seconds:
            return None

        if key not in self._sliding_window:
            self._sliding_window[key] = []

        # Append timestamped context
        self._sliding_window[key].append((now, ctx))

        # Prune expired entries older than window (1 hour)
        cutoff = now - self.window_seconds
        self._sliding_window[key] = [e for e in self._sliding_window[key] if e[0] >= cutoff]

        current_entries = self._sliding_window[key]
        domains_present: Set[str] = {e[1]["domain"] for e in current_entries}
        types_present: Set[str] = {
            e[1].get("event_type") or e[1]["domain"] for e in current_entries
        }

        # Calculate composite Flashpoint Index (0.0 to 100.0)
        avg_score = sum(e[1]["score"] for e in current_entries) / len(current_entries) if current_entries else 0.0
        
        # Hawkes cross-domain intensity boost (if hawkes_tracker present)
        hawkes_boost = 0.0
        if self.hawkes_tracker:
            try:
                # Average excitation ratio across present domains
                ratios = [self.hawkes_tracker.get_excitation_ratio(d, now) for d in domains_present]
                if ratios:
                    avg_hawkes = sum(ratios) / len(ratios)
                    if avg_hawkes > 1.2:
                        hawkes_boost = min(20.0, (avg_hawkes - 1.0) * 10.0)
            except Exception as e:
                logger.debug(f"Hawkes intensity check in cascade engine failed: {e}")

        flashpoint_index = round(min(100.0, (len(domains_present) * 25.0) + (avg_score * 50.0) + hawkes_boost), 1)

        # Tightened Cascade Trigger Rules:
        # 1. Multi-domain: at least 2 distinct domains AND flashpoint_index >= 35.0 AND total events >= 2
        # 2. High-Severity Single Domain: total events >= 4 AND avg_score >= 0.45 AND max score >= 0.60
        max_score = max((e[1]["score"] for e in current_entries), default=0.0)
        
        is_multi_domain_cascade = (len(domains_present) >= 2 and flashpoint_index >= 35.0 and len(current_entries) >= 2)
        is_single_domain_storm = (len(domains_present) == 1 and len(current_entries) >= 4 and avg_score >= 0.45 and max_score >= 0.60)

        if is_multi_domain_cascade or is_single_domain_storm:
            # Mark cooldown timestamp for this key
            self._last_trigger[key] = now

            kind_slug, kind_label = _classify_cluster(
                domains_present, types_present, is_multi_domain_cascade
            )

            # Displayed as written. key.title() was turning a wallet address
            # into 0Xd9695C855Ea4477C3290Dec8Adc8E3F6C5B1C30E -- a string that
            # matches nothing, cannot be pasted into a block explorer, and reads
            # as a different address to anyone comparing by eye. Title case is
            # for prose; an identifier is quoted, never restyled.
            display_key = key if _looks_like_identifier(key) else key.title()

            # Collect rich context for log + description
            top_entries = current_entries[-5:]  # most recent 5
            unique_entities = list(dict.fromkeys(e[1]["entity_name"] for e in current_entries))[:5]
            # Deduplicated. unique_entities already collapses repeats, but the
            # headline list did not, so one vessel reporting twice inside the
            # window appeared as two pieces of evidence -- observed live, with
            # SAGA VOYAGER filling two of three slots in a Taiwan Strait
            # cluster. The same observation seen twice is one observation.
            headlines = list(dict.fromkeys(e[1]["headline"] for e in top_entries))[:3]
            supporting_ids = [e[1]["event_id"] for e in current_entries]
            domain_list = sorted(domains_present)

            logger.warning(
                f"🚨 {kind_label.upper()} DETECTED in '{key}' | "
                f"Flashpoint Index: {flashpoint_index}/100 | "
                f"Domains ({len(domains_present)}): {domain_list} | "
                f"Entities: {unique_entities} | "
                f"Headlines: {headlines}"
            )

            # Build a readable summary
            entity_summary = ", ".join(unique_entities[:3])
            domain_summary = ", ".join(d.replace("_", " ").title() for d in domain_list)
            headline_summary = "; ".join(headlines)

            import uuid
            cluster = CorrelationCluster(
                correlation_id=str(uuid.uuid4()),
                rule_id=f"rule_{kind_slug}_{key.lower().replace(' ', '_')}",
                rule_name=f"{kind_label} ({display_key})",
                # CRITICAL is reserved for a genuine cross-domain cascade.
                #
                # A single-domain cluster is not a cascade by this engine's own
                # definition -- nothing cascaded, one detector fired repeatedly
                # on one entity -- so it cannot reach the tier that pages
                # someone. It was reaching it routinely: four ETHUSDT candle
                # anomalies, the largest a 1.10% move, scored 75.0/100 and went
                # out as CRITICAL, because avg_score sat at ~1.0 where the
                # domain scorers saturate.
                alert_tier=(
                    AlertTier.CRITICAL
                    if (flashpoint_index >= 75.0 and is_multi_domain_cascade)
                    else AlertTier.ELEVATED
                ),
                # The index this engine exists to compute, carried where
                # something can read it.
                #
                # This was left unset, so every cascade published the model
                # default of 0.85 -- 963 clusters in 24 hours, one distinct
                # value -- while flashpoint_index ranged 52.3 to 100.0 and lived
                # only inside the prose of `description`. The engine measured
                # the thing and then threw the measurement away.
                confidence_score=_cascade_confidence(
                    flashpoint_index, is_multi_domain_cascade
                ),
                # The domain the cluster is actually about.
                #
                # main.py fills this with the literal "geopolitical" whenever it
                # is unset, and it was always unset -- so a cluster of wallet
                # transfers and a cluster of BGP withdrawals were both filed as
                # geopolitical, which is the exact error _classify_cluster above
                # was written to stop. That judgement reached the title and not
                # the field every downstream filter reads.
                primary_domain=(
                    next(iter(sorted(domains_present)), None)
                    if len(domains_present) == 1
                    else ("geopolitical" if domains_present & _WORLD_DOMAINS else "cross_asset")
                ),
                # Rankable, rather than only readable.
                metrics_summary={
                    "flashpoint_index": flashpoint_index,
                    "domain_count": len(domains_present),
                    "domains": sorted(domains_present),
                    "event_types": sorted(types_present),
                    "window_events": len(current_entries),
                    "avg_anomaly_score": round(avg_score, 4),
                    # Mean over the entries that reported it, read by the
                    # reasoning budget's ranking. None when none of them did.
                    "evidence_coverage": (
                        round(sum(_covs) / len(_covs), 4) if (_covs := [
                            e[1]["coverage"] for e in current_entries
                            if e[1].get("coverage") is not None
                        ]) else None
                    ),
                    "max_anomaly_score": round(max_score, 4),
                    "hawkes_boost": round(hawkes_boost, 2),
                    "is_multi_domain": is_multi_domain_cascade,
                },
                trigger_event_id=supporting_ids[0],
                supporting_event_ids=supporting_ids[1:],
                primary_entity_id=ctx["entity_id"],
                primary_entity_name=ctx["entity_name"],
                entity_ids=[e[1]["entity_id"] for e in current_entries[:10]],
                entity_names=unique_entities,
                description=(
                    f"{kind_label} (Flashpoint: {flashpoint_index}/100) in '{display_key}'. "
                    f"Entities: {entity_summary}. "
                    f"Domains: {domain_summary}. "
                    f"Activity: {headline_summary}"
                ),
                tags=[kind_slug, key.lower().replace(" ", "_")]
                    + [f"entity:{n}" for n in unique_entities[:3]]
                    + [f"domain:{d}" for d in domain_list]
            )
            return cluster

        return None

