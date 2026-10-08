"""
shared/utils/signal_families.py

What kind of signal an event is, which instruments it points at, and which way
it is expected to move them -- the input to the reaction study and the signal
engine.

An event becomes zero or more `Observation`s: (family, instrument, prior
direction). Observations of one family on one instrument close together in
time are one `Episode`: fifty headlines about one attack are one piece of
evidence, not fifty, and counting them separately is the commonest way a
backtest finds an edge that is not there.

Prior directions are hypotheses with a stated reason. Families with no sound
prior carry 0 and let the data decide; families whose direction comes from the
tape (a volume spike on a rising price) take it from the bars at the moment the
event became known, never after (see `PRICE_DIRECTED`).

Pure: rows in, observations out.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.utils.conflict_zones import REGION_ZONES, escalation_direction
from shared.utils.headline_mapping import conflict_polarity, map_headline
from shared.utils.osint_bridges import text_exposure_tags

# Sentiment past which a headline counts as clearly positive or negative.
SENTIMENT_THRESHOLD = 0.3
# Options flow worth counting: smaller sweeps are noise.
MIN_OPTIONS_PREMIUM_USD = 50_000.0
MIN_OPTIONS_VOL_OI = 1.0
# A volume spike this strong or more.
MIN_VOLUME_Z = 4.0

# Families whose direction is read from the price tape at the knowable time.
PRICE_DIRECTED = frozenset({
    "volume_spike", "block_trade",
    # 8-K items with no reliable sign of their own: the study directs them by
    # the price move into the filing and measures continuation or reversal.
    "8k_officer_change", "8k_material_agreement", "8k_acquisition_completed",
})

# 8-K items read as signals, with their prior direction. Negative priors follow
# the event-study record: Item 4.02 non-reliance averages about -1.1% the next
# day and -2% over 20 days across 8,000+ filings (2004-2023); delisting
# notices, bankruptcy, impairments, auditor changes and unregistered equity
# sales (dilution) are adverse by construction. CEO departures move a stock
# about 1.3x its normal daily range against 1.06x for routine appointments, but
# without a reliable sign -- so those are price-directed above.
EIGHT_K_ITEMS: Dict[str, Tuple[str, int]] = {
    "4.02": ("8k_nonreliance", -1),
    "3.01": ("8k_delisting_notice", -1),
    "1.03": ("8k_bankruptcy", -1),
    "2.06": ("8k_impairment", -1),
    "4.01": ("8k_auditor_change", -1),
    "3.02": ("8k_dilution", -1),
    "5.02": ("8k_officer_change", 0),
    "1.01": ("8k_material_agreement", 0),
    "2.01": ("8k_acquisition_completed", 0),
}

# How far apart two observations may be and still be one episode.
EPISODE_GAP: Dict[str, timedelta] = {
    "insider_cluster": timedelta(days=14),
    "activist_13d": timedelta(days=14),
    "institutional_accumulation": timedelta(days=60),
    "institutional_distribution": timedelta(days=60),
    "contract_award": timedelta(days=3),
    "earnings_surprise_positive": timedelta(days=5),
    "earnings_surprise_negative": timedelta(days=5),
    **{fam: timedelta(days=2) for fam, _d in EIGHT_K_ITEMS.values()},
}
DEFAULT_EPISODE_GAP = timedelta(hours=12)

# Item codes as the filings collector writes them: "4.02: Non-Reliance...".
_EIGHT_K_CODE = re.compile(r"\b(\d\.\d\d):")

# Plain-language description of each family, for the provenance card.
FAMILY_DESCRIPTIONS: Dict[str, str] = {
    "zone_escalation": "Escalation reported in a conflict zone, on the instruments the zone moves",
    "zone_deescalation": "A ceasefire or truce in a conflict zone; expected moves reversed",
    "sector_news_positive": "Clearly positive news about a sector",
    "sector_news_negative": "Clearly negative news about a sector",
    "country_news_positive": "Clearly positive economic or political news about a country",
    "country_news_negative": "Clearly negative economic or political news about a country",
    "company_news_positive": "Clearly positive news naming a company",
    "company_news_negative": "Clearly negative news naming a company",
    "dark_vessel_in_zone": "A vessel's transponder going dark inside a conflict zone",
    "tanker_sts_in_zone": "A ship-to-ship transfer involving a tanker inside a conflict zone",
    "radar_no_ais": "Radar returns where AIS saw no vessel (ships not transmitting)",
    "radar_traffic_drop": "Radar traffic through a chokepoint well below its baseline",
    "activist_13d": "An activist crossing 5% under Schedule 13D",
    "insider_cluster": "Two or more insiders buying on the open market within 14 days",
    "contract_award": "A federal contract award to a listed company",
    "earnings_surprise_positive": "Earnings above estimates",
    "earnings_surprise_negative": "Earnings below estimates",
    "options_call_sweep": "A large call sweep (premium and volume over open interest)",
    "options_put_sweep": "A large put sweep",
    "volume_spike": "A volume spike, directed by the price move into it",
    "block_trade": "A large block trade, directed by the price move into it",
    "institutional_accumulation": "Three or more active managers opening or adding 25%+ to a position in one quarter",
    "institutional_distribution": "Three or more active managers cutting 25%+ or exiting a position in one quarter",
    "8k_nonreliance": "8-K Item 4.02: prior financial statements can no longer be relied on",
    "8k_delisting_notice": "8-K Item 3.01: notice of delisting or failure to meet a listing rule",
    "8k_bankruptcy": "8-K Item 1.03: bankruptcy or receivership",
    "8k_impairment": "8-K Item 2.06: material impairment",
    "8k_auditor_change": "8-K Item 4.01: change in the company's auditor",
    "8k_dilution": "8-K Item 3.02: unregistered sale of equity",
    "8k_officer_change": "8-K Item 5.02: a director or principal officer departs or is appointed",
    "8k_material_agreement": "8-K Item 1.01: entry into a material definitive agreement",
    "8k_acquisition_completed": "8-K Item 2.01: completion of an acquisition or disposition",
    "short_volume_spike": "Short share of volume at least two standard deviations above the name's own norm",
}


@dataclass(frozen=True)
class Observation:
    family: str
    instrument: str
    direction: int            # prior: +1 up, -1 down, 0 none stated
    known_at: datetime        # when the platform knew -- never earlier
    event_id: str
    key: str                  # what makes two observations the same story


@dataclass
class Episode:
    family: str
    instrument: str
    direction: int
    known_at: datetime
    key: str
    event_ids: List[str] = field(default_factory=list)


def _tags(row: Dict[str, Any]) -> List[str]:
    return [str(t) for t in (row.get("tags") or [])]


def _exposed(row: Dict[str, Any]) -> List[str]:
    return [t[len("exposes:"):] for t in _tags(row) if t.startswith("exposes:")]


def _zones(row: Dict[str, Any]) -> List[str]:
    """Zones from the event's tags, or from its region for events stored before
    zone tagging began on 5 October."""
    tagged = [t[len("zone:"):] for t in _tags(row) if t.startswith("zone:")]
    return tagged or list(REGION_ZONES.get(str(row.get("region") or ""), ()))


def _fd(row: Dict[str, Any]) -> Dict[str, Any]:
    fd = row.get("financial_data") or {}
    return fd if isinstance(fd, dict) else {}


def _num(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _known_at(row: Dict[str, Any]) -> Optional[datetime]:
    """When the platform could have acted: ingestion, not the event's own clock.

    A filing dated last Tuesday and collected today was not knowable on
    Tuesday, and measuring from Tuesday is look-ahead.
    """
    for k in ("created_at", "collected_at", "occurred_at"):
        v = row.get(k)
        if isinstance(v, datetime):
            return v
        if v:
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except ValueError:
                continue
    return None


def _ticker(row: Dict[str, Any]) -> Optional[str]:
    t = _fd(row).get("ticker") or row.get("primary_entity_id")
    t = str(t or "").strip().upper()
    return t or None


def observations(row: Dict[str, Any]) -> List[Observation]:
    """Every (family, instrument, prior direction) an event row stands for."""
    when = _known_at(row)
    if when is None:
        return []
    etype = str(row.get("type") or "")
    source = str(row.get("source") or "")
    eid = str(row.get("event_id") or "")
    tags = _tags(row)
    out: List[Observation] = []

    def add(family: str, sym: str, direction: int, key_part: str) -> None:
        out.append(Observation(family, sym, direction, when, eid, f"{family}|{key_part}|{sym}"))

    if etype in ("headline", "social_signal"):
        text = str(row.get("headline") or "")
        affected = map_headline(text)
        polarity = conflict_polarity(text)
        mapped = {a.symbol for a in affected}
        for a in affected:
            kind, _, name = a.via.partition(":")
            if kind == "zone" and polarity:
                fam = "zone_escalation" if polarity > 0 else "zone_deescalation"
                add(fam, a.symbol, polarity * escalation_direction(a.symbol), name)
        sentiment = _num(row.get("sentiment"))
        if sentiment is not None and abs(sentiment) >= SENTIMENT_THRESHOLD:
            sign = 1 if sentiment > 0 else -1
            suffix = "positive" if sign > 0 else "negative"
            for a in affected:
                kind, _, name = a.via.partition(":")
                if kind in ("sector", "country"):
                    add(f"{kind}_news_{suffix}", a.symbol, sign, name)
            # Companies the enricher named, net of the sector/zone/country and
            # commodity mappings: what is left is a named company.
            commodity = {t[len("exposes:"):] for t in text_exposure_tags(text)}
            for sym in _exposed(row):
                if sym in mapped or sym in commodity or sym.endswith("=F"):
                    continue
                if "named_company" in tags or "resolved_tickers" in tags:
                    add(f"company_news_{suffix}", sym, sign, "company")
        return out

    if etype in ("vessel_dark", "vessel_sts") and _zones(row):
        fam = "dark_vessel_in_zone" if etype == "vessel_dark" else "tanker_sts_in_zone"
        for sym in _exposed(row):
            add(fam, sym, escalation_direction(sym), _zones(row)[0])
        return out

    if etype == "supply_chain_metric":
        region = str(row.get("region") or "")
        if "radar_no_ais" in tags:
            for sym in _exposed(row):
                add("radar_no_ais", sym, escalation_direction(sym), region)
        if "quieter_than_usual" in tags:
            for sym in _exposed(row):
                add("radar_traffic_drop", sym, escalation_direction(sym), region)
        return out

    ticker = _ticker(row)
    if source == "sec_13d" and "activist" in tags:
        for sym in _exposed(row) or ([ticker] if ticker else []):
            add("activist_13d", sym, 1, "13d")
        return out

    if etype == "insider_cluster" and ticker:
        add("insider_cluster", ticker, 1, "insiders")
        return out

    # Diether, Lee & Werner (2009): increases in short selling predict negative
    # abnormal returns. A spike against the name's own history, from FINRA.
    if source == "finra_short_volume" and "short_volume_spike" in tags and ticker:
        add("short_volume_spike", ticker, -1, "finra")
        return out

    if etype == "filing" and ticker:
        summary = str(row.get("summary") or "")
        if summary.startswith("Material 8-K Event"):
            for code in sorted(set(_EIGHT_K_CODE.findall(summary))):
                if code in EIGHT_K_ITEMS:
                    fam, direction = EIGHT_K_ITEMS[code]
                    add(fam, ticker, direction, code)
        return out

    if source == "usaspending":
        for sym in _exposed(row):
            add("contract_award", sym, 1, "award")
        return out

    if etype == "earnings_surprise" and ticker:
        s = _num(_fd(row).get("eps_surprise_pct"))
        if s is not None and abs(s) >= 1.0:
            fam = "earnings_surprise_positive" if s > 0 else "earnings_surprise_negative"
            add(fam, ticker, 1 if s > 0 else -1, "earnings")
        return out

    if etype == "options_flow" and ticker:
        fd = _fd(row)
        premium = _num(fd.get("premium_usd")) or 0.0
        vol_oi = _num(fd.get("volume_oi_ratio")) or 0.0
        kind = str(fd.get("option_type") or "").upper()
        if premium >= MIN_OPTIONS_PREMIUM_USD and vol_oi >= MIN_OPTIONS_VOL_OI and kind in ("CALL", "PUT"):
            fam = "options_call_sweep" if kind == "CALL" else "options_put_sweep"
            add(fam, ticker, 1 if kind == "CALL" else -1, "options")
        return out

    if etype == "market_anomaly" and source == "alpaca_quant_radar" and ticker:
        z = _num(_fd(row).get("z_score"))
        if z is not None and z >= MIN_VOLUME_Z:
            add("volume_spike", ticker, 0, "volume")
        return out

    if etype == "equity_block" and ticker:
        add("block_trade", ticker, 0, "block")
        return out

    if etype == "thirteen_f" and ticker:
        if "institutional_accumulation" in tags:
            add("institutional_accumulation", ticker, 1, "13f")
        elif "institutional_distribution" in tags:
            add("institutional_distribution", ticker, -1, "13f")
        return out
    return out


def episodes(obs: Iterable[Observation]) -> List[Episode]:
    """Observations grouped into independent episodes, earliest first.

    Within one key (family, story, instrument), an observation within the
    family's gap of the previous one joins its episode; the episode's time is
    its first observation's, which is the only moment it could have been
    traded on.
    """
    by_key: Dict[str, List[Observation]] = {}
    for o in obs:
        by_key.setdefault(o.key, []).append(o)
    out: List[Episode] = []
    for key, items in by_key.items():
        items.sort(key=lambda o: o.known_at)
        gap = EPISODE_GAP.get(items[0].family, DEFAULT_EPISODE_GAP)
        current: Optional[Episode] = None
        last: Optional[datetime] = None
        for o in items:
            if current is None or last is None or o.known_at - last > gap:
                current = Episode(o.family, o.instrument, o.direction, o.known_at, key, [o.event_id])
                out.append(current)
            else:
                current.event_ids.append(o.event_id)
            last = o.known_at
    out.sort(key=lambda e: e.known_at)
    return out


def backfill_universe() -> List[str]:
    """Every listed equity or ETF a signal family can point at, plus benchmarks.

    The reaction study can only measure an instrument it has prices for, and
    the live market stream has fifty slots. Historical bars for these are
    loaded on a schedule instead, so a signal's instrument has a history
    whether or not it is being streamed. Futures are priced elsewhere.
    """
    from shared.utils.conflict_zones import ZONES
    from shared.utils.headline_mapping import COUNTRY_ETFS, THEMES
    from shared.utils.osint_bridges import CRYPTO_EQUITIES
    from shared.utils.ownership import DEFENSE_PRIMES

    syms = {"SPY", "QQQ"}
    for z in ZONES:
        syms.update(z.instruments)
    for theme in THEMES:
        syms.update(theme.instruments)
    for _iso, _pattern, etfs in COUNTRY_ETFS:
        syms.update(etfs)
    syms.update(DEFENSE_PRIMES)
    for names in CRYPTO_EQUITIES.values():
        syms.update(names)
    return sorted(s for s in syms if not s.endswith("=F"))


def instrument_families(eps: Iterable[Episode]) -> Dict[Tuple[str, str], int]:
    """Episode counts per (family, instrument), for reporting."""
    counts: Dict[Tuple[str, str], int] = {}
    for e in eps:
        counts[(e.family, e.instrument)] = counts.get((e.family, e.instrument), 0) + 1
    return counts


__all__ = [
    "DEFAULT_EPISODE_GAP", "EPISODE_GAP", "backfill_universe", "Episode", "FAMILY_DESCRIPTIONS", "MIN_OPTIONS_PREMIUM_USD",
    "MIN_VOLUME_Z", "Observation", "PRICE_DIRECTED", "SENTIMENT_THRESHOLD", "episodes",
    "instrument_families", "observations",
]
