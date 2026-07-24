"""
services/enrichment/enrichers/news.py

Handles RSS/news raw events from collector-news.
Runs spaCy NER to extract named entities.
Scores sentiment and anomaly.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List
import re
import spacy

from shared.models import NormalizedEvent, EventType, Entity, EntityType
from shared.kafka import Topics
from shared.utils.sanctions import check_sanctions
from shared.utils.equities import is_valid_primary_equity

logger = logging.getLogger("enrichment.news")

COMPANY_SUFFIX_PATTERN = re.compile(
    r"\b(Inc|Corp|Corporation|Co|Company|Ltd|Limited|LLC|PLC|SA|AG|NV|SE|Holdings|Group|"
    r"Technologies|Tech|Therapeutics|Pharmaceuticals|Pharma|Biotech|Capital|Partners|Bank|"
    r"Financial|Motors|Airlines|Energy|Resources|Mining|Semiconductor|Systems|Software|Labs|"
    r"Brands|Enterprises|Stores|Aviation|Maritime)\b",
    re.IGNORECASE
)

GOV_COUNTRY_KEYWORDS = {
    "US", "USA", "UNITED STATES", "CHINA", "RUSSIA", "NATO", "EU", "UN", "UKRAINE", 
    "IRAN", "ISRAEL", "TAIWAN", "FED", "SEC", "TREASURY", "PENTAGON", "KREMLIN", "WHITE HOUSE", "CISA"
}

# Dynamic Stock Ticker & Cashtag Extractor Pattern:
# Matches $NVDA, (AAPL), [TSLA], NASDAQ: MSFT, NYSE: BABA, etc.
RE_TICKER_EXTRACT = re.compile(
    r"\b\$([A-Z]{1,5})\b|\((?:NASDAQ|NYSE|AMEX|INDEX):\s*([A-Z]{1,5})\)|\b(?:NASDAQ|NYSE|AMEX):\s*([A-Z]{1,5})\b|\(([A-Z]{1,5})\)|\[([A-Z]{1,5})\]",
    re.IGNORECASE
)

def resolve_news_entity_type(name: str, source: str) -> EntityType:
    """Dynamically classifies entity names into EntityType (ticker symbols, corporations, countries, media)."""
    if not name:
        return EntityType.UNKNOWN
    
    clean = name.strip("$,.():;[]{}'\"").strip()
    clean_upper = clean.upper()

    # 1. Dynamic Stock Ticker / Equity Instrument Check (e.g. $NVDA, AAPL, TSLA, BTC)
    if is_valid_primary_equity(clean_upper) or (clean.isupper() and 1 <= len(clean) <= 5 and clean.isalpha()):
        return EntityType.COMPANY

    # 2. Corporate Suffix & Industry Keyword Check
    if COMPANY_SUFFIX_PATTERN.search(clean):
        return EntityType.COMPANY

    # 3. Geopolitical & Government / Country Check
    if clean_upper in GOV_COUNTRY_KEYWORDS:
        return EntityType.COUNTRY

    # 4. Media Source Fallback
    if clean_upper == source.strip().upper() or any(m in clean.lower() for m in ("reuters", "bloomberg", "cnbc", "wsj", "ft", "feed", "news")):
        return EntityType.MEDIA_SOURCE

    return EntityType.COMPANY if (clean[0].isupper() and " " in clean) else EntityType.UNKNOWN

try:
    nlp = spacy.load("en_core_web_sm")
except Exception as e:
    logger.warning(f"Could not load spaCy en_core_web_sm: {e}. Named entity extraction will fall back to empty lists.")
    nlp = None

NEG_PATTERNS = [
    r"\battack(?:s|ed|ing)?\b", r"\bwars?\b", r"\bkill(?:s|ed|ing)?\b", r"\bexplod(?:e|es|ed|ing)?\b", r"\bexplosions?\b",
    r"\bsanction(?:s|ed|ing)?\b", r"\bseiz(?:e|es|ed|ure|ures|uring)?\b", r"\bcrash(?:es|ed|ing)?\b", r"\bdisasters?\b",
    r"\bcollaps(?:e|es|ed|ing)?\b", r"\bcrisis\b", r"\bcrises\b", r"\bthreat(?:s|ened|ening)?\b", r"\bmissiles?\b", r"\bdetain(?:ed|s|ing)?\b",
    r"\bconflicts?\b", r"\bstrik(?:e|es|ing)?\b", r"\bblockad(?:e|es|ed|ing)?\b", r"\barrest(?:ed|s|ing)?\b", r"\bhijack(?:s|ed|ing)?\b",
    r"\bpiracy\b", r"\bbankruptcy\b", r"\bdefault(?:s|ed|ing)?\b", r"\bliquidat(?:e|es|ed|ion|ions)?\b", r"\bplung(?:e|es|ed|ing)?\b",
    r"\brecessions?\b", r"\binflations?\b", r"\bmargin_calls?\b", r"\bdowngrad(?:e|es|ed|ing)?\b", r"\bbearish\b",
    r"\bbreach(?:es|ed|ing)?\b", r"\bransomware\b", r"\bhack(?:s|ed|ing)?\b", r"\bvulnerabilit(?:y|ies)\b", r"\bexploit(?:s|ed|ing)?\b",
    r"\boutages?\b", r"\bmalware\b", r"\bescalat(?:e|es|ed|ion|ions)?\b", r"\binvad(?:e|es|ed)?\b", r"\binvasions?\b",
    r"\bcoups?\b", r"\briot(?:s|ed|ing)?\b", r"\bembargo(?:es|ed)?\b", r"\btensions?\b", r"\bterroris(?:m|t|ts)\b",
    r"\bfrauds?\b", r"\bscam(?:s|med|ming)?\b", r"\bindict(?:ed|s|ment|ments)?\b", r"\blawsuits?\b", r"\bselloffs?\b",
    r"\bpenalt(?:y|ies)\b", r"\brugpulls?\b", r"\bdelist(?:ed|s|ing)?\b", r"\bslump(?:s|ed|ing)?\b", r"\btank(?:ed|s|ing)?\b",
    r"\bunderperform(?:s|ed|ing)?\b", r"\bcapitulat(?:e|es|ed|ion)?\b", r"\bdilution\b", r"\bdeflat(?:e|es|ed|ion)?\b",
    r"\bdiv(?:e|es|ed|ing)?\b", r"\bsink(?:s|ing)?\b", r"\btumbl(?:e|es|ed|ing)?\b", r"\bplummet(?:s|ed|ing)?\b",
    r"\bdrop(?:s|ped|ping)?\b", r"\bfall(?:s|ing)?\b"
]

POS_PATTERNS = [
    r"\bdeals?\b", r"\bagreements?\b", r"\bpeace\b", r"\bgrowth\b", r"\brecovery\b", r"\bcooperation\b",
    r"\bceasefires?\b", r"\bdiplomatic\b", r"\balliances?\b", r"\btrades?\b", r"\baccords?\b",
    r"\bbullish\b", r"\bmergers?\b", r"\bacquisitions?\b", r"\bprofit(?:s|ed|ing)?\b", r"\bdividends?\b",
    r"\bsurg(?:e|es|ed|ing)?\b", r"\brall(?:y|ies|ied|ying)?\b", r"\bstimulus\b", r"\bbreakouts?\b", r"\bupgrad(?:e|es|ed|ing)?\b",
    r"\btreat(?:y|ies)\b", r"\bnegotiat(?:e|es|ed|ion|ions)?\b", r"\bpartnerships?\b", r"\baids?\b", r"\bpatch(?:ed|es|ing)?\b",
    r"\bsecur(?:e|ed|es|ing)?\b", r"\bresolv(?:e|ed|es|ing)?\b", r"\brescue[ds]?\b", r"\bfunding\b", r"\badoption\b",
    r"\bapprov(?:e|ed|es|al|ing)?\b", r"\bbreakthroughs?\b", r"\bunder-valued\b", r"\bbull-run\b", r"\bsoar(?:s|ed|ing)?\b",
    r"\bjump(?:s|ed|ing)?\b", r"\bbeat(?:s|ing|en)?\b", r"\bclimb(?:s|ed|ing)?\b", r"\bgain(?:s|ed|ing)?\b", r"\brecords?\b"
]

NEG_REGEXES = [re.compile(p, re.IGNORECASE) for p in NEG_PATTERNS]
POS_REGEXES = [re.compile(p, re.IGNORECASE) for p in POS_PATTERNS]

NEGATION_WORDS = {"no", "not", "never", "none", "neither", "nor", "prevent", "avoid", "halt", "stop", "fail", "don't", "can't", "won't"}

THREAT_KEYWORDS = {
    # Kinetic / Warfare
    r"\bnuclear\b", r"\binva(sion|de|ded)\b", r"\bmissile\s+strike\b", r"\bassassina(ted|tion)\b",
    r"\bdeclaration\s+of\s+war\b", r"\bweapons?\s+grade\b", r"\bkinetic\s+attack\b",
    # Financial / Macro
    r"\bsovereign\s+default\b", r"\bdefault\s+on\s+(debt|bonds)\b", r"\bsystemic\s+collapse\b",
    # Cyber / Infrastructure
    r"\bcyber\s*war(fare)?\b", r"\bgrid\s+blackout\b", r"\bcritical\s+infrastructure\s+compromise\b"
}
THREAT_REGEXES = [re.compile(p, re.IGNORECASE) for p in THREAT_KEYWORDS]

INFLATION_COST_BURDEN_KEYWORDS = [
    r"\bgasoline\b", r"\bgas\s+price[s]*\b", r"\bfuel\s+price[s]*\b", r"\binflation\b",
    r"\bconsumer\s+price[s]*\b", r"\bcpi\b", r"\bmortgage\s+rate[s]*\b", r"\bcost\s+of\s+living\b"
]
INFLATION_COST_REGEXES = [re.compile(p, re.IGNORECASE) for p in INFLATION_COST_BURDEN_KEYWORDS]

def _sentiment(text: str) -> float:
    t = text.lower()
    is_cost_burden_topic = any(rx.search(t) for rx in INFLATION_COST_REGEXES)
    tokens = re.findall(r'\b\w+\b', t)
    
    neg = 0
    pos = 0
    
    for idx, token in enumerate(tokens):
        is_neg_match = any(rx.match(token) for rx in NEG_REGEXES)
        is_pos_match = any(rx.match(token) for rx in POS_REGEXES)
        
        negated = False
        start_lookback = max(0, idx - 3)
        for j in range(start_lookback, idx):
            if tokens[j] in NEGATION_WORDS or tokens[j].endswith("n't"):
                negated = True
                break
                
        if is_cost_burden_topic and token in {"climb", "climbs", "climbing", "climbed", "surge", "surges", "surging", "surged", "soar", "soars", "soaring", "soared", "jump", "jumps", "jumped", "jumping", "spike", "spikes", "spiked", "spiking", "rise", "rises", "rising", "rose"}:
            if negated:
                pos += 1
            else:
                neg += 1
        elif is_neg_match:
            if negated:
                pos += 1
            else:
                neg += 1
        elif is_pos_match:
            if negated:
                neg += 1
            else:
                pos += 1
                
    total = neg + pos
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 3)

FINANCIAL_KEYWORDS = {
    # ── MACRO & MONETARY POLICY ──
    "fed": "macro", "fomc": "macro", "powell": "macro", "interest rates": "macro",
    "inflation": "macro", "cpi": "macro", "ppi": "macro", "gdp": "macro",
    "treasury": "macro", "yield curve": "macro", "recession": "macro", 
    "payrolls": "macro", "unemployment": "macro", "ecb": "macro", "boj": "macro",
    "liquidity": "macro", "stimulus": "macro", "quantitative tightening": "macro",

    # ── CORPORATE & EQUITIES ──
    "earnings": "corporate", "guidance": "corporate", "ipo": "corporate",
    "merger": "corporate", "acquisition": "corporate", "buyback": "corporate",
    "dividend": "corporate", "bankruptcy": "corporate", "chapter 11": "corporate",
    "sec": "regulatory", "insider trading": "regulatory", "antitrust": "regulatory",
    "subpoena": "regulatory", "ftc": "regulatory", "doj": "regulatory",

    # ── ENERGY & COMMODITIES ──
    "opec": "energy", "crude oil": "energy", "brent": "energy", "wti": "energy",
    "natgas": "energy", "lng": "energy", "aramco": "energy", "refinery": "energy",
    "spr": "energy", "strategic petroleum reserve": "energy", "gold": "metals",
    "copper": "metals", "uranium": "metals", "agriculture": "commodities",

    # ── GEOPOLITICAL & DEFENSE ──
    "pentagon": "defense", "nato": "geopolitical", "sanctions": "geopolitical",
    "embargo": "geopolitical", "tariff": "macro", "taiwan strait": "geopolitical",
    "south china sea": "geopolitical", "kremlin": "geopolitical", "idf": "defense",
    "houthis": "geopolitical", "red sea": "geopolitical", "defense contract": "defense",
    "missile": "defense", "drone strike": "defense", "dod": "defense", "hormuz": "geopolitical", 
    "iran nuclear": "geopolitical", "nuclear test": "geopolitical",

    # ── TECH & SEMICONDUCTORS ──
    "semiconductor": "tech", "artificial intelligence": "tech", "ai": "tech",
    "chip foundry": "tech", "gpu": "tech", "data center": "tech", 
    "export controls": "geopolitical", "tsmc": "tech", "asml": "tech",

    # ── LOGISTICS, MARITIME & AVIATION ──
    "supply chain": "logistics", "suez": "maritime", "panama canal": "maritime",
    "baltic dry": "logistics", "freight": "logistics", "port strike": "logistics",
    "faa": "aviation", "grounding": "aviation", "airspace": "aviation",

    # ── CRYPTO & DEFI ──
    "crypto": "crypto", "bitcoin": "crypto", "ethereum": "crypto", "etf": "crypto",
    "binance": "crypto", "coinbase": "crypto", "stablecoin": "crypto", 
    "tether": "crypto", "defi": "crypto", "halving": "crypto", "airdrop": "crypto",

    # ── CYBERSECURITY ──
    "ransomware": "cyber", "data breach": "cyber", "ddos": "cyber", 
    "zero-day": "cyber", "cisa": "cyber", "apt": "cyber", "malware": "cyber"
}

class NewsEnricher:

    def __init__(self, scorer, redis_client, graph_writer):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        p     = raw.raw_payload
        title = (p.get("title") or "").strip()
        if not title:
            return None

        summary     = (p.get("summary") or "")[:1000]
        url         = p.get("url", "")
        reliability = float(p.get("reliability", 0.8))
        combined_text = f"{title} {summary}"
        lower_text = combined_text.lower()

        sentiment = _sentiment(combined_text[:200])

        tags = list(p.get("tags", []))
        tags.append(p.get("category", "news"))

        for kw, category in FINANCIAL_KEYWORDS.items():
            if kw in lower_text:
                if category not in tags:
                    tags.append(category)
                    
        # SANCTIONS CHECK via Aho-Corasick Text Search
        ofac_hits = check_sanctions(combined_text)
        if ofac_hits:
            tags.append("sanctioned_ofac_mention")
            tags.extend(ofac_hits)

        # Extract named entities using spaCy
        # 1. Dynamic Stock Ticker & Cashtag Extractor ($NVDA, (AAPL), [TSLA], NASDAQ: MSFT)
        extracted_tickers = []
        for groups in RE_TICKER_EXTRACT.findall(combined_text[:600]):
            for candidate in groups:
                if candidate:
                    clean_t = candidate.strip("$,.():;[]{}'\"").upper()
                    if is_valid_primary_equity(clean_t):
                        extracted_tickers.append(clean_t)

        # 2. Standalone word equity check for major tickers
        for word in combined_text[:400].split():
            clean_word = word.strip("$,.():;[]{}'\"").upper()
            if 1 <= len(clean_word) <= 5 and clean_word.isalpha() and is_valid_primary_equity(clean_word):
                extracted_tickers.append(clean_word)

        # 3. spaCy Named Entity Recognition
        named_entities = list(dict.fromkeys(extracted_tickers))
        if nlp:
            try:
                doc = nlp(combined_text[:500])
                for ent in doc.ents:
                    if ent.label_ in ("ORG", "GPE", "PERSON", "LOC", "PRODUCT"):
                        named_entities.append(ent.text)
            except Exception as e:
                logger.debug(f"spaCy NER extraction failed: {e}")
        
        unique_entities = list(dict.fromkeys(named_entities))
        tags.extend(unique_entities)
            
        # Rigorous Mathematical Anomaly Scoring
        sentiment_score = abs(float(sentiment or 0.0)) * 0.40
        rel_weight = min(1.0, max(0.2, float(reliability or 0.8)))
        entity_score = min(0.30, len(unique_entities) * 0.05)
        raw_anomaly = (sentiment_score + entity_score) * rel_weight

        if ofac_hits:
            raw_anomaly = min(1.0, raw_anomaly + 0.40)

        is_threat = any(rx.search(combined_text) for rx in THREAT_REGEXES)
        if is_threat:
            raw_anomaly = min(1.0, max(0.65, raw_anomaly + 0.35))

        anomaly = round(raw_anomaly, 3)

        if anomaly < 0.35:
            return None
        
        logger.info(f"Enriched News | Anomaly: {anomaly} | Sentiment: {sentiment} | {title[:60]}...")

        if anomaly >=0.5 and self.graph:
            topic_str = "High_Impact_News"

            try:
                await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                    "entity_id": topic_str,
                    "action": "MERGE_ONTOLOGY_NODE",
                    "data": {
                        "label": "NewsEvent",
                        "primary_domain": "global",
                        "confidence": anomaly,
                        "sentiment": sentiment
                    }
                }, key=topic_str)
            except Exception as e:
                logger.debug(f"Failed to push to ontology proposals: {e}")

        primary_name = unique_entities[0] if unique_entities else raw.source
        entity_type = resolve_news_entity_type(primary_name, raw.source)
        entity = Entity(
            id=primary_name,
            type=entity_type,
            name=primary_name
        )
    
        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.HEADLINE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=reliability,
            primary_entity=entity,
            headline=title,
            summary=summary,
            url=url,
            tags=tags,
            named_entities=unique_entities,
            sentiment=sentiment,
            anomaly_score=anomaly,
        )