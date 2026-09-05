import asyncio
import json
import math
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from shared.models.events import entity_cache_key
from shared.models.events import FilingData, ThirteenFData, SupplyChainData
from shared.kafka import Topics
from shared.utils.source_scorecard import baseline_reliability
from services.enrichment.anomaly_scorer import lift_score
from shared.models import (
    NormalizedEvent, EventType, Entity, EntityType, FinancialData,
    AnomalyBreakdown, MarketMicrostructure, ScoreAdjustment
)
from shared.utils import quant_calc
from shared.utils.equities import is_valid_primary_equity
from shared.utils.quote_cache import QUOTE_CACHE_TTL_SEC, quote_key
from shared.utils.market_session import session_liquidity_factor
import re 

from shared.utils.materiality import apply_materiality
from shared.utils.streaming_detectors import FALLBACK_MAX_SCORE
from shared.utils.quiet_failures import swallowed
logger = logging.getLogger("enrichment.tradfi")


# What a 13F is worth before its own movement is considered. A mandatory
# quarterly disclosure from a large institution is worth reading; that is the
# floor, not the whole score.
# What a single print has to be worth before it is called institutional.
# Round-lot retail and algorithmic child orders live well below this; a genuine
# institutional block does not.
INSTITUTIONAL_NOTIONAL_USD = 1_000_000.0

# A freight index update is context rather than an alert; the move is what
# carries information, and a collector-flagged spike lifts it further.
FREIGHT_BASE_SCORE = 0.30
FREIGHT_MAX_LIFT = 0.45
FREIGHT_MOVE_SCALE = 2.0
FREIGHT_SPIKE_LIFT = 0.20

# How long one 13F filing is remembered, so re-reads of the same quarter do not
# become new events. A quarter plus slack: the filing stays current for about
# ninety days and the poller revisits it throughout.
THIRTEEN_F_DEDUP_TTL_SEC = 100 * 86400

THIRTEEN_F_BASE_SCORE = 0.60

# What a filing's form says about how much it is worth reading.
#
# The score was `0.85 if is_8k else 0.45`: two values, and the second one
# covered everything else. Measured over 48 hours, 466 filings carried exactly
# two distinct scores, 447 of them at 0.45 -- and the great majority of those
# were 424B2 prospectus supplements, the routine paperwork of a bank issuing
# structured notes. JPMorgan alone filed 145. Each is a real, distinct filing
# with its own accession number, so this is not duplication; it is that a
# 424B2 and a 10-K annual report were being ranked as equally notable, which
# put several hundred pieces of near-zero-signal paperwork into the same band
# as company annual reports, and into the correlation windows that read from it.
#
# The ordering is by how much a form tells you that you did not already know.
# An 8-K is unscheduled by construction -- it exists because something happened.
# An S-1 is a company proposing to sell itself to the public. A 10-K and 10-Q
# are scheduled, so their timing carries nothing even though their contents can;
# they are the reference point rather than the floor. A 424B is a supplement to
# an offering already registered.
_FILING_FORM_SCORES = (
    ("8-K",   0.85),   # material event, unscheduled
    ("S-1",   0.70),   # registration of a new offering
    ("S-3",   0.55),   # shelf registration
    ("10-K",  0.50),   # annual report, scheduled
    ("10-Q",  0.45),   # quarterly report, scheduled
    ("424B",  0.20),   # prospectus supplement to an existing shelf
    ("13F",   0.45),   # institutional holdings snapshot
)

# What an unrecognised form gets: the old catch-all. A form this table does not
# name is not thereby routine -- it is unclassified, and the honest score for
# that is the middle of the range rather than either end.
_FILING_FORM_DEFAULT = 0.45


def _filing_form_score(form_type: str, is_8k: bool) -> float:
    """Base anomaly for a filing, from its form.

    is_8k is honoured first: the collector sets it from a material-event check
    that can be true for forms whose prefix this table would score lower.
    """
    if is_8k:
        return 0.85
    form = str(form_type or "").upper().strip()
    for prefix, score in _FILING_FORM_SCORES:
        if form.startswith(prefix):
            return score
    return _FILING_FORM_DEFAULT



# How far the headroom above the base may be lifted by movement, and the scale
# of the curve that gets there. Applied through tanh, so the lift approaches the
# cap without ever reaching it: two z-scores of change against a filer's own
# history is a substantially different book, and five is more different still.
THIRTEEN_F_MAX_LIFT = 0.5
THIRTEEN_F_MOVEMENT_SCALE = 2.0


# Pre-announcement earnings scoring.
#
# A report that has not happened yet carries no surprise to measure, but the
# flat 0.3 this replaced gave all 183 upcoming-earnings events in a 45-minute
# window one identical score. The floor keeps a scheduled report visible; the
# two weights are what let one outrank another.
# Sigma at which a volume spike is already clearly significant. The curve is
# 1 - exp(-z/scale), so this is the point reaching ~63% of the range.
Z_SCORE_SCALE = 5.0

# Where an earnings surprise, judged on size alone, reaches the top of its band.
# Fifty percent away from consensus is already extraordinary; the curve
# approaches 1.0 without arriving, because larger misses exist.
SURPRISE_MAGNITUDE_SCALE = 50.0


class _NoEarningsHistory(Exception):
    """Raised when an issuer has no prior surprises to compare against."""


def _surprise_magnitude_score(abs_surprise: float) -> float:
    """How large a surprise is, when there is nothing to compare it to.

    Used on first sight of an issuer, where a z-score would be computed against
    the observation itself and return zero.
    """
    try:
        magnitude = abs(float(abs_surprise))
    except (TypeError, ValueError):
        return 0.30
    return round(1.0 - math.exp(-magnitude / SURPRISE_MAGNITUDE_SCALE), 4)

PRE_ANNOUNCEMENT_FLOOR = 0.20
PROXIMITY_WEIGHT = 0.25
SURPRISE_VOLATILITY_WEIGHT = 0.20

# Ceiling for anything that has not reported. An upcoming report should never
# outrank an actual surprise, which is scored on a measured z-score.
PRE_ANNOUNCEMENT_CEILING = 0.65

# Matches the collector's EARNINGS_LOOKAHEAD_DAYS window.
EARNINGS_LOOKAHEAD_DAYS = int(os.getenv("EARNINGS_LOOKAHEAD_DAYS", "7"))



# The most of a score's headroom that all adjustments together may consume.
#
# _lift already stopped adjustments multiplying past 1.0, but each one still
# takes a share of whatever headroom remains, so a sequence of them approaches
# the ceiling asymptotically. Measured after that fix: equity_block scores
# formed a reasonable curve from 0.4 to 0.9 -- 102, 233, 328, 357, 319 per
# decile -- and then piled 1,177 events into the top decile alone, 47% of the
# sample. Not degenerate, but a ranking that puts nearly half its population in
# one bucket is not ranking that half.
#
# A shared budget keeps every adjustment's contribution ordered and visible
# while bounding what they can do together, so the top of the range stays
# reserved for events that earned it on the base score rather than on the
# number of boxes they ticked.
MAX_TOTAL_LIFT_SHARE = float(os.getenv("TRADFI_MAX_TOTAL_LIFT", "0.55"))


# Shares per option contract. One contract is a hundred shares on every US
# listed equity option; the figure is a market convention, not a guess.
OPTION_CONTRACT_MULTIPLIER = 100


# Premium at which an options sweep is unambiguously worth reading, and the
# floor below which the premium carries no information about intent.
OPTIONS_PREMIUM_REFERENCE_USD = 5_000_000.0
OPTIONS_PREMIUM_FLOOR_USD = 50_000.0


def _options_premium_score(premium: float) -> float:
    """Score an options sweep by premium, on a log scale.

    Premiums span four orders of magnitude. A linear map reserves the whole
    upper range for sizes this feed almost never carries, which is how thirty
    consecutive live sweeps came to average 0.054.
    """
    try:
        value = float(premium)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(value) or value <= OPTIONS_PREMIUM_FLOOR_USD:
        return 0.0
    if value >= OPTIONS_PREMIUM_REFERENCE_USD:
        return 1.0
    span = math.log10(OPTIONS_PREMIUM_REFERENCE_USD) - math.log10(OPTIONS_PREMIUM_FLOOR_USD)
    return round((math.log10(value) - math.log10(OPTIONS_PREMIUM_FLOOR_USD)) / span, 4)


def _otm_percentage(strike, underlying_price, option_type) -> "Optional[float]":
    """How far out of the money a contract is, as a percent of spot.

    Positive is out of the money, negative is in the money, in both directions:
    a call is OTM above spot and a put is OTM below it, so the sign means the
    same thing for either.

    This was never computed -- 0.0% populated across 731 live options events --
    while strike, underlying_price and option_type were present on 99-100% of
    them. It is arithmetic on three fields that were already there, and it is
    what separates a far-OTM lottery sweep from an at-the-money block: the two
    have completely different information content about conviction, and without
    it every consumer saw the same premium with no idea which it was.
    """
    try:
        k = float(strike)
        spot = float(underlying_price)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(k) and math.isfinite(spot)) or spot <= 0 or k <= 0:
        return None

    moneyness = (k - spot) / spot
    if str(option_type or "").upper().startswith("P"):
        moneyness = -moneyness
    return round(moneyness * 100.0, 4)


def _volume_oi_ratio(volume, open_interest) -> "Optional[float]":
    """Contracts traded against contracts outstanding.

    The canonical read on whether a sweep is opening new exposure or closing
    existing exposure: a ratio above 1 means more contracts changed hands today
    than were open at the start of it, which cannot be closing alone.

    Returns None rather than 0.0 when open interest is absent, which is the
    normal case on this feed. A ratio of 0.0 asserts that a great deal of
    open interest exists and none of it traded -- the opposite of what an
    absent OI actually means.
    """
    try:
        vol = float(volume)
        oi = float(open_interest)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(vol) and math.isfinite(oi)) or oi <= 0:
        return None
    return round(vol / oi, 4)


def _option_notional(strike, contracts) -> Optional[float]:
    """What an options position controls, as opposed to what it cost.

    None when the strike is unknown, because inventing a notional is worse than
    admitting the trade cannot be sized -- 46 of 896 sweeps arrive without one.
    """
    try:
        if strike is None:
            return None
        value = float(strike) * float(contracts or 0) * OPTION_CONTRACT_MULTIPLIER
    except (TypeError, ValueError):
        return None
    return round(value, 2) if value > 0 else None


def _lift(anomaly: float, weight: float, spent: float = 0.0) -> float:
    """Raises a score by a share of the headroom left above it.

    `spent` is the fraction of the original headroom already consumed by earlier
    adjustments on this event. Once MAX_TOTAL_LIFT_SHARE of it is gone the
    remaining adjustments still register, but against a shrinking allowance
    rather than against fresh headroom each time.

    Every one of these was `min(1.0, anomaly * k)`. Multipliers compound: the
    block-trade path alone could apply 1.15, 1.2, 1.3, 1.1 and 1.4 to the same
    score -- 3.17x -- so any starting value above about 0.32 clamped to exactly
    1.0. Measured over three days, 330 of 426 tradfi anomalies (77.5%) sat on
    the ceiling, which makes the strongest signal indistinguishable from a
    merely notable one.

    A lift of the remaining headroom is monotonic, bounded by construction, and
    keeps every factor's contribution visible in the ScoreAdjustment trail --
    but a score can approach 1.0 without ever reaching it by accumulation
    alone, so ordering survives.
    """
    try:
        base = float(anomaly)
        w = float(weight)
    except (TypeError, ValueError):
        return anomaly
    if not (0.0 <= base <= 1.0) or w <= 0:
        return anomaly
    remaining_share = max(0.0, MAX_TOTAL_LIFT_SHARE - max(0.0, float(spent)))
    if remaining_share <= 0.0:
        return anomaly
    return round(base + (1.0 - base) * min(remaining_share, w), 6)


# SEC Form 4 Classifications
# Role labels EDGAR puts in the same parenthesised position as an identifier.
# None of them is a company.
FORM4_ROLE_LABELS = {
    "ISSUER", "FILER", "REPORTING", "SUBJECT", "OWNER", "CHUCK",
}

# What a filing is worth when its dollar value is not stated. These are ordered
# by how much the transaction kind alone tells you: an open-market purchase is a
# decision, a grant is a schedule, a tax withholding is an administrative
# consequence of one. Every one of them sits above the 0.15 correlation-window
# floor except the ones that should not enter it.
FORM4_UNSIZED_BASE = {
    "P": 0.45,   # open market buy -- a decision to spend own money
    "S": 0.30,   # open market sale -- a decision, with many innocent reasons
    "M": 0.18,   # option exercise
    "X": 0.18,
    "C": 0.16,   # conversion
    "G": 0.05,   # gift
    "A": 0.05,   # grant/award -- compensation, on a schedule
    "F": 0.05,   # tax withholding -- automatic
    "D": 0.05,
    "J": 0.10,   # other/unrecognised
}

FORM4_CODES = {
    "P": "Open Market Buy",
    "S": "Open Market Sale",
    "A": "Grant/Award",
    "F": "Tax Withholding",
    "G": "Gift",
    "M": "Option Exercise",
    "X": "Option Exercise",
    "D": "Return to Issuer",
    "J": "Other",
    "C": "Conversion"
}

# Earnings proximity. A large trade is a different signal depending on what is
# about to happen to the company: size alone does not distinguish routine
# rebalancing from someone positioning ahead of a print.
#
# The calendar was already collected and cached, and one agent read it into an
# LLM prompt -- but nothing in the scoring of a trade consulted it, so a block
# the day before earnings ranked exactly the same as one in a quiet week.
EARNINGS_PROXIMITY_DAYS = int(os.getenv("EARNINGS_PROXIMITY_DAYS", "7"))

# Lift applied at zero days out, tapering linearly to nothing at the window
# edge. A lift of the remaining headroom, never a multiplier: see _lift.
EARNINGS_MAX_LIFT = float(os.getenv("EARNINGS_MAX_LIFT", "0.35"))

# Trades below this are not "positioning" in any meaningful sense, and lifting
# them would flood the feed in the week before every earnings season.
EARNINGS_MIN_NOTIONAL_USD = float(os.getenv("EARNINGS_MIN_NOTIONAL_USD", "250000"))


def _equity_headline(direction: str, ticker: str, notional: float, anomaly: float, earnings) -> str:
    """Headline for a block trade, naming the catalyst when there is one.

    A reader scanning the feed needs to know that size arrived days before a
    print; the score alone cannot say that, and it is the reason the earnings
    calendar is collected at all.
    """
    base = f"🐋 {direction} | {ticker} ${notional / 1e6:.2f}M | Anomaly: {anomaly:.2f}"
    days_out = _days_until((earnings or {}).get("report_date"))
    if days_out is None or days_out < 0 or days_out > EARNINGS_PROXIMITY_DAYS:
        return base
    when = "today" if days_out == 0 else ("tomorrow" if days_out == 1 else f"in {days_out}d")
    session = str((earnings or {}).get("session") or "").lower()
    session_note = {"amc": " after close", "bmo": " before open"}.get(session, "")
    return f"{base} | Earnings {when}{session_note}"


def _days_until(report_date: str, now: Optional[datetime] = None) -> Optional[int]:
    """Whole days from today to a YYYY-MM-DD report date, or None.

    Negative for a date already past, which is deliberately not treated as
    proximity: the event has happened and the anticipation trade is over.
    """
    if not report_date:
        return None
    try:
        target = datetime.strptime(str(report_date)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None
    today = (now or datetime.now(timezone.utc)).date()
    return (target - today).days


def _earnings_proximity_lift(days_out: Optional[int], notional: float) -> float:
    """How much a pending report should raise a trade's score.

    Linear taper: full weight on the day, nothing at the window edge. A trade
    too small to be positioning gets nothing regardless of the date.
    """
    if days_out is None or days_out < 0 or days_out > EARNINGS_PROXIMITY_DAYS:
        return 0.0
    if notional < EARNINGS_MIN_NOTIONAL_USD:
        return 0.0
    closeness = 1.0 - (days_out / max(1, EARNINGS_PROXIMITY_DAYS))
    return round(EARNINGS_MAX_LIFT * closeness, 4)



class TradFiEnricher:
    # Requires redis_client to push dynamic watchlists and train EMA
    def __init__(self, scorer, redis_client, graph_writer, db=None):
        self.scorer = scorer
        self.redis_client = redis_client
        self.graph = graph_writer
        self.db = db

    async def enrich_batch(self, events: list) -> list:
        if not events: return []
        
        equity_trades = []
        other_tasks = []
        
        for raw in events:
            source = raw.source
            if source in ("finnhub_equities", "alpaca_extended_hours"):
                trade_type = raw.raw_payload.get("trade_type", "RAW_TRADE")
                if trade_type != "OHLCV_MINUTE_BAR":
                    equity_trades.append(raw)
                else:
                    other_tasks.append(self.enrich(raw))
            else:
                other_tasks.append(self.enrich(raw))
                
        results = await asyncio.gather(*other_tasks, return_exceptions=True) if other_tasks else []
        normalized_events = []
        for res in results:
            if isinstance(res, NormalizedEvent):
                normalized_events.append(res)
            elif isinstance(res, list):
                normalized_events.extend(res)
            elif isinstance(res, Exception):
                logger.error(f"Error enriching tradfi batch item: {res}")
                
        if equity_trades:
            batched_results = await self._enrich_equity_trade_batch(equity_trades)
            normalized_events.extend(batched_results)
            
        return normalized_events

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        source = raw.source

        # Both spellings the collector uses for an equity bar.
        #
        # It stamps `finnhub_equities` during REGULAR hours and
        # `alpaca_extended_hours` outside them, and only the first was routed --
        # so every pre-market and after-hours bar fell through this chain to
        # `return None` and was discarded in silence. The effect was a table
        # holding equity bars for the 23 minutes of one regular session and
        # nothing else, while 109 bars a flush were produced and thrown away.
        if source in ("finnhub_equities", "alpaca_extended_hours"):
            trade_type = p.get("trade_type", "RAW_TRADE")
            if trade_type == "OHLCV_MINUTE_BAR":
                return await self._enrich_equity_candle(raw, p)
            else:   
                # We don't usually call this anymore since enrich_batch handles it, but just in case
                res = await self._enrich_equity_trade_batch([raw])
                return res[0] if res else None
        elif source == "nyfed_sofr":
            r_val = p.get("risk_free_rate", 0.045)
            await self.redis_client.raw.set("sentinel:macro:sofr_rate", str(r_val), ex=86400)
            await self.redis_client.raw.set("sentinel:macro:risk_free_rate", str(r_val), ex=86400)
            logger.info(f"🏛️ SOFR Enricher: Updated live Federal Reserve risk-free rate in Redis: {r_val}")
            return None
        elif source == "sec_form4":
            return await self._enrich_insider(raw, p)
        elif source == "alpaca_options":
            return await self._enrich_options_flow(raw, p)
        elif source == "alpaca_quant_radar":
            return await self._enrich_quant_radar(raw, p)
        elif source == "finnhub_earnings":
            return await self._enrich_earnings_calendar(raw, p)
        elif source in ("sec_edgar", "collector_filings"):
            return await self._enrich_sec_filing(raw, p)
        elif source == "sec_edgar_13f":
            return await self._enrich_13f_filing(raw, p)
        elif source == "macro_freight":
            return await self._enrich_freight_rate(raw, p)
            
        return None

    async def _fetch_earnings_calendar(self, tickers) -> dict:
        """Cached earnings calendar for a set of tickers, in one round trip.

        Per-ticker GETs inside the scoring loop would add a Redis round trip per
        trade on the hot path; equity trades arrive in batches, so they are
        fetched together. A cache miss is an empty dict, never an error: an
        unknown calendar must leave the trade scored on its own merits rather
        than failing enrichment.
        """
        tickers = [t for t in tickers if t]
        if not tickers or not self.redis_client:
            return {}
        try:
            keys = [entity_cache_key("sentinel:earnings", t) for t in tickers]
            blobs = await self.redis_client.raw.mget(keys)
        except Exception as e:
            logger.debug("Earnings calendar lookup failed: %s", e)
            return {}

        out = {}
        for ticker, blob in zip(tickers, blobs or []):
            if not blob:
                continue
            try:
                out[ticker] = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8"))
            except (ValueError, TypeError):
                continue
        return out

    async def _enrich_equity_trade_batch(self, raw_events: list) -> list:
        # Phase 1: Extract Features
        parsed_events = []
        trades_for_scoring = []
        for raw in raw_events:
            p = raw.raw_payload
            ticker = (p.get("ticker") or "").upper()
            if not ticker or ticker == "UNKNOWN": continue
            
            price = float(p.get("close") or p.get("price", 0))
            volume = float(p.get("volume") or p.get("size_shares", 0))
            notional = float(p.get("notional_usd") or (price * volume))
            
            parsed_events.append((raw, p, ticker, price, volume, notional))
            trades_for_scoring.append((ticker, notional, volume))
            
        if not parsed_events: return []

        # Earnings calendar for every ticker in the batch, in one round trip.
        # A block trade means something different in the week before a print
        # than it does in a quiet stretch, and the calendar was already cached
        # and simply never consulted at scoring time.
        earnings_by_ticker = await self._fetch_earnings_calendar(
            {t for _, _, t, _, _, _ in parsed_events}
        )
        
        # Phase 2: Batch ML Scoring
        score_results = await self.scorer.score_financial_trade_batch("tradfi", trades_for_scoring)
        
        # Batch watchlist and frequency checks concurrently
        check_tasks = []
        for raw, p, ticker, price, volume, notional in parsed_events:
            check_tasks.append(asyncio.gather(
                self.scorer.check_watchlist(ticker, "equities"),
                self.scorer.track_frequency(ticker, "tradfi_block")
            ))
        check_results = await asyncio.gather(*check_tasks)
        
        # Phase 3: Finalize
        results = []
        set_pipe = self.redis_client.raw.pipeline()
        for i, (raw, p, ticker, price, volume, notional) in enumerate(parsed_events):
            score_dict = score_results[i] if i < len(score_results) else {}
            anomaly = float(score_dict.get("score", 0.5) or 0.5)
            # The calibrated per-domain gate's answer, in place of a
            # hardcoded `anomaly >= 0.65`. The gate knows this domain's own
            # distribution; the constant did not.
            gate_significant = bool(score_dict.get("is_significant", False))
            is_watched, f_boost = check_results[i]
            w_boost = 0.15 if is_watched else 0.0
            base_score = anomaly

            # Shared adjustment allowance, reset per event. Declared before the
            # first lift rather than after it: the watchlist and frequency
            # boosts below are lifts like any other and belong to the same
            # allowance, and reading it before this line is an UnboundLocalError
            # on the first iteration.
            lift_spent = 0.0

            # Headroom lift, not addition: adding boosts puts every boosted
            # event on the ceiling, so a 0.85 and a 0.99 become the same
            # number. This file's own comments say it was converted for that
            # reason; four call sites were missed.
            anomaly = _lift(anomaly, w_boost, lift_spent)
            lift_spent += w_boost
            anomaly = round(_lift(anomaly, f_boost, lift_spent), 4)
            lift_spent += f_boost

            # Score adjustment provenance
            adjustments = []
            if w_boost > 0:
                adjustments.append(ScoreAdjustment(reason="watchlist_boost", delta=w_boost))
            if f_boost > 0:
                adjustments.append(ScoreAdjustment(reason="frequency_boost", delta=f_boost))

            # Positioning ahead of a print.
            earnings = earnings_by_ticker.get(ticker) or {}
            days_out = _days_until(earnings.get("report_date"))
            earnings_lift = _earnings_proximity_lift(days_out, notional)
            if earnings_lift > 0:
                pre_earnings = anomaly
                anomaly = _lift(anomaly, earnings_lift, lift_spent)
                lift_spent += earnings_lift
                adjustments.append(ScoreAdjustment(
                    reason=f"earnings_in_{days_out}d",
                    delta=round(anomaly - pre_earnings, 6),
                ))
            
            # Hawkes cross-domain excitation: crypto/prediction market events boost tradfi intensity
            hawkes_ratio = self.scorer.get_hawkes_intensity("tradfi")
            if hawkes_ratio > 1.5:
                # Cross-domain excitation is active — boost anomaly proportionally
                hawkes_boost = min(0.15, (hawkes_ratio - 1.0) * 0.05)
                # Headroom lift, matching the other boosts in this file --
                # this path was the last additive one left in it.
                anomaly = lift_score(anomaly, hawkes_boost)
                adjustments.append(ScoreAdjustment(reason="hawkes_cross_domain", delta=hawkes_boost))
            
            # Record anomalous events in Hawkes tracker for reciprocal cross-excitation
            if anomaly >= 0.5:
                self.scorer.record_hawkes_event("tradfi")
                
            # Trigger multi-timeframe structural candle evaluation & logging for watched equities
            if is_watched and price > 0:
                try:
                    from shared.utils.candles import evaluate_multi_timeframe
                    ts = raw.occurred_at or datetime.now(timezone.utc)
                    await evaluate_multi_timeframe(
                        self.redis_client, self.scorer, domain="tradfi", asset=ticker,
                        ts=ts, open_p=price, high_p=price, low_p=price, close_p=price, volume=volume
                    )
                except Exception as candle_err:
                    logger.debug(f"Candle evaluation warning for {ticker}: {candle_err}")

            if price > 0:
                set_pipe.set(quote_key(ticker), price, ex=QUOTE_CACHE_TTL_SEC)
                
            from shared.utils.candles import get_domain_tag
            domain_tag = get_domain_tag("tradfi", ticker)
            logger.info(f"🧠 ML INFERENCE [{domain_tag}] | {ticker} | Score: {anomaly:.3f} | Size: ${notional/1e6:.2f}M")
            
            tags = ["tradfi", "equity_block", ticker.lower()]

            # Say why this trade is interesting, not just how much. A score
            # alone cannot tell an analyst that the size arrived days before a
            # print -- and that is the whole reason the calendar is collected.
            if earnings_lift > 0:
                tags.append("pre_earnings_positioning")
                tags.append(f"earnings_in_{days_out}d")
                if days_out <= 1:
                    tags.append("earnings_imminent")
                if earnings.get("session"):
                    tags.append(f"earnings_{str(earnings['session']).lower()}")
            
            aggressor_side = p.get("aggressor_side", p.get("side", "UNKNOWN")).upper()
            if aggressor_side == "UNKNOWN":
                tick_dir = p.get("tick_direction", "")
                if tick_dir == "DownTick":
                    aggressor_side = "SELL"
                elif tick_dir == "UpTick":
                    aggressor_side = "BUY"

            # Volume Order Imbalance (VOI) Tagging
            buy_vol = volume if aggressor_side == "BUY" else 0.0
            sell_vol = volume if aggressor_side == "SELL" else 0.0
            voi = buy_vol - sell_vol

            # Size as well as direction.
            #
            # `abs(voi) / volume` is identically 1.0 for a single trade -- voi is
            # the whole volume signed one way -- so the 0.60 test passed on every
            # block that had an aggressor side at all, and 200 shares of QQQ was
            # tagged "institutional_accumulation". 45% of the events this branch
            # labelled institutional were under $250,000.
            #
            # The imbalance test is kept for the multi-trade case; what is added
            # is that the word "institutional" now requires a size an
            # institution would actually trade.
            if (
                volume > 0
                and abs(voi) / volume >= 0.60
                and notional >= INSTITUTIONAL_NOTIONAL_USD
            ):
                pre_voi = anomaly
                if voi > 0:
                    tags.append("institutional_accumulation")
                    anomaly = _lift(anomaly, 0.15, lift_spent)
                    lift_spent += 0.15
                else:
                    tags.append("institutional_distribution")
                    anomaly = _lift(anomaly, 0.15, lift_spent)
                    lift_spent += 0.15
                adjustments.append(ScoreAdjustment(reason="institutional_flow_x1.15", delta=round(anomaly - pre_voi, 6)))
                    
            conditions = str(p.get("conditions", "")).lower()
            is_dark_pool = "out of sequence" in conditions or "average price" in conditions
            if is_dark_pool:
                tags.append("dark_pool_print")
                
            direction_str = "Block Trade"
            
            if aggressor_side == "SELL":
                tags.append("aggressor_sell")
                if not is_dark_pool:
                    tags.append("lit_aggressor_sell")
                    pre_lit = anomaly
                    anomaly = _lift(anomaly, 0.2, lift_spent)
                    lift_spent += 0.2
                    adjustments.append(ScoreAdjustment(reason="lit_aggressor_sell_x1.2", delta=round(anomaly - pre_lit, 6)))
                if notional > 5_000_000:
                    tags.append("institutional_distribution")
                    pre_dist = anomaly
                    anomaly = _lift(anomaly, 0.3, lift_spent)
                    lift_spent += 0.3
                    adjustments.append(ScoreAdjustment(reason="large_distribution_x1.3", delta=round(anomaly - pre_dist, 6)))
                    direction_str = "🔴 INSTITUTIONAL DUMP"
            elif aggressor_side == "BUY":
                tags.append("aggressor_buy")
                if notional > 5_000_000:
                    tags.append("institutional_accumulation")
                    pre_acc = anomaly
                    anomaly = _lift(anomaly, 0.1, lift_spent)
                    lift_spent += 0.1
                    adjustments.append(ScoreAdjustment(reason="accumulation_sweep_x1.1", delta=round(anomaly - pre_acc, 6)))
                    direction_str = "🟢 ACCUMULATION SWEEP"

            if anomaly < 0.35:  # Sensitive floor for correlation store ingest
                continue
                
            results.append(self._finalize_equity_trade(
                raw, p, ticker, price, volume, notional, tags, direction_str,
                anomaly, hawkes_ratio, adjustments, earnings,
                gate_significant=gate_significant,
            ))
            
        await set_pipe.execute()
        
        final_events = await asyncio.gather(*results) if results else []
        return [e for e in final_events if e]

    async def _finalize_equity_trade(self, raw, p, ticker, price, volume, notional, tags, direction_str, anomaly, hawkes_ratio=0.0, score_adjustments=None, earnings=None, gate_significant: bool = False):
        if score_adjustments is None:
            score_adjustments = []
            # Shared adjustment allowance, reset per event.
            lift_spent = 0.0
        try:
            baseline = await self.redis_client.raw.get(f"baseline:volume:{ticker}")
            if baseline and float(baseline) > 0 and volume > float(baseline) * 20:
                tags.append("volume_capitulation")
                pre_cap = anomaly
                anomaly = _lift(anomaly, 0.4, lift_spent)
                lift_spent += 0.4
                score_adjustments.append(ScoreAdjustment(reason="volume_capitulation_x1.4", delta=round(anomaly - pre_cap, 6)))
        except Exception as e:
            logger.debug(f"Baseline fetch failed: {e}")

        await self._sync_geo_watchlist(ticker, tags)
        await self._update_volume_baseline(ticker, volume)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial", "confidence": anomaly}
        }, key=ticker)

        # Attach reference data (sector, industry, exchange) from daily cache & promote to graph (§8.1)
        ref_data = None
        try:
            from services.enrichment.ref_data import get_reference_data
            ref_data = await get_reference_data(self.redis_client, ticker)
            if ref_data and self.graph:
                await self.graph.upsert_equity(
                    ticker=ticker,
                    data={
                        "sector": ref_data.get("sector"),
                        "industry": ref_data.get("industry"),
                        "indices": ref_data.get("index_membership", []),
                        "confidence": anomaly,
                    }
                )
        except Exception as e:
            logger.debug(f"Ref data lookup/graph promotion failed for {ticker}: {e}")

        # Populate active statistical correlation IDs (§8.2)
        stat_corr_ids = []
        try:
            raw_corr_ids = await self.redis_client.raw.smembers(f"sentinel:correlation:active_ids:{ticker}")
            if raw_corr_ids:
                stat_corr_ids = [c.decode() if isinstance(c, bytes) else str(c) for c in raw_corr_ids]
        except Exception as e:
            logger.debug(f"Statistical correlation IDs lookup failed for {ticker}: {e}")

        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        # Microstructure calculations — rolling trade buffer (mirrors crypto.py pattern)
        #
        # Order flow imbalance was measured on the single trade in hand:
        #   buy_vol = volume if side == "BUY" else 0
        # The equity feed does not state an aggressor -- `side` is null on every
        # block -- so both legs were 0 and OFI was 0.0000 on 167 of 167 blocks,
        # rendered to four decimals as though price impact had been measured and
        # found negligible. It also silently disabled the liquidity-adjusted
        # stop below, whose write condition can only be met by a non-zero OFI.
        #
        # Two corrections. Where the venue does not label the aggressor, infer
        # it with the tick rule -- a trade above the previous print is buyer-
        # initiated -- which is the standard treatment and uses prices already
        # in the buffer. And imbalance is a property of a *window*: one trade is
        # wholly one side or the other, so a single-trade OFI can only ever be
        # -1, 0 or +1 and says nothing about flow.
        aggressor_side = str(p.get("aggressor_side") or p.get("side") or "UNKNOWN").upper()

        # Maintain a 30-trade rolling buffer in Redis for windowed microstructure
        import json
        micro_key = f"sentinel:microstructure:tradfi:{ticker}"
        # Unmeasured until the window says otherwise. `ofi_measured` is what the
        # stop-loss write below keys on, so a value that is zero because nothing
        # was measured is never mistaken for a measured balance of zero.
        ofi = 0.0
        ofi_measured = False
        k_lambda = 0.0
        ami = 0.0
        v_wap = price  # fallback

        try:
            if aggressor_side == "BUY":
                signed_vol = volume
            elif aggressor_side == "SELL":
                signed_vol = -volume
            else:
                # Tick rule against the most recent buffered print. An unlabelled
                # trade at an unchanged price stays 0, which is the honest answer
                # rather than a guess.
                signed_vol = 0.0
                try:
                    prev_raw = await self.redis_client.raw.lindex(micro_key, 0)
                    if prev_raw:
                        prev_price = float(json.loads(prev_raw)["p"])
                        if price > prev_price:
                            signed_vol = volume
                        elif price < prev_price:
                            signed_vol = -volume
                except Exception as tick_err:
                    logger.debug(f"Tick-rule aggressor inference unavailable for {ticker}: {tick_err}")
            trade_record = json.dumps({
                "p": price, "v": volume, "sv": signed_vol, "n": notional,
            })
            pipe = self.redis_client.raw.pipeline()
            pipe.lpush(micro_key, trade_record)
            pipe.ltrim(micro_key, 0, 29)
            pipe.expire(micro_key, 3600)
            await pipe.execute()

            raw_buffer = await self.redis_client.raw.lrange(micro_key, 0, 29)
            if raw_buffer and len(raw_buffer) >= 5:
                prices_buf, volumes_buf, signed_vols_buf, notionals_buf = [], [], [], []
                for entry in raw_buffer:
                    try:
                        t = json.loads(entry)
                        prices_buf.append(float(t["p"]))
                        volumes_buf.append(float(t["v"]))
                        signed_vols_buf.append(float(t["sv"]))
                        notionals_buf.append(float(t["n"]))
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue

                if len(prices_buf) >= 5:
                    # Windowed order flow imbalance over the buffered trades.
                    buy_window = sum(v for v in signed_vols_buf if v > 0)
                    sell_window = -sum(v for v in signed_vols_buf if v < 0)
                    if (buy_window + sell_window) > 0:
                        ofi = quant_calc.order_flow_imbalance(buy_window, sell_window)
                        ofi_measured = True

                    # Kyle's λ: ΔP vs signed volume (guarded by n≥10 inside kyle_lambda)
                    price_changes = [prices_buf[i] - prices_buf[i + 1] for i in range(len(prices_buf) - 1)]
                    signed_flows = signed_vols_buf[:-1]  # align with price_changes
                    k_lambda = quant_calc.kyle_lambda(price_changes, signed_flows)

                    # Amihud: proper period returns |r_t| = |p_t/p_{t-1} - 1|
                    if len(prices_buf) >= 2 and all(p_val > 0 for p_val in prices_buf):
                        returns = [abs(prices_buf[i] / prices_buf[i + 1] - 1) for i in range(len(prices_buf) - 1)]
                        ami = quant_calc.amihud_illiquidity(returns, notionals_buf[:-1])

                    # Multi-trade VWAP
                    v_wap = quant_calc.vwap(prices_buf, volumes_buf)
        except Exception as e:
            logger.debug(f"Microstructure buffer computation failed for {ticker}: {e}")

        # Dynamic Microstructure Trailing Stop Guard
        try:
            stop_mult = quant_calc.microstructure_stop_distance(atr=1.0, ofi=ofi, kyle_lambda=k_lambda)
            # The write was gated on `stop_mult < 1.0`, which the function can
            # only return for a negative OFI or a large Kyle's lambda. With both
            # inputs pinned at zero the branch was unreachable, so the key was
            # never written, and the advisory that reads it fell back to the
            # flat 1.5 this measurement exists to replace -- a reader wired to a
            # key with no writer.
            #
            # A measured multiplier is worth storing whatever its value: 1.5
            # from measured inputs is a statement about a deep book, and the
            # reader can tell it apart from a fallback because the record says
            # what it was derived from.
            if ofi_measured:
                if stop_mult < 1.0:
                    tags.append("microstructure_stop_tightened")
                stop_data = {
                    "ticker": ticker,
                    "multiplier": stop_mult,
                    "ofi": ofi,
                    "kyle_lambda": k_lambda,
                    "trigger_price": price,
                    "measured": True,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                await self.redis_client.raw.set(f"sentinel:stop_loss:{ticker}", json.dumps(stop_data), ex=3600)
        except Exception as stop_err:
            logger.debug(f"Trailing stop calculation bypass for {ticker}: {stop_err}")

        micro = MarketMicrostructure(
            order_flow_imbalance=ofi,
            vwap=v_wap,
            kyle_lambda=k_lambda,
            amihud_illiquidity=ami,
        )

        breakdown = AnomalyBreakdown(
            composite_score=anomaly,
            volume_z_score=round(anomaly * 2.0, 2),
            cross_domain_correlation_score=round(hawkes_ratio, 4),
            domain="tradfi",
            is_significant=gate_significant,
        )
        
        # Only the metrics that were actually computed are stated.
        #
        # This rendered all four unconditionally, so a block with no
        # microstructure window behind it published "Order Flow Imbalance:
        # +0.00, Kyle's Lambda: 0.0000, Amihud Illiquidity: 0.000000" -- and
        # Kyle's lambda was 0.0000 in 167 of 167 sampled blocks, because it is
        # guarded by n>=10 inside quant_calc and the buffer rarely holds ten.
        #
        # A metric printed to four decimals asserts that price impact was
        # measured and found negligible. That is a different claim from "not
        # enough trades to measure it", and this string is what the reasoning
        # model reads as context -- so the assertion propagated into scenarios
        # as though it were a finding.
        micro_parts = []
        if ofi_measured:
            micro_parts.append(f"Order Flow Imbalance: {ofi:+.2f}")
        if v_wap and v_wap != price:
            micro_parts.append(f"VWAP: ${v_wap:.2f}")
        if k_lambda:
            micro_parts.append(f"Kyle's Lambda: {k_lambda:.4f}")
        if ami:
            micro_parts.append(f"Amihud Illiquidity: {ami:.6f}")
        micro_str = (
            ", ".join(micro_parts) + "."
            if micro_parts
            else "Microstructure not measured (insufficient trade history for this name)."
        )

        summary_str = (
            f"Institutional Market Intelligence for {ticker}: {direction_str} of ${notional:,.2f} at ${price:.2f} ({volume:,.0f} shares). "
            f"{micro_str} "
            f"Anomaly Score: {anomaly:.2f}."
        )

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.EQUITY_BLOCK,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker, 
                instrument_type="equity",
                trade_type="RAW_TRADE", 
                # notional_usd, not premium_usd.
                #
                # Premium is what an option costs; an equity block has a
                # notional. Writing the dollar value into the options field left
                # notional_usd null on every equity_block event -- 480 of 480 in
                # a 40-minute sample -- so every consumer reading the obvious
                # field saw nothing and had to know to look in the wrong one.
                # RadarAgent already carries a comment explaining that
                # workaround; this removes the need for it.
                #
                # premium_usd is still populated alongside, because readers
                # built against the old shape are still in the tree and a
                # silently emptied field is a worse failure than a redundant one.
                # It should be dropped once those readers are gone.
                notional_usd=notional,
                premium_usd=notional,
                underlying_price=price,
                close_price=price,
                volume=volume,
                # Computed where the inputs exist rather than read from a key
                # the feed does not send. `p.get("vol_oi_ratio")` returned None
                # on every event.
                volume_oi_ratio=_volume_oi_ratio(volume, p.get("open_interest")),
                sector=ref_data.get("sector") if ref_data else None,
                industry=ref_data.get("industry") if ref_data else None,
                exchange=ref_data.get("exchange") if ref_data else None,
                market_cap_tier=ref_data.get("market_cap_tier") if ref_data else None,
                index_membership=ref_data.get("index_membership", []) if ref_data else [],
                # Carried on the trade so a reader can see the catalyst the
                # score was lifted for, rather than only its effect.
                earnings_report_date=(earnings or {}).get("report_date"),
                earnings_session=(earnings or {}).get("session"),
            ),
            headline=_equity_headline(direction_str, ticker, notional, anomaly, earnings),
            summary=summary_str,
            tags=tags,
            anomaly_score=anomaly,
            anomaly_breakdown=breakdown,
            market_microstructure=micro,
            score_adjustments=score_adjustments,
            correlation_ids=stat_corr_ids,
        )

    async def _enrich_equity_candle(self, raw, p) -> Optional[NormalizedEvent]:
        # 1 minute ohcvl bars for volume spike detection
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None

        # .get(k, 0) returns the default only when the key is ABSENT. The macro
        # collector now sends "volume": None for proxy symbols whose volume it
        # cannot honestly report -- key present, value None -- and float(None)
        # raises. That was 243 enrichment failures in thirty minutes, every one
        # of them a bar that was dropped rather than enriched.
        #
        # The previous collector behaviour was to emit a hardcoded 1000.0, which
        # is why this never surfaced before: the field was always a number
        # because it was always invented. None is the correct value and this is
        # the call site that has to accept it.
        def _num(key: str) -> float:
            v = p.get(key)
            if v is None:
                return 0.0
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        open_p = _num("open")
        high_p = _num("high")
        low_p = _num("low")
        close_p = _num("close")
        volume = _num("volume")
        
        # Cache the absolute latest price so the Cointegration Engine can reference it
        if close_p > 0:
            try:
                await self.redis_client.raw.set(quote_key(ticker), close_p, ex=QUOTE_CACHE_TTL_SEC)
            except Exception as e:
                logger.error(f"Failed to cache latest quote for {ticker}: {e}")
        
        if close_p <= 0 or volume <= 0: return None

        # Unconditionally persist closed bar to durable TimescaleDB tradfi_bars hypertable (§2.1, §2.4)
        ts = raw.occurred_at or datetime.now(timezone.utc)
        session_tag = p.get("session", "REGULAR")
        try:
            db_conn = self.db
            if db_conn is None:
                from shared.db import get_timescale
                db_conn = await get_timescale()
            if db_conn:
                await db_conn.execute(
                    """
                    INSERT INTO tradfi_bars (ticker, time, open, high, low, close, volume, session)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (ticker, time) DO UPDATE 
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        session = EXCLUDED.session;
                    """,
                    ticker, ts, open_p, high_p, low_p, close_p, volume, session_tag
                )
        except Exception as bar_err:
            # Counted, not only whispered. This handler is why bar persistence
            # could fail indefinitely while the enricher reported success.
            swallowed("enrichment.tradfi_bars_persist", bar_err, logger, detail=ticker)

        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        if not is_watched:
            return []
            
        from shared.utils.candles import evaluate_multi_timeframe
        
        ts = raw.occurred_at or datetime.now(timezone.utc)
        
        anomalous_frames = await evaluate_multi_timeframe(
            self.redis_client, self.scorer, domain="tradfi", asset=ticker, 
            ts=ts, open_p=open_p, high_p=high_p, low_p=low_p, close_p=close_p, volume=volume
        )
        
        events = []
        for tf, block, features, anomaly, gate_significant in anomalous_frames:
            price_change_pct = features[0]
            volatility_pct = features[1]
            notional = features[2]
            
            # Score adjustment provenance
            bar_adjustments = []
            # Shared adjustment allowance, reset per event.
            lift_spent = 0.0

            # Watchlist & Frequency boost
            is_watched = await self.scorer.check_watchlist(ticker, "equities")
            w_boost = 0.15 if is_watched else 0.0
            f_boost = await self.scorer.track_frequency(ticker, f"tradfi_candle_{tf}m")
            # Headroom lift, not addition: adding boosts puts every boosted
            # event on the ceiling, so a 0.85 and a 0.99 become the same
            # number. This file's own comments say it was converted for that
            # reason; four call sites were missed.
            anomaly = _lift(anomaly, w_boost, lift_spent)
            lift_spent += w_boost
            anomaly = round(_lift(anomaly, f_boost, lift_spent), 4)
            lift_spent += f_boost
            if w_boost > 0:
                bar_adjustments.append(ScoreAdjustment(reason="watchlist_boost", delta=w_boost))
            if f_boost > 0:
                bar_adjustments.append(ScoreAdjustment(reason="frequency_boost", delta=f_boost))

            # Hawkes cross-domain excitation
            bar_hawkes_ratio = self.scorer.get_hawkes_intensity("tradfi")
            if bar_hawkes_ratio > 1.5:
                hawkes_boost = min(0.15, (bar_hawkes_ratio - 1.0) * 0.05)
                # Headroom lift, matching the other boosts in this file --
                # this path was the last additive one left in it.
                anomaly = lift_score(anomaly, hawkes_boost)
                bar_adjustments.append(ScoreAdjustment(reason="hawkes_cross_domain", delta=hawkes_boost))
            
            tags = ["tradfi", "market_structure", f"volatile_{tf}m_candle", ticker.lower()]
            await self._sync_geo_watchlist(ticker, tags)
            await self._update_volume_baseline(ticker, block["volume"])
    
            await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                "entity_id": ticker,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {"label": "Company", "primary_domain": "financial", "confidence": anomaly}
            }, key=ticker)
            
            entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
            direction = "🟢 Bullish" if block["close"] >= block["open"] else "🔴 Bearish"
            headline = f"{direction} Structural Anomaly: {ticker} {tf}-min moved {price_change_pct*100:+.2f}% on ${notional/1e6:.1f}M vol"
    
            # Compute Parkinson volatility for the bar
            parkinson = quant_calc.parkinson_volatility([block["high"]], [block["low"]])

            # Multi-bar microstructure from 15-bar rolling history
            history_vol_key = f"tradfi:history{tf}m:{ticker}:volumes"
            history_not_key = f"tradfi:history{tf}m:{ticker}:notionals"
            history_cls_key = f"tradfi:history{tf}m:{ticker}:closes"

            bar_k_lambda = 0.0
            bar_ami = 0.0
            bar_vwap = block["close"]  # fallback

            try:
                cls_bytes, vol_bytes, not_bytes = await asyncio.gather(
                    self.redis_client.raw.lrange(history_cls_key, 0, 14),
                    self.redis_client.raw.lrange(history_vol_key, 0, 14),
                    self.redis_client.raw.lrange(history_not_key, 0, 14),
                )
                hist_closes = [float(c) for c in reversed(cls_bytes)] if cls_bytes else []
                hist_volumes = [float(v) for v in reversed(vol_bytes)] if vol_bytes else []
                hist_notionals = [float(n) for n in reversed(not_bytes)] if not_bytes else []

                # Append current bar
                hist_closes.append(block["close"])
                hist_volumes.append(block["volume"])
                hist_notionals.append(block["close"] * block["volume"])

                if len(hist_closes) >= 2:
                    # Kyle's λ: ΔP vs signed volumes (guarded by n≥10 inside kyle_lambda)
                    price_changes = [hist_closes[i] - hist_closes[i - 1] for i in range(1, len(hist_closes))]
                    hist_signed_vols = [hist_volumes[i] if hist_closes[i] >= hist_closes[i - 1] else -hist_volumes[i] for i in range(1, len(hist_closes))]
                    bar_k_lambda = quant_calc.kyle_lambda(price_changes, hist_signed_vols)

                    # Amihud: proper period returns |r_t| = |(close_t - close_{t-1}) / close_{t-1}|
                    if all(c > 0 for c in hist_closes):
                        returns = [abs((hist_closes[i] - hist_closes[i - 1]) / hist_closes[i - 1]) for i in range(1, len(hist_closes))]
                        valid_notionals = hist_notionals[1:]
                        if valid_notionals and all(n > 0 for n in valid_notionals):
                            bar_ami = quant_calc.amihud_illiquidity(returns, valid_notionals)

                # Multi-bar VWAP
                if hist_volumes and sum(hist_volumes) > 0:
                    bar_vwap = quant_calc.vwap(hist_closes, hist_volumes)
            except Exception as e:
                logger.debug(f"Multi-bar microstructure failed for {ticker} {tf}m: {e}")

            micro = MarketMicrostructure(
                parkinson_volatility=parkinson,
                vwap=bar_vwap,
                twap=(block["high"] + block["low"] + block["close"]) / 3.0,
                realized_volatility=volatility_pct,
                kyle_lambda=bar_k_lambda,
                amihud_illiquidity=bar_ami,
            )

            breakdown = AnomalyBreakdown(
                composite_score=anomaly,
                volatility_z_score=round(volatility_pct * 10.0, 2),
                cross_domain_correlation_score=round(bar_hawkes_ratio, 4),
                domain="tradfi",
                is_significant=gate_significant,
            )
            
            bar_summary = f"Multi-Timeframe Structural Candle Anomaly on {ticker} ({tf}-minute frame): moved {price_change_pct*100:+.2f}% to ${block['close']:.2f} on ${notional/1e6:.2f}M volume. High: ${block['high']:.2f}, Low: ${block['low']:.2f}. VWAP: ${bar_vwap:.2f}, Parkinson Volatility: {parkinson:.4f}. Anomaly Score: {anomaly:.2f}."

            # Populate active statistical correlation IDs (§8.2)
            stat_corr_ids = []
            try:
                raw_corr_ids = await self.redis_client.raw.smembers(f"sentinel:correlation:active_ids:{ticker}")
                if raw_corr_ids:
                    stat_corr_ids = [c.decode() if isinstance(c, bytes) else str(c) for c in raw_corr_ids]
            except Exception:
                pass

            events.append(NormalizedEvent(
                event_id=raw.event_id, trace_id=raw.trace_id,
                type=EventType.MARKET_ANOMALY,
                occurred_at=datetime.fromisoformat(block["start_ts"]),
                source=raw.source,
                source_reliability=baseline_reliability(raw.source),
                primary_entity=entity,
                financial_data=FinancialData(
                    ticker=ticker, 
                    instrument_type="equity",
                    trade_type=f"OHLCV_{tf}M_BAR", 
                    # As above: a bar's dollar volume is a notional, not a premium.
                    notional_usd=notional,
                    premium_usd=notional,
                    underlying_price=block["close"],
                    volume=block["volume"],
                    open_price=block["open"],
                    high_price=block["high"],
                    low_price=block["low"],
                    close_price=block["close"]
                ),
                headline=headline,
                summary=bar_summary,
                tags=tags,
                anomaly_score=anomaly,
                anomaly_breakdown=breakdown,
                market_microstructure=micro,
                score_adjustments=bar_adjustments,
                correlation_ids=stat_corr_ids,
            ))
            
        return events

    async def _sync_geo_watchlist(self, ticker, tags):
        try:
            import json, time
            cached = await self.redis_client.raw.get(f"sentinel:ontology:entity:{ticker.lower()}")
            if cached:
                data = json.loads(cached)
                concepts = data.get("macro_concepts", [])
                if concepts:
                    tags.append("geo_linked_asset")
                    tags.extend(concepts)
                    async with self.redis_client.raw.pipeline(transaction=True) as pipe:
                        pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                        pipe.zremrangebyrank("sentinel:watched:equities", 0, -51)
                        await pipe.execute()
        except Exception as e:
            logger.error(f"Failed to update geo watchlist for {ticker}: {e}", exc_info=True)
                
    async def _update_volume_baseline(self, ticker: str, volume: float):
        """EMA baselining (α=0.001) for tick-level block sizes to handle heavy-tailed distributions."""
        try:
            key = f"baseline:volume:{ticker}"
            current = await self.redis_client.raw.get(key)
            updated = (0.999 * float(current) + 0.001 * volume) if current else volume
            await self.redis_client.raw.set(key, str(round(updated, 3)), ex=604800)
        except Exception as e:
            logger.error(f"Failed to update volume baseline for {ticker}: {e}")

    async def _enrich_insider(self, raw, p) -> Optional[NormalizedEvent]:
        # Shared adjustment allowance, reset per event -- as in the other
        # enrichment paths. Omitting it here raised NameError on every insider
        # event and, because the caller swallows per-event failures, the whole
        # path went silent with errors=0 on the heartbeat.
        lift_spent = 0.0
        ticker = (p.get("ticker") or "").upper()
        if not ticker:
            title = p.get("title", "")
            # EDGAR titles read "4 - ISABELLA BANK CORP (0000842517) (ISSUER)".
            #
            # The alphabetic match here skipped the numeric CIK and landed on
            # the trailing role, so 497 insider events in six hours carried five
            # distinct entities between them: CHUCK, FILER, ISSUER, REPORTING,
            # SUBJECT. CHUCK is the tell -- the title was
            # "SCHWAB CHARLES (0000316709) (CHUCK)".
            #
            # The CIK is cleanly extractable in every case and is the issuer's
            # actual identifier, so it is read first and resolved through the
            # map the platform already maintains. A role label is never an
            # entity, so those are excluded by name rather than by position.
            ticker = None
            cik_match = re.search(r'\(\s*(\d{6,10})\s*\)', title)
            if cik_match:
                ticker = await self._resolve_cik(cik_match.group(1))

            if not ticker:
                for cand in re.findall(r'\(\s*([A-Za-z.\-]{1,6})\s*\)', title):
                    if cand.upper() not in FORM4_ROLE_LABELS:
                        ticker = cand.upper()
                        break

            if not ticker:
                # An issuer that cannot be named is not an insider signal about
                # anybody. Recording it under a role label is worse than
                # dropping it, because it groups unrelated companies together.
                logger.debug(f"Form 4 skipped: no resolvable issuer in title {title!r}")
                return None
        
        # Robustly parse code, value, and role from summary HTML if absent from raw_payload
        summary_html = p.get("summary") or ""
        
        code = p.get("transaction_code")
        if not code:
            code_match = re.search(r'(?:<b>Code:</b>|Code:)\s*([A-Z])', summary_html, re.IGNORECASE)
            code = code_match.group(1).upper() if code_match else "J"
            
        # An unparseable value means the size is unknown, which is not the same
        # fact as a size of zero.
        #
        # The EDGAR summary carries only Filed:, AccNo: and Size:, so the Value:
        # regex found nothing and every filing was priced at $0. The score below
        # is `value / 10_000_000 * 0.3`, so insider_trade averaged 0.0053 with a
        # daily maximum of 0.20 against a correlation-window floor of 0.15 --
        # 679 events in twenty-four hours and exactly one cleared it. Two rules
        # written for this signal have never fired.
        value = p.get("transaction_value_usd")
        value_known = False
        if value:
            value = float(value)
            value_known = True
        else:
            val_match = re.search(r'(?:<b>Value:</b>|Value:)\s*\$([0-9,]+(?:\.[0-9]+)?)', summary_html, re.IGNORECASE)
            if not val_match:
                # EDGAR's own field name for the same quantity.
                val_match = re.search(r'(?:<b>Size:</b>|Size:)\s*\$?([0-9,]+(?:\.[0-9]+)?)', summary_html, re.IGNORECASE)
            if val_match:
                try:
                    value = float(val_match.group(1).replace(",", ""))
                    value_known = True
                except ValueError:
                    value = None
            else:
                value = None
            
        title = p.get("role") or p.get("title") or ""
        if not title:
            rel_match = re.search(r'(?:<b>Relationship:</b>|Relationship:)\s*([^<]+)', summary_html, re.IGNORECASE)
            if rel_match:
                title = rel_match.group(1).strip()
            else:
                title = p.get("title") or ""
        title = title.upper()
        
        # Suppress noise: standard compensation and tax withholding below $500k.
        # Only where the size is actually known -- an unsizeable award is not
        # evidence that it was small.
        if code in ("A", "F") and value_known and value < 500_000:
            return None
            
        code_label = FORM4_CODES.get(code, "Transaction")
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
        
        # Role-based weighting multiplier
        # The base is the size where the size is known, and the transaction's own
        # kind where it is not. An open-market purchase by an officer is the
        # highest-signal thing Form 4 reports whatever its dollar value, and
        # scoring it zero for want of a number the filing did not print is what
        # kept the entire insider path below every downstream floor.
        if value_known:
            anomaly = min(1.0, value / 10_000_000 * 0.3)
        else:
            anomaly = FORM4_UNSIZED_BASE.get(code, 0.05)
        if "CEO" in title:
            anomaly = _lift(anomaly, 0.5, lift_spent)
            lift_spent += 0.5
        elif "CFO" in title:
            anomaly = _lift(anomaly, 0.4, lift_spent)
            lift_spent += 0.4
        elif "COO" in title or "PRESIDENT" in title:
            anomaly = _lift(anomaly, 0.2, lift_spent)
            lift_spent += 0.2
        elif "DIRECTOR" in title:
            anomaly = _lift(anomaly, 0.1, lift_spent)
            lift_spent += 0.1
        elif any(w in title for w in ("TEN PERCENT OWNER", "10% OWNER", "10 PERCENT")):
            anomaly = _lift(anomaly, 0.3, lift_spent)
            lift_spent += 0.3
        # Open market buys are high conviction
        if code == "P":
            anomaly = _lift(anomaly, 0.2, lift_spent)
            lift_spent += 0.2
        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "insider_trade")
        anomaly = _lift(anomaly, w_boost, lift_spent)
        lift_spent += w_boost
        anomaly = round(_lift(anomaly, f_boost, lift_spent), 4)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial"}
        }, key=ticker)

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.INSIDER_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker, instrument_type="equity", premium_usd=value
            ),
            headline=(
                f"Insider {code_label}: {ticker} ${value/1e6:.1f}M by {title}"
                if value_known else
                f"Insider {code_label}: {ticker} by {title} (size not disclosed)"
            ),
            tags=(
                ["tradfi", "insider_trade", ticker.lower(), code_label.lower().replace(" ", "_")]
                + ([] if value_known else ["size_unknown"])
            ),
            anomaly_score=round(anomaly, 3),
        )

    async def _resolve_cik(self, cik: str) -> Optional[str]:
        """Issuer ticker for an SEC CIK, or None.

        The newer filings collector starts from a ticker and resolves forward,
        so it never needed this. The Form 4 path runs backwards from an EDGAR
        title and had no map at all, which is why it fell through to matching
        the role label instead.

        `sentinel:sec:ticker_by_cik` is written by the filings collector as it
        walks its watchlist -- the reverse of the map it already kept, which
        only ran ticker to CIK. A miss returns None rather than a guess: an
        issuer that cannot be named is not an insider signal about anybody.
        """
        if not cik:
            return None
        key = str(cik).lstrip("0") or "0"
        try:
            for candidate in (str(cik), key, str(cik).zfill(10)):
                hit = await self.redis_client.raw.hget("sentinel:sec:ticker_by_cik", candidate)
                if hit:
                    resolved = hit.decode() if isinstance(hit, bytes) else str(hit)
                    resolved = resolved.strip().upper()
                    if resolved and is_valid_primary_equity(resolved):
                        return resolved
        except Exception as e:
            logger.debug(f"CIK resolution unavailable for {cik}: {e}")
        return None

    async def _underlying_spot(self, ticker: str) -> Optional[float]:
        """The underlying's last traded price, or None.

        Reads the quote cache the equities path already maintains. Returns None
        rather than a fallback: every consumer of underlying_price is better
        served by an absent value than by the option's own price wearing the
        underlying's name, which is the defect this replaces.
        """
        if not ticker or not self.redis_client:
            return None
        try:
            raw = await self.redis_client.raw.get(quote_key(ticker))
            if not raw:
                return None
            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            value = data.get("price") or data.get("close") or data.get("last")
            spot = float(value) if value is not None else None
            return spot if spot and spot > 0 else None
        except Exception:
            return None

    async def _enrich_options_flow(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None
        
        option_symbol = p.get("option_symbol", "")
        price = float(p.get("price", 0.0))
        volume = float(p.get("volume", 0.0))
        premium = float(p.get("premium_usd", 0.0))
        
        option_type = p.get("option_type")
        strike = p.get("strike")
        expiry = p.get("expiry")
        if (not option_type or strike is None or not expiry) and option_symbol:
            from shared.utils.equities import parse_occ_option_symbol
            parsed = parse_occ_option_symbol(option_symbol)
            if parsed:
                option_type = option_type or parsed.get("option_type")
                strike = strike if strike is not None else parsed.get("strike")
                expiry = expiry or parsed.get("expiry")
        
        option_type = str(option_type or "CALL").upper()

        # The underlying's last price, if the platform has one.
        underlying_spot = await self._underlying_spot(ticker)
        implied_volatility = float(p["implied_volatility"]) if p.get("implied_volatility") is not None else None
        open_interest = int(p["open_interest"]) if p.get("open_interest") is not None else None
        
        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "options_flow")
        
        # Log-scaled against what an options sweep actually costs.
        #
        # This was `premium / 1_000_000 * 0.5`, linear, which gave a $118,700
        # AAPL sweep a score of 0.059 and needed a $2M premium to reach 1.0.
        # Measured live: thirty consecutive options events, highest 0.231, mean
        # 0.054 -- so options flow never cleared any correlation threshold, and
        # the informed-trading rule that requires options_flow at min_anomaly
        # 0.3 could not fire at all.
        #
        # Premiums span four orders of magnitude and the interesting range
        # starts around $50k, so the scale is logarithmic between the floor and
        # the reference: $50k reads 0.0, $500k reads about 0.5, $5M reads 1.0.
        base_score = _options_premium_score(premium)

        # Sized against the session it happened in.
        #
        # A $250k sweep at 10:00 and one at 03:00 are different claims: overnight
        # and pre-market books are a fraction as deep, so the same premium buys a
        # far larger share of available liquidity and says more about urgency
        # than about conviction. session_liquidity_factor was written for exactly
        # this and had no call site anywhere in the tree.
        #
        # Dividing rather than multiplying: the same absolute size is *more*
        # notable in a thin session, not less. Bounded at 1.0 so a thin-session
        # sweep cannot exceed the scale.
        _session_depth = session_liquidity_factor(asset_class="equities")
        if 0 < _session_depth < 1.0:
            base_score = min(1.0, base_score / _session_depth)
        lift_spent = 0.0
        anomaly = _lift(base_score, w_boost, lift_spent)
        lift_spent += w_boost
        anomaly = round(_lift(anomaly, f_boost, lift_spent), 4)
        
        tags = ["tradfi", "options_flow", ticker.lower(), option_type.lower()]
        if premium >= 100000.0:
            tags.append("options_sweep")
            
        entity = Entity(id=ticker, type=EntityType.COMPANY, name=ticker)
        
        # Sector, industry and index membership for the underlying. The
        # equity path has fetched these for some time; this one never did,
        # so every options sweep reached the reasoning layer unable to say
        # what it was exposed to.
        ref_data = None
        try:
            from services.enrichment.ref_data import get_reference_data
            ref_data = await get_reference_data(self.redis_client, ticker)
        except Exception as e:
            logger.debug(f"Reference data lookup failed for {ticker}: {e}")

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.OPTIONS_FLOW,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker,
                instrument_type="option",
                side=option_type,
                trade_type="OPTIONS_FLOW",
                strike=float(strike) if strike is not None else None,
                expiry=expiry,
                premium_usd=premium,
                # Contract notional, not the premium.
                #
                # notional_usd was null on all 896 options_flow events in a
                # two-hour window while strike and volume were both present, so
                # it was never missing information -- only uncomputed. This is
                # the same confusion already corrected on the equity_block path,
                # where every consumer reading the obvious field saw nothing and
                # had to know to look in the wrong one.
                #
                # Premium is what the position cost; notional is what it
                # controls, and the two differ by orders of magnitude on an
                # out-of-the-money sweep. A size filter written against either
                # one means something quite different.
                notional_usd=_option_notional(strike, volume),
                underlying_price=underlying_spot,
                # The contract's own traded price, under a name that says so.
                close_price=price,
                volume=int(volume),
                open_interest=open_interest,
                # The two fields that make options flow readable, and that were
                # 0.0% populated across every event this enricher has produced.
                #
                # otm_percentage is pure arithmetic on strike, underlying and
                # type, all of which were already on the event. volume_oi_ratio
                # needs open interest, which this feed does not supply -- so it
                # stays null rather than becoming a 0.0 that would read as
                # "plenty of open interest, none of it traded".
                # The underlying's price, not the contract's.
                #
                # `price` here is the option's own traded price -- what one
                # share of the contract changed hands at. It was being written
                # to underlying_price and close_price, and the moneyness
                # calculation added earlier in this audit inherited the mistake
                # and made it visible: AAPL 310 calls reported 2511% out of the
                # money, which implies a spot of $11.87.
                #
                # The spot comes from the quote cache where it exists; where it
                # does not, moneyness is None rather than computed from the
                # wrong number. An absent field is recoverable and a wrong one
                # is not.
                otm_percentage=_otm_percentage(strike, underlying_spot, option_type),
                volume_oi_ratio=_volume_oi_ratio(volume, open_interest),
                implied_volatility=implied_volatility,
                option_type=option_type,
                # Reference data was fetched for the equity path and not this
                # one, so an options sweep reached the reasoning layer with no
                # sector attached and nothing could say what it was exposed to.
                sector=(ref_data or {}).get("sector"),
                industry=(ref_data or {}).get("industry"),
                exchange=(ref_data or {}).get("exchange"),
                index_membership=(ref_data or {}).get("index_membership", []),
            ),
            headline=f"🐋 OPTIONS FLOW {option_type} Sweep | {ticker} ({option_symbol}) | Premium: ${premium/1e3:.1f}k",
            tags=tags,
            anomaly_score=anomaly
        )

    async def _enrich_quant_radar(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None

        z_score = float(p.get("z_score", 0.0))
        volume = float(p.get("volume", 0.0))
        price = float(p.get("price", 0.0))
        notional = float(p.get("notional_usd", 0.0))

        import time as _time
        try:
            await self.redis_client.raw.zadd("sentinel:watched:equities", mapping={ticker: _time.time()})
            await self.redis_client.raw.zremrangebyrank("sentinel:watched:equities", 0, -51)
        except Exception:
            pass

        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "quant_radar")

        # A z-score is unbounded; the mapping onto 0-1 must not have a cliff.
        #
        # `min(1.0, z / 5.0)` saturated at five sigma, so a 5.02 and a 12.76 --
        # observed together in one fifteen-minute window -- were both exactly
        # 1.00, and 45 of the last 30 minutes' market_anomaly events sat on the
        # ceiling. That discards precisely the information separating a notable
        # spike from an extraordinary one, which is the whole content of a
        # z-score.
        #
        # Exponential saturation keeps the existing calibration where it
        # matters and stops the cliff: five sigma still clears the 0.6 the
        # downstream thresholds use, and ten and twenty sigma remain
        # distinguishable from it and from each other. The curve approaches 1.0
        # and never arrives, which is the honest shape for an unbounded
        # statistic -- there is always a larger spike.
        base_score = 1.0 - math.exp(-max(0.0, z_score) / Z_SCORE_SCALE)
        lift_spent = 0.0
        anomaly = _lift(base_score, w_boost, lift_spent)
        lift_spent += w_boost
        anomaly = round(_lift(anomaly, f_boost, lift_spent), 4)
        lift_spent += f_boost

        # Unusual for this instrument, then weighted by whether the flow was
        # big enough to matter.
        #
        # Live on 4 September the only two market_anomaly events at exactly
        # 1.000 were TBIL and SGOV -- ultra-short Treasury ETFs, both at
        # "Z-Score: 50.00", on flows of $1.41M and $1.35M. Both numbers are
        # explained by the same thing: a cash-equivalent fund has almost no
        # volume variance, so the denominator collapses, the z-score pins to
        # the collector's reporting cap, and a million dollars of a money-market
        # proxy is ranked as the most extreme event on the platform.
        #
        # The z-score is right that this is unusual for TBIL. It cannot know
        # that $1.4M of TBIL is not worth waking anyone for. Against the $25M
        # market_anomaly reference this attenuates to about 0.59 -- still
        # anomalous, no longer top of the book.
        anomaly = apply_materiality(anomaly, notional, "market_anomaly")

        # And never exactly certain.
        #
        # The curve above is documented as approaching 1.0 and never arriving,
        # which is true of the arithmetic and not of the value stored: at the
        # capped z of 50, 1 - exp(-10) is 0.99995, and round(x, 4) arrives.
        # Every other detector in this platform is bounded by
        # FALLBACK_MAX_SCORE; this path was bounded only by a rounding mode.
        anomaly = round(min(anomaly, FALLBACK_MAX_SCORE), 4)

        tags = ["tradfi", "radar_anomaly", ticker.lower()]
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.MARKET_ANOMALY,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker,
                instrument_type="equity",
                trade_type="RADAR_ANOMALY",
                premium_usd=notional,
                underlying_price=price,
                volume=volume,
            ),
            headline=f"⚡ QUANT RADAR VOLUME SPIKE | {ticker} | Z-Score: {z_score:.2f} | Flow: ${notional/1e6:.2f}M",
            tags=tags,
            anomaly_score=anomaly,
        )

    async def _pre_announcement_score(self, ticker: str, report_date: str) -> float:
        """How much an upcoming earnings report is worth watching.

        No surprise exists yet, so the two available signals are timing and the
        issuer's own track record:

          Proximity   A report tomorrow is actionable in a way one in seven
                      days is not. Linear over the lookahead window.

          Volatility  The per-ticker surprise variance this enricher already
                      maintains. An issuer that habitually lands far from
                      consensus has a genuinely more uncertain report coming.

        Both are read from what the system measured rather than asserted, and
        an issuer with no history simply scores on proximity alone.
        """
        proximity = 0.0
        try:
            report_dt = datetime.strptime(str(report_date), "%Y-%m-%d").date()
            days_out = (report_dt - datetime.now(timezone.utc).date()).days
            if 0 <= days_out <= EARNINGS_LOOKAHEAD_DAYS:
                proximity = 1.0 - (days_out / max(1.0, float(EARNINGS_LOOKAHEAD_DAYS)))
        except (ValueError, TypeError):
            proximity = 0.0

        volatility = 0.0
        try:
            raw_var = await self.redis_client.raw.get(
                f"sentinel:earnings:surprise_var:{ticker}"
            )
            if raw_var:
                # Standard deviation of past surprises, in percent. Ten points
                # of typical deviation is already an unpredictable issuer.
                volatility = min(1.0, (float(raw_var) ** 0.5) / 10.0)
        except (ValueError, TypeError, AttributeError):
            volatility = 0.0

        score = (
            PRE_ANNOUNCEMENT_FLOOR
            + PROXIMITY_WEIGHT * proximity
            + SURPRISE_VOLATILITY_WEIGHT * volatility
        )
        return round(min(PRE_ANNOUNCEMENT_CEILING, score), 4)

    async def _enrich_earnings_calendar(self, raw, p) -> Optional[NormalizedEvent]:
        """Enriches Finnhub earnings calendar events into NormalizedEvents.
        Uses dynamic EMA z-score on surprise % for anomaly scoring."""
        import os

        ticker = (p.get("ticker") or "").upper()
        if not ticker:
            return None

        trade_type = p.get("trade_type", "EARNINGS_REPORT")
        report_date = p.get("report_date", "")
        session = p.get("session", "")
        eps_estimate = p.get("eps_estimate")
        eps_actual = p.get("eps_actual")
        eps_surprise_pct = p.get("eps_surprise_pct")
        revenue_estimate = p.get("revenue_estimate")
        revenue_actual = p.get("revenue_actual")

        # Determine event type
        if trade_type == "EARNINGS_SURPRISE" and eps_actual is not None:
            event_type = EventType.EARNINGS_SURPRISE
        else:
            event_type = EventType.EARNINGS_REPORT

        # Anomaly scoring — dynamic EMA z-score on abs(surprise_pct)
        ref_data = None
        try:
            from services.enrichment.ref_data import get_reference_data
            ref_data = await get_reference_data(self.redis_client, ticker)
        except Exception as e:
            logger.debug(f"Reference data lookup failed for {ticker}: {e}")

        # Pre-announcement scoring.
        #
        # This was a flat 0.3, which made all 183 upcoming-earnings events in a
        # 45-minute window carry one score. A pre-announcement has no surprise
        # to measure yet, but that does not make every one of them equally
        # interesting, and a detector whose output never varies ranks nothing.
        #
        # Two things are already measured here and both bear on how much a
        # report is worth watching: how soon it lands, and how unpredictable
        # this issuer has been. The surprise EMA and variance are maintained a
        # few lines below for exactly this ticker, so an issuer that habitually
        # surprises raises its own upcoming report without anyone asserting it.
        anomaly = await self._pre_announcement_score(ticker, report_date)
        if eps_surprise_pct is not None:
            abs_surprise = abs(eps_surprise_pct)
            # Dynamic z-score against historical surprises for this ticker
            try:
                ema_alpha = float(os.getenv("EARNINGS_EMA_ALPHA", "0.1"))
                ema_key = f"sentinel:earnings:surprise_ema:{ticker}"
                var_key = f"sentinel:earnings:surprise_var:{ticker}"
                raw_mean = await self.redis_client.raw.get(ema_key)
                raw_var = await self.redis_client.raw.get(var_key)
                # No history means no z-score, not a z-score of zero.
                #
                # ema_mean defaulted to abs_surprise, so the first observation
                # of a ticker was compared against itself: z = 0, which the
                # max(0.3, ...) floor then reported as 0.300. Issuers report
                # quarterly and this EMA expires in four weeks, so there is
                # never a second observation -- every surprise scored 0.300
                # forever. Measured live: a +505.7% beat, a +2.4% beat, a -100%
                # miss and a -26.6% miss, all at exactly 0.300.
                #
                # With no baseline, the magnitude of the surprise is the only
                # thing actually known, so that is what gets scored. Once a
                # ticker has a history the z-score takes over and says the more
                # useful thing: how unusual this surprise is *for this issuer*.
                has_history = raw_mean is not None
                if not has_history:
                    raise _NoEarningsHistory

                ema_mean = float(raw_mean)
                ema_var = float(raw_var) if raw_var else 1.0
                std = max(ema_var ** 0.5, 0.01)
                z = (abs_surprise - ema_mean) / std
                # Update EMA
                new_mean = ema_alpha * abs_surprise + (1 - ema_alpha) * ema_mean
                new_var = ema_alpha * (abs_surprise - ema_mean) ** 2 + (1 - ema_alpha) * ema_var
                pipe = self.redis_client.raw.pipeline()
                pipe.set(ema_key, str(new_mean), ex=604800 * 4)
                pipe.set(var_key, str(new_var), ex=604800 * 4)
                await pipe.execute()
                # Map z-score to anomaly: z>=2 is significant
                anomaly = min(1.0, max(0.3, abs(z) / 4.0))
            except _NoEarningsHistory:
                # First sight of this issuer. Score what is known -- the size of
                # the surprise -- and let the EMA below record it for next time.
                anomaly = _surprise_magnitude_score(abs_surprise)
            except Exception:
                # Fallback: simple scaled surprise
                anomaly = _surprise_magnitude_score(abs_surprise)

        # Watchlist & Frequency boost
        # One allowance per event, shared by every lift below, so a run of
        # boosts cannot each take a share of what the last one left.
        lift_spent = 0.0
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "earnings")
        # Headroom lift rather than addition: `anomaly + w_boost + f_boost`
        # clamped at the ceiling, so a 0.85 score and a 0.99 score with the same
        # boosts became indistinguishable. The rest of this file was converted
        # for that reason; this path was missed.
        anomaly = _lift(anomaly, w_boost, lift_spent)
        lift_spent += w_boost
        anomaly = round(_lift(anomaly, f_boost, lift_spent), 4)
        lift_spent += f_boost

        # Direction tags
        direction = "beat" if (eps_surprise_pct or 0) > 0 else "miss" if (eps_surprise_pct or 0) < 0 else "inline"
        tags = ["tradfi", "earnings", ticker.lower(), direction]

        if event_type == EventType.EARNINGS_SURPRISE:
            surprise_str = f"{eps_surprise_pct:+.1f}%" if eps_surprise_pct is not None else "N/A"
            headline = f"📊 EARNINGS {'BEAT' if direction == 'beat' else 'MISS' if direction == 'miss' else 'INLINE'} | {ticker} | EPS: {eps_actual} vs Est {eps_estimate} ({surprise_str})"
        else:
            session_label = {"bmo": "Pre-Market", "amc": "After-Close", "dmh": "During Hours"}.get(session, session.upper() if session else "TBD")
            headline = f"📅 EARNINGS UPCOMING | {ticker} | Date: {report_date} ({session_label}) | Est EPS: {eps_estimate}"

        entity = Entity(id=ticker, type=EntityType.COMPANY, name=ticker)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial"}
        }, key=ticker)

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=event_type,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker,
                instrument_type="equity",
                trade_type=trade_type,
                earnings_report_date=report_date,
                earnings_session=session,
                eps_estimate=float(eps_estimate) if eps_estimate is not None else None,
                eps_actual=float(eps_actual) if eps_actual is not None else None,
                eps_surprise_pct=float(eps_surprise_pct) if eps_surprise_pct is not None else None,
                revenue_estimate=float(revenue_estimate) if revenue_estimate is not None else None,
                revenue_actual=float(revenue_actual) if revenue_actual is not None else None,
                # Which sector is reporting.
                #
                # "Bad earnings at X moves its peers" is the question this event
                # exists to feed, and it cannot be asked of a record that does
                # not say what X is comparable to. The lookup is a Redis read
                # the equity and options paths already make.
                sector=(ref_data or {}).get("sector"),
                industry=(ref_data or {}).get("industry"),
                index_membership=(ref_data or {}).get("index_membership", []),
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )

    async def _enrich_sec_filing(self, raw, p) -> Optional[NormalizedEvent]:
        """Enriches SEC EDGAR 8-K, 10-K, 10-Q, S-1 corporate filings."""
        ticker = (p.get("ticker") or "").upper().strip()
        form_type = p.get("form_type", "8-K")
        company_name = p.get("company_name", ticker)
        is_8k = p.get("is_material_8k", False) or form_type.startswith("8-K")
        items = p.get("items", [])
        doc_url = p.get("primary_doc_url", "")
        f_date = p.get("filing_date", "")

        tags = list(p.get("tags", []))
        tags.extend(["corporate_disclosure", "sec_edgar", f"form:{form_type}"])
        if is_8k:
            tags.extend(["material_event", "ground_truth"])

        anomaly = _filing_form_score(form_type, is_8k)
        headline = p.get("title") or f"📄 SEC FILING: {company_name} ({ticker}) filed Form {form_type}"
        summary = p.get("summary") or f"SEC filing {form_type} for {company_name} ({ticker}) on {f_date}."

        entity = Entity(id=ticker or company_name, type=EntityType.COMPANY, name=company_name)

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.FILING,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=0.99,
            primary_entity=entity,
            filing_data=FilingData(
                ticker=ticker,
                cik=p.get("cik"),
                company_name=company_name,
                form_type=form_type,
                filing_date=f_date,
                report_date=p.get("report_date"),
                items=items,
                primary_doc_url=doc_url,
                accession_number=p.get("accession_number"),
                is_material_8k=is_8k,
                description=summary,
            ),
            headline=headline,
            summary=summary,
            url=doc_url,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )

    async def _enrich_freight_rate(self, raw, p) -> Optional[NormalizedEvent]:
        """Enriches a freight-rate index update into a supply-chain event.

        The collector has been running and publishing FREIGHT_RATE_UPDATE to
        RAW_TRADFI, and this enricher had no branch for it -- so the events fell
        through the routing chain and returned None. A live feed of Baltic Dry,
        container and tanker rates was being collected, paid for in polling and
        bandwidth, and discarded on arrival.

        SupplyChainData exists in the model for exactly this shape and had never
        been constructed once: every field below maps onto a key the collector
        already sends. It was one of three sub-models declared on NormalizedEvent
        that nothing populated.

        Freight rates are a real macro transmission channel -- a spike in the
        Shanghai-to-Rotterdam container rate reaches goods inflation and the
        margins of every importer on the watchlist -- which is why the collector
        was written. This is what makes it readable downstream.
        """
        index_name = p.get("index_name") or p.get("index_symbol") or "FREIGHT_INDEX"
        symbol = str(p.get("index_symbol") or index_name).upper()

        def _num(key):
            try:
                v = p.get(key)
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        current = _num("current_rate") or 0.0
        previous = _num("previous_rate")
        change_pct = _num("change_pct")
        is_spike = bool(p.get("is_rate_spike"))

        # Scored on the size of the move against the index's own recent history,
        # like every other magnitude in this system, rather than on the
        # collector's own boolean alone. The boolean still lifts it: the
        # collector applies a threshold this enricher cannot see.
        anomaly = FREIGHT_BASE_SCORE
        try:
            normalised = await self.scorer._dynamic_normalize(
                f"freight:{symbol}", "change_pct", abs(change_pct or 0.0)
            )
            anomaly = lift_score(
                FREIGHT_BASE_SCORE,
                FREIGHT_MAX_LIFT * math.tanh(abs(float(normalised)) / FREIGHT_MOVE_SCALE),
            )
        except Exception as e:
            logger.debug("Freight move scoring fell back to base for %s: %s", symbol, e)
        if is_spike:
            anomaly = lift_score(anomaly, FREIGHT_SPIKE_LIFT)

        direction = "surging" if (change_pct or 0) > 0 else "falling"
        corridor = p.get("corridor")
        headline = (
            f"🚢 FREIGHT {direction.upper()}: {index_name} "
            f"{current:,.0f}" + (f" ({change_pct:+.1f}%)" if change_pct is not None else "")
        )

        tags = list(p.get("tags") or [])
        for t in ("macro", "supply_chain", "freight"):
            if t not in tags:
                tags.append(t)

        entity = Entity(id=symbol, type=EntityType.INSTRUMENT, name=index_name)

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.MACRO_RELEASE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity=entity,
            supply_chain_data=SupplyChainData(
                index_name=index_name,
                category=str(p.get("category") or "FREIGHT"),
                current_value=current,
                previous_value=previous,
                change_14d_pct=change_pct,
                route_corridor=corridor,
                # The collector's benchmark table names a vessel class in the
                # description for tanker and dry-bulk routes; absent for
                # container indices, and left as None rather than guessed.
                vessel_class=p.get("vessel_class"),
                anomaly_flag=is_spike,
            ),
            headline=headline,
            summary=(
                f"{index_name} ({p.get('category', 'FREIGHT')}) at {current:,.2f}"
                + (f", previously {previous:,.2f}" if previous is not None else "")
                + (f", {change_pct:+.2f}%" if change_pct is not None else "")
                + (f". Corridor: {corridor}." if corridor else ".")
            ),
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )

    async def _enrich_13f_filing(self, raw, p) -> Optional[NormalizedEvent]:
        """Enriches quarterly 13F institutional portfolio filings."""
        filer_name = p.get("filer_name", "Institutional Manager")
        manager_name = p.get("manager_name", filer_name)
        filer_id = p.get("filer_id", "institutional")
        total_val = float(p.get("total_value_usd", 0.0))
        pos_count = int(p.get("positions_count", 0))
        period = p.get("report_period", "Current Quarter")

        tags = list(p.get("tags", []))
        tags.extend(["institutional_holdings", "13f_report", f"filer:{filer_id}"])

        # The filer is the firm. The manager is metadata, and it decays.
        #
        # This read "Jim Simons / Peter Brown (Renaissance Technologies LLC)
        # filed portfolio for 2026-06-30" -- Simons died in 2024. The roster of
        # principals is hand-maintained in thirteen_f.py and nothing revalidates
        # it, so every stale name in that table is asserted, in a headline, as
        # the person who filed a portfolio this quarter.
        #
        # An entity that files a 13F is the registered adviser, which is also
        # the only party the filing itself names. Leading with it is both more
        # accurate and stable; the principals stay available as the descriptive
        # field they actually are.
        headline = f"🏛️ 13F REPORT: {filer_name} filed portfolio for {period}"
        summary = (
            f"Institutional 13F-HR filing by {filer_name}. "
            f"Total Portfolio Value: ${total_val/1e9:.2f}B across {pos_count} holdings. "
            f"Principals on record: {manager_name}."
        )

        entity = Entity(id=filer_id, type=EntityType.COMPANY, name=filer_name)

        # One filing, one event. The poller re-reads the same quarter.
        #
        # Live over 48 hours: ten filers, sixty events, six per filer -- and per
        # filer exactly ONE distinct headline and ZERO distinct portfolio
        # values. The same quarterly filing was being re-polled and re-emitted
        # six times, because a 13F stays "current" for three months and nothing
        # remembered having seen it.
        #
        # That is also the whole reason the movement score below was stuck: it
        # measures a filing against the filer's own EMA, and feeding that EMA
        # six copies of one value drives the deviation to zero AND collapses
        # the variance the next real filing would be measured against. Every
        # thirteen_f event on the platform carried exactly 0.600, the base
        # score, and the scoring written to prevent that constant was running
        # correctly on an input that could not vary.
        #
        # Keyed on the values as well as the period, so an amended filing --
        # genuinely new information about the same quarter -- is not suppressed
        # along with the re-reads.
        try:
            fingerprint = f"{filer_id}:{period}:{total_val:.0f}:{pos_count}"
            claimed = await self.redis_client.raw.set(
                f"sentinel:enrich:13f:seen:{fingerprint}", "1",
                ex=THIRTEEN_F_DEDUP_TTL_SEC, nx=True,
            )
            if not claimed:
                return None
        except Exception as e:
            # A dedup failure must not drop the filing. Emitting a duplicate is
            # recoverable; losing a quarterly disclosure is not.
            logger.debug("13F dedup check failed for %s: %s", filer_id, e)

        # Scored against the filer's own history rather than published flat.
        #
        # This was `anomaly_score=0.60`, a constant, so every 13F ever emitted
        # carried the same number and the field ranked nothing: a manager whose
        # book doubled and one who reported an unchanged portfolio were
        # identical to every consumer, including the correlation layer that
        # derives confidence from anomaly and the reasoning scheduler that
        # decides what is worth an inference slot.
        #
        # A 13F is a quarterly snapshot, so what makes one notable is movement
        # against that filer's own previous filings -- not the absolute size of
        # the book, which says only how large the manager is. Both magnitudes
        # are on the event already; the same EMA normaliser the rest of the
        # system scores against turns them into a position in that filer's own
        # distribution.
        anomaly = THIRTEEN_F_BASE_SCORE
        try:
            norm_value, norm_count = await self.scorer._dynamic_normalize_batch([
                (f"13f:{filer_id}", "total_value_usd", total_val),
                (f"13f:{filer_id}", "positions_count", float(pos_count)),
            ])
            # A filing is interesting in proportion to how far it moved, in
            # either direction: a manager who halved a book is as notable as one
            # who doubled it.
            movement = max(abs(float(norm_value)), abs(float(norm_count)))
            # tanh, not min(). A hard `min(MAX_LIFT, movement / SCALE)` is
            # exhausted the moment movement reaches one sigma, which would put
            # every substantial filing back on a single value -- the same cliff
            # this change exists to remove, one level up.
            anomaly = lift_score(
                THIRTEEN_F_BASE_SCORE,
                THIRTEEN_F_MAX_LIFT * math.tanh(movement / THIRTEEN_F_MOVEMENT_SCALE),
            )
        except Exception as e:
            # The base score is a defensible floor: a 13F is a mandatory
            # disclosure from a large institution and is worth reading whether
            # or not this normalisation succeeded.
            swallowed("enrichment.13f_movement_score", e, logger, detail=str(filer_id))

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.THIRTEEN_F,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=0.99,
            primary_entity=entity,
            thirteen_f_data=ThirteenFData(
                filer_id=filer_id,
                filer_name=filer_name,
                manager_name=manager_name,
                cik=p.get("cik", ""),
                report_period=period,
                total_value_usd=total_val,
                holdings_count=pos_count,
            ),
            headline=headline,
            summary=summary,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )