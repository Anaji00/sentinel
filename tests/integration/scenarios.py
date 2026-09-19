"""Eight domains, fourteen situations, each one a thing that actually happens.

Written as an analyst would describe them, not as the rules are written. That
direction matters: a scenario derived from a rule can only ever confirm the
rule evaluates itself, and three of the gaps this file found were rules that
did not exist for situations the platform collects data on every day.

Each scenario names the real pattern it stands for. None of them is exotic --
these are the recurring shapes of market and geopolitical intelligence, the
ones a desk would expect any system worth running to flag:

  tradfi      Form 4 sale -> put accumulation -> gap down (informed trading)
              earnings surprise into pre-positioned flow
              13F reveals a new stake that the tape was already showing
  crypto      perp funding flips, cascade liquidations, listed proxies follow
              a stablecoin depeg drains an exchange
  news        a headline about one company, that company's stock moves
  cyber       ransomware on a port operator, ships at that port go dark
              routing interference in a theatre, and the aircraft in it
              a breach is disclosed and the stock reprices
  maritime    a tanker goes dark in a chokepoint, freight rates move
  aviation    military transport surge into a theatre
  macro       a hot CPI print moves rates, equities and gold together
  prediction  a contract repricing ahead of the equity it implicates

The timings are the real ones. A Form 4 has two business days to be filed, so
the insider leg sits days ahead of the trade; options positioning runs hours to
days ahead of a catalyst; a liquidation cascade completes in minutes. Those
offsets are what the sequence rules are actually tested against.
"""
from __future__ import annotations

from shared.models.events import EntityType

from tests.integration.market_scenarios import Beat, Scenario

HOUR = 60.0
DAY = 24 * HOUR


# ── TRADFI ──────────────────────────────────────────────────────────────────

INFORMED_TRADING = Scenario(
    name="Form 4 sale, then put accumulation, then the gap down",
    domain="tradfi",
    why=(
        "The canonical informed-trading sequence and the single highest-value "
        "pattern a platform like this can find. An officer sells, unusual put "
        "volume follows, and the stock gaps down on news days later. The order "
        "is the signal: the same three events shuffled are a coincidence."
    ),
    trigger=Beat("price_anomaly", "ACME", 0, 0.81, headline="ACME -14% on guidance cut",
             source="alpaca"),
    evidence=[
        Beat("insider_trade", "ACME", 4 * DAY, 0.55,
             headline="ACME CFO sells 180,000 shares, Form 4",
             source="sec_edgar"),
        Beat("options_flow", "ACME", 2 * DAY, 0.62,
             headline="ACME Jan puts, 11x average volume",
             source="alpaca_options"),
        Beat("dark_pool", "ACME", 1 * DAY, 0.44,
             headline="ACME dark pool print, 420k shares",
             source="institutional_fix"),
    ],
    expect_rule="rule_informed_trading_sequence",
    expect_evidence_types=("insider_trade", "options_flow"),
)

EARNINGS_SURPRISE = Scenario(
    name="Earnings surprise into flow that was already positioned",
    domain="tradfi",
    why=(
        "A large beat or miss with options and block activity in the days "
        "before it is the difference between a surprise and a leak. Both legs "
        "are collected; the question is whether they are ever joined."
    ),
    trigger=Beat("earnings_surprise", "NVDA", 0, 0.74,
                 headline="NVDA beats by 22%, guides above consensus",
             source="finnhub_earnings"),
    evidence=[
        Beat("options_flow", "NVDA", 30 * HOUR, 0.58,
             headline="NVDA weekly calls sweep, 8x average",
             source="alpaca_options"),
        Beat("equity_block", "NVDA", 20 * HOUR, 0.51,
             headline="NVDA block, 1.2m shares above ask",
             source="finnhub_equities"),
    ],
    expect_rule="rule_earnings_surprise_flow",
    expect_evidence_types=("options_flow", "equity_block"),
)

THIRTEEN_F_SHIFT = Scenario(
    name="A 13F reveals a stake the tape had already shown",
    domain="tradfi",
    why=(
        "13F is 45 days stale by definition, so its value is entirely in "
        "joining it to the microstructure from the quarter it covers. On its "
        "own it is a press release."
    ),
    trigger=Beat("thirteen_f", "OXY", 0, 0.60,
                 headline="Berkshire discloses increased OXY position",
             source="sec_edgar"),
    evidence=[
        Beat("equity_block", "OXY", 5 * DAY, 0.47, headline="OXY block, 2.1m shares",
             source="finnhub_equities"),
        Beat("dark_pool", "OXY", 3 * DAY, 0.42, headline="OXY dark pool accumulation",
             source="institutional_fix"),
    ],
    expect_rule="rule_institutional_position_shift",
    expect_evidence_types=("equity_block", "dark_pool"),
)

FILING_POSITIONING = Scenario(
    name="An 8-K lands on top of options positioning",
    domain="tradfi",
    why=(
        "Material 8-K items -- a bankruptcy, an auditor resignation, a merger "
        "agreement -- are the highest-signal filings the SEC publishes, and "
        "positioning ahead of one is exactly what a surveillance desk looks "
        "for."
    ),
    trigger=Beat("filing", "XYZ", 0, 0.66,
                 headline="XYZ 8-K Item 1.03, Chapter 11 petition",
             source="sec_edgar"),
    evidence=[
        Beat("options_flow", "XYZ", 36 * HOUR, 0.61,
             headline="XYZ far out-of-the-money puts, unusual size",
             source="alpaca_options"),
        Beat("equity_block", "XYZ", 12 * HOUR, 0.44, headline="XYZ block, seller",
             source="finnhub_equities"),
    ],
    expect_rule="rule_insider_filing_flow",
    expect_evidence_types=("options_flow",),
)


# ── CRYPTO ──────────────────────────────────────────────────────────────────

LIQUIDATION_CASCADE = Scenario(
    name="Funding flips, longs liquidate, the listed proxies follow",
    domain="crypto",
    why=(
        "The most reliable intraday pattern in crypto: crowded funding, a "
        "cascade of forced selling, and the equity proxies (COIN, MSTR, the "
        "miners) tracking it within the hour. It is also the clearest "
        "cross-asset contagion path this platform can observe directly."
    ),
    trigger=Beat("crypto_liquidation", "BTC", 0, 0.79,
                 entity_type=EntityType.INSTRUMENT,
                 headline="BTC perp liquidations $840m in 40 minutes",
             source="okx_swap"),
    evidence=[
        Beat("crypto_perp_funding", "BTC", 6 * HOUR, 0.56,
             entity_type=EntityType.INSTRUMENT,
             headline="BTC funding at 0.09%, 97th percentile",
             source="binance_futures"),
        Beat("crypto_trade", "BTC", 30, 0.52,
             entity_type=EntityType.INSTRUMENT,
             headline="BTC spot -7% on 4x volume",
             source="coinbase_spot"),
        # After the cascade, not before it. Crypto trades overnight and the
        # proxies gap at the open, which is the direction the word "spillover"
        # is claiming.
        Beat("price_anomaly", "COIN", -45, 0.58,
             headline="COIN -9%, tracking crypto complex",
             source="alpaca"),
    ],
    expect_rule="rule_crypto_equity_contagion",
    expect_evidence_types=("crypto_trade", "price_anomaly"),
    expect_min_domains=2,
)

STABLECOIN_DEPEG = Scenario(
    name="A stablecoin depegs and the exchange bleeds reserves",
    domain="crypto",
    why=(
        "Depegs are preceded by large on-chain transfers out of the issuer or "
        "the exchange holding the reserve. The transfer leg is collected and "
        "the trade leg is collected; a depeg is the two of them together."
    ),
    trigger=Beat("crypto_liquidation", "USDX", 0, 0.77,
                 entity_type=EntityType.INSTRUMENT,
                 headline="USDX at 0.94, forced unwinds across venues",
             source="okx_swap"),
    evidence=[
        Beat("crypto_transfer", "USDX", 3 * HOUR, 0.68,
             entity_type=EntityType.INSTRUMENT,
             headline="USDX 400m transferred off exchange",
             source="crypto_divergence"),
        Beat("crypto_trade", "USDX", 1 * HOUR, 0.59,
             entity_type=EntityType.INSTRUMENT,
             headline="USDX/USDT redemption volume 12x",
             source="coinbase_spot"),
    ],
    expect_rule="rule_crypto_stress_cascade",
    expect_evidence_types=("crypto_transfer", "crypto_trade"),
)


# ── NEWS ────────────────────────────────────────────────────────────────────

HEADLINE_IMPACT = Scenario(
    name="A headline about a company, and that company's stock",
    domain="news",
    why=(
        "The simplest cross-domain claim the platform makes, and the one its "
        "users will check first. A recall, an investigation, a lost contract "
        "-- the headline names the company and the tape reacts inside the "
        "session."
    ),
    trigger=Beat(
        "headline", "BOEING", 0, 0.72,
        entity_type=EntityType.COMPANY,
        headline="FAA opens investigation into Boeing 737 production line",
        tags=["BA", "aviation", "regulatory"],
        named_entities=["BOEING", "BA", "FAA"],
        source="reddit"),
    evidence=[
        Beat("price_anomaly", "BA", 4 * HOUR, 0.63,
             headline="BA -6.2% intraday", tags=["BA", "regulatory"],
             named_entities=["BOEING", "BA"],
             source="alpaca"),
        Beat("options_flow", "BA", 3 * HOUR, 0.55,
             headline="BA put volume 5x average", tags=["BA"],
             named_entities=["BOEING", "BA"],
             source="alpaca_options"),
    ],
    expect_rule="rule_news_financial_impact",
    expect_min_tier="ALERT",
    expect_evidence_types=("price_anomaly", "options_flow"),
    expect_min_domains=2,
)

HEADLINE_UNRELATED = Scenario(
    name="A headline about one company, a move in an unrelated one",
    domain="news",
    why=(
        "The negative case, and the reason the rule declares a join at all. A "
        "semiconductor export headline and a move in an unrelated retailer "
        "share nothing but a 24-hour window. If this fires, the rule is "
        "asserting a relationship it has no evidence for -- which is how the "
        "layer came to publish 69% of its output on a shared window alone."
    ),
    trigger=Beat(
        "headline", "TSMC", 0, 0.70,
        headline="TSMC raises capex guidance for advanced packaging",
        tags=["TSM", "semiconductors"],
        named_entities=["TSMC", "TSM"],
        source="reddit"),
    evidence=[
        Beat("price_anomaly", "KROGER", 6 * HOUR, 0.61,
             headline="KR -4% on grocery margin miss",
             tags=["KR", "retail"], named_entities=["KROGER", "KR"],
             source="alpaca"),
    ],
    expect_rule="",  # must produce nothing
)

HEADLINE_SHARED_CATEGORY = Scenario(
    name="A headline and a move that share only a news category",
    domain="news",
    why=(
        "The subtler negative case. Two unrelated companies can both be "
        "tagged 'regulatory' in the same week -- one is under an FTC review, "
        "the other had a licence renewed. A shared category is a filing "
        "cabinet, not a relationship, and a tag join that accepts it "
        "reintroduces the coincidence matching it was added to stop."
    ),
    trigger=Beat(
        "headline", "PFIZER", 0, 0.69,
        headline="FTC opens review of Pfizer acquisition",
        tags=["PFE", "regulatory", "healthcare"],
        named_entities=["PFIZER", "PFE", "FTC"],
        source="reddit"),
    evidence=[
        Beat("price_anomaly", "DUK", 8 * HOUR, 0.58,
             headline="DUK moves on a utility rate case",
             tags=["DUK", "regulatory", "utilities"],
             named_entities=["DUKE ENERGY", "DUK"],
             source="alpaca"),
    ],
    expect_rule="",  # must produce nothing
)


# ── CYBER ───────────────────────────────────────────────────────────────────

PORT_RANSOMWARE = Scenario(
    name="Ransomware on a terminal operator, ships at that port go dark",
    domain="cyber",
    why=(
        "A real and repeated pattern -- Maersk in 2017, the Port of Nagoya in "
        "2023. When terminal operating systems go down, AIS behaviour at the "
        "berth changes within hours. It is the platform's flagship "
        "cross-domain claim and its CRITICAL tier depends on getting it right."
    ),
    trigger=Beat(
        "ransomware", "NORTHPORT TERMINALS", 0, 0.83,
        entity_type=EntityType.COMPANY, region="North Sea",
        headline="Ransomware halts terminal operating system at Northport",
        source="ransomware_feed"),
    evidence=[
        Beat("vessel_dark", "IMO9273947", 8 * HOUR, 0.66,
             entity_type=EntityType.VESSEL, region="North Sea",
             headline="Container vessel AIS silent at Northport approaches",
             source="aisstream"),
        Beat("vessel_sts", "IMO9401238", 20 * HOUR, 0.58,
             entity_type=EntityType.VESSEL, region="North Sea",
             headline="STS transfer outside designated anchorage",
             source="aisstream"),
    ],
    # Retired with the cyber domain, and kept as the price of retiring it.
    #
    # This is a real situation the platform could answer and now cannot. The
    # scenario stays written down, asserting silence rather than a finding, so
    # the cost is visible and the acceptance criteria survive: if the cyber
    # feeds come back, changing this one line back is how you check the
    # capability came back with them.
    expect_rule="",  # the cyber domain is withdrawn; nothing should fire
)

GPS_INTERFERENCE = Scenario(
    name="Routing interference in a theatre, and the aircraft in it",
    domain="cyber",
    why=(
        "GNSS jamming and spoofing around the Baltic and the eastern "
        "Mediterranean is continuous, well documented, and shows up in ADS-B "
        "as transponder anomalies across many aircraft at once. Several "
        "aircraft misbehaving in one corridor is the observation; one aircraft "
        "misbehaving is an avionics fault. That distinction -- several subjects "
        "rather than several kinds -- is what the convergence gate has to get "
        "right."
    ),
    trigger=Beat(
        "bgp_anomaly", "AS15169", 0, 0.80,
        entity_type=EntityType.INFRASTRUCTURE, region="Eastern Mediterranean",
        source="ripe_ris",
        headline="Unexpected origin announcing regional carrier prefix",
    ),
    evidence=[
        Beat("flight_anomaly", "ABC123", 5 * HOUR, 0.62,
             entity_type=EntityType.AIRCRAFT, region="Eastern Mediterranean",
             source="opensky",
             headline="Position jumps inconsistent with track"),
        Beat("flight_anomaly", "DEF456", 9 * HOUR, 0.58,
             entity_type=EntityType.AIRCRAFT, region="Eastern Mediterranean",
             source="opensky",
             headline="Second aircraft, same corridor, same signature"),
        Beat("flight_dark", "GHI789", 14 * HOUR, 0.61,
             entity_type=EntityType.AIRCRAFT, region="Eastern Mediterranean",
             source="opensky",
             headline="Transponder off mid-route"),
    ],
    # Retired with the cyber domain, and kept as the price of retiring it.
    #
    # This is a real situation the platform could answer and now cannot. The
    # scenario stays written down, asserting silence rather than a finding, so
    # the cost is visible and the acceptance criteria survive: if the cyber
    # feeds come back, changing this one line back is how you check the
    # capability came back with them.
    expect_rule="",  # the cyber domain is withdrawn; nothing should fire
)

CYBER_MARKET_IMPACT = Scenario(
    name="A breach is disclosed and the stock reprices",
    domain="cyber",
    why=(
        "Change Healthcare in 2024, MGM in 2023, SolarWinds in 2020. A "
        "disclosed incident at a listed company moves the stock, and the "
        "options market usually moves first. Both feeds exist; the cyber one "
        "names the victim and the market one names the ticker, which is "
        "precisely the join that was declared and never implemented."
    ),
    trigger=Beat(
        "breach_detected", "CHANGE HEALTHCARE", 0, 0.79,
        entity_type=EntityType.COMPANY, source="cisa",
        headline="Claims processing halted after intrusion; systems isolated",
        tags=["UNH", "healthcare"],
        named_entities=["CHANGE HEALTHCARE", "UNH"],
    ),
    evidence=[
        Beat("options_flow", "UNH", 6 * HOUR, 0.63,
             entity_type=EntityType.INSTRUMENT, source="options_feed",
             headline="UNH put volume 6x average",
             tags=["tradfi", "options_flow", "unh"]),
        Beat("price_anomaly", "UNH", 3 * HOUR, 0.57,
             entity_type=EntityType.INSTRUMENT, source="polygon",
             headline="UNH -4.1% intraday",
             tags=["tradfi", "unh"]),
    ],
    # Retired with the cyber domain, and kept as the price of retiring it.
    #
    # This is a real situation the platform could answer and now cannot. The
    # scenario stays written down, asserting silence rather than a finding, so
    # the cost is visible and the acceptance criteria survive: if the cyber
    # feeds come back, changing this one line back is how you check the
    # capability came back with them.
    expect_rule="",  # the cyber domain is withdrawn; nothing should fire
)


# ── MARITIME ────────────────────────────────────────────────────────────────

TANKER_GOES_DARK = Scenario(
    name="A sanctioned tanker goes dark in the Strait of Hormuz",
    domain="maritime",
    why=(
        "The single most-watched maritime intelligence pattern there is. A "
        "tanker disabling AIS in a chokepoint, followed by a ship-to-ship "
        "transfer, is the standard sanctions-evasion signature -- and 20% of "
        "seaborne crude passes through that strait, so it is a macro event as "
        "well as a maritime one."
    ),
    trigger=Beat(
        "vessel_dark", "IMO9402304", 0, 0.78,
        entity_type=EntityType.VESSEL, region="Strait of Hormuz",
        latitude=26.57, longitude=56.25,
        headline="VLCC AIS silent for 31 hours in Strait of Hormuz",
        source="aisstream"),
    evidence=[
        Beat("vessel_sts", "IMO9402304", 18 * HOUR, 0.71,
             entity_type=EntityType.VESSEL, region="Strait of Hormuz",
             latitude=26.4, longitude=56.4,
             headline="STS transfer, two tankers, no port call",
             source="aisstream"),
        Beat("vessel_spoof", "IMO9511344", 30 * HOUR, 0.69,
             entity_type=EntityType.VESSEL, region="Strait of Hormuz",
             latitude=26.6, longitude=56.1,
             headline="AIS position inconsistent with track",
             source="sar"),
    ],
    expect_rule="rule_maritime_chokepoint_evasion",
    expect_evidence_types=("vessel_sts",),
)

# The moves these scenarios describe, as data rather than as prose.
#
# Every headline below already stated one -- "+41% week over week", "+3.4%",
# "+28%" -- which is exactly what the platform itself was doing: computing the
# percentage, printing it into the headline, and storing nothing. A rule named
# for a repricing now requires one, so the scenarios that describe a repricing
# have to carry it, and the fourth headline here had no number at all until it
# was asked for a measurement.
CHOKEPOINT_TO_FREIGHT = Scenario(
    name="Chokepoint disruption shows up in freight and energy",
    domain="maritime",
    why=(
        "Red Sea 2024: attacks on shipping diverted traffic around the Cape, "
        "container rates tripled within weeks, and energy followed. A "
        "maritime disruption that never reaches a macro or market series is "
        "an observation, not intelligence."
    ),
    trigger=Beat(
        "vessel_dark", "IMO9631783", 0, 0.75,
        entity_type=EntityType.VESSEL, region="Red Sea",
        latitude=13.6, longitude=42.8,
        headline="Container vessel goes dark south of Bab el-Mandeb",
        named_entities=["MAERSK", "RED SEA"],
        source="aisstream"),
    evidence=[
        Beat("supply_chain_metric", "FREIGHT-CONTAINER", 20 * HOUR, 0.68,
             entity_type=EntityType.INSTRUMENT, region="Red Sea",
             headline="Asia-Europe container rate +41% week over week",
             move_pct=41.0,
             named_entities=["MAERSK", "RED SEA"],
             source="freightos"),
        Beat("price_anomaly", "BRENT", 14 * HOUR, 0.62,
             entity_type=EntityType.INSTRUMENT,
             headline="Brent +3.4% on shipping disruption",
             move_pct=3.4,
             named_entities=["RED SEA"],
             source="alpaca"),
    ],
    expect_rule="rule_physical_disruption_repricing",
    expect_evidence_types=("supply_chain_metric",),
    expect_min_domains=2,
)


# ── AVIATION ────────────────────────────────────────────────────────────────

MILITARY_AIRLIFT = Scenario(
    name="Transport aircraft surge into a theatre",
    domain="aviation",
    why=(
        "Open-source flight tracking of military transports is one of the most "
        "reliable early indicators of escalation -- it preceded both the 2021 "
        "Kabul evacuation and the 2022 build-up. The platform collects "
        "ADS-B; a surge that produces no finding is a collector, not an "
        "intelligence system."
    ),
    trigger=Beat(
        "flight_anomaly", "RCH512", 0, 0.76,
        entity_type=EntityType.AIRCRAFT, region="Eastern Mediterranean",
        latitude=35.1, longitude=33.4,
        headline="C-17 squawking unusual profile, unscheduled arrival",
        source="opensky"),
    evidence=[
        Beat("flight_dark", "RCH287", 10 * HOUR, 0.70,
             entity_type=EntityType.AIRCRAFT, region="Eastern Mediterranean",
             latitude=35.4, longitude=33.1,
             headline="Transport aircraft transponder off mid-route",
             source="opensky"),
        Beat("flight_anomaly", "RCH904", 16 * HOUR, 0.64,
             entity_type=EntityType.AIRCRAFT, region="Eastern Mediterranean",
             latitude=34.9, longitude=33.8,
             headline="Second transport, same corridor, no filed plan",
             source="opensky"),
    ],
    expect_rule="rule_aviation_activity_surge",
    expect_evidence_types=("flight_dark",),
)


# ── MACRO ───────────────────────────────────────────────────────────────────

CPI_SURPRISE = Scenario(
    name="A hot CPI print moves rates, equities and gold together",
    domain="macro",
    why=(
        "The most-traded scheduled event in the calendar. A 0.3pp surprise "
        "moves the entire risk complex inside sixty seconds, and every leg is "
        "something this platform ingests. If a CPI surprise produces no "
        "correlation, the macro domain is decorative."
    ),
    trigger=Beat(
        "macro_release", "US-CPI", 0, 0.82,
        entity_type=EntityType.INSTRUMENT, region="United States",
        headline="US CPI 3.8% vs 3.5% expected, core accelerating",
        named_entities=["US-CPI", "FED"],
        source="fred"),
    evidence=[
        Beat("price_anomaly", "SPY", 25, 0.67,
             entity_type=EntityType.INSTRUMENT,
             headline="SPY -1.8% in the first 30 minutes",
             named_entities=["US-CPI"],
             source="alpaca"),
        Beat("options_flow", "TLT", 20, 0.61,
             entity_type=EntityType.INSTRUMENT,
             headline="TLT put sweep on the print",
             named_entities=["US-CPI"],
             source="alpaca_options"),
        Beat("crypto_trade", "BTC", 15, 0.58,
             entity_type=EntityType.INSTRUMENT,
             headline="BTC -3% on the print",
             named_entities=["US-CPI"],
             source="coinbase_spot"),
    ],
    expect_rule="rule_macro_release_repricing",
    expect_evidence_types=("price_anomaly", "options_flow"),
    expect_min_domains=2,
)

CLIMATE_TO_COMMODITY = Scenario(
    name="Drought on a shipping artery reaches freight and grain",
    domain="macro",
    why=(
        "Panama Canal 2023: drought cut daily transits by a third, and the "
        "effect showed up in freight rates and in grain routing. A climate "
        "series that cannot reach a market series is a chart."
    ),
    trigger=Beat(
        "climate_stress", "PANAMA-CANAL", 0, 0.73,
        entity_type=EntityType.INFRASTRUCTURE, region="Central America",
        headline="Gatun Lake at record low, transit slots cut to 24/day",
        named_entities=["PANAMA CANAL"],
        source="noaa"),
    evidence=[
        Beat("supply_chain_metric", "FREIGHT-DRYBULK", 2 * DAY, 0.66,
             entity_type=EntityType.INSTRUMENT,
             headline="Dry bulk rates +28% on Panama routing",
             move_pct=28.0,
             named_entities=["PANAMA CANAL"],
             source="freightos"),
        Beat("price_anomaly", "CORN", 1 * DAY, 0.59,
             entity_type=EntityType.INSTRUMENT,
             headline="Corn futures +2.1% on export routing costs",
             move_pct=2.1,
             named_entities=["PANAMA CANAL"],
             source="alpaca"),
    ],
    expect_rule="rule_physical_disruption_repricing",
    expect_evidence_types=("supply_chain_metric",),
    expect_min_domains=2,
)


# ── PREDICTION ──────────────────────────────────────────────────────────────

PREDICTION_REPRICING = Scenario(
    name="A contract reprices ahead of the equity it implicates",
    domain="prediction",
    why=(
        "Prediction markets reprice on information before it is priced in "
        "listed instruments -- that is their entire claim to being worth "
        "collecting. A sharp move in a regulatory or political contract with "
        "flow in the affected name is the pattern that justifies the feed."
    ),
    trigger=Beat(
        "prediction_market_trade", "fed-cuts-march", 0, 0.71,
        entity_type=EntityType.PREDICTION_MARKET,
        headline="March cut contract 31% -> 58% on heavy volume",
        named_entities=["FED", "US-CPI"],
        source="polymarket"),
    evidence=[
        Beat("options_flow", "TLT", 5 * HOUR, 0.60,
             entity_type=EntityType.INSTRUMENT,
             headline="TLT call sweep, front month",
             named_entities=["FED"],
             source="alpaca_options"),
        Beat("equity_block", "XLF", 8 * HOUR, 0.54,
             entity_type=EntityType.INSTRUMENT,
             headline="XLF block, buyer",
             named_entities=["FED"],
             source="finnhub_equities"),
    ],
    expect_rule="rule_prediction_market_divergence",
    expect_min_tier="ALERT",
    expect_evidence_types=("options_flow",),
    expect_min_domains=2,
)


# ── A SECOND PASS ───────────────────────────────────────────────────────────
#
# The first eighteen were written to cover the eight domains. These were written
# to cover the ways a platform like this is normally *wrong* -- right types in
# the wrong name, right name outside the window, right everything in the wrong
# place -- and the handful of patterns a desk would notice were missing.
#
# Every event type below is one the platform actually emits, checked by
# tests/integration/test_rules_can_fire.py. A scenario built on a type no
# collector produces cannot fail, and would have been the most comfortable kind
# of test to write.


# ── TRADFI ──────────────────────────────────────────────────────────────────

MERGER_POSITIONING = Scenario(
    name="Options and blocks accumulate, then the merger 8-K lands",
    domain="tradfi",
    why=(
        "Pre-announcement accumulation is what securities regulators open the "
        "most cases on, and both legs are collected. What makes it a case "
        "rather than a coincidence is that the positioning precedes the filing "
        "-- afterwards it is everyone reading the same 8-K."
    ),
    trigger=Beat("filing", "TGTCO", 0, 0.71, source="sec_edgar",
                 headline="TGTCO 8-K Item 1.01, merger agreement executed"),
    evidence=[
        Beat("options_flow", "TGTCO", 30 * HOUR, 0.66, source="alpaca_options",
             headline="TGTCO short-dated calls, 14x average, far out of the money"),
        Beat("equity_block", "TGTCO", 22 * HOUR, 0.58, source="finnhub_equities",
             headline="TGTCO block, buyer, 900k shares"),
    ],
    expect_rule="rule_insider_filing_flow",
    expect_evidence_types=("options_flow", "equity_block"),
)

SHORT_SQUEEZE = Scenario(
    name="A squeeze: the move, the blocks and the call buying together",
    domain="tradfi",
    why=(
        "A high-short-interest name gapping on volume with call sweeps and "
        "block prints in the same session is the most legible microstructure "
        "event there is, and the one a user will check the platform against "
        "first because it is on the front page that afternoon."
    ),
    trigger=Beat("equity_block", "SQZCO", 0, 0.74, source="finnhub_equities",
                 headline="SQZCO block, 3.4m shares, buyer"),
    evidence=[
        Beat("options_flow", "SQZCO", 3 * HOUR, 0.69, source="alpaca_options",
             headline="SQZCO weekly calls, 22x average volume"),
        Beat("market_anomaly", "SQZCO", 90, 0.64, source="alpaca_quant_radar",
             headline="SQZCO +31% intraday on 9x volume"),
    ],
    expect_rule="rule_financial_block_volume_spike",
    expect_evidence_types=("options_flow", "market_anomaly"),
)

INSIDER_CLUSTER_BUYING = Scenario(
    name="An officer buys on the open market, into building positioning",
    domain="tradfi",
    why=(
        "Open-market insider buying is the highest-precision public signal in "
        "equities -- an officer has many reasons to sell and one to buy. "
        "Joined to the options and block activity around it, it is the clearest "
        "thing the tradfi side of this platform can say."
    ),
    trigger=Beat("insider_trade", "CLSTR", 0, 0.63, source="sec_edgar",
                 headline="CLSTR CEO buys 40,000 shares on the open market"),
    evidence=[
        Beat("options_flow", "CLSTR", 40 * HOUR, 0.55, source="alpaca_options",
             headline="CLSTR call open interest building in the back months"),
        Beat("equity_block", "CLSTR", 20 * HOUR, 0.52, source="finnhub_equities",
             headline="CLSTR block, buyer"),
    ],
    expect_rule="rule_insider_options_convergence",
    expect_evidence_types=("options_flow", "equity_block"),
)

WRONG_NAME = Scenario(
    name="The right pattern, in two different companies",
    domain="tradfi",
    why=(
        "The failure the same-entity join exists for, stated plainly. An ACME "
        "block and ZENITH options inside 48 hours is two ordinary days in two "
        "ordinary names. Published together under ACME's headline it reads as "
        "accumulation, and that is what the layer did before the join: an AAPL "
        "block over supporting evidence reading MTZ, KKR and DELL."
    ),
    trigger=Beat("equity_block", "ACME", 0, 0.72, source="finnhub_equities",
                 headline="ACME block, 1.1m shares"),
    evidence=[
        Beat("options_flow", "ZENITH", 20 * HOUR, 0.66, source="alpaca_options",
             headline="ZENITH calls, 9x average"),
        Beat("market_anomaly", "ORBIS", 14 * HOUR, 0.61, source="alpaca_quant_radar",
             headline="ORBIS +6% intraday"),
    ],
    expect_rule="",
)

OUTSIDE_THE_WINDOW = Scenario(
    name="The right name and the right types, eleven days apart",
    domain="tradfi",
    why=(
        "A window is a claim about how long a relationship stays true. The "
        "institutional rule looks back seven days; evidence from eleven is not "
        "weaker evidence, it is a different quarter. This is also the case that "
        "would pass silently if the deep-window read ignored the clause's own "
        "bound and returned everything the database holds."
    ),
    trigger=Beat("thirteen_f", "OXY", 0, 0.60, source="sec_edgar",
                 headline="Berkshire discloses increased OXY position"),
    evidence=[
        Beat("equity_block", "OXY", 11 * DAY, 0.58, source="finnhub_equities",
             headline="OXY block, 2.1m shares"),
        Beat("options_flow", "OXY", 12 * DAY, 0.55, source="alpaca_options",
             headline="OXY calls, back months"),
    ],
    expect_rule="",
)

BELOW_THE_FLOOR = Scenario(
    name="Every leg present, every one of them unremarkable",
    domain="tradfi",
    why=(
        "The floor is the platform's own judgement that something is not worth "
        "reporting. A rule firing on evidence beneath it has overruled that "
        "judgement by aggregation, and three shrugs are not a finding."
    ),
    trigger=Beat("earnings_surprise", "MEHCO", 0, 0.24, source="finnhub_earnings",
                 headline="MEHCO in line with consensus"),
    evidence=[
        Beat("options_flow", "MEHCO", 20 * HOUR, 0.18, source="alpaca_options",
             headline="MEHCO options, near average"),
        Beat("equity_block", "MEHCO", 12 * HOUR, 0.16, source="finnhub_equities",
             headline="MEHCO block, ordinary size"),
    ],
    expect_rule="",
)


# ── CRYPTO ──────────────────────────────────────────────────────────────────

EXCHANGE_OUTFLOW = Scenario(
    name="Reserves leave the exchange, then the book thins",
    domain="crypto",
    why=(
        "The on-chain tell that precedes almost every exchange failure: large "
        "transfers out, then redemption volume, then the venue halts "
        "withdrawals. The transfer leg is the one only a crypto platform has, "
        "and joining it to the trade leg is the whole reason to collect it."
    ),
    trigger=Beat("crypto_perp_funding", "ETH", 0, 0.72,
                 entity_type=EntityType.INSTRUMENT, source="binance_futures",
                 headline="ETH perp funding dislocated from spot across venues"),
    evidence=[
        Beat("crypto_transfer", "ETH", 5 * HOUR, 0.70,
             entity_type=EntityType.INSTRUMENT, source="crypto_divergence",
             headline="310,000 ETH moved off the venue in four transactions"),
        Beat("crypto_trade", "ETH", 2 * HOUR, 0.61,
             entity_type=EntityType.INSTRUMENT, source="coinbase_spot",
             headline="ETH/USD spread widens, 6x volume"),
    ],
    expect_rule="rule_crypto_stress_cascade",
    expect_evidence_types=("crypto_transfer", "crypto_trade"),
)

CRYPTO_UNRELATED_ASSETS = Scenario(
    name="Two different assets, both busy, on the same afternoon",
    domain="crypto",
    why=(
        "Crypto trades continuously and something is always moving, so a rule "
        "joined only by a time window finds a cascade every day. The asset is "
        "the join, and this is what it protects against."
    ),
    trigger=Beat("crypto_perp_funding", "BTC", 0, 0.71,
                 entity_type=EntityType.INSTRUMENT, source="binance_futures",
                 headline="BTC funding at the 96th percentile"),
    evidence=[
        Beat("crypto_trade", "DOGE", 3 * HOUR, 0.64,
             entity_type=EntityType.INSTRUMENT, source="coinbase_spot",
             headline="DOGE +18% on retail volume"),
        Beat("crypto_transfer", "SOL", 90, 0.62,
             entity_type=EntityType.INSTRUMENT, source="crypto_divergence",
             headline="SOL treasury movement"),
    ],
    expect_rule="",
)


# ── NEWS ────────────────────────────────────────────────────────────────────

SOCIAL_PUSH = Scenario(
    name="A coordinated social push, then the tape",
    domain="news",
    why=(
        "The news enricher splits its output: a primary_social source becomes "
        "SOCIAL_SIGNAL and everything else becomes HEADLINE. Only the second "
        "was ever a trigger, so the entire social half of the feed -- the half "
        "where a coordinated push actually appears -- could neither start a "
        "correlation nor corroborate one."
    ),
    trigger=Beat(
        "social_signal", "MEMECO", 0, 0.70, source="reddit",
        headline="MEMECO mention volume 40x baseline across three subreddits",
        tags=["MEME", "retail"],
        named_entities=["MEMECO", "MEME"],
    ),
    evidence=[
        Beat("options_flow", "MEME", 6 * HOUR, 0.63, source="alpaca_options",
             headline="MEME weekly calls, 18x average",
             tags=["tradfi", "options_flow", "meme"]),
        Beat("market_anomaly", "MEME", 3 * HOUR, 0.60, source="alpaca_quant_radar",
             headline="MEME +24% on 11x volume",
             tags=["tradfi", "meme"]),
    ],
    expect_rule="rule_news_financial_impact",
    expect_min_tier="ALERT",
    expect_evidence_types=("options_flow", "market_anomaly"),
    expect_min_domains=2,
)

SANCTIONS_HEADLINE = Scenario(
    name="A sanctions designation, and the name it touches",
    domain="news",
    why=(
        "A designation is the one headline with a mechanical market "
        "consequence: counterparties have to unwind, and the unwinding is "
        "visible as blocks. It is also the headline most likely to name an "
        "entity the platform already tracks."
    ),
    trigger=Beat(
        "headline", "OFAC", 0, 0.76, source="reddit",
        headline="OFAC designates a shipping company over crude transfers",
        tags=["sanctions", "shipping", "SOVCOMFLOT"],
        named_entities=["SOVCOMFLOT", "OFAC"],
    ),
    evidence=[
        Beat("equity_block", "SOVCOMFLOT", 5 * HOUR, 0.64, source="finnhub_equities",
             headline="Seller, size, no offsetting bid",
             tags=["tradfi", "equity_block", "sovcomflot"],
             named_entities=["SOVCOMFLOT"]),
        Beat("options_flow", "SOVCOMFLOT", 8 * HOUR, 0.58, source="alpaca_options",
             headline="Protective puts, front month",
             tags=["tradfi", "options_flow", "sovcomflot"],
             named_entities=["SOVCOMFLOT"]),
    ],
    expect_rule="rule_news_financial_impact",
    expect_min_tier="ALERT",
    expect_evidence_types=("equity_block", "options_flow"),
    expect_min_domains=2,
)


# ── CYBER ───────────────────────────────────────────────────────────────────

KEV_EXPLOITATION = Scenario(
    name="A known-exploited vulnerability, and the company running it",
    domain="cyber",
    why=(
        "CISA's KEV catalogue is the authoritative list of what is being "
        "exploited right now. A KEV entry naming a listed vendor, plus the "
        "market reacting to it, is the difference between a bulletin and an "
        "incident -- and both feeds are collected."
    ),
    trigger=Beat(
        "breach_detected", "EDGEVPN", 0, 0.80, source="cisa_kev",
        entity_type=EntityType.COMPANY, region="North America",
        headline="Authentication bypass added to KEV; exploitation observed",
        tags=["EDGE", "vpn"],
        named_entities=["EDGEVPN", "EDGE"],
    ),
    evidence=[
        Beat("options_flow", "EDGE", 9 * HOUR, 0.67, source="alpaca_options",
             entity_type=EntityType.INSTRUMENT,
             headline="EDGE put volume 7x average",
             tags=["tradfi", "options_flow", "edge"]),
        Beat("equity_block", "EDGE", 14 * HOUR, 0.61, source="finnhub_equities",
             entity_type=EntityType.INSTRUMENT,
             headline="EDGE block, seller",
             tags=["tradfi", "equity_block", "edge"]),
    ],
    # Retired with the cyber domain, and kept as the price of retiring it.
    #
    # This is a real situation the platform could answer and now cannot. The
    # scenario stays written down, asserting silence rather than a finding, so
    # the cost is visible and the acceptance criteria survive: if the cyber
    # feeds come back, changing this one line back is how you check the
    # capability came back with them.
    expect_rule="",  # the cyber domain is withdrawn; nothing should fire
)

CYBER_WRONG_REGION = Scenario(
    name="A cyber incident in one theatre, aircraft in another",
    domain="cyber",
    why=(
        "The finding that was 69% of the correlation layer's entire output: a "
        "ransomware disclosure about one company correlated with the positions "
        "of unrelated aircraft anywhere on earth, on nothing but both falling "
        "inside 48 hours. The region join is what makes the chokepoint rule a "
        "claim about a chokepoint."
    ),
    trigger=Beat(
        "ransomware", "NORDPORT", 0, 0.81, source="ransomware_feed",
        entity_type=EntityType.COMPANY, region="North Sea",
        headline="Terminal operating system encrypted",
    ),
    evidence=[
        Beat("flight_dark", "ABC999", 10 * HOUR, 0.68, source="opensky",
             entity_type=EntityType.AIRCRAFT, region="South China Sea",
             headline="Transponder off mid-route"),
        Beat("flight_anomaly", "DEF888", 16 * HOUR, 0.64, source="opensky",
             entity_type=EntityType.AIRCRAFT, region="South China Sea",
             headline="Profile inconsistent with filed plan"),
    ],
    expect_rule="",
)


# ── MARITIME ────────────────────────────────────────────────────────────────

TWO_TANKERS_DARK = Scenario(
    name="Two different tankers go dark in the same strait",
    domain="maritime",
    why=(
        "One AIS gap is a fault. Several vessels in one chokepoint inside a day "
        "is the transshipment signature, and it is the case that decides "
        "whether convergence means two kinds of evidence or two independent "
        "observations -- because vessel_sts and vessel_spoof have no detector, "
        "so vessel_dark is the only type this rule can actually match."
    ),
    trigger=Beat(
        "vessel_dark", "IMO9402304", 0, 0.76,
        entity_type=EntityType.VESSEL, region="Strait of Hormuz",
        latitude=26.57, longitude=56.25, source="aisstream",
        headline="VLCC AIS silent for 26 hours",
    ),
    evidence=[
        Beat("vessel_dark", "IMO9511344", 9 * HOUR, 0.71,
             entity_type=EntityType.VESSEL, region="Strait of Hormuz",
             latitude=26.61, longitude=56.18, source="aisstream",
             headline="Second VLCC silent in the same corridor"),
        Beat("vessel_dark", "IMO9273947", 19 * HOUR, 0.68,
             entity_type=EntityType.VESSEL, region="Strait of Hormuz",
             latitude=26.44, longitude=56.39, source="aisstream",
             headline="Third gap, same window, same waters"),
    ],
    expect_rule="rule_maritime_chokepoint_evasion",
    expect_evidence_types=("vessel_dark",),
)

DARK_IN_DIFFERENT_STRAITS = Scenario(
    name="Two tankers dark, half a world apart",
    domain="maritime",
    why=(
        "Vessels lose AIS everywhere, every day. Without the place, a "
        "chokepoint rule is a global vessel-outage counter wearing the name of "
        "a strait."
    ),
    trigger=Beat(
        "vessel_dark", "IMO9402304", 0, 0.76,
        entity_type=EntityType.VESSEL, region="Strait of Hormuz",
        latitude=26.57, longitude=56.25, source="aisstream",
        headline="VLCC AIS silent for 26 hours",
    ),
    evidence=[
        Beat("vessel_dark", "IMO9511344", 9 * HOUR, 0.71,
             entity_type=EntityType.VESSEL, region="Strait of Malacca",
             latitude=1.30, longitude=103.80, source="aisstream",
             headline="Unrelated gap, other side of the world"),
    ],
    expect_rule="",
)


# ── AVIATION ────────────────────────────────────────────────────────────────

CORRIDOR_CLOSURE = Scenario(
    name="A corridor empties: several aircraft, one airspace",
    domain="aviation",
    why=(
        "Airspace closures and GNSS denial show up first as several aircraft in "
        "one corridor behaving unusually within the hour. The platform collects "
        "ADS-B continuously, so this is the pattern it is best placed of "
        "anything to see."
    ),
    trigger=Beat(
        "flight_anomaly", "QRS101", 0, 0.74,
        entity_type=EntityType.AIRCRAFT, region="Black Sea",
        latitude=44.2, longitude=33.9, source="opensky",
        headline="Sharp deviation, no filed reroute",
    ),
    evidence=[
        Beat("flight_dark", "QRS202", 2 * HOUR, 0.70,
             entity_type=EntityType.AIRCRAFT, region="Black Sea",
             latitude=44.6, longitude=33.4, source="opensky",
             headline="Transponder off over the same waters"),
        Beat("flight_anomaly", "QRS303", 5 * HOUR, 0.66,
             entity_type=EntityType.AIRCRAFT, region="Black Sea",
             latitude=43.9, longitude=34.2, source="opensky",
             headline="Third aircraft, same corridor, same hour"),
    ],
    expect_rule="rule_aviation_activity_surge",
    expect_evidence_types=("flight_dark", "flight_anomaly"),
)


# ── MACRO ───────────────────────────────────────────────────────────────────

PAYROLLS_SURPRISE = Scenario(
    name="Payrolls come in 180k above consensus",
    domain="macro",
    why=(
        "The second most-traded release in the calendar, and the one that moves "
        "the front end hardest. A platform that sees CPI and not payrolls has a "
        "rule, not a capability."
    ),
    trigger=Beat(
        "macro_release", "US-NFP", 0, 0.79, source="fred",
        entity_type=EntityType.INSTRUMENT, region="United States",
        headline="Nonfarm payrolls +430k vs +250k expected",
        named_entities=["US-NFP", "FED"],
    ),
    evidence=[
        Beat("market_anomaly", "TLT", 18, 0.68, source="alpaca_quant_radar",
             entity_type=EntityType.INSTRUMENT,
             headline="TLT -2.1% in twenty minutes",
             named_entities=["US-NFP"]),
        Beat("options_flow", "SPY", 22, 0.63, source="alpaca_options",
             entity_type=EntityType.INSTRUMENT,
             headline="SPY put sweep on the print",
             named_entities=["US-NFP"]),
        Beat("equity_block", "XLF", 26, 0.57, source="finnhub_equities",
             entity_type=EntityType.INSTRUMENT,
             headline="XLF block, buyer",
             named_entities=["US-NFP"]),
    ],
    expect_rule="rule_macro_release_repricing",
    expect_evidence_types=("market_anomaly", "options_flow"),
    expect_min_domains=2,
)

MACRO_UNRELATED_TICKERS = Scenario(
    name="A release, and names that moved for their own reasons",
    domain="macro",
    why=(
        "Something always moves within an hour of a scheduled release. The join "
        "is that the move names the print -- an instrument repricing on its own "
        "earnings during the same hour is not a macro reaction, and a rule that "
        "counts it reports the whole tape as repricing on every release."
    ),
    trigger=Beat(
        "macro_release", "US-CPI", 0, 0.80, source="fred",
        entity_type=EntityType.INSTRUMENT, region="United States",
        headline="US CPI in line with consensus",
        named_entities=["US-CPI", "FED"],
    ),
    evidence=[
        Beat("market_anomaly", "SMALLCO", 40, 0.62, source="alpaca_quant_radar",
             headline="SMALLCO -12% on a guidance cut",
             tags=["tradfi", "smallco"], named_entities=["SMALLCO"]),
        Beat("options_flow", "OTHERCO", 50, 0.58, source="alpaca_options",
             headline="OTHERCO calls ahead of its own earnings",
             tags=["tradfi", "otherco"], named_entities=["OTHERCO"]),
    ],
    expect_rule="",
)


# ── PREDICTION ──────────────────────────────────────────────────────────────

ELECTION_CONTRACT = Scenario(
    name="An election contract moves, and so do the sectors it implies",
    domain="prediction",
    why=(
        "Political contracts reprice on information hours before the equities "
        "that depend on the outcome. Defence, energy and healthcare are the "
        "standard expressions, and the platform collects both sides."
    ),
    trigger=Beat(
        "prediction_market_trade", "senate-control-gop", 0, 0.73,
        entity_type=EntityType.PREDICTION_MARKET, source="polymarket",
        headline="Senate control contract 44% to 67% on heavy volume",
        named_entities=["SENATE", "ELECTION"],
    ),
    evidence=[
        Beat("equity_block", "ITA", 7 * HOUR, 0.61, source="finnhub_equities",
             entity_type=EntityType.INSTRUMENT,
             headline="Defence ETF block, buyer",
             named_entities=["ELECTION"]),
        Beat("options_flow", "XLV", 9 * HOUR, 0.57, source="alpaca_options",
             entity_type=EntityType.INSTRUMENT,
             headline="Healthcare puts, front month",
             named_entities=["ELECTION"]),
    ],
    expect_rule="rule_prediction_market_divergence",
    expect_min_tier="ALERT",
    expect_evidence_types=("equity_block", "options_flow"),
    expect_min_domains=2,
)


ALL_SCENARIOS = [
    # First pass: the eight domains, and whether each can produce a finding.
    INFORMED_TRADING,
    EARNINGS_SURPRISE,
    THIRTEEN_F_SHIFT,
    FILING_POSITIONING,
    LIQUIDATION_CASCADE,
    STABLECOIN_DEPEG,
    HEADLINE_IMPACT,
    HEADLINE_UNRELATED,
    HEADLINE_SHARED_CATEGORY,
    PORT_RANSOMWARE,
    GPS_INTERFERENCE,
    CYBER_MARKET_IMPACT,
    TANKER_GOES_DARK,
    CHOKEPOINT_TO_FREIGHT,
    MILITARY_AIRLIFT,
    CPI_SURPRISE,
    CLIMATE_TO_COMMODITY,
    PREDICTION_REPRICING,
    # Second pass: the ways this kind of platform is normally wrong, and the
    # patterns a desk would notice were missing.
    MERGER_POSITIONING,
    SHORT_SQUEEZE,
    INSIDER_CLUSTER_BUYING,
    WRONG_NAME,
    OUTSIDE_THE_WINDOW,
    BELOW_THE_FLOOR,
    EXCHANGE_OUTFLOW,
    CRYPTO_UNRELATED_ASSETS,
    SOCIAL_PUSH,
    SANCTIONS_HEADLINE,
    KEV_EXPLOITATION,
    CYBER_WRONG_REGION,
    TWO_TANKERS_DARK,
    DARK_IN_DIFFERENT_STRAITS,
    CORRIDOR_CLOSURE,
    PAYROLLS_SURPRISE,
    MACRO_UNRELATED_TICKERS,
    ELECTION_CONTRACT,
]

# Every domain the platform claims to reason across must be the *trigger* of at
# least one scenario. Being evidence for someone else's rule is not coverage.
#
# Nor is asserting silence. When the four cyber scenarios were converted to
# expect nothing, cyber went on counting as covered by scenarios that cannot
# produce a finding -- the check passed while its own failure message said "one
# with no scenario is one nobody has checked can produce a finding". A scenario
# has to expect a rule before it is evidence that the domain works.
COVERED_DOMAINS = {s.domain for s in ALL_SCENARIOS if s.expect_rule}

# Every domain that appears at all, including the ones only asserting silence.
# Used to check that a retired domain has kept its situations written down.
DOMAINS_WITH_SCENARIOS = {s.domain for s in ALL_SCENARIOS}
