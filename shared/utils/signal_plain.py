"""
shared/utils/signal_plain.py

The signal matrix in plain English: one sentence per signal saying what the
number is and what, if anything, it has been shown to mean; everyday names for
the evidence tiers; and a short verdict per ticker that says how much stands
behind it. Wording never claims more than the tier allows.
"""
from __future__ import annotations

from typing import Any, Dict

TIER_LABELS = {
    "proven": "Proven here",
    "supported": "Model-backed",
    "published": "Research-backed",
    "no_edge": "Tested: doesn't work",
    "context": "Background",
}
TIER_MEANING = {
    "proven": "our own five-year testing shows this pattern has reliably moved prices",
    "supported": "our ranking model, tested on years of data it never saw, found an edge here",
    "published": "academic research documents this pattern; we have not yet measured it ourselves",
    "no_edge": "we tested this and it did not predict prices, so it is shown for information only",
    "context": "useful background; not a prediction",
}


def _up(direction: int, up: str, down: str, flat: str) -> str:
    return up if direction > 0 else down if direction < 0 else flat


def explain(s: Dict[str, Any]) -> str:
    """One plain sentence for a signal dict (Signal.as_dict())."""
    name, d, disp, v = s.get("name", ""), int(s.get("direction") or 0), s.get("display", ""), s.get("value")
    group = s.get("group", "")
    if name.startswith("return "):
        return f"The stock moved {disp} over the past {name[7:]}."
    if name.startswith("excess vs SPY"):
        return f"Compared with the S&P 500 it did {disp} over the past {name[14:]}."
    if name == "12-1 month momentum":
        return (f"Over the past year, leaving out the last month, it moved {disp}. "
                + _up(d, "Stocks that have risen over a year have tended to keep rising.",
                      "Stocks that have fallen over a year have tended to keep lagging.",
                      "Not strong enough to count either way."))
    if name == "1-month reversal":
        return (f"It moved {disp} in the past month. "
                + _up(d, "Sharp one-month drops have often partly bounced back.",
                      "Sharp one-month jumps have often partly given back.", "Not an extreme move."))
    if name == "price / 52-week high":
        return (f"It trades at {disp} of its 1-year high. "
                + _up(d, "Stocks close to their highs have tended to keep rising.",
                      "Stocks far below their highs have tended to keep lagging.", "Neither near the high nor far below it."))
    if name == "52-week range position":
        return f"Within its 1-year price range it sits at {disp} (0% = the low, 100% = the high)."
    if name == "max daily gain, 1 month":
        return (f"Its biggest one-day jump this month was {disp}. "
                + _up(d, "", "Lottery-like jumps have tended to be followed by weaker returns.", "Nothing unusual."))
    if name.startswith("vs ") and name.endswith("-day average"):
        return f"The price is {disp} versus its {name[3:-12]}-day average: trend background, not a prediction."
    if name == "RSI 14":
        return (f"RSI is {disp}. Above 70 is often called overbought and below 30 oversold; "
                "it has not predicted returns in our testing.")
    if name == "realised volatility 21d":
        try:
            return f"It has been moving about {float(v) / 15.87:.1%} on a typical day ({disp} a year)."
        except (TypeError, ValueError):
            return f"Annualised volatility: {disp}."
    if name.startswith("beta to SPY"):
        return f"It tends to move about {disp} times as much as the overall market."
    if name == "volume vs 20-day average":
        return f"Trading volume is running at {disp} its usual level."
    if group == "model":
        where = everyday(s)["shown"]
        return (f"Our ranking model puts it in the {where} of stocks for the next {name.split()[0].replace('daily', 'day').replace('weekly', 'week').replace('monthly', 'month')}. "
                + _up(d, "That is among the names it expects to beat the market.",
                      "That is among the names it expects to lag the market.",
                      "Only its top and bottom fifth count as a lean, and only where the model has proven itself."))
    if group == "events":
        verdict = (TIER_MEANING.get(s.get("status", ""), "") if s.get("status") != "context"
                   else "not yet measured here, so it is shown for information only")
        lead = _up(d, "Points up", "Points down", "No direction")
        return f"{name.capitalize()}: {disp}. {lead}; {verdict}."
    if group == "fundamentals":
        lead = {"value (book-to-market)": "Its book value relative to its price is",
                "earnings yield": "Its earnings relative to its price are",
                "profitability (ROA)": "Its profit relative to its assets is",
                "asset growth": "Its balance-sheet growth over a year is",
                "accruals": "The share of its earnings not backed by cash is",
                "gross profitability": "Its gross profit relative to its assets is",
                "value vs its industry": "How cheap it is next to companies in its own industry:",
                "quality vs its industry": "How profitable and cash-backed it is next to its industry:",
                }.get(name, name.capitalize() + " is")
        tail = _up(d, "Research links that to better returns.", "Research links that to weaker returns.",
                   "Middle of the pack.")
        return f"{lead} {disp} (from its SEC filings). {tail}"
    if name == "put/call volume":
        return (f"Options traders traded {disp} puts for every call today. "
                + _up(d, "Unusually light put buying has preceded better returns.",
                      "Unusually heavy put buying has preceded weaker returns.", "Within its normal range."))
    if name.startswith("IV skew"):
        return (f"Crash protection is priced {disp} above ordinary options. "
                + _up(d, "Cheap protection has preceded better returns.",
                      "Expensive protection has preceded weaker returns.", "A normal level."))
    if name == "call - put IV spread":
        return (f"Call options are priced {disp} versus matching puts. "
                + _up(d, "Pricier calls have preceded outperformance.", "Pricier puts have preceded underperformance.",
                      "Roughly balanced."))
    if name == "implied / realised vol":
        try:
            ratio = float(v)
            look = "cheap" if ratio < 1.05 else "expensive" if ratio > 1.35 else "fairly priced"
            inner = disp.split("(", 1)[1].rstrip(")").split(" vs ")
            return (f"Options price in {inner[0]} yearly movement against {inner[1]} actually seen, so options look "
                    f"{look}. This decides whether to buy or sell options, not which way the stock goes.")
        except (TypeError, ValueError, IndexError):
            return "Compares option prices with the stock's real movement; it decides buy-or-sell options, not direction."
    if name.startswith("sweep premium"):
        return f"Large option trades: {disp} more call than put money in five days. Our testing found these do not predict direction."
    if name == "days to cover":
        when = disp.split("(", 1)[1].rstrip(")") if "(" in disp else ""
        return (f"Short sellers hold about {disp.split(' (')[0]} of normal trading volume"
                + (f" (as of {when}). " if when else ". ")
                + _up(d, "", "Crowded short positions have preceded weaker returns over months.", "Not unusually crowded."))
    if name == "short interest change":
        return f"Short interest changed {disp} at the last twice-monthly report."
    if name.startswith("open-market net"):
        return (f"Company insiders net {'bought' if (v or 0) > 0 else 'sold'} {disp.split(' (')[0]} on the open market in 90 days, "
                "leaving out pre-scheduled plan trades. "
                + _up(d, "Insiders buying with their own money has preceded outperformance.", "",
                      "Insider selling is common and says little on its own."))
    if name.startswith("insider clusters"):
        return f"Several insiders bought together {disp} time(s): a stronger version of the insider-buying signal."
    if name.startswith("earnings reaction"):
        return (f"After its latest earnings report it moved {disp} against the market. "
                "Stocks have tended to keep drifting in the direction of that reaction for weeks.")
    if name.startswith("headlines"):
        count, _, tone = disp.partition(", tone ")
        return (f"{count} news headline{'s' if count != '1' else ''} named it in three days"
                + (f" (overall tone {tone})" if tone else "") + ". Background only.")
    if name.startswith("daily dollar volume"):
        try:
            x = float(v)
            amount = f"${x / 1e9:.1f} billion" if x >= 1e9 else f"${x / 1e6:.0f} million"
        except (TypeError, ValueError):
            amount = disp
        return f"About {amount} of it changes hands on a typical day."
    if name.startswith("cost to trade"):
        return f"Buying or selling $1 million would cost roughly {disp.split(' (')[0]} in spreads and price impact."
    if name.startswith("$1M as share"):
        try:
            share = "less than 0.01%" if float(v) < 0.0001 else disp
        except (TypeError, ValueError):
            share = disp
        return f"$1 million would be {share} of a normal day's trading."
    if name == "borrow":
        return f"For short selling: {disp}."
    return f"{name}: {disp}."


def verdict(tally: Dict[str, Any]) -> Dict[str, Any]:
    """A short, honest summary of what stands behind a ticker's lean."""
    by = tally.get("by_status") or {}
    net = float(tally.get("net") or 0)

    def n(tier: str) -> int:
        c = by.get(tier) or {}
        return int(c.get("bull", 0)) + int(c.get("bear", 0))

    lean = "leans bullish" if net > 0 else "leans bearish" if net < 0 else "no clear lean"
    if n("proven"):
        confidence = "higher"
    elif n("supported"):
        confidence = "moderate"
    elif n("published"):
        confidence = "low"
    else:
        confidence = "none"
    parts = []
    for tier in ("proven", "supported", "published"):
        c = by.get(tier) or {}
        if c.get("bull") or c.get("bear"):
            parts.append(f"{TIER_LABELS[tier].lower()}: {c.get('bull', 0)} up, {c.get('bear', 0)} down")
    why = "; ".join(parts) if parts else "no signal that counts"
    caveat = "" if n("proven") else " Nothing here is proven by our own testing yet."
    return {
        "headline": f"{lean.capitalize()} ({confidence} confidence)" if lean != "no clear lean" else "No clear lean",
        "lean": lean,
        "confidence": confidence,
        "why": f"{why.capitalize()}.{caveat}",
    }


# Everyday names for the matrix's signal names. The names stay as they are --
# the study, the tests and the tally key on them -- and the board shows these.
EVERYDAY_NAME = {
    "12-1 month momentum": "1-year trend",
    "1-month reversal": "Last month's move",
    "price / 52-week high": "Distance from its 1-year high",
    "52-week range position": "Where it sits in its 1-year range",
    "max daily gain, 1 month": "Biggest one-day jump this month",
    "RSI 14": "Overbought / oversold gauge",
    "realised volatility 21d": "How much it swings (a year, at this month's pace)",
    "beta to SPY, 6 months": "How much it moves with the market",
    "volume vs 20-day average": "Trading volume vs usual",
    "put/call volume": "Options bets on a fall vs a rise",
    "IV skew (OTM put - ATM call)": "Price of crash protection",
    "call - put IV spread": "Which way option traders lean",
    "implied / realised vol": "Expected swings vs actual swings",
    "sweep premium, calls - puts, 5d": "Big options bets, last 5 days",
    "days to cover": "Days for short sellers to buy back",
    "short interest change": "Change in short selling",
    "open-market net, 90d (plans excluded)": "Insiders' own buying and selling, 90 days",
    "insider clusters, 90d": "Several insiders buying together, 90 days",
    "earnings reaction (last 2.02)": "How it reacted to its last earnings",
    "headlines naming it, 72h": "Headlines naming it, last 3 days",
    "value (book-to-market)": "Cheapness, against what the company owns",
    "earnings yield": "Cheapness, against its profits",
    "profitability (ROA)": "Profitability",
    "asset growth": "How fast the company is expanding",
    "accruals": "Profits backed by real cash",
    "gross profitability": "Gross profit against what it owns",
    "value vs its industry": "Cheapness within its industry",
    "quality vs its industry": "Quality within its industry",
    "daily dollar volume (est.)": "Dollars traded a day",
    "cost to trade $1M (est.)": "Cost to trade $1M",
    "$1M as share of volume": "$1M as a share of a day's trading",
    "borrow": "Can it be bet against (shorted)",
}


def everyday(s: Dict[str, Any]) -> Dict[str, str]:
    """The board's label and value for a signal, in everyday words: "1-year
    trend" for "12-1 month momentum", "bottom 12%" for "p12 (quintile 1)"."""
    name, disp, v = str(s.get("name", "")), str(s.get("display", "")), s.get("value")
    label = EVERYDAY_NAME.get(name)
    if label is None:
        if name.startswith("return "):
            label = f"Price change, {name[7:]}"
        elif name.startswith("excess vs SPY "):
            label = f"Against the S&P 500, {name[14:]}"
        elif name.startswith("vs ") and name.endswith("-day average"):
            label = f"Price against its {name[3:-12]}-day average"
        elif name.endswith(" rank") and s.get("group") == "model":
            label = f"Our model's {name[:-5]} ranking"
        else:
            label = name[:1].upper() + name[1:]
    shown = disp
    if name.endswith(" rank") and s.get("group") == "model" and isinstance(v, (int, float)):
        pct = round(float(v) * 100)
        shown = f"top {max(1, 100 - pct)}%" if pct >= 50 else f"bottom {max(1, pct)}%"
    elif name == "cost to trade $1M (est.)" and isinstance(v, (int, float)):
        shown = f"{float(v) / 100:.2f}%"
    return {"label": label, "shown": shown}


def explain_matrix(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Adds plain text, everyday labels, tier labels and the verdict to a
    matrix dict in place."""
    for s in doc.get("signals", []):
        s["plain"] = explain(s)
        s.update(everyday(s))
        s["tier"] = TIER_LABELS.get(s.get("status", ""), s.get("status", ""))
    doc["verdict"] = verdict(doc.get("tally") or {})
    return doc


# What each model feature means for one stock, in everyday words: (when the
# stock ranks high, when it ranks low). Interaction features read "high" when
# the stock's trait and the market's state point the same way.
DRIVER_TEXT = {
    "mom_12_1": ("has risen strongly over the past year", "has fallen over the past year"),
    "mom_6_1": ("has risen strongly over six months", "has fallen over six months"),
    "rev_5": ("jumped this week", "dropped this week"),
    "rev_21": ("is up a lot this month", "is down a lot this month"),
    "hi52": ("trades near its 52-week high", "trades far below its 52-week high"),
    "max_21": ("had a very large one-day jump this month", "has had no big one-day jumps lately"),
    "vol_63": ("swings more than most stocks", "is calmer than most stocks"),
    "ivol_63": ("moves a lot on its own news", "moves little apart from the market"),
    "beta_126": ("moves more than the market", "moves less than the market"),
    "dvol_21": ("is heavily traded", "is lightly traded"),
    "vtrend": ("is seeing rising trading volume", "is seeing falling trading volume"),
    "amihud_21": ("moves a lot per dollar traded", "trades in a deep, liquid market"),
    "resmom_12_1": ("beat the market on its own over the year", "lagged the market on its own over the year"),
    "up_share_21": ("rose on most days this month", "fell on most days this month"),
    "rsv_21": ("has had large downside swings", "has had small downside swings"),
    "skew_63": ("is prone to sudden jumps up", "is prone to sudden drops"),
    "pv_corr_21": ("rises on heavy volume", "falls on heavy volume"),
    "dtc": ("has heavy short interest", "has little short interest"),
    "ear": ("rose after its last earnings", "fell after its last earnings"),
    "ind_mom": ("is in a strong industry", "is in a weak industry"),
    "ind_rev_21": ("outpaced its industry this month", "lagged its industry this month"),
    "mom_on_12_1": ("has gained mostly overnight, between the close and the open",
                    "has lost ground overnight, between the close and the open"),
    "id_21": ("rose during trading hours this month", "fell during trading hours this month"),
    "quality_ind": ("is more profitable and cash-backed than its industry",
                    "is less profitable than its industry"),
    "ey": ("is cheap for its earnings", "is expensive for its earnings"),
    "beta_x_trend": ("has the market sensitivity the current trend favours",
                     "has market sensitivity out of step with the current trend"),
    "ratebeta_x_rates": ("is positioned for the current move in interest rates",
                         "is exposed to the current move in interest rates"),
    "mom_x_vol": ("has momentum in a calm market", "has momentum at risk in a turbulent market"),
    "svr_20": ("is being sold short heavily in off-exchange trading", "sees little short selling off-exchange"),
    "svr_chg": ("has seen short selling rise lately", "has seen short selling fall lately"),
    "beta_x_credit": ("has a risk level credit conditions currently favour",
                      "has a risk level credit conditions currently weigh on"),
}


def idea_reasons(drivers, direction: int, k: int = 3):
    """The model's top reasons for an idea, from its exact contributions:
    drivers pushing the score in the idea's direction, described by where the
    stock actually ranks. [] when the model is not linear."""
    out = []
    for feat, contribution, rank in drivers or []:
        if contribution * direction <= 0 or feat not in DRIVER_TEXT:
            continue
        high, low = DRIVER_TEXT[feat]
        out.append(high if rank > 0 else low)
        if len(out) >= k:
            break
    return out



def stand_out(features, k: int = 6, floor: float = 0.3):
    """What is most unusual about one stock, in everyday words: the model
    features where it ranks furthest from the middle (ranks run -0.5..+0.5),
    described by which end it sits at. Only features with a description count."""
    picked = []
    for feat, rank in (features or {}).items():
        if feat not in DRIVER_TEXT or not isinstance(rank, (int, float)) or abs(rank) < floor:
            continue
        high, low = DRIVER_TEXT[feat]
        picked.append({"feature": feat, "rank": round(float(rank), 3), "text": high if rank > 0 else low})
    picked.sort(key=lambda t: -abs(t["rank"]))
    return picked[:k]


__all__ = ["DRIVER_TEXT", "EVERYDAY_NAME", "TIER_LABELS", "TIER_MEANING", "everyday", "explain", "explain_matrix", "idea_reasons", "stand_out", "verdict"]
