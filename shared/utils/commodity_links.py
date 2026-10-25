"""
shared/utils/commodity_links.py

What companies make and buy, priced daily, and which listed stocks each price
bears on. Prices underreact: oil price changes predict stock market returns
with a lag (Driesprong, Jacobsen & Maat 2008, "Striking oil: another puzzle?"),
and news about a firm's inputs or outputs reaches its price slowly when the
link takes work to see (Cohen & Frazzini 2008). So each series is mapped to the
stocks that produce it (+1: a higher price helps) or consume it (-1: a higher
price hurts), and the stock's exposure is the signed recent move of its series.

  futures      Yahoo continuous front-month contracts, daily since 2015
  memory       DRAMeXchange daily spot (DRAM, NAND flash, GDDR, modules),
               collected from 22 Oct 2026; the history begins there
  gpu          RunPod's public per-GPU hourly rates (secure and lowest
               community), collected from 22 Oct 2026

Pure: series and prices in; links, rows and features out.
"""
from __future__ import annotations

import html
import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class Series:
    key: str          # stored series name
    name: str         # in words
    group: str        # links attach to groups


# Yahoo continuous futures (and bitcoin), by group.
FUTURES: Dict[str, Series] = {s.key: s for s in [
    Series("CL=F", "WTI crude oil", "oil"), Series("BZ=F", "Brent crude oil", "oil"),
    Series("HO=F", "Heating oil (diesel)", "refined"), Series("RB=F", "Gasoline (RBOB)", "refined"),
    Series("NG=F", "US natural gas", "natgas"), Series("TTF=F", "European gas (TTF)", "eu_gas"),
    Series("HG=F", "Copper", "copper"), Series("ALI=F", "Aluminum", "aluminum"),
    Series("GC=F", "Gold", "gold"), Series("SI=F", "Silver", "silver"),
    Series("PL=F", "Platinum", "pgm"), Series("PA=F", "Palladium", "pgm"),
    Series("UX=F", "Uranium", "uranium"), Series("LTH=F", "Lithium hydroxide", "lithium"),
    Series("ZC=F", "Corn", "grain"), Series("ZW=F", "Wheat", "grain"), Series("ZS=F", "Soybeans", "grain"),
    Series("KC=F", "Coffee", "coffee"), Series("CC=F", "Cocoa", "cocoa"), Series("SB=F", "Sugar", "sugar"),
    Series("LE=F", "Live cattle", "cattle"), Series("HE=F", "Lean hogs", "hogs"),
    Series("LBR=F", "Lumber", "lumber"), Series("BTC-USD", "Bitcoin", "bitcoin"),
]}

# Which stocks a group's price bears on: +1 a producer (a higher price helps),
# -1 a buyer (a higher price hurts). US listings only.
LINKS: Dict[str, Dict[str, int]] = {
    "oil": {"XOM": 1, "CVX": 1, "COP": 1, "OXY": 1, "EOG": 1, "DVN": 1, "FANG": 1, "APA": 1, "HAL": 1,
            "SLB": 1, "DAL": -1, "UAL": -1, "AAL": -1, "LUV": -1},
    "refined": {"VLO": 1, "MPC": 1, "PSX": 1},
    "natgas": {"EQT": 1, "AR": 1, "RRC": 1, "CTRA": 1, "CF": -1},
    "eu_gas": {"LNG": 1},
    "copper": {"FCX": 1, "SCCO": 1, "TECK": 1},
    "aluminum": {"AA": 1, "CENX": 1},
    "gold": {"NEM": 1, "AEM": 1, "KGC": 1, "GOLD": 1},
    "silver": {"PAAS": 1, "HL": 1, "AG": 1},
    "pgm": {"SBSW": 1},
    "uranium": {"CCJ": 1, "UEC": 1, "NXE": 1, "UUUU": 1, "DNN": 1},
    "lithium": {"ALB": 1, "SQM": 1, "LAC": 1},
    "grain": {"ADM": 1, "BG": 1, "NTR": 1, "MOS": 1, "TSN": -1},
    "coffee": {"SBUX": -1},
    "cocoa": {"HSY": -1, "MDLZ": -1},
    "sugar": {},
    "cattle": {"TSN": -1},
    "hogs": {"HRL": -1},
    "lumber": {"WY": 1, "PCH": 1, "LPX": 1, "DHI": -1, "LEN": -1},
    "bitcoin": {"COIN": 1, "MSTR": 1, "MARA": 1, "RIOT": 1, "CLSK": 1, "HUT": 1, "HOOD": 1},
    # Memory: the makers gain when spot rises; the box builders pay for it.
    "dram": {"MU": 1, "DELL": -1, "HPQ": -1, "HPE": -1, "SMCI": -1},
    "nand": {"SNDK": 1, "MU": 1, "WDC": 1, "SIMO": 1, "DELL": -1, "HPQ": -1},
    "gddr": {"MU": 1},
    # GPU rental rates: scarce compute lifts the chip makers and the GPU clouds.
    "gpu": {"NVDA": 1, "AMD": 1, "CRWV": 1, "NBIS": 1, "SMCI": 1, "VRT": 1},
}


def group_of(series: str) -> Optional[str]:
    if series in FUTURES:
        return FUTURES[series].group
    head = series.split(":", 1)[0]
    return head if head in LINKS else None


def links_for(ticker: str) -> List[Tuple[str, int]]:
    """(group, sign) for every group a stock is linked to."""
    t = ticker.upper()
    return [(g, s) for g, names in LINKS.items() for name, s in names.items() if name == t]


def linked_tickers() -> List[str]:
    return sorted({t for names in LINKS.values() for t in names})


# ── memory spot (DRAMeXchange) ───────────────────────────────────────────────

_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_TAG = re.compile(r"<[^>]+>")
_NUM = re.compile(r"^-?\d+(\.\d+)?$")


def memory_group(item: str) -> Optional[str]:
    """Which memory group a DRAMeXchange spot item belongs to."""
    s = item.upper()
    if "GDDR" in s:
        return "gddr"
    if s.startswith(("DDR", "LPDDR")) or "DIMM" in s:
        return "dram"
    if any(k in s for k in ("TLC", "MLC", "SLC", "QLC", "MICROSD")):
        return "nand"
    return None


def parse_dramexchange(page: str) -> Dict[str, float]:
    """{"dram:<item>": session average} for every spot row on the page.

    Spot rows read: item, daily high, daily low, session high, session low,
    session average, change. Retail SSD rows (brand first) are not spot prices
    and are skipped."""
    out: Dict[str, float] = {}
    for m in _ROW.finditer(page or ""):
        cells = [html.unescape(_TAG.sub("", c)).strip() for c in _CELL.findall(m.group(1))]
        cells = [c for c in cells if c]
        if len(cells) < 6 or not all(_NUM.match(c.replace(",", "")) for c in cells[1:6]):
            continue
        group = memory_group(cells[0])
        if group is None:
            continue
        item = re.sub(r"\s+", " ", cells[0])
        out[f"{group}:{item}"] = float(cells[5].replace(",", ""))
    return out


# ── GPU rental (RunPod) ──────────────────────────────────────────────────────

GPU_QUERY = ("query { gpuTypes { id displayName securePrice communityPrice "
             "lowestPrice(input:{gpuCount:1}){ uninterruptablePrice } } }")
GPUS = ("H100 SXM", "H100 PCIe", "H100 NVL", "H200 SXM", "B200", "A100 SXM", "A100 PCIe", "MI300X", "L40S",
        "RTX 4090", "RTX 5090")


def parse_runpod(body: Mapping[str, Any]) -> Dict[str, float]:
    """{"gpu:<name>:secure" | "gpu:<name>:lowest": $ per GPU-hour} for the tracked GPUs."""
    out: Dict[str, float] = {}
    for g in ((body or {}).get("data") or {}).get("gpuTypes") or []:
        name = str(g.get("displayName") or "")
        if name not in GPUS:
            continue
        sec = g.get("securePrice")
        low = ((g.get("lowestPrice") or {}).get("uninterruptablePrice"))
        if isinstance(sec, (int, float)) and sec > 0:
            out[f"gpu:{name}:secure"] = float(sec)
        if isinstance(low, (int, float)) and low > 0:
            out[f"gpu:{name}:lowest"] = float(low)
    return out


# ── features ─────────────────────────────────────────────────────────────────

def log_return(closes: Sequence[Tuple[date, float]], day: date, window: int) -> Optional[float]:
    """Log change over the last `window` observations up to and including `day`."""
    pts = [v for d, v in closes if d <= day and v and v > 0]
    if len(pts) <= window:
        return None
    return math.log(pts[-1] / pts[-1 - window])


def exposure(ticker: str, day: date, series_by_group: Mapping[str, Iterable[str]],
             closes: Mapping[str, Sequence[Tuple[date, float]]], window: int) -> Optional[float]:
    """The signed recent move of what a stock makes or buys: the mean over its
    linked groups of sign x the group's mean log change over `window`."""
    parts: List[float] = []
    for group, sign in links_for(ticker):
        moves = [r for r in (log_return(closes.get(s, ()), day, window) for s in series_by_group.get(group, ()))
                 if r is not None]
        if moves:
            parts.append(sign * sum(moves) / len(moves))
    return sum(parts) / len(parts) if parts else None


__all__ = ["FUTURES", "GPUS", "GPU_QUERY", "LINKS", "Series", "exposure", "group_of", "linked_tickers",
           "links_for", "log_return", "memory_group", "parse_dramexchange", "parse_runpod"]
