"""
shared/utils/airlines.py

Which listed company is flying an aircraft, from its callsign.

An airline flight's ATC callsign begins with the operator's three-letter ICAO
designator -- DAL1234 is Delta, FDX5701 is FedEx -- so the operator of most
commercial traffic is already in every ADS-B message. Measured on 3 October,
24,778 aircraft in the graph had edges only to a region and a flag: none to the
company operating them, so nothing an aircraft did could reach a stock.

Limited to operators with a US listing (common stock or a listed ADR) and to
regional carriers whose flying is sold under a listed parent, where the
designator unambiguously belongs to that parent. Private operators and
carriers without a US listing are absent on purpose: an unmapped callsign means
"no listed operator", never a guess.
"""

from __future__ import annotations

from typing import Dict, Optional

# ICAO designator -> US-listed ticker of the operator or its parent.
AIRLINE_TICKERS: Dict[str, str] = {
    # US mainline
    "AAL": "AAL",    # American Airlines
    "DAL": "DAL",    # Delta Air Lines
    "UAL": "UAL",    # United Airlines
    "SWA": "LUV",    # Southwest Airlines
    "JBU": "JBLU",   # JetBlue
    "ASA": "ALK",    # Alaska Airlines
    "HAL": "ALK",    # Hawaiian Airlines, Alaska Air Group since 2024
    "QXE": "ALK",    # Horizon Air, Alaska Air Group
    "AAY": "ALGT",   # Allegiant Air
    "FFT": "ULCC",   # Frontier Airlines
    "SCX": "SNCY",   # Sun Country
    "SKW": "SKYW",   # SkyWest
    # Wholly owned regionals flying under a listed parent
    "ENY": "AAL",    # Envoy Air, American
    "PDT": "AAL",    # Piedmont Airlines, American
    "JIA": "AAL",    # PSA Airlines, American
    "EDV": "DAL",    # Endeavor Air, Delta
    # Cargo
    "FDX": "FDX",    # FedEx Express
    "UPS": "UPS",    # UPS Airlines
    # Foreign carriers with a US listing
    "RYR": "RYAAY",  # Ryanair
    "RUK": "RYAAY",  # Ryanair UK
    "AZU": "AZUL",   # Azul
    "LAN": "LTM",    # LATAM Airlines
    "TAM": "LTM",    # LATAM Brasil
    "LPE": "LTM",    # LATAM Peru
    "CMP": "CPA",    # Copa Airlines
    "VOI": "VLRS",   # Volaris
}


def operator_designator(callsign: Optional[str]) -> Optional[str]:
    """The ICAO designator in a flight callsign, or None.

    Three letters then a digit: DAL1234, BAW9X. A registration used as a
    callsign -- N123AB, GABCD -- does not match, because a private flight has
    no airline to map.
    """
    if not callsign:
        return None
    cs = str(callsign).strip().upper()
    if len(cs) < 4 or not cs[:3].isalpha() or not cs[3].isdigit():
        return None
    return cs[:3]


def operator_ticker(callsign: Optional[str]) -> Optional[str]:
    """The listed ticker of the aircraft's operator, or None."""
    code = operator_designator(callsign)
    return AIRLINE_TICKERS.get(code) if code else None
