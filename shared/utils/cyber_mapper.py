"""
shared/utils/cyber_mapper.py

CPE VENDOR TO PUBLIC EQUITY TICKER MAPPER
==========================================
Maps cybersecurity vulnerability vendors and software publishers (CPE vendors,
CISA KEV vendors, breach organizations) to US primary public equity tickers.
Translates cross-domain cyber intelligence signals (CVEs) into actionable equity tickers.
"""

from typing import Dict, Optional, Tuple

# Mapping of lowercase vendor names / aliases to public equity tickers
CPE_VENDOR_TICKER_MAP: Dict[str, str] = {
    # Big Tech & Infrastructure
    "microsoft": "MSFT",
    "msft": "MSFT",
    "cisco": "CSCO",
    "cisco systems": "CSCO",
    "apple": "AAPL",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "oracle": "ORCL",
    "ibm": "IBM",
    "international business machines": "IBM",
    "redhat": "IBM",
    "red hat": "IBM",
    "intel": "INTC",
    "amd": "AMD",
    "advanced micro devices": "AMD",
    "nvidia": "NVDA",
    "dell": "DELL",
    "dell technologies": "DELL",
    "hp": "HPQ",
    "hp inc": "HPQ",
    "hewlett packard": "HPQ",
    "hewlett-packard": "HPQ",
    "hewlett packard enterprise": "HPE",
    "hpe": "HPE",
    "amazon": "AMZN",
    "aws": "AMZN",
    "amazon web services": "AMZN",
    "broadcom": "AVGO",
    "vmware": "AVGO",
    "qualcomm": "QCOM",
    "salesforce": "CRM",
    "servicenow": "NOW",
    "snowflake": "SNOW",
    "palantir": "PLTR",
    "palantir technologies": "PLTR",

    # Cybersecurity Vendors
    "palo alto": "PANW",
    "palo alto networks": "PANW",
    "paloaltonetworks": "PANW",
    "panw": "PANW",
    "fortinet": "FTNT",
    "crowdstrike": "CRWD",
    "sentinelone": "S",
    "cloudflare": "NET",
    "zscaler": "ZS",
    "adobe": "ADBE",
    "sap": "SAP",
    "sap se": "SAP",
    "juniper": "JNPR",
    "juniper networks": "JNPR",
    "f5": "FFIV",
    "f5 networks": "FFIV",
    "arista": "ANET",
    "arista networks": "ANET",
    "check point": "CHKP",
    "checkpoint": "CHKP",
    "check point software": "CHKP",
    "symantec": "GEN",
    "gen digital": "GEN",
    "norton": "GEN",
    "solarwinds": "SWI",
    "rapid7": "RPD",
    "tenable": "TENB",
    "varonis": "VRNS",
    "datadog": "DDOG",
    "splunk": "CSCO",
    "okta": "OKTA",
    "cyberark": "CYBR",
    "trend micro": "TMICY",
    "qualys": "QLYS",

    # Telecom & Defense Enterprise
    "at&t": "T",
    "verizon": "VZ",
    "t-mobile": "TMUS",
    "comcast": "CMCSA",
    "charter": "CHTR",
    "boeing": "BA",
    "lockheed": "LMT",
    "lockheed martin": "LMT",
    "northrop": "NOC",
    "northrop grumman": "NOC",
    "general dynamics": "GD",
    "rtx": "RTX",
    "raytheon": "RTX",
}


def map_cve_to_equity(
    cve_id: str,
    vendor_name: Optional[str] = None,
    description: str = ""
) -> Tuple[Optional[str], str]:
    """
    Translates a CVE identifier (e.g. 'CVE-2026-16812') and vendor context
    into a public equity ticker.

    Returns:
        (ticker, "DIRECT_VENDOR_VULNERABILITY") if a matching public vendor is found.
        (None, "INFRASTRUCTURE_CYBER_RISK") if no public equity ticker can be resolved.
    """
    if vendor_name:
        clean_vendor = str(vendor_name).strip().lower()
        if clean_vendor in CPE_VENDOR_TICKER_MAP:
            return (CPE_VENDOR_TICKER_MAP[clean_vendor], "DIRECT_VENDOR_VULNERABILITY")

        for key, ticker in CPE_VENDOR_TICKER_MAP.items():
            if key in clean_vendor or clean_vendor in key:
                return (ticker, "DIRECT_VENDOR_VULNERABILITY")

    if description:
        clean_desc = str(description).lower()
        for key, ticker in CPE_VENDOR_TICKER_MAP.items():
            if len(key) > 3 and key in clean_desc:
                return (ticker, "DIRECT_VENDOR_VULNERABILITY")

    return (None, "INFRASTRUCTURE_CYBER_RISK")
