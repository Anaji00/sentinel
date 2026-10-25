"""
shared/utils/civic_law.py

"Is it legal here?" -- each agency's published Flock practice checked against
its state's licence-plate-reader law.

State rules are transcribed from NCSL's 50-state summary (September 2026,
https://www.ncsl.org/technology-and-communication/automated-license-plate-readers-state-statutes),
with California's sharing rule from the Attorney General's October 2023
bulletin on SB 34 (contested: El Cajon reads the law differently and is being
sued over it). Agency practice comes from Flock transparency portals as
aggregated by EyesOnFlock (CC BY-SA 4.0) and from ICE's 287(g) list.

Every finding is a *possible* conflict with a citation and the evidence, worded
as something to ask about. Statutes carry exceptions (warrants, active
investigations, declarations on file) that published portal data cannot show.
Pure: no I/O.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

NCSL_URL = "https://www.ncsl.org/technology-and-communication/automated-license-plate-readers-state-statutes"
CA_AG_URL = "https://www.oag.ca.gov/news/press-releases/attorney-general-bonta-sues-el-cajon-illegally-sharing-license-plate-data-out"


@dataclass(frozen=True)
class StateLaw:
    state: str
    citation: str
    retention_days: Optional[float] = None          # statutory maximum for routine data
    retention_text: Optional[str] = None
    immigration_use_banned: bool = False
    immigration_sharing_banned: bool = False
    out_of_state_sharing_banned: bool = False
    out_of_state_allowed: FrozenSet[str] = frozenset()  # states sharing is permitted with
    declaration_required: bool = False              # out-of-state recipients must sign one
    federal_sharing_banned: bool = False
    note: Optional[str] = None
    source: str = NCSL_URL


def _law(**kw: Any) -> StateLaw:
    return StateLaw(**kw)


STATE_LAWS: Dict[str, StateLaw] = {s.state: s for s in [
    _law(state="AL", citation="Ala. Admin. Code 265-X-6 (under §41-9-620)", retention_days=1825, retention_text="five years"),
    _law(state="AR", citation="Ark. Code §12-12-1801 to 1808", retention_days=150, retention_text="150 days"),
    _law(state="CA", citation="Civ. Code §1798.90.5 et seq. (SB 34); AG bulletin 2023",
         out_of_state_sharing_banned=True, federal_sharing_banned=True,
         note="The Attorney General reads SB 34 as barring sharing with out-of-state and federal agencies and sued "
              "El Cajon over it in October 2025; El Cajon disputes that reading.", source=CA_AG_URL),
    _law(state="CO", citation="C.R.S. §24-72-113", retention_days=1095, retention_text="three years (passive surveillance)"),
    _law(state="CT", citation="SB 397 (2026), effective 1 Oct 2026", retention_days=21, retention_text="21 days",
         immigration_use_banned=True, immigration_sharing_banned=True, out_of_state_sharing_banned=True,
         out_of_state_allowed=frozenset({"NY", "RI", "MA"}), declaration_required=True, federal_sharing_banned=True,
         note="Sharing only with Connecticut agencies, or New York, Rhode Island and Massachusetts agencies with a "
              "written declaration, or others with a warrant."),
    _law(state="GA", citation="O.C.G.A. §35-1-22", retention_days=913, retention_text="30 months"),
    _law(state="IA", citation="Iowa Code §321P.4", retention_days=30, retention_text="30 days"),
    _law(state="IL", citation="5 ILCS 140/7.5; 625 ILCS 5/2-130", immigration_sharing_banned=True,
         declaration_required=True,
         note="No sharing for immigration-status investigations; out-of-state recipients must sign a written declaration."),
    _law(state="KY", citation="KRS §189.632", retention_days=90, retention_text="90 days"),
    _law(state="ME", citation="29-A MRSA §2117-A", retention_days=21, retention_text="21 days"),
    _law(state="MD", citation="Md. Public Safety Code §3-509", out_of_state_sharing_banned=True,
         note="Historical plate data may not be uploaded to agencies or entities outside the state."),
    _law(state="MN", citation="Minn. Stat. §13.824", retention_days=60, retention_text="60 days"),
    _law(state="MT", citation="MCA §46-5-117 to -119", retention_days=90, retention_text="90 days"),
    _law(state="NE", citation="Neb. Rev. Stat. §60-3201 to 3209", retention_days=180, retention_text="180 days"),
    _law(state="NH", citation="RSA 261:75-b", retention_days=0.0021, retention_text="three minutes"),
    _law(state="NJ", citation="A.B. 4070 (2026)", immigration_sharing_banned=True,
         note="Recipients must certify the data will not be used for civil immigration enforcement."),
    _law(state="NM", citation="NMSA §29-22-1 to -5", immigration_sharing_banned=True, declaration_required=True,
         note="No sharing where it may be used for immigration enforcement; out-of-state recipients must sign a declaration."),
    _law(state="NC", citation="N.C.G.S. §20-183.30 to .33", retention_days=90, retention_text="90 days"),
    _law(state="OR", citation="2026 Or. Laws c. 77 §4-9", retention_days=30, retention_text="30 days",
         out_of_state_sharing_banned=True, federal_sharing_banned=True,
         note="No access for entities not created under Oregon law except as specified; applies as existing vendor "
              "contracts expire."),
    _law(state="TN", citation="Tenn. Code §55-10-302", retention_days=90, retention_text="90 days"),
    _law(state="UT", citation="Utah Code §41-6a-2001 to 2006", retention_days=274, retention_text="nine months"),
    _law(state="VT", citation="23 V.S.A. §1607 to 1609", retention_days=548, retention_text="18 months"),
    _law(state="VA", citation="Va. Code §2.2-5517", retention_days=21, retention_text="21 days"),
    _law(state="WA", citation="SB 6002 (2026)", retention_days=21, retention_text="21 days",
         immigration_use_banned=True, immigration_sharing_banned=True,
         note="Use for immigration enforcement prohibited; disclosure and sharing restricted."),
]}

STATES = frozenset("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ "
                   "NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split())
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}
AMBIGUOUS_CODES = frozenset({"LA", "CO", "SD"})
_CODE = re.compile(r"(?<![A-Za-z])([A-Z]{2})(?![A-Za-z])")
_FEDERAL = re.compile(r"^\s*\[Federal\]", re.I)


def is_federal(org: str) -> bool:
    """Flock marks federal recipients '[Federal] ...' in sharing lists."""
    return bool(_FEDERAL.match(org or ""))


def org_state(org: str) -> Optional[str]:
    """The state a Flock organisation name places itself in ('Acworth GA PD',
    'Arlington PD (WA)', 'Arizona Department of Public Safety'), or None."""
    if not org or is_federal(org):
        return None
    m = re.search(r"\(([A-Z]{2})\)", org)                 # 'Arlington PD (WA)'
    if m and m.group(1) in STATES:
        return m.group(1)
    m = re.match(r"^([A-Z]{2})\s*-\s", org)               # 'MN - Isanti CO SO'
    if m and m.group(1) in STATES:
        return m.group(1)
    tokens = _CODE.findall(org)
    codes = [c for c in tokens if c in STATES]
    if len(codes) > 1:
        # Agency-type abbreviations that are also state codes: 'CO' for county
        # ('Cameron CO TX SO') and a closing 'SD' for sheriff's department
        # ('Los Angeles County CA SD', which counted as South Dakota in 146
        # California sharing lists before 10 Oct 2026). Dropped when another
        # state code is present.
        codes = [c for c in codes if c != "CO"] or codes
        if len(codes) > 1 and tokens and tokens[-1] == "SD" and codes[-1] == "SD":
            codes = codes[:-1]
    if codes:
        return codes[-1]
    low = org.lower()
    for name in sorted(STATE_NAMES, key=len, reverse=True):
        if re.search(rf"\b{name}\b", low):
            return STATE_NAMES[name]
    return None


@dataclass
class Finding:
    code: str
    severity: str                  # 'conflict' (contradicts the statute on its face) or 'check' (ask for records)
    title: str
    detail: str
    citation: str
    source: str
    evidence: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


def check_portal(portal: Mapping[str, Any], law: Optional[StateLaw],
                 has_287g: Optional[Sequence[Mapping[str, Any]]] = None) -> List[Finding]:
    """Possible conflicts between one agency's published Flock practice and its
    state's law. `portal` carries state, data_retention (days),
    organizations_shared_with (names); `has_287g` the agency's 287(g) rows."""
    out: List[Finding] = []
    st = portal.get("state")
    shared = list(portal.get("organizations_shared_with") or [])
    if law is not None:
        ret = portal.get("data_retention")
        if law.retention_days is not None and isinstance(ret, (int, float)) and ret > law.retention_days:
            out.append(Finding("retention_over_limit", "conflict", "Keeps plate data longer than state law allows",
                               f"The portal lists {ret:g} days of retention; {law.state} law sets {law.retention_text} "
                               f"for routine data (investigative holds excepted).",
                               law.citation, law.source, {"retention_days": ret, "limit_days": law.retention_days}))
        foreign = []
        for o in shared:
            s = org_state(o)
            if s and s != st and s not in law.out_of_state_allowed:
                foreign.append(o)
        if foreign and (law.out_of_state_sharing_banned or law.declaration_required):
            states = sorted({org_state(o) for o in foreign if org_state(o)})
            # 'LA', 'CO' and 'SD' double as Los Angeles, County and Sheriff's
            # Department; a finding resting only on them is a question.
            unsure = all(org_state(o) in AMBIGUOUS_CODES for o in foreign)
            hedge = (" The recipient's state is read from its name and may be ambiguous ("
                     + ", ".join(foreign[:3]) + ").") if unsure else ""
            if law.out_of_state_sharing_banned:
                out.append(Finding("out_of_state_sharing",
                                   "conflict" if not (law.declaration_required or unsure) else "check",
                                   "Shares plate data with out-of-state agencies",
                                   f"Shares with {len(foreign)} agencies in {len(states)} other states; {law.state} law "
                                   f"restricts sharing outside the state." + hedge + (f" {law.note}" if law.note else ""),
                                   law.citation, law.source,
                                   {"count": len(foreign), "states": states, "examples": foreign[:8]}))
            else:
                out.append(Finding("out_of_state_declarations", "check",
                                   "Out-of-state sharing needs signed declarations",
                                   f"Shares with {len(foreign)} agencies in {len(states)} other states; {law.state} law "
                                   f"requires each to sign a written declaration. Ask for them." +
                                   (f" {law.note}" if law.note else ""),
                                   law.citation, law.source,
                                   {"count": len(foreign), "states": states, "examples": foreign[:8]}))
        fed = [o for o in shared if is_federal(o)]
        if fed and (law.federal_sharing_banned or law.immigration_sharing_banned):
            out.append(Finding("federal_sharing", "conflict" if law.federal_sharing_banned else "check",
                               "Shares plate data with federal agencies",
                               f"Shares with {len(fed)} federal agencies; {law.state} law "
                               + ("restricts federal sharing." if law.federal_sharing_banned
                                  else "bars sharing for immigration enforcement. Ask what limits each recipient accepted.")
                               + (f" {law.note}" if law.note else ""),
                               law.citation, law.source, {"count": len(fed), "examples": fed[:8]}))
        prohibited = str(portal.get("prohibited_uses") or "").lower()
        if (law.immigration_use_banned or law.immigration_sharing_banned) and prohibited and "immigration" not in prohibited:
            out.append(Finding("policy_omits_immigration", "check",
                               "Published policy does not list immigration enforcement as prohibited",
                               f"{law.state} law restricts immigration use or sharing; the portal's prohibited-uses "
                               f"statement does not mention it.", law.citation, law.source, {}))
    for row in has_287g or []:
        out.append(Finding("ice_287g", "check", "Has a 287(g) immigration agreement with ICE",
                           f"{row.get('agency')} signed a {row.get('support_type')} agreement"
                           f"{' on ' + str(row['signed']) if row.get('signed') else ''}, while running a Flock network "
                           f"shared with {len(shared)} agencies.",
                           "8 U.S.C. §1357(g)", row.get("moa") or "https://www.ice.gov/identify-and-arrest/287g",
                           {"support_type": row.get("support_type"), "signed": str(row.get("signed") or "")}))
    return out


def records_request(agency: str, state: str, findings: Sequence[Mapping[str, Any]], today: str,
                    extra_asks: Sequence[str] = ()) -> Dict[str, Any]:
    """A public-records request a person can send themselves, asking for the
    records that would settle each open question about this agency. Generic on
    the statute ("the state public records law"): every state has one, and
    naming the wrong one helps no one."""
    asks = [
        "All contracts, amendments, renewals and invoices with Flock Safety (Flock Group Inc.), including the "
        "number and locations of cameras.",
        "The current list of agencies and organisations with access to this agency's licence-plate-reader data, and "
        "the agencies whose data this agency can search.",
        "The data-retention setting configured in the Flock system and any written retention policy.",
        "Audit logs of every search of the Flock system for the past 12 months, including the date, the searching "
        "agency and user role, the stated reason or case number, and the number of networks searched.",
        "Policies governing hotlists, data sharing and permitted uses, and any training materials.",
    ]
    codes = {str(f.get("code")) for f in findings}
    if "out_of_state_sharing" in codes or "out_of_state_declarations" in codes:
        asks.append("Any written declarations, certifications or agreements from out-of-state agencies given access "
                    "to this agency's data, as state law requires.")
    if "federal_sharing" in codes:
        asks.append("Records of any federal agency's access to or searches of this agency's data, and any "
                    "limits those agencies accepted on immigration-related use.")
    if "ice_287g" in codes:
        asks.append("The 287(g) memorandum of agreement with ICE and any records of licence-plate-reader data used "
                    "in 287(g) activity.")
    asks.extend(extra_asks)
    why = [f"- {f.get('title')}: {f.get('detail')} ({f.get('citation')})" for f in findings]
    nl = "\n"
    body = (f"{today}{nl}{nl}To the public records officer, {agency} ({state}):{nl}{nl}"
            f"Under the state public records law, I request copies of the following records:{nl}{nl}"
            + nl.join(f"{i}. {a}" for i, a in enumerate(asks, 1))
            + f"{nl}{nl}Electronic copies are preferred. If any portion is withheld, please cite the specific "
              "exemption for each withholding and release all reasonably segregable portions. If fees will exceed "
              f"$25, please tell me before proceeding.{nl}{nl}Thank you,{nl}[Your name]{nl}"
              "[Your email or mailing address]")
    if why:
        body += f"{nl}{nl}Context (published data that prompts this request):{nl}" + nl.join(why)
    return {"subject": f"Public records request: licence-plate reader (Flock) records, {agency}",
            "body": body, "asks": asks,
            "file_with": "https://www.muckrock.com/foi/create/",
            "note": "Send it yourself, by email to the agency's records officer or through MuckRock, which tracks "
                    "deadlines and replies. Sentinel does not send requests on anyone's behalf."}


# What a search's stated reason is counted as, by published keyword. Matching is
# deliberately conservative: 'ICE' only in capitals or dotted (so "black ice"
# and "price" never count), whole words elsewhere. Only the category and the
# term are kept -- never the reason text, which can name people.
REASON_TERMS: Dict[str, List[Tuple[str, str]]] = {
    "immigration": [
        ("ICE", r"(?<![A-Za-z])(?:ICE|I\.C\.E\.?)(?![A-Za-z])"),
        ("immigration", r"(?i)\bimmigra\w*"),
        ("deportation", r"(?i)\bdeport\w*"),
        ("border patrol", r"(?i)\bborder\s+patrol\b"),
        ("CBP", r"\bCBP\b"),
        ("HSI", r"\bHSI\b|(?i:homeland\s+security\s+investigations)"),
        ("ERO", r"\bERO\b"),
        ("287(g)", r"(?i)\b287\s*\(?g\)?"),
        ("undocumented", r"(?i)\bundocumented\b|\billegal\s+alien"),
    ],
    "first_amendment": [
        ("protest", r"(?i)\bprotest\w*"),
        ("rally", r"(?i)\brall(?:y|ies)\b"),
        # Demonstrators, not "demonstration": in search logs that word mostly
        # means a product or training demo (Cabot AR, Greenfield CA, 10 Oct 2026).
        ("demonstrators", r"(?i)\bdemonstrators?\b"),
        ("activist", r"(?i)\bactivis\w*"),
        ("journalist", r"(?i)\bjournalis\w*|\breporters?\b"),
        ("picket", r"(?i)\bpicket\w*"),
        ("vigil", r"(?i)\bvigils?\b"),
        ("first amendment", r"(?i)\bfirst\s+amendment\b|\bfree\s+speech\b"),
    ],
}
_REASON_RE = {cat: [(term, re.compile(rx)) for term, rx in terms] for cat, terms in REASON_TERMS.items()}
# A search that reached this many agencies' networks is a nationwide lookup.
WIDE_SEARCH_NETWORKS = 100


def classify_reason(reason: Optional[str]) -> Dict[str, List[str]]:
    """{category: [terms matched]} for one search's stated reason."""
    out: Dict[str, List[str]] = {}
    if not reason:
        return out
    for cat, terms in _REASON_RE.items():
        hit = [term for term, rx in terms if rx.search(reason)]
        if hit:
            out[cat] = hit
    return out


def check_audit(stats: Mapping[str, Any], law: Optional[StateLaw]) -> List[Finding]:
    """Findings from an agency's own released search log: searches whose stated
    reason cites immigration, where state law restricts immigration use or sharing."""
    n = int(stats.get("immigration_hits") or 0)
    if not n or law is None or not (law.immigration_use_banned or law.immigration_sharing_banned):
        return []
    terms = stats.get("immigration_terms") or {}
    top = ", ".join(f"{k} {v}" for k, v in sorted(terms.items(), key=lambda kv: -kv[1])[:4])
    return [Finding("immigration_searches", "conflict" if law.immigration_use_banned else "check",
                    "Its own search log cites immigration",
                    f"{n} searches in this agency's released audit log (last {stats.get('days', 90)} days) give a "
                    f"reason matching immigration terms ({top}); {law.state} law "
                    + ("bars using plate data for immigration enforcement." if law.immigration_use_banned
                       else "bars sharing plate data for immigration enforcement. Ask who ran them and why.")
                    + " Keyword matches can include unrelated uses; the terms are listed.",
                    law.citation, law.source, {"count": n, "terms": terms})]


def norm_agency(name: str) -> str:
    """Loose key for matching an agency across sources: 'Sarasota County FL SO',
    "Sarasota County Sheriff's Office" -> 'sarasota county sheriff'."""
    n = (name or "").lower()
    n = re.sub(r"\[inactive\]|\(.*?\)", " ", n)
    n = re.sub(r"\b(so|sheriff'?s? office|sheriffs? department|sheriff'?s? dept)\b", " sheriff ", n)
    n = re.sub(r"\b(pd|police department|police dept|police)\b", " police ", n)
    n = re.sub(r"\b(" + "|".join(s.lower() for s in STATES) + r")\b", " ", n)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


__all__ = ["CA_AG_URL", "Finding", "NCSL_URL", "STATE_LAWS", "StateLaw", "check_portal", "is_federal",
           "norm_agency", "org_state", "records_request", "REASON_TERMS", "WIDE_SEARCH_NETWORKS",
           "check_audit", "classify_reason"]
