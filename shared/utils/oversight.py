"""
shared/utils/oversight.py

Local surveillance oversight laws ("Community Control Over Police
Surveillance", CCOPS): where a council must approve surveillance technology
and its use policy before police buy or use it. Pure; curated with a source
for every entry.

Base list: EFF, "Community Control of Police Spy Tech" (May 2021), which links
each ordinance's code. Later laws are added only with their own source. A year
is given only where a source states it. The ACLU counts 26 such laws; this list
is what could be verified, not a census.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

EFF_LIST = "https://www.eff.org/deeplinks/2021/05/community-control-police-spy-tech"
# Where to start when a place has no oversight law: the ACLU's CCOPS model bill and campaign.
CAMPAIGN_URL = "https://www.aclu.org/community-control-over-police-surveillance"


@dataclass(frozen=True)
class OversightLaw:
    place: str
    state: str
    level: str                  # city | county | transit
    year: Optional[int]
    law_url: str                # the ordinance (code) itself
    source: str                 # where its adoption is reported
    note: Optional[str] = None
    aliases: Tuple[str, ...] = ()


OVERSIGHT_LAWS: Tuple[OversightLaw, ...] = (
    OversightLaw("Bay Area Rapid Transit", "CA", "transit", None,
                 "https://www.bart.gov/sites/default/files/docs/BART%20Surveillance%20Technology%20Ordinance%20%28Second%20Reading%20for%20Ordinance%29.pdf",
                 EFF_LIST, aliases=("BART",)),
    OversightLaw("Berkeley", "CA", "city", None,
                 "https://www.codepublishing.com/CA/Berkeley/html/Berkeley02/Berkeley0299/Berkeley0299.html", EFF_LIST),
    OversightLaw("Boston", "MA", "city", None, "https://www.boston.gov/sites/default/files/file/2020/10/0684.PDF",
                 "https://www.aclum.org/press-releases/boston-passes-law-requiring-community-control-police-surveillance/"),
    OversightLaw("Cambridge", "MA", "city", 2018,
                 "https://library.municode.com/ma/cambridge/codes/code_of_ordinances?nodeId=TIT2ADPE_CH2.128SUTEOR",
                 "https://www.aclum.org/press-releases/cambridge-passes-law-requiring-community-control-police-surveillance/"),
    OversightLaw("Davis", "CA", "city", None, "https://ecode360.com/44639712", EFF_LIST),
    OversightLaw("Grand Rapids", "MI", "city", None,
                 "https://www.grandrapidsmi.gov/Government/Policies/Administrative-Policies", EFF_LIST,
                 note="An administrative policy, not an ordinance."),
    OversightLaw("Lawrence", "MA", "city", None,
                 "https://library.municode.com/ma/lawrence/codes/code_of_ordinances?nodeId=TIT9PUPEWE_CH9.25SUTE", EFF_LIST),
    OversightLaw("Madison", "WI", "city", None,
                 "https://library.municode.com/wi/madison/codes/code_of_ordinances?nodeId=COORMAWIVOIICH20--31_CH23OFAGPUPO_23.63USSUTE",
                 EFF_LIST),
    OversightLaw("Nashville", "TN", "city", None,
                 "https://library.municode.com/tn/metro_government_of_nashville_and_davidson_county/codes/code_of_ordinances?nodeId=CD_TIT13STSIPUPL_DIVIGERE_CH13.08STALSI_13.08.080DESUELDAGADEONPURI-WREMECOAP",
                 EFF_LIST, aliases=("Metropolitan Nashville", "Metro Nashville")),
    OversightLaw("New York City", "NY", "city", 2020,
                 "https://codelibrary.amlegal.com/codes/newyorkcity/latest/NYCadmin/0-0-0-124303",
                 "https://www.aclu.org/community-control-over-police-surveillance",
                 note="The POST Act: disclosure and use policies; the council does not vote on each tool. Expanded in 2025.",
                 aliases=("New York City", "NYPD", "New York Police Department")),
    OversightLaw("Northampton", "MA", "city", None, "https://ecode360.com/32934785", EFF_LIST),
    OversightLaw("Oakland", "CA", "city", 2018,
                 "https://library.municode.com/ca/oakland/codes/code_of_ordinances?nodeId=TIT9PUPEMOWE_CH9.64REACUSSUTE",
                 "https://www.eff.org/deeplinks/2018/05/oakland-new-gold-standard-community-control-police-surveillance"),
    OversightLaw("Palo Alto", "CA", "city", None,
                 "https://codelibrary.amlegal.com/codes/paloalto/latest/paloalto_ca/0-0-0-55385", EFF_LIST),
    OversightLaw("San Francisco", "CA", "city", 2019,
                 "https://codelibrary.amlegal.com/codes/san_francisco/latest/sf_admin/0-0-0-47320",
                 "https://www.aclu.org/community-control-over-police-surveillance",
                 note="Also banned city use of face recognition."),
    OversightLaw("Santa Clara County", "CA", "county", None,
                 "https://library.municode.com/ca/santa_clara_county/codes/code_of_ordinances?nodeId=TITAGEAD_DIVA40SUECCOAF",
                 EFF_LIST),
    OversightLaw("Seattle", "WA", "city", None,
                 "https://library.municode.com/wa/seattle/codes/municipal_code?nodeId=TIT14HURI_CH14.18ACUSSUTE", EFF_LIST),
    OversightLaw("Somerville", "MA", "city", None,
                 "https://library.municode.com/ma/somerville/codes/code_of_ordinances?nodeId=PTIICOOR_CH10PO_ARTIIIPUOVSUTE",
                 EFF_LIST),
    OversightLaw("Yellow Springs", "OH", "city", None,
                 "https://codelibrary.amlegal.com/codes/yellowsprings/latest/yellowsprings_oh/0-0-0-23917", EFF_LIST),
    # Adopted after EFF's list, each with its own source.
    OversightLaw("Detroit", "MI", "city", 2021, "https://detroitmi.gov/sites/detroitmi.localhost/files/2022-10/CIOGS Final.pdf",
                 "https://www.bridgedetroit.com/detroiters-get-oversight-of-surveillance-technology-but-is-it-enough/",
                 note="Hearings and annual reports; advocates say police carve-outs weaken it."),
    OversightLaw("San Diego", "CA", "city", 2022, "https://docs.sandiego.gov/council_reso_ordinance/rao2024/O-21762.pdf",
                 "https://timesofsandiego.com/tech/2022/04/05/san-diego-city-council-creates-privacy-board-to-oversee-surveillance-technologies/",
                 note="TRUST ordinance and a Privacy Advisory Board; technologies are re-approved yearly."),
    OversightLaw("Medford", "MA", "city", 2023,
                 "https://www.aclum.org/press-releases/medford-passes-law-requiring-community-control-public-surveillance/",
                 "https://www.aclum.org/press-releases/medford-passes-law-requiring-community-control-public-surveillance/"),
    OversightLaw("Austin", "TX", "city", 2026, "https://services.austintexas.gov/edims/document.cfm?id=467215",
                 "https://www.spartnerships.com/austin-trust-act-surveillance-technology/",
                 note="TRUST Act (April 2026): impact assessments, use policies, review of tools already in use."),
    OversightLaw("Urbana", "IL", "city", 2026,
                 "https://www.newschannel20.com/news/local/urbana-approves-surveillance-technology-oversight-ordinance/article_8785a8b4-1d67-5cbd-b20d-c097106e26d9.html",
                 "https://www.newschannel20.com/news/local/urbana-approves-surveillance-technology-oversight-ordinance/article_8785a8b4-1d67-5cbd-b20d-c097106e26d9.html",
                 note="Council approval before new tools, including remotely accessible private cameras (August 2026)."),
)

ASK_FOR = ("the council-approved surveillance use policy and impact report for each technology, "
           "and the most recent annual surveillance report")


def _names(law: OversightLaw) -> Tuple[str, ...]:
    return (law.place,) + law.aliases


# A local law binds that government's own agencies -- not universities, school
# districts, federal or state offices, airports and ports, or regional fusion
# centers that merely carry the city's name.
_NOT_LOCAL = re.compile(r"(?i)\b(universit\w*|universty|college|schools?|state|regional|intelligence|airport|harbor|"
                        r"port|town|customs|federal|unified|coordination|u\.s\.)\b")


def law_for_agency(agency: Optional[str], state: Optional[str]) -> Optional[OversightLaw]:
    """The oversight law covering an agency, from its name and state.

    The name must begin with the place ('Oakland Police Department', 'City of
    Davis'), so 'East Palo Alto' and 'South San Francisco' are not covered. A
    city law does not cover the same-named county ('Oakland County Sheriff');
    a county law covers only county agencies."""
    name = re.sub(r"(?i)^(the\s+)?(city|county|town)\s+of\s+", "", (agency or "").strip())
    st = (state or "").upper()
    if not name or _NOT_LOCAL.search(name):
        return None
    for law in OVERSIGHT_LAWS:
        if law.state != st:
            continue
        for n in _names(law):
            m = re.match(rf"{re.escape(n)}\b", name, re.I)
            if not m:
                continue
            after = name[m.end():]
            if after.startswith("-") or (law.level == "city" and re.match(r"(?i)\s*county\b", after)):
                continue
            return law
    return None


def law_for_portal(city: Optional[str], county: Optional[str], agency_type: Optional[str],
                   state: Optional[str]) -> Optional[OversightLaw]:
    """A Flock portal's place ('Oakland' + 'PD', or a county sheriff) -> its oversight law."""
    if city:
        return law_for_agency(f"{city} {'Sheriff' if agency_type == 'SD' else 'Police Department'}", state)
    if county:
        c = county if county.lower().endswith("county") else f"{county} County"
        return law_for_agency(f"{c} Sheriff", state)
    return None


def records_ask(law: OversightLaw) -> str:
    """The records an oversight law obliges the government to have."""
    return (f"The surveillance use policy and surveillance impact report for the licence-plate-reader system "
            f"approved under {law.place}'s surveillance oversight ordinance, the record of that approval, and the "
            f"most recent annual surveillance report covering it.")


def _row(law: OversightLaw) -> Dict[str, Any]:
    d = asdict(law)
    d.pop("aliases", None)
    d["ask_for"] = ASK_FOR
    return d


def summary(law: Optional[OversightLaw]) -> Optional[Dict[str, Any]]:
    return _row(law) if law is not None else None


def laws() -> List[Dict[str, Any]]:
    return [_row(x) for x in sorted(OVERSIGHT_LAWS, key=lambda x: (x.state, x.place))]


__all__ = ["ASK_FOR", "CAMPAIGN_URL", "EFF_LIST", "OVERSIGHT_LAWS", "OversightLaw", "law_for_agency", "law_for_portal", "laws",
           "records_ask", "summary"]
