"""
shared/utils/privacy_plan.py

A personal privacy plan: a few answers in, a ranked list of free steps out.
Pure -- no I/O, nothing stored. The API passes in what the platform knows (the
brokers that declared selling to police, whether the person's state has a
plate-reader law) and returns the plan in the response only.

Every step is something a person can do today without paying anyone. Steps
are general guidance, not legal advice; where a step depends on a law, the
law is named.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

CONCERNS = ("immigration", "reproductive", "activism_press", "domestic_violence")


@dataclass
class Answers:
    state: Optional[str] = None
    has_car: bool = False
    phone: Optional[str] = None                   # "android" | "iphone" | None
    has_doorbell_camera: bool = False
    concerns: Sequence[str] = ()


@dataclass
class Step:
    id: str
    title: str
    why: str
    how: str
    links: List[Dict[str, str]] = field(default_factory=list)
    weight: int = 1                               # higher first

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


def parse_answers(body: Mapping[str, Any]) -> Answers:
    """Validated answers from a request body; unknown values are dropped."""
    st = str(body.get("state") or "").upper()[:2] or None
    phone = str(body.get("phone") or "").lower()
    concerns = [c for c in (body.get("concerns") or []) if c in CONCERNS] if isinstance(body.get("concerns"), list) else []
    return Answers(state=st if st and st.isalpha() else None, has_car=bool(body.get("has_car")),
                   phone=phone if phone in ("android", "iphone") else None,
                   has_doorbell_camera=bool(body.get("has_doorbell_camera")), concerns=concerns)


def build_plan(a: Answers, police_brokers: Sequence[Mapping[str, Any]] = (),
               state_has_alpr_law: Optional[bool] = None) -> List[Dict[str, Any]]:
    """Steps for these answers, most protective first. `police_brokers`: brokers
    that declared selling data to law enforcement, each with name and privacy_url."""
    steps: List[Step] = []
    c = set(a.concerns)

    if a.state == "CA":
        steps.append(Step("ca_drop", "Delete yourself from every registered data broker at once",
                          "California's Delete Act lets one free request reach all 500+ registered brokers, who must "
                          "process it and keep you deleted.",
                          "Use the state's DROP portal (Delete Request and Opt-Out Platform).",
                          [{"label": "California DROP", "url": "https://cppa.ca.gov/data_broker_registry/"}], 10))
    if police_brokers:
        steps.append(Step("police_brokers", "Opt out of the brokers that sell to police",
                          f"{len(police_brokers)} brokers told California's registry they sold personal data to law "
                          "enforcement in the past year without a subpoena or court order.",
                          "Use each broker's own opt-out page below; most take a few minutes.",
                          [{"label": b["name"], "url": b["privacy_url"]} for b in police_brokers if b.get("privacy_url")],
                          9 if c & {"immigration", "activism_press", "reproductive"} else 7))
    steps.append(Step("people_search", "Remove yourself from people-search sites",
                      "People-search sites publish home addresses, relatives and phone numbers to anyone who pays.",
                      "Start with the sites the Big Ass Data Broker Opt-Out List marks as crucial.",
                      [{"label": "Big Ass Data Broker Opt-Out List",
                        "url": "https://github.com/yaelwrites/Big-Ass-Data-Broker-Opt-Out-List"}],
                      9 if "domestic_violence" in c else 6))
    if a.phone == "android":
        how = "Settings > Privacy (or Google) > Ads > Delete advertising ID. Then Settings > Location > App location " \
              "permissions: set each app to 'Allow only while using' or 'Don't allow', and turn off precise location " \
              "where it is not needed."
    elif a.phone == "iphone":
        how = "Settings > Privacy & Security > Tracking: turn off 'Allow Apps to Request to Track'. Then Location " \
              "Services: set each app to 'While Using' or 'Never', and turn off Precise Location where it is not needed."
    else:
        how = "Delete or reset your phone's advertising ID, and give location only to apps that need it, only while in use."
    steps.append(Step("phone_location", "Cut off the location trail your apps sell",
                      "Apps sell location tied to your advertising ID; brokers resell it, and police have bought it "
                      "(the FTC has banned several brokers from selling sensitive locations).",
                      how, [], 8 if c & {"immigration", "reproductive"} else 6))
    if a.has_car:
        steps.append(Step("car", "Check what your car reports",
                          "Connected cars can share trips and driving behaviour with data brokers and insurers; licence-"
                          "plate readers log the car itself wherever it goes.",
                          "In the car's app or the maker's privacy portal, turn off data sharing and 'driving behaviour' "
                          "programs. Request your free consumer disclosure from LexisNexis and Verisk to see what was shared. "
                          "See whether police searched your plate on Have I Been Flocked (a hit means a search, not a sighting).",
                          [{"label": "Have I Been Flocked", "url": "https://haveibeenflocked.com"},
                           {"label": "LexisNexis consumer disclosure", "url": "https://consumer.risk.lexisnexis.com/"}],
                          6))
    if a.has_doorbell_camera:
        steps.append(Step("doorbell", "Decide how your doorbell camera talks to police",
                          "Sharing a clip through Ring's Community Requests also hands the investigator your home address "
                          "and account email.",
                          "In the camera app's control centre, turn off community or police requests if you do not want "
                          "them; share footage only when you choose, and know it can also be demanded by warrant.",
                          [], 5))
    if state_has_alpr_law is not None:
        steps.append(Step("local_records", "Ask your police department what it runs",
                          ("Your state has a licence-plate-reader law; " if state_has_alpr_law else
                           "Your state has no licence-plate-reader statute; ")
                          + "your agency's contract, retention setting and sharing list are public records.",
                          "Open your agency on the Surveillance Watch map and use 'Draft a public-records request', then "
                          "send it yourself or through MuckRock.",
                          [{"label": "MuckRock", "url": "https://www.muckrock.com"}], 4))
    if "immigration" in c:
        steps.append(Step("immigration", "Limit what immigration enforcement can buy or match",
                          "ICE buys commercial data and has run plate-reader searches through local agencies; twelve "
                          "registered brokers say they collect citizenship or immigration status.",
                          "Prioritise the broker opt-outs above, keep location off on your phone, and know your rights "
                          "before any encounter; the Immigrant Legal Resource Center publishes plain-language guides.",
                          [{"label": "ILRC know-your-rights", "url": "https://www.ilrc.org/red-cards"}],
                          9))
    if "reproductive" in c:
        steps.append(Step("reproductive", "Protect reproductive-health searches and trips",
                          "Location data near clinics has been sold, and eight registered brokers say they collect "
                          "reproductive-health data.",
                          "Turn off location for period-tracking and maps apps, prefer apps that store data on the phone, "
                          "and use a private browser for searches; EFF keeps an up-to-date guide.",
                          [{"label": "EFF Surveillance Self-Defense", "url": "https://ssd.eff.org"}], 9))
    if "activism_press" in c:
        steps.append(Step("activism", "Protect organising and sources",
                          "Protest locations and contacts are what plate readers, phone location and cell-site simulators "
                          "reveal.",
                          "Use Signal with disappearing messages, leave the phone home or in airplane mode at actions where "
                          "you can, and check for fake cell towers with EFF's Rayhunter.",
                          [{"label": "EFF Surveillance Self-Defense", "url": "https://ssd.eff.org"},
                           {"label": "Rayhunter", "url": "https://github.com/EFForg/rayhunter"}], 8))
    if "domestic_violence" in c:
        steps.append(Step("dv", "Keep your address out of reach",
                          "People-search sites and public records are how an abuser finds a new address.",
                          "Ask your state's address-confidentiality program (often called Safe at Home) whether you "
                          "qualify, check your phone and car for shared location and trackers, and remove yourself from "
                          "people-search sites first.",
                          [], 10))
    steps.append(Step("credit_freeze", "Freeze your credit",
                      "A free freeze at the three bureaus stops new accounts in your name, and the bureaus are brokers too.",
                      "Freeze at Equifax, Experian and TransUnion; it is free by federal law and can be lifted any time.",
                      [], 3))
    steps.sort(key=lambda s: -s.weight)
    return [s.to_dict() for s in steps]


__all__ = ["Answers", "CONCERNS", "Step", "build_plan", "parse_answers"]
