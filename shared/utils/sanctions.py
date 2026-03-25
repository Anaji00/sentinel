"""
shared/utils/sanctions.py
 
Single source of truth for sanctions keyword matching and MMSI→country mapping.
 
FIX (code review): Previously check_sanctions() and MMSI_COUNTRY were defined
  in enrichers/maritime.py AND entity_resolver.py — two copies that would
  inevitably drift. Both files now import from here.
 
Phase 2: replace keyword matching with full OFAC SDN list sync
  (daily download from https://sanctionslist.ofac.treas.gov/Home/SdnList)
  and fuzzy name matching.
"""

from typing import List

SANCTIONED_KEYWORDS = [
    # Iran
    "irgc", "iran", "quds", "nioc", "sepah", "national iranian oil", "naftiran", "iranian oil", "iranian petroleum", "iranian shipping", "iranian maritime", "irisl",
    # North Korea
    "ocean maritime management", "ocean maritime", "korea national insurance", "dprk",
    # Russia
    "novorossiysk", "sovcomflot", "gazprom", "rosneft", "lukoil", "transneft", "wagner", "rostec", "sberbank", "vtb", "uralvagonzavod",
    # Syria
    "syrian arab", "general organization for refining",
    # Venezuela
    "pdvsa", "petroleos de venezuela",
]

# MMSI MID (Maritime Identification Digit) prefix → ISO 3166-1 alpha-2
# Full table: https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx
MMSI_COUNTRY: dict = {
    "201":"AL","203":"AT","209":"CY","210":"CY","211":"DE","212":"CY",
    "213":"SM","215":"MT","218":"DE","219":"DK","220":"DK","224":"ES",
    "225":"ES","226":"FR","227":"FR","228":"FR","229":"MT","230":"FI",
    "231":"FO","232":"GB","233":"GB","234":"GB","235":"GB","236":"GI",
    "237":"GR","238":"HR","239":"GR","240":"GR","241":"GR","242":"MA",
    "244":"NL","245":"NL","246":"NL","247":"IT","248":"MT","249":"MT",
    "250":"IE","251":"IS","252":"LI","253":"LU","254":"MC","255":"PT",
    "256":"MT","257":"NO","258":"NO","259":"NO","261":"PL","263":"PT",
    "264":"RO","265":"SE","266":"SE","270":"CZ","271":"TR","272":"UA",
    "273":"RU","275":"LV","276":"EE","277":"LT","278":"SI","279":"RS",
    "301":"AG","303":"US","304":"AG","305":"AG","306":"CW","307":"AW",
    "308":"BS","309":"BS","310":"BM","311":"BS","312":"BZ","314":"BB",
    "316":"CA","319":"KY","321":"CR","323":"CU","325":"DM","327":"DO",
    "329":"GP","330":"GD","331":"GL","332":"GT","334":"HN","336":"HT",
    "338":"US","339":"JM","341":"KN","343":"LC","345":"MX","347":"MQ",
    "348":"MS","350":"NI","351":"PA","352":"PA","353":"PA","354":"PA",
    "355":"PA","356":"PA","357":"PA","358":"PR","359":"SV","361":"PM",
    "362":"TT","364":"TC","366":"US","367":"US","368":"US","369":"US",
    "370":"PA","371":"PA","372":"PA","373":"PA","374":"PA","375":"VC",
    "376":"VC","377":"VC","378":"VG","379":"VI","401":"AF","403":"SA",
    "405":"BD","408":"BH","410":"BT","412":"CN","413":"CN","414":"CN",
    "416":"TW","417":"LK","419":"IN","422":"IR","423":"AZ","425":"IQ",
    "428":"IL","431":"JP","432":"JP","434":"TM","436":"KZ","437":"UZ",
    "438":"JO","440":"KR","441":"KR","443":"PS","445":"KP","447":"KP",
    "450":"KW","451":"LB","453":"MO","455":"MV","457":"MN","459":"NP",
    "461":"OM","463":"PK","466":"QA","468":"SY","470":"AE","472":"TJ",
    "473":"YE","477":"HK","478":"BA","503":"AU","506":"MM","508":"BN",
    "510":"FM","511":"PW","512":"NZ","514":"KH","515":"KH","516":"CX",
    "518":"CK","520":"FJ","523":"CC","525":"ID","529":"KI","531":"LA",
    "533":"MY","536":"MP","538":"MH","540":"NC","542":"NZ","544":"NR",
    "546":"PF","548":"PH","553":"PG","555":"PN","557":"SB","559":"WS",
    "561":"SG","563":"SG","564":"SG","565":"SG","566":"SG","567":"TH",
    "570":"TO","572":"TV","574":"VN","576":"VU","578":"WF","601":"ZA",
    "603":"AO","605":"DZ","607":"TF","608":"SH","609":"BI","610":"BJ",
    "611":"BW","612":"CF","613":"CM","615":"CG","616":"CI","617":"KM",
    "618":"CV","619":"KP","620":"DJ","621":"EG","622":"ER","624":"ET",
    "625":"GA","626":"GH","627":"GM","629":"GW","630":"GQ","631":"GN",
    "632":"BF","633":"KE","634":"SS","635":"LR","636":"LR","637":"LR",
    "638":"LS","642":"LY","644":"MU","645":"MG","647":"ML","649":"MR",
    "650":"MW","654":"MZ","655":"NA","656":"NE","657":"NG","659":"RE",
    "660":"RW","661":"SD","662":"SN","663":"SL","664":"SO","665":"ST",
    "666":"SZ","667":"TD","668":"TG","669":"TN","670":"TZ","671":"UG",
    "672":"ZM","674":"ZW","675":"ZW",
}

def check_sanctions(name: str, mmsi: str = "") -> List[str]:
    """
    Return list of flag strings for a vessel.
    Checks name against known sanctioned keywords and MMSI prefix
    against sanctioned flag states.
 
    Returns empty list if no flags.
    """
    flags = []
    name_lower = (name or "").lower()

    for kw in SANCTIONED_KEYWORDS:
        if kw in name_lower:
            flags.append("sanctioned_ofac")
            break  # No need to check other keywords if we already found one

    prefix = (mmsi or "")[:3]
    # High risk: Iran (422), DPRK (442, 445, 447, 619), Russia (273), Syria (468), Cuba (323), Venezuela (775)
    if prefix in ("422", "442", "445", "447", "619", "273", "468", "323", "775"):
        flags.append("sanctions_adjacent_flag_state")

    return flags

def mmsi_to_country(mmsi: str) -> str:
    """Return ISO 3166-1 alpha-2 country code for an MMSI, or empty string."""
    return MMSI_COUNTRY.get((mmsi or "")[:3], "")
