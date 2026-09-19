"""Cutting the cyber domain, and narrowing aviation to what is acted on.

Two deliberate reductions, and the tests that keep them honest in both
directions -- that the cut happened, and that what was kept still works.

Cyber was withdrawn because it does not join to the other domains, which is
what this platform is for: an AS number resolves to no ticker, no vessel and no
region, and the one attempt to bridge it through the RIR registrant put law
firms and a university at the head of live cross-domain correlations. Its
scoring was degenerate as well -- all 219 BGP events in a 45-minute window
scored exactly 0.850.

Aviation published every state vector in three macro sweep boxes: 90,108 events
over two days, 36,178 of them with no squawk, which the gap detector turned
into 10,370 `flight_dark` events in 48 hours with subjects it could not name.
It now publishes emergency squawks anywhere in the sweep, and everything inside
five chokepoint and conflict boxes.

Neither is a deletion. `make cyber` still starts the collector, the events
table keeps 90 days of what it wrote, `cyber_mapper` still holds the
vendor-to-ticker map that is the one cyber signal worth rebuilding, and the
retired watch zones keep their coordinates.
"""
import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _adsb():
    spec = importlib.util.spec_from_file_location(
        "adsb_collector", ROOT / "services" / "collector-adsb" / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# -- the cyber rules are withdrawn, and the withdrawal reaches a deployment --


def test_neither_cyber_rule_ships_any_more():
    from services.correlation.main import RETIRED_RULE_IDS, SHIPPED_RULES

    shipped = {r["rule_id"] for r in SHIPPED_RULES}
    for rule_id in ("rule_cyber_aviation_chokepoint", "rule_cyber_market_impact"):
        assert rule_id not in shipped
        assert rule_id in RETIRED_RULE_IDS, (
            "removing a rule from SHIPPED_RULES is not enough -- reconciliation "
            "has never deleted anything, so a deployment that already has it "
            "goes on evaluating it against a feed that no longer exists"
        )
        assert RETIRED_RULE_IDS[rule_id], "a retirement needs a stated reason"


class _Raw:
    def __init__(self, hash_contents=None):
        self.hash = dict(hash_contents or {})
        self.deleted = []
        self.published = []

    async def hdel(self, key, field):
        self.deleted.append(field)
        self.hash.pop(field, None)

    async def hset(self, key, field, value):
        self.hash[field] = value

    async def publish(self, channel, payload):
        self.published.append((channel, json.loads(payload)))


class _Redis:
    def __init__(self, hash_contents=None):
        self.raw = _Raw(hash_contents)


async def test_reconciliation_withdraws_a_retired_rule_from_a_live_deployment():
    """The tombstone clears the caches; the hdel stops it coming back.

    The listener's `deprecated` branch removes a rule from the in-memory cache
    and does not touch the hash, so a tombstone alone would let the rule return
    on the next restart.
    """
    from services.correlation import main as corr

    corr._dynamic_rules_cache.clear()
    corr._dynamic_rules_cache["rule_cyber_market_impact"] = {
        "rule_id": "rule_cyber_market_impact", "definition_version": 4,
    }
    redis = _Redis({"rule_cyber_market_impact": "{}"})

    await corr._reconcile_shipped_rules(redis)

    assert "rule_cyber_market_impact" in redis.raw.deleted
    assert "rule_cyber_market_impact" not in corr._dynamic_rules_cache
    channel, tombstone = redis.raw.published[0]
    assert channel == "sentinel:correlation:rule_updates"
    assert tombstone == {"rule_id": "rule_cyber_market_impact", "deprecated": True}


def test_the_cold_start_path_does_not_reinstate_a_retired_rule():
    """Two paths write the shipped set, and both have to know about retirement."""
    source = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert 'default_rules = [r for r in SHIPPED_RULES if r["rule_id"] not in RETIRED_RULE_IDS]' in source


def test_the_definition_version_moved():
    """Reconciliation only updates a stored rule when the shipped version is
    higher, so a change that does not bump it reaches nothing."""
    from services.correlation.main import RULE_DEFINITION_VERSION

    assert RULE_DEFINITION_VERSION >= 5


# -- retired, not deleted ----------------------------------------------------


def test_the_collector_is_out_of_the_default_profile_and_still_startable():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    profiles = compose["services"]["collector-cyber"].get("profiles") or []
    assert "collectors" not in profiles, "still starts with `make analyst`"
    assert profiles == ["cyber"]
    # test_compose_startup_invariants requires every profile to have a target;
    # this asserts the one that makes the retirement reversible.
    assert "--profile cyber up" in (ROOT / "Makefile").read_text(encoding="utf-8")


def test_the_one_cyber_signal_worth_keeping_is_kept():
    """A known-exploited vulnerability in a listed company's product is the
    only cyber signal that reaches markets, and the map for it already exists."""
    from shared.utils.cyber_mapper import map_cve_to_equity

    ticker, basis = map_cve_to_equity("CVE-2026-0001", vendor_name="Microsoft")
    assert ticker == "MSFT"
    assert basis == "DIRECT_VENDOR_VULNERABILITY"
    assert map_cve_to_equity("CVE-2026-0002", vendor_name="a private co")[0] is None


def test_the_cyber_panel_does_not_claim_to_be_live():
    """The moment the collector left the default profile, "REAL-TIME" became a
    false claim in the one place a viewer looks to decide whether a threat is
    current."""
    import re

    raw = (
        ROOT / "frontend" / "src" / "components" / "CyberIntelligencePanel.tsx"
    ).read_text(encoding="utf-8")
    # Comments stripped: the file quotes the old badge text in the note that
    # records why it is gone, and a claim in a comment is not a claim to a user.
    panel = re.sub(r"/\*.*?\*/", "", raw, flags=re.S)
    panel = re.sub(r"^\s*//.*$", "", panel, flags=re.M)
    assert "CISA & BGP REAL-TIME" not in panel
    assert 'variant="live"' not in panel
    assert "RETIRED FEED" in panel
    assert "refreshInterval: 5000" not in raw, "polling an archive every 5s"


def test_the_history_stays_readable():
    """The column and the read path are kept: 90 days of cyber events exist and
    deleting the way to read them would be deleting the reason to keep them."""
    events_route = (
        ROOT / "services" / "api_gateway" / "routes" / "events.py"
    ).read_text(encoding="utf-8")
    assert '"cyber": "security_data"' in events_route
    assert "WHEN security_data IS NOT NULL THEN 'cyber'" in events_route
    # NEWS_PREDICATE defines news as the absence of every payload column. It and
    # the domain CASE must agree, or /events/news and /events/all disagree about
    # what a news row is.
    assert "security_data IS NULL" in events_route


# -- aviation publishes what is acted on -------------------------------------


def test_the_watch_zones_now_decide_something():
    """Twelve boxes declared "for precise event tagging", used to print their
    own count in a startup log and for nothing else."""
    adsb = _adsb()
    assert adsb.zone_for(25.5, 58.0) == "Strait of Hormuz"
    assert adsb.zone_for(24.0, 121.0) == "Taiwan Strait"
    assert adsb.zone_for(24.7, 46.7) is None, "Riyadh is inside the macro sweep only"
    assert adsb.zone_for(None, None) is None
    assert len(adsb.WATCH_ZONES) == 5
    assert len(adsb.RETIRED_ZONES) == 7, "the coordinates are kept, not deleted"


class _Resp:
    def __init__(self, payload):
        self.status = 200
        self.headers = {"X-Rate-Limit-Remaining": "100"}
        self._payload = payload

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, payload):
        self._payload = payload

    def get(self, *a, **k):
        return _Resp(self._payload)


class _Auth:
    async def get_token(self, session):
        return None


class _Producer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, data, key):
        self.sent.append(data)


def _state(icao24, lat, lon, squawk=None):
    """A 17-field OpenSky state vector."""
    v = [None] * 17
    v[0], v[1], v[2] = icao24, "TEST123 ", "Testland"
    v[5], v[6] = lon, lat
    v[8] = False
    v[14] = squawk
    return v


async def test_an_ordinary_aircraft_outside_the_watch_zones_is_not_published():
    """Every state vector used to be sent; is_emergency changed only a log line."""
    adsb = _adsb()
    producer = _Producer()
    await adsb.poll_zone(
        _Session({"states": [_state("abc123", 24.7, 46.7, "1000")], "time": 0}),
        _Auth(), producer, "Middle East & Red Sea", 11.5, 47.0, 32.0, 63.0,
    )
    assert producer.sent == []


async def test_an_emergency_squawk_is_published_from_anywhere_in_the_sweep():
    adsb = _adsb()
    producer = _Producer()
    await adsb.poll_zone(
        _Session({"states": [_state("abc123", 24.7, 46.7, "7700")], "time": 0}),
        _Auth(), producer, "Middle East & Red Sea", 11.5, 47.0, 32.0, 63.0,
    )
    assert len(producer.sent) == 1
    payload = producer.sent[0]["raw_payload"]
    assert payload["is_emergency"] is True
    assert payload["emergency_type"] == "General Emergency"
    # Outside every watch zone, so the macro region is the honest tag.
    assert payload["zone_name"] == "Middle East & Red Sea"


async def test_traffic_inside_a_watch_zone_is_published_and_tagged_precisely():
    """`flight_dark` needs the continuous stream, and this is where it is kept."""
    adsb = _adsb()
    producer = _Producer()
    await adsb.poll_zone(
        _Session({"states": [_state("abc123", 25.5, 58.0, "1000")], "time": 0}),
        _Auth(), producer, "Middle East & Red Sea", 11.5, 47.0, 32.0, 63.0,
    )
    assert len(producer.sent) == 1
    payload = producer.sent[0]["raw_payload"]
    assert payload["zone_name"] == "Strait of Hormuz"
    assert payload["macro_region"] == "Middle East & Red Sea"


async def test_a_quiet_sweep_reports_itself_filtered_rather_than_silent():
    """The freshness monitor calls a source stale after six hours of silence
    whatever its measured cadence, and this feed can now be quiet all night.

    A monitor that cries wolf is how a real outage gets ignored, so a sweep
    that fetched aircraft and published none says which of the two it was.
    """
    adsb = _adsb()
    marked = []

    async def _mark(redis_client, source):
        marked.append(source)

    adsb.mark_source_filtered = _mark
    producer = _Producer()
    await adsb.poll_zone(
        _Session({"states": [_state("abc123", 24.7, 46.7, "1000")], "time": 0}),
        _Auth(), producer, "Middle East & Red Sea", 11.5, 47.0, 32.0, 63.0,
        redis_client=object(),
    )
    assert producer.sent == []
    assert marked == ["opensky"]


async def test_a_sweep_that_published_something_is_not_marked_filtered():
    adsb = _adsb()
    marked = []

    async def _mark(redis_client, source):
        marked.append(source)

    adsb.mark_source_filtered = _mark
    producer = _Producer()
    await adsb.poll_zone(
        _Session({"states": [_state("abc123", 25.5, 58.0, "1000")], "time": 0}),
        _Auth(), producer, "Middle East & Red Sea", 11.5, 47.0, 32.0, 63.0,
        redis_client=object(),
    )
    assert len(producer.sent) == 1
    assert marked == []


# -- and the price of both cuts is written down ------------------------------


def test_the_retired_situations_still_assert_something():
    """Four real market situations the platform could answer and now cannot.

    Kept as scenarios expecting silence rather than deleted, so the cost is
    visible and the acceptance criteria survive: if the cyber feeds come back,
    changing `expect_rule` back is how you check the capability did too.
    """
    from tests.integration.scenarios import (
        CYBER_MARKET_IMPACT,
        GPS_INTERFERENCE,
        KEV_EXPLOITATION,
        PORT_RANSOMWARE,
    )

    for scenario in (PORT_RANSOMWARE, GPS_INTERFERENCE, CYBER_MARKET_IMPACT, KEV_EXPLOITATION):
        assert scenario.expect_rule == "", f"{scenario.name} still expects a rule"
        assert scenario.why, "the situation itself is still described"


def test_no_start_target_brings_up_a_retired_collector_alongside_the_others():
    """A retirement has to survive the next person who types "start everything".

    `test_the_collector_is_out_of_the_default_profile_and_still_startable`
    above checks the compose side: collector-cyber carries `profiles: [cyber]`
    and `make cyber` can still raise it. Nothing checked the Makefile side --
    that no *other* start target pulls that profile in alongside the rest.

    It matters because the profile name is the only thing standing between a
    retired collector and a running one, and the obvious way to redeploy after
    a code change is to name every profile you can see:

        COMPOSE_PROFILES=collectors,agents,cyber,obs docker compose up -d

    which is how this deployment came to be ingesting a retired domain again on
    16 September -- 200 ransomware events in 45 minutes, into a domain whose
    two rules are withdrawn and whose scoring the retirement commit measured as
    degenerate. Nothing failed. The collector came up healthy and stayed
    healthy, because a retired collector and a live one are the same program.

    `make budget` already encodes the right answer -- its four operating modes
    are {collectors}, {agents}, {collectors,obs}, {agents,obs}, and cyber is in
    none of them. This ties that arrangement to RETIRED_DOMAINS so the two
    cannot drift apart, and so the next retirement inherits the check instead
    of having to remember it.
    """
    from shared.models.events import RETIRED_DOMAINS

    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

    assert RETIRED_DOMAINS, "nothing is retired; this check has no subject"

    for domain in sorted(RETIRED_DOMAINS):
        service = f"collector-{domain}"
        if service not in compose["services"]:
            continue
        profiles = compose["services"][service].get("profiles") or []
        assert profiles, f"{service} is retired but starts in the default profile"

        # Every line that raises containers, and the profiles it names.
        for line in makefile.splitlines():
            if "up -d" not in line or "--profile" not in line:
                continue
            named = set(re.findall(r"--profile\s+([a-z-]+)", line))
            overlap = named.intersection(profiles)
            if not overlap:
                continue
            # The one deliberate escape hatch: a target named for the retired
            # domain itself, which is what makes the retirement reversible.
            assert named == overlap, (
                f"{line.strip()!r} starts the retired {service} alongside "
                f"{sorted(named - overlap)}. A retired collector may only be "
                f"raised on its own, by `make {domain}`."
            )
