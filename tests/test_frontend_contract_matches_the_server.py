"""Every field the browser declares, against everything the server can send.

The two lists nobody keeps together. On one side, `frontend/src/lib/types.ts`
-- the contract the whole UI is typed against. On the other, the Pydantic
models that define the JSONB payloads, the columns of the tables the routes
`SELECT *` from, and the keys the routes add themselves.

A name on one side and not the other has two very different outcomes, and they
are indistinguishable from either end of the wire:

  * a dead type declaration nothing renders, which costs nothing until someone
    trusts it; and
  * a count, a badge or a detail row a person has been reading all along, which
    has been showing them zero, "N/A" or blank for as long as it has existed.

`CyberIntelligencePanel` counted CISA known-exploited vulnerabilities with
`filter(e => e.security_data?.cisa_kev).length` against a `SecurityData` that
had no such field. The scenario feed rendered `s.narrative || s.description`
against a table whose column is `narrative_summary`. Both are the second kind.
Twenty-four other names were the first.

This test is that comparison, run every build. Exemptions are allowed and must
prove themselves: a `SERVER_EXTRAS` entry names the file that produces the
field and fails if that file stops producing it, and a stale entry -- one whose
field has since been added to the model, or removed from the interface -- fails
too, so the allowlist cannot quietly become the contract.
"""
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TYPES_TS = ROOT / "frontend" / "src" / "lib" / "types.ts"
INIT_SQL = ROOT / "shared" / "db" / "init.sql"
MIGRATIONS = ROOT / "shared" / "db" / "migrate.py"
EVENTS_ROUTE = ROOT / "services" / "api_gateway" / "routes" / "events.py"
SCENARIOS_ROUTE = ROOT / "services" / "api_gateway" / "routes" / "scenarios.py"
SESSION_ROUTE = ROOT / "services" / "api_gateway" / "routes" / "auth.py"


# -- the two lists ----------------------------------------------------------


def _ts_interfaces() -> dict:
    """Field names per exported interface, comments stripped.

    Indentation-independent. This matched ``^\\s{4}`` -- exactly four leading
    spaces -- and the project's Prettier config produces two, so running the
    repository's own formatter over `types.ts` emptied every interface. The
    gate above then found nothing unreachable and passed, while the two
    allowlist tests failed with "no longer declared" against fields that were
    still right there. A contract check that depends on how many spaces precede
    a field is checking the formatter, not the contract.

    Brace depth is what actually separates a field of this interface from a
    field of an object nested inside it, so that is what is tracked.
    """
    text = TYPES_TS.read_text(encoding="utf-8")
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"^\s*//.*$", "", text, flags=re.M)
    out = {}
    for match in re.finditer(r"export interface (\w+)\s*\{(.*?)\n\}", text, flags=re.S):
        fields, depth = [], 0
        for line in match.group(2).split("\n"):
            stripped = line.strip()
            if depth == 0:
                field = re.match(r"(\w+)\??\s*:", stripped)
                if field:
                    fields.append(field.group(1))
            depth += line.count("{") - line.count("}")
        out[match.group(1)] = fields
    # An empty parse is a broken parser, not an empty contract: every check
    # built on this one passes trivially when it returns nothing.
    assert out, "no exported interfaces parsed out of types.ts"
    assert all(out.values()), (
        "these interfaces parsed with no fields: "
        + ", ".join(sorted(k for k, v in out.items() if not v))
    )
    return out


_CONSTRAINTS = ("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CONSTRAINT", "CHECK")


def _table_columns(table: str) -> set:
    """A table's columns, including the ones added by later ALTERs.

    The scenarios route answers with `SELECT *`, so the table -- not the
    Pydantic model -- is what the client actually receives.
    `narrative_summary` is a column and not a model field;
    `primary_entity_name` was a model field and not a column, which is why
    every scenario in the product read "Multi-Entity".

    Both files, because the schema lives in both. `init.sql` runs once, from
    docker-entrypoint-initdb.d, on a database's first start -- a column added
    there reaches no deployment that already exists -- and every additive
    change since has gone through the versioned migrations instead.
    """
    sql = INIT_SQL.read_text(encoding="utf-8") + chr(10) + MIGRATIONS.read_text(encoding="utf-8")
    sql = re.sub(r"--.*$", "", sql, flags=re.M)
    block = re.search(
        r"CREATE TABLE IF NOT EXISTS " + table + r"\s*\((.*?)\n\);", sql, flags=re.S
    )
    assert block, "no CREATE TABLE for " + table + " in init.sql"
    columns = set()
    depth = 0
    for line in block.group(1).splitlines():
        stripped = line.strip()
        if stripped and depth == 0 and not stripped.upper().startswith(_CONSTRAINTS):
            name = re.match(r"(\w+)\s", stripped)
            if name:
                columns.add(name.group(1))
        depth += line.count("(") - line.count(")")
    columns.update(
        re.findall(
            r"ALTER TABLE " + table + r" ADD COLUMN IF NOT EXISTS (\w+)", sql
        )
    )
    return columns


def _server_fields() -> dict:
    """What the server can put in each shape, by the interface's own name."""
    import shared.models.events as ev
    from shared.utils.corroboration import CorroborationAssessment
    from services.agents.consensus_engine import EvidenceContributor

    fields = {
        name: set(getattr(ev, name).model_fields)
        for name in (
            "ScenarioHypothesis", "Entity", "MarketMicrostructure", "FinancialData",
            "ScoreAdjustment", "CryptoData", "VesselData", "FlightData",
            "SecurityData", "PredictionMarketData", "NormalizedEvent", "Scenario",
        )
    }
    fields["Corroboration"] = set(CorroborationAssessment.__dataclass_fields__)
    fields["EvidenceItem"] = set(EvidenceContributor.model_fields)
    # /scenarios answers with the row, so the table is part of the contract.
    fields["Scenario"] |= _table_columns("scenarios")
    return fields


# Fields the server really sends that no model declares, each with the file
# that produces it. If that file stops producing it, this fails.
SERVER_EXTRAS = {
    ("NormalizedEvent", "primary_entity_id"): (EVENTS_ROUTE, "primary_entity_id"),
    ("NormalizedEvent", "primary_entity_name"): (EVENTS_ROUTE, "primary_entity_name"),
    ("NormalizedEvent", "entity_name"): (EVENTS_ROUTE, "primary_entity_name as entity_name"),
    ("NormalizedEvent", "domain"): (EVENTS_ROUTE, "END AS domain"),
    ("NormalizedEvent", "domain_data"): (EVENTS_ROUTE, "AS domain_data"),
    ("NormalizedEvent", "raw_payload"): (EVENTS_ROUTE, "SELECT * FROM events WHERE event_id = $1"),
    ("EvidenceItem", "score"): (SCENARIOS_ROUTE, '"score": ass.get("conviction"'),
    ("SessionIdentity", "email"): (SESSION_ROUTE, "email"),
    ("SessionIdentity", "role"): (SESSION_ROUTE, "role"),
}

# Fields the browser sets on rows it built itself. Never sent by anything.
CLIENT_ONLY = {
    ("NormalizedEvent", "data_provenance"):
        "set by the client on rows it fetched directly, because the backend "
        "returned nothing for that domain; its absence is the signal that the "
        "row came through the platform's own pipeline",
}

# Interfaces with no single server-side shape to compare against, and why.
NO_SERVER_SHAPE = {
    "SessionIdentity": "the session route builds it field by field; both of "
                       "its fields are covered by SERVER_EXTRAS",
}


def test_every_declared_field_can_actually_arrive():
    """The gate. A field the client declares must be one the server can send."""
    interfaces = _ts_interfaces()
    server = _server_fields()
    unreachable = []
    for name, declared in sorted(interfaces.items()):
        known = server.get(name)
        assert known is not None or name in NO_SERVER_SHAPE, (
            name + " is declared in types.ts and has no counterpart on the "
            "server side of this test. Add one, or record here why it has none."
        )
        for field in declared:
            if field in (known or set()):
                continue
            if (name, field) in SERVER_EXTRAS or (name, field) in CLIENT_ONLY:
                continue
            unreachable.append(name + "." + field)
    assert not unreachable, (
        "the frontend declares fields the server cannot send: "
        + ", ".join(unreachable)
        + ". Either carry the value -- the collector or the enricher usually "
        "has it already -- or drop the declaration. A field that cannot arrive "
        "reads as zero, 'N/A' or blank to whoever is looking at it."
    )


@pytest.mark.parametrize("key", sorted(SERVER_EXTRAS))
def test_each_exemption_still_has_its_producer(key):
    """An allowlist that cannot rot. The named file must still emit the field."""
    path, needle = SERVER_EXTRAS[key]
    assert path.exists(), ".".join(key) + ": " + str(path) + " is gone"
    assert needle in path.read_text(encoding="utf-8"), (
        ".".join(key) + " is exempted because " + path.name + " produces it, "
        "and " + path.name + " no longer contains " + repr(needle) + ". Either "
        "the field stopped being sent -- in which case the declaration is now "
        "fiction -- or the producer moved and this entry needs to follow it."
    )


@pytest.mark.parametrize("key", sorted(list(SERVER_EXTRAS) + list(CLIENT_ONLY)))
def test_no_exemption_outlives_its_field(key):
    """A stale entry is how an allowlist quietly becomes the contract."""
    name, field = key
    interfaces = _ts_interfaces()
    assert name in interfaces, name + " is no longer declared; drop this entry"
    assert field in interfaces[name], (
        name + "." + field + " is no longer declared in types.ts; drop this entry"
    )
    server = _server_fields()
    assert field not in server.get(name, set()), (
        name + "." + field + " is now a real field on the server. Remove the "
        "exemption so the gate above covers it."
    )


# -- the second half: the payload has to arrive under the declared name -----


def test_the_events_route_returns_the_payload_under_its_own_name():
    """Declaring `security_data` is no use if the endpoint sends `domain_data`.

    Both /events branches project the payload as a single generic key so one
    component can flatten any row. Every typed reader asks for the specific
    name, because that is what `NormalizedEvent` declares and what the
    websocket feed sends. Nothing bridged the two, so /events/cyber returned
    rows on which `e.security_data` was undefined -- and the panel's KEV count,
    its CVE detail block and `domain.ts`'s cyber test all read exactly that.
    """
    src = EVENTS_ROUTE.read_text(encoding="utf-8")
    assert 'item[column] = item["domain_data"]' in src, (
        "the route no longer names the payload; every typed reader of "
        "/events/* goes back to reading undefined"
    )
    assert 'column.endswith("_data")' in src, (
        'the "news" domain maps to `headline`, which is not a payload column'
    )


def test_the_scenario_body_is_read_by_the_name_the_table_gives_it():
    """`s.narrative || s.description` against a column called narrative_summary.

    The reasoning service writes the synthesis to `narrative_summary` and
    /scenarios returns the row as it stands. Neither name the card asked for
    has ever existed on either side, so every scenario in the feed rendered a
    headline above an empty paragraph.
    """
    feed = (ROOT / "frontend" / "src" / "components" / "IntelligenceFeed.tsx").read_text(
        encoding="utf-8"
    )
    assert "s.narrative_summary" in feed
    assert "s.narrative ||" not in feed
    assert "narrative_summary" in _table_columns("scenarios")


def test_the_scenario_subject_reaches_the_table():
    """Carried on the model since it was written, and never persisted.

    `Scenario` resolves `primary_entity_id` and `primary_entity_name` from
    `entity_ids`/`entity_names` in a validator on every scenario built. The
    table had no column for either and the insert named neither, so the feed's
    card and its detail modal -- both of which read `primary_entity_name` --
    showed "Multi-Entity" for every scenario the platform has produced.
    """
    columns = _table_columns("scenarios")
    assert {"primary_entity_id", "primary_entity_name"} <= columns
    insert = (ROOT / "services" / "reasoning" / "main.py").read_text(encoding="utf-8")
    block = insert[insert.index("INSERT INTO scenarios"):]
    block = block[: block.index("VALUES")]
    assert "primary_entity_id" in block and "primary_entity_name" in block, (
        "the columns exist and the insert does not name them, which is the "
        "state this test was written for"
    )


def test_the_scenario_status_filter_is_postgres_and_not_cypher():
    """`toLower()` is Cypher. This query runs against PostgreSQL.

    Every /scenarios request naming a status raised `function tolower(text)
    does not exist`, was caught by the route's own handler and returned as a
    500 -- so the feed's HYPOTHESIS / CONFIRMED / UNDER_REVISE / DENIED tabs
    rendered "NO ACTIVE SCENARIOS FOUND" whatever the table held.
    """
    src = SCENARIOS_ROUTE.read_text(encoding="utf-8")
    assert "toLower(status)" not in src
    assert "lower(status) = lower(" in src


def test_the_map_draws_no_arc_it_cannot_source():
    """`from_coords`/`to_coords` appear in no producer anywhere in the tree.

    The BGP layer plotted an arc between two names the platform has never
    emitted, so the toggle beside it always read "CYBER (0)". Nor is the arc
    derivable: a BGP event knows the origin AS and its country, and the other
    end is an announced prefix this deployment cannot place on a map.
    """
    tree = list((ROOT / "services").rglob("*.py")) + list((ROOT / "shared").rglob("*.py"))
    producers = [
        str(p.relative_to(ROOT))
        for p in tree
        if "__pycache__" not in p.parts
        and "from_coords" in p.read_text(encoding="utf-8", errors="replace")
    ]
    map_src = (ROOT / "frontend" / "src" / "components" / "GlobalMap.tsx").read_text(
        encoding="utf-8"
    )
    if producers:
        assert "from_coords" in map_src, (
            str(producers) + " now emit from_coords; the map layer that read "
            "it was removed and should come back"
        )
    else:
        assert "d.from_coords" not in map_src, (
            "the map reads from_coords and nothing in the platform produces it"
        )
