"""
services/agents/oil_flow_job.py

Hourly oil-flow indices (shared/utils/oil_flow.py) into oil_flow_daily, and a
report in REPORT_KEY for the API.

1. Ship registry. The enricher's `vessel:info:{mmsi}` cache (name, class,
   draught) expires after ten days; each pass copies it into vessel_registry
   and widens each ship's seen draught range, which is what a laden call needs.
2. Daily measures, today and yesterday every pass, and any day missing from
   the last BACKFILL_DAYS once:
   - Singapore Strait gate transits, all ships and tankers by direction, and
     loaded tankers by direction (live days only: the draught is the one the
     registry holds now, so a backfilled day would be judged by a later load);
   - tankers present at the ports AIS covers, and how many are loaded;
   - Sentinel-1 target density and radar contacts with no AIS, per site;
   - dark-ship and ship-to-ship events per region, and ship-to-ship by tankers.
3. Report: each series' latest value against its own history, the episodes
   (|z| >= EPISODE_Z), lead-lag against Brent, weekly sums against EIA's US
   crude-stock change, and what each site can and cannot be measured with.

No model calls and no alerts: this is a measurement layer for people and for
the agents that read the report.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from shared.utils import oil_flow as of
from shared.utils.quiet_failures import dropped, swallowed

logger = logging.getLogger("agent.oil_flow")

INTERVAL_SEC = 3600
BACKFILL_DAYS = 45
HISTORY_DAYS = 120
REPORT_KEY = "sentinel:oil_flow:report"
VESSEL_INFO_MATCH = "vessel:info:*"        # written by services/enrichment/enrichers/maritime.py
SAR_SOURCE = "copernicus_sentinel1"
PRICE_SYMBOL = "BZ=F"
EIA_SERIES = "crude_commercial"

_REGISTRY_SQL = """
    INSERT INTO vessel_registry AS r (mmsi, name, vessel_type, imo, draught, min_draught, max_draught, updated_at)
    SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[], $5::float8[], $5::float8[],
                         $6::timestamptz[])
    ON CONFLICT (mmsi) DO UPDATE SET
        name = COALESCE(EXCLUDED.name, r.name),
        vessel_type = COALESCE(EXCLUDED.vessel_type, r.vessel_type),
        imo = COALESCE(EXCLUDED.imo, r.imo),
        draught = COALESCE(EXCLUDED.draught, r.draught),
        min_draught = LEAST(r.min_draught, EXCLUDED.min_draught),
        max_draught = GREATEST(r.max_draught, EXCLUDED.max_draught),
        updated_at = EXCLUDED.updated_at
"""
_UPSERT_SQL = """
    INSERT INTO oil_flow_daily (day, site, metric, value, n, source)
    SELECT * FROM unnest($1::date[], $2::text[], $3::text[], $4::float8[], $5::int[], $6::text[])
    ON CONFLICT (day, site, metric) DO UPDATE SET value = EXCLUDED.value, n = EXCLUDED.n, source = EXCLUDED.source
"""
_POSITIONS_SQL = """
    SELECT mmsi, occurred_at, lat, lon FROM vessel_positions
    WHERE lat BETWEEN $1 AND $2 AND lon BETWEEN $3 AND $4 AND occurred_at >= $5 AND occurred_at < $6
    ORDER BY mmsi, occurred_at
"""
_PORT_SQL = """
    SELECT DISTINCT mmsi FROM vessel_positions
    WHERE lat BETWEEN $1 AND $2 AND lon BETWEEN $3 AND $4 AND occurred_at >= $5 AND occurred_at < $6
"""
_SAR_SQL = "SELECT headline, summary FROM events WHERE source = $1 AND occurred_at >= $2 AND occurred_at < $3"
_DARK_SQL = """
    SELECT region, type, count(*) AS n, count(DISTINCT vessel_data->>'mmsi') AS ships,
           count(*) FILTER (WHERE vessel_data->>'vessel_type' = 'Tanker') AS tankers
    FROM events WHERE type IN ('vessel_dark', 'vessel_sts') AND occurred_at >= $1 AND occurred_at < $2
      AND region IS NOT NULL
    GROUP BY region, type
"""
# A day is measured once it has the gate's receiver volume, the last metric added.
_DONE_SQL = "SELECT day FROM oil_flow_daily WHERE day >= $1 AND site = $2 AND metric = 'ais_positions'"
_SERIES_SQL = "SELECT day, site, metric, value, n, source FROM oil_flow_daily WHERE day >= $1 ORDER BY day"
_BRENT_SQL = "SELECT day, close FROM macro_daily_bars WHERE symbol = $1 AND day >= $2"
_EIA_SQL = "SELECT week_ending, value FROM eia_weekly WHERE series = $1 AND week_ending >= $2"

Row = Tuple[date, str, str, float, Optional[int], str]
# The series the price and EIA comparisons are run on: the ones with a
# physical reason to lead crude, and enough days to say anything.
_LINKED = (("singapore_strait", "tanker_transits"), ("singapore_strait", "laden_tanker_transits_west"),
           ("singapore_anchorages", "tankers_present"), ("jebel_ali", "tankers_present"),
           ("strait_of_hormuz", "radar_density"), ("kharg_island", "radar_density"),
           ("ras_tanura", "radar_density"), ("fujairah_anchorage", "radar_density"),
           ("primorsk", "radar_density"), ("novorossiysk", "radar_density"),
           ("iranian_territorial", "dark_events"), ("singapore_approach", "sts_events"))


def _f(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _ts(x: Any) -> datetime:
    """The DB wrapper returns timestamps as ISO strings."""
    t = x if isinstance(x, datetime) else datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def _day_bounds(day: date) -> Tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


class OilFlowJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)
        self._episodes_logged: Set[Tuple[str, str, str]] = set()

    async def run(self) -> None:
        await asyncio.sleep(180)
        while True:
            try:
                rep = await self.run_once()
                logger.info("Oil flow: %d series, %d episodes, registry %s",
                            len(rep["series"]), len(rep["episodes"]), rep["registry"])
            except Exception as e:
                swallowed("agents.oil_flow.pass", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        registry = await self._snapshot_registry(now)
        today = now.date()
        done = {r["day"] for r in await self.db.query(_DONE_SQL, today - timedelta(days=BACKFILL_DAYS),
                                                          of.GATES[0].name) or []}
        days = [today - timedelta(days=i) for i in range(BACKFILL_DAYS, -1, -1)]
        for d in days:
            live = d >= today - timedelta(days=1)
            if not live and d in done:
                continue
            try:
                await self._store(await self._measure(d, live))
            except Exception as e:
                swallowed("agents.oil_flow.day", e, logger, detail=d.isoformat())
        rep = await self._report(now, registry)
        await self.raw.set(REPORT_KEY, json.dumps(rep, default=str))
        return rep

    async def _snapshot_registry(self, now: datetime) -> Dict[str, int]:
        keys = [k async for k in self.raw.scan_iter(match=VESSEL_INFO_MATCH, count=1000)]
        cols: Tuple[List[Any], ...] = ([], [], [], [], [], [])
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            for k, blob in zip(chunk, await self.raw.mget(chunk)):
                if not blob:
                    continue
                try:
                    info = json.loads(blob)
                except ValueError:
                    dropped("agents.oil_flow.registry", "unparseable vessel:info", logger)
                    continue
                key = k.decode() if isinstance(k, bytes) else str(k)
                for col, val in zip(cols, (key.rsplit(":", 1)[-1], info.get("name") or None,
                                           info.get("vessel_type") or None,
                                           str(info["imo"]) if info.get("imo") else None,
                                           _f(info.get("draught")), now)):
                    col.append(val)
        if cols[0]:
            await self.db.execute(_REGISTRY_SQL, *cols)
        row = await self.db.query_one(
            "SELECT count(*) AS ships, count(*) FILTER (WHERE vessel_type = 'Tanker') AS tankers, "
            "count(*) FILTER (WHERE max_draught >= min_draught * $1) AS ranged FROM vessel_registry",
            of.MIN_RANGE)
        return {"cached": len(cols[0]), **{k: int(v) for k, v in dict(row or {}).items()}}

    async def _ships(self, mmsis: Set[str]) -> Dict[str, Dict[str, Any]]:
        if not mmsis:
            return {}
        rows = await self.db.query(
            "SELECT mmsi, vessel_type, draught, min_draught, max_draught FROM vessel_registry WHERE mmsi = ANY($1)",
            list(mmsis))
        return {str(r["mmsi"]): dict(r) for r in rows or []}

    def _state(self, ship: Optional[Dict[str, Any]]) -> Optional[str]:
        if not ship:
            return None
        return of.draught_state(ship.get("draught"), ship.get("min_draught"), ship.get("max_draught"))

    async def _measure(self, day: date, live: bool) -> List[Row]:
        start, end = _day_bounds(day)
        rows: List[Row] = []
        for gate in of.GATES:
            la0, la1, lo0, lo1 = gate.box
            pos = await self.db.query(_POSITIONS_SQL, la0, la1, lo0, lo1,
                                      start - timedelta(hours=of.MAX_STEP_H), end)
            tracks: Dict[str, List[Tuple[datetime, float, float]]] = defaultdict(list)
            for p in pos or []:
                tracks[str(p["mmsi"])].append((_ts(p["occurred_at"]), float(p["lat"]), float(p["lon"])))
            crossings = [(m, d) for m, tr in tracks.items() for t, d in of.transits(tr, gate) if t >= start]
            ships = await self._ships({m for m, _ in crossings})
            tankers = [(m, d) for m, d in crossings if (ships.get(m) or {}).get("vessel_type") == "Tanker"]
            known = sum(1 for m, _ in crossings if (ships.get(m) or {}).get("vessel_type"))
            src = "ais_gate"
            rows += [(day, gate.name, "ais_positions", float(len(pos or [])), len(tracks), src),
                     (day, gate.name, "transits", float(len(crossings)), known, src),
                     (day, gate.name, "tanker_transits", float(len(tankers)), known, src)]
            for direction in (gate.positive, gate.negative):
                rows.append((day, gate.name, f"tanker_transits_{direction}",
                             float(sum(1 for _, d in tankers if d == direction)), known, src))
            if live:
                states = [(d, self._state(ships.get(m))) for m, d in tankers]
                judged = sum(1 for _, s in states if s)
                for direction in (gate.positive, gate.negative):
                    rows.append((day, gate.name, f"laden_tanker_transits_{direction}",
                                 float(sum(1 for d, s in states if d == direction and s == "laden")), judged, src))
        for port in of.PORTS:
            la0, la1, lo0, lo1 = port.box
            present = {str(r["mmsi"]) for r in await self.db.query(_PORT_SQL, la0, la1, lo0, lo1, start, end) or []}
            ships = await self._ships(present)
            tankers = [m for m in present if (ships.get(m) or {}).get("vessel_type") == "Tanker"]
            rows.append((day, port.name, "tankers_present", float(len(tankers)), len(present), "ais_port"))
            if live:
                states = [self._state(ships.get(m)) for m in tankers]
                judged = sum(1 for s in states if s)
                if judged:
                    rows.append((day, port.name, "laden_share",
                                 sum(1 for s in states if s == "laden") / judged, judged, "ais_port"))
        rows += await self._radar(day, start, end)
        for r in await self.db.query(_DARK_SQL, start, end) or []:
            site = of.site_key(str(r["region"]))
            kind = "dark" if r["type"] == "vessel_dark" else "sts"
            rows += [(day, site, f"{kind}_events", float(r["n"]), int(r["ships"]), "ais_behaviour"),
                     (day, site, f"{kind}_tanker_events", float(r["tankers"]), int(r["ships"]), "ais_behaviour")]
        return rows

    async def _radar(self, day: date, start: datetime, end: datetime) -> List[Row]:
        by_site: Dict[Tuple[str, str], List[of.SarReading]] = defaultdict(list)
        for e in await self.db.query(_SAR_SQL, SAR_SOURCE, start, end) or []:
            reading = of.parse_sar(str(e["headline"] or ""), str(e["summary"] or ""))
            if reading is None:
                dropped("agents.oil_flow.sar", "not a calibrated reading", logger)
                continue
            by_site[(reading.site, reading.calibration)].append(reading)
        rows: List[Row] = []
        for (site, cal), readings in by_site.items():
            targets, water = sum(r.targets for r in readings), sum(r.water for r in readings)
            src = f"sentinel1:{cal}"
            rows.append((day, site, "radar_density", targets / water, targets, src))
            no_ais = [r.no_ais for r in readings if r.no_ais is not None]
            if no_ais:
                rows.append((day, site, "radar_no_ais", float(max(no_ais)), len(no_ais), src))
        return rows

    async def _store(self, rows: List[Row]) -> None:
        if rows:
            await self.db.execute(_UPSERT_SQL, *(list(c) for c in zip(*rows)))

    async def _report(self, now: datetime, registry: Dict[str, int]) -> Dict[str, Any]:
        since = now.date() - timedelta(days=HISTORY_DAYS)
        series: Dict[Tuple[str, str], Dict[date, float]] = defaultdict(dict)
        meta: Dict[Tuple[str, str], Tuple[Optional[int], str]] = {}
        ships_seen: Dict[str, Dict[date, float]] = defaultdict(dict)
        for r in await self.db.query(_SERIES_SQL, since) or []:
            key = (str(r["site"]), str(r["metric"]))
            if key[1] == "tankers_present" and r["n"] is not None:
                ships_seen[key[0]][r["day"]] = float(r["n"])
            # A radar series is one calibration: a new threshold starts a new series.
            if key in meta and meta[key][1] != r["source"] and str(r["source"]).startswith("sentinel1:"):
                series[key].clear()
            series[key][r["day"]] = float(r["value"])
            meta[key] = (r["n"], str(r["source"]))
        # Receiver volume per AIS site: the gate's positions, a port's ships seen.
        gaps: Dict[str, Set[date]] = {}
        for gate in of.GATES:
            gaps[gate.name] = of.coverage_gaps(series.get((gate.name, "ais_positions"), {}))
        for port in of.PORTS:
            gaps[port.name] = of.coverage_gaps(ships_seen.get(port.name, {}))
        latest, episodes = [], []
        for (site, metric), s in sorted(series.items()):
            radar = metric.startswith("radar_")
            # A count for today is a partial day; a radar pass is a snapshot.
            days = [d for d in s if radar or d < now.date()]
            if not days:
                continue
            last = max(days)
            gap = gaps.get(site, set())
            hist = [v for d, v in s.items() if d != last and d < now.date() and d not in gap]
            z = None if last in gap else of.zscore(s[last], hist)
            item = {"site": site, "metric": metric, "day": last, "value": round(s[last], 6),
                    "n": meta[(site, metric)][0], "source": meta[(site, metric)][1],
                    "history_days": len(hist), "coverage_gap": last in gap,
                    "z": None if z is None else round(z, 2)}
            latest.append(item)
            if z is not None and abs(z) >= of.EPISODE_Z and last >= now.date() - timedelta(days=2):
                episodes.append(item)
                tag = (site, metric, last.isoformat())
                if tag not in self._episodes_logged:
                    self._episodes_logged.add(tag)
                    logger.warning("Oil-flow episode: %s %s = %.4g (z=%.1f on %d days)",
                                   site, metric, s[last], z, len(hist))
        brent = {r["day"]: float(r["close"]) for r in await self.db.query(_BRENT_SQL, PRICE_SYMBOL, since) or []}
        brent_ret = of.changes(brent, log=True)
        eia = {r["week_ending"]: float(r["value"]) for r in await self.db.query(_EIA_SQL, EIA_SERIES, since) or []}
        eia_chg = of.changes(eia)
        links = []
        for key in _LINKED:
            gap = gaps.get(key[0], set())
            s = {d: v for d, v in series.get(key, {}).items() if d < now.date() and d not in gap}
            if not s:
                continue
            ll = of.lead_lag(of.changes(s), brent_ret)
            scored = [x for x in ll if x["r"] is not None]
            best = max(scored, key=lambda x: abs(x["r"] or 0.0), default=None)
            beats_chance = bool(best and abs(best["r"] or 0.0) >= of.chance_r(best["n"]))
            weekly = of.weekly_sum(s, sorted(eia_chg))
            eia_r = of.lead_lag(of.changes(weekly), eia_chg, max_lag=0)[0]
            links.append({"site": key[0], "metric": key[1], "days": len(s), "brent_best": best,
                          "brent_beats_chance": beats_chance, "brent_lags": ll, "eia_weekly_r": eia_r["r"], "eia_weeks": eia_r["n"]})
        coverage = {}
        recent = now.date() - timedelta(days=30)
        for (site, metric), s in series.items():
            c = coverage.setdefault(site, {"metrics": {}, "note": of.COVERAGE_NOTES.get(site)})
            c["metrics"][metric] = sum(1 for d in s if d >= recent)
        return {"generated_at": now.isoformat(), "registry": registry, "series": latest, "episodes": episodes,
                "links": links, "coverage": coverage,
                "caveats": ["AIS gate counts are a relative index: shore receivers see about a tenth of "
                            "Singapore Strait traffic and almost none of Hormuz or Bab-el-Mandeb.",
                            "Radar density is a target density, not a ship count; compare a site only with itself.",
                            "The best of eleven lags is reported; brent_beats_chance says whether its r clears "
                            "what chance gives the best of eleven on that many days.",
                            "Days when the receivers went quiet are coverage gaps, left out of every baseline.",
                            "EIA crude stocks are US; the comparison tests a global flow signal, not a direct link."]}


__all__ = ["REPORT_KEY", "OilFlowJob"]
