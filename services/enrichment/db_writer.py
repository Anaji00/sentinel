import logging 
import uuid
from datetime import datetime
from shared.models import NormalizedEvent

logger = logging.getLogger("enrichment.db")

def _as_uuid(value) -> str:
    """A UUID string, deterministically derived when the value is not one.

    Several producers mint readable identifiers and several columns are typed
    uuid. Rather than each caller knowing which is which, anything that is
    already a UUID passes through and anything else is mapped through uuid5 --
    stable, so the same input always yields the same UUID and any join on it
    still holds.
    """
    try:
        uuid.UUID(str(value))
        return str(value)
    except (ValueError, TypeError, AttributeError):
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(value)))


class DBWriter:
    def __init__(self, timescale_client):
        self.db = timescale_client

    def _extract_tuple(self, e: NormalizedEvent) -> tuple:
        def _dump(attr):
            val = getattr(e, attr, None)
            if not val:
                return None
            # Most payloads are pydantic models; corroboration is a plain dict
            # already in its final shape. Assuming model_dump() exists raised on
            # the first plain-dict field added to this table.
            return val.model_dump() if hasattr(val, "model_dump") else val
        
        pe = e.primary_entity

        event_id = _as_uuid(e.event_id)

        pe_id = pe.id if pe else "UNKNOWN"
        pe_name = (pe.name if (pe and pe.name) else pe_id) or "UNKNOWN"

        lat = getattr(e, 'latitude', None)
        if lat is None and getattr(e, 'vessel_data', None):
            lat = getattr(e.vessel_data, 'latitude', None)
        if lat is None and getattr(e, 'flight_data', None):
            lat = getattr(e.flight_data, 'latitude', None)

        lon = getattr(e, 'longitude', None)
        if lon is None and getattr(e, 'vessel_data', None):
            lon = getattr(e.vessel_data, 'longitude', None)
        if lon is None and getattr(e, 'flight_data', None):
            lon = getattr(e.flight_data, 'longitude', None)

        return (
            event_id,
            e.type.value if hasattr(e.type, 'value') else e.type,
            e.occurred_at,
            getattr(e, 'collected_at', datetime.now()),
            e.source,
            getattr(e, 'source_reliability', 1.0),
            pe_id,
            pe.type.value if (pe and hasattr(pe.type, 'value')) else (pe.type if pe else "unknown"),
            pe_name,
            getattr(pe, 'flags', []),
            lon,
            lat,
            getattr(e, 'region', None),
            getattr(e, 'country_code', None),
            getattr(e, 'headline', None),
            getattr(e, 'summary', None),
            getattr(e, 'url', None),
            _dump('vessel_data'),
            _dump('flight_data'),
            _dump('financial_data'),
            _dump('security_data'),
            _dump('prediction_market_data'),
            _dump('crypto_data'),
            _dump('cyber_data'),
            _dump('supply_chain_data'),
            _dump('filing_data'),
            getattr(e, 'tags', []),
            getattr(e, 'named_entities', []),
            getattr(e, 'sentiment', None),
            getattr(e, 'anomaly_score', 0.0),
            # Coerced, not trusted.
            #
            # correlation_ids is a uuid[] column, and the statistical discovery
            # engine mints semantic identifiers -- "granger:AAPL:MSFT:lag2",
            # "corr:stat:NVDA:TSM" -- which the tradfi enricher attaches to
            # every event for a ticker that has one. asyncpg rejects the whole
            # executemany batch on the first such value, so a single discovered
            # correlation made every event batched beside it unwritable:
            # measured at 2,130 errors against 4,838 processed, a 44% loss rate,
            # with equity blocks disappearing while the collector was plainly
            # emitting them.
            #
            # uuid5 is deterministic, so the same semantic id always maps to the
            # same UUID and the linkage survives; the readable form is still in
            # Redis where it was minted. Dropping the value instead would lose
            # the link, and letting it through loses the entire batch.
            [_as_uuid(c) for c in (getattr(e, 'correlation_ids', None) or [])],
            # Independent corroboration, where the event type supports it.
            # Serialised rather than stored as a column per field: the shape is
            # a judgement about a claim, not a fixed record, and it is read
            # whole or not at all.
            _dump('corroboration'),
        )

    async def write_events_batch(self, events: list[NormalizedEvent]):
        if not events:
            return
            
        values = [self._extract_tuple(e) for e in events]

        # FIX: Include longitude ($11) and latitude ($12) columns explicitly in INSERT query
        query = """
            INSERT INTO events (
                event_id, type, occurred_at, collected_at, source, source_reliability,
                primary_entity_id, primary_entity_type, primary_entity_name, primary_entity_flags,
                longitude, latitude, region, country_code, headline, summary, url,
                vessel_data, flight_data, financial_data, security_data,
                prediction_market_data, crypto_data, cyber_data, supply_chain_data,
                filing_data,
                tags, named_entities, sentiment, anomaly_score, correlation_ids,
                coordinates, corroboration
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20, $21, $22, $23, $24,
                $25,
                $26,
                $27, $28, $29, $30, $31,
                CASE 
                    WHEN $11::float IS NOT NULL AND $12::float IS NOT NULL 
                    THEN ST_SetSRID(ST_MakePoint($11::float, $12::float), 4326)
                    ELSE NULL 
                END
            ,
                $32
            )
            ON CONFLICT (event_id, occurred_at) DO UPDATE SET
                latitude = COALESCE(EXCLUDED.latitude, events.latitude),
                longitude = COALESCE(EXCLUDED.longitude, events.longitude),
                coordinates = COALESCE(EXCLUDED.coordinates, events.coordinates),
                vessel_data = COALESCE(EXCLUDED.vessel_data, events.vessel_data),
                flight_data = COALESCE(EXCLUDED.flight_data, events.flight_data),
                filing_data = COALESCE(EXCLUDED.filing_data, events.filing_data),
                anomaly_score = GREATEST(EXCLUDED.anomaly_score, events.anomaly_score)
        """

        try:
            await self.db.execute_many(query, values)
        except Exception as e:
            logger.error(f"Failed to batch write {len(events)} events: {e}", exc_info=True)
            raise

    async def write_vessel_position(self, mmsi: str, lat: float, lon: float, speed: float, heading: int, nav_status: str, occurred_at: datetime):
        try:
            await self.db.execute("""
                INSERT INTO vessel_positions
                    (mmsi, occurred_at, lat, lon, speed_knots, heading, nav_status)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
            """, mmsi, occurred_at, lat, lon, speed, heading, nav_status)
        except Exception as e:
            logger.error(f"Failed to write vessel position for MMSI {mmsi} to DB: {e}")

    async def write_vessel_positions_batch(self, positions: list[tuple]):
        if not positions:
            return
        query = """
            INSERT INTO vessel_positions
                (mmsi, occurred_at, lat, lon, speed_knots, heading, nav_status)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT DO NOTHING
        """
        try:
            await self.db.execute_many(query, positions)
        except Exception as e:
            logger.error(f"Failed to batch write {len(positions)} vessel positions: {e}")