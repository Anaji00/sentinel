import logging 
from datetime import datetime
import json
from shared.models import NormalizedEvent

logger = logging.getLogger("enrichment.db")

class DBWriter:
    def __init__(self, timescale_client):
        self.db = timescale_client

    def _extract_tuple(self, e: NormalizedEvent) -> tuple:
        def _dump(attr):
            val = getattr(e, attr, None)
            # Return dict, asyncpg's JSONB codec handles the stringification
            return val.model_dump() if val else None
        
        pe = e.primary_entity

        return (
            e.event_id,
            e.type.value if hasattr(e.type, 'value') else e.type,
            e.occurred_at,
            getattr(e, 'collected_at', datetime.now()),
            e.source,
            getattr(e, 'source_reliability', 1.0),
            pe.id,
            pe.type.value if hasattr(pe.type, 'value') else pe.type,
            pe.name,
            getattr(pe, 'flags', []),
            getattr(e, 'longitude', None),
            getattr(e, 'latitude', None),
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
            getattr(e, 'tags', []),
            getattr(e, 'named_entities', []),
            getattr(e, 'sentiment', None),
            getattr(e, 'anomaly_score', 0.0),
            getattr(e, 'correlation_ids', [])
        )

    async def write_events_batch(self, events: list[NormalizedEvent]):
        if not events:
            return
            
        values = [self._extract_tuple(e) for e in events]

        # FIX: asyncpg uses $1, $2 instead of %s.
        # ST_SetSRID parses the float parameters correctly.
        query = """
            INSERT INTO events (
                event_id, type, occurred_at, collected_at, source, source_reliability,
                primary_entity_id, primary_entity_type, primary_entity_name, primary_entity_flags,
                region, country_code, headline, summary, url,
                vessel_data, flight_data, financial_data, security_data,
                prediction_market_data, crypto_data, cyber_data,
                tags, named_entities, sentiment, anomaly_score, correlation_ids,
                coordinates
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $13, $14, $15, $16, $17,
                $18, $19, $20, $21, $22, $23, $24,
                $25, $26, $27, $28, $29,
                ST_SetSRID(ST_MakePoint(COALESCE($11::float, 0.0), COALESCE($12::float, 0.0)), 4326)
            )
            ON CONFLICT (event_id, occurred_at) DO NOTHING
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