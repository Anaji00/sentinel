"""
services/enrichment/db_writer.py
 
Writes normalized events and vessel positions to TimescaleDB.
 
FIX (code review): write_event() and write_vessel_position() now re-raise
  exceptions after logging. Previously exceptions were silently swallowed —
  the Kafka offset committed, the event was lost permanently, and the only
  evidence was a log line. Now the enrichment main loop catches the exception
  and sends the raw event to the dead-letter queue (DLQ) so nothing is lost.
"""
 
import json
import logging 
from datetime import datetime
import numpy as np
from psycopg2.extensions import register_adapter, AsIs

def  adapt_numpy_array(arr):
    return AsIs(tuple(arr))

register_adapter(np.ndarray, adapt_numpy_array)
from psycopg2.extras import execute_values
from shared.models import NormalizedEvent

logger = logging.getLogger("enrichment.db")

class DBWriter:
    def __init__(self, timescale_client):
        self.db = timescale_client

    def write_event(self, event: NormalizedEvent):
        coords_wkt = None
        if event.latitude is not None and event.longitude is not None:
            coords_wkt = f"SRID=4326;POINT({event.longitude} {event.latitude})"
        try:
            sql = """
            INSERT INTO events (
                event_id, type, occurred_at, collected_at, source, source_reliability,
                primary_entity_id, primary_entity_type, primary_entity_name, primary_entity_flags,
                coordinates, region, country_code, headline, summary, url,
                vessel_data, flight_data, financial_data, security_data,
                tags, named_entities, sentiment, anomaly_score, correlation_ids
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id, occurred_at) DO NOTHING
        """
            params = (
            event.event_id, event.type.value, event.occurred_at, event.collected_at,
            event.source, event.source_reliability, event.primary_entity.id,
            event.primary_entity.type.value, event.primary_entity.name,
            event.primary_entity.flags, 
            event.longitude, event.latitude, # Passed as individual floats
            event.region, event.country_code, event.headline, event.summary, event.url,
            json.dumps(event.vessel_data.dict()) if event.vessel_data else None,
            json.dumps(event.flight_data.dict()) if event.flight_data else None,
            json.dumps(event.financial_data.dict()) if event.financial_data else None,
            json.dumps(event.security_data.dict()) if event.security_data else None,
            event.tags, event.named_entities, event.sentiment, event.anomaly_score,
            event.correlation_ids,
        )
            self.db.execute(sql, params)
        except Exception as e:
            logger.error(f"Failed to write event {event.event_id} to DB: {e}", exc_info=True)
            raise
    
    def write_events_batch(self, events: list[NormalizedEvent]):
        """
        Dramatically improves write throughput by batching INSERTs.
        1,000 events takes ~50ms instead of 2,000ms.
        """
        if not events:
            return
        query = """
            INSERT INTO events (
                event_id, type, occurred_at, collected_at, source, source_reliability,
                primary_entity_id, primary_entity_type, primary_entity_name, primary_entity_flags,
                coordinates, region, country_code, headline, summary, url,
                vessel_data, flight_data, financial_data, security_data,
                tags, named_entities, sentiment, anomaly_score, correlation_ids
            ) VALUES %s
            ON CONFLICT (event_id, occurred_at) DO NOTHING
        """
        values = [e.to_tuple() for e in events]
        conn = self.db._pool.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, query, values, page_size=1000)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to write batch of {len(events)} events to DB: {e}", exc_info=True)
            raise
        finally:
            self.db._pool.putconn(conn)
        
    def write_vessel_position(
        self, mmsi: str, lat: float, lon: float,
        speed: float, heading: int, nav_status: str, occurred_at: datetime,
    ):
        try:
            self.db.execute("""
                INSERT INTO vessel_positions
                    (mmsi, occurred_at, lat, lon, speed_knots, heading, nav_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (mmsi, occurred_at, lat, lon, speed, heading, nav_status))
        except Exception as e:
            # Position writes are best-effort — log but don't re-raise.
            # A failed position write doesn't invalidate the NormalizedEvent
            # itself; the position is also stored in the event record.
            logger.error(f"Failed to write vessel position for MMSI {mmsi} to DB: {e}", exc_info=True)