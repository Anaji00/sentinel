# services/enrichment/db_writer.py (AFTER)
import json
import logging 
from datetime import datetime
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import execute_values
from shared.models import NormalizedEvent

# Adapt numpy arrays to standard Postgres tuples natively
def adapt_numpy_array(arr):
    return AsIs(tuple(arr))

register_adapter(np.ndarray, adapt_numpy_array)
logger = logging.getLogger("enrichment.db")

class DBWriter:
    def __init__(self, timescale_client):
        self.db = timescale_client

    def write_event(self, event: NormalizedEvent):
        data = event.to_tuple()
        try:
            # FIX: Explicit column-to-variable mapping.
            sql = """
                INSERT INTO events (
                    event_id, type, occurred_at, collected_at, source, source_reliability,
                    primary_entity_id, primary_entity_type, primary_entity_name, primary_entity_flags,
                    coordinates, region, country_code, headline, summary, url,
                    vessel_data, flight_data, financial_data, security_data,
                    tags, named_entities, sentiment, anomaly_score, correlation_ids
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (event_id, occurred_at) DO NOTHING
            """
            self.db.execute(sql, data)
        except Exception as e:
            logger.error(f"Failed to write event {event.event_id} to DB: {e}", exc_info=True)
            raise
    
    def write_events_batch(self, events: list[NormalizedEvent]):
        if not events:
            return
            
        values = [e.to_tuple() for e in events]

        # FIX: The target schema is strictly defined.
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

        # FIX: The custom template safely wraps the %s tuple iteration 
        # around the ST_MakePoint function for every single row in the matrix.
        template = """(
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )"""

        conn = self.db._pool.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, query, values, template=template, page_size=1000)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to batch write {len(events)} events: {e}", exc_info=True)
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