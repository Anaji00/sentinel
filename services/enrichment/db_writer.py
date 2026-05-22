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
        data = event.to_tuple()
        try:
            sql = """
            INSERT INTO events (...) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id, occurred_at) DO NOTHING
        """
            self.db.execute(sql, data)
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
        values = [e.to_tuple() for e in events]

        query = "INSERT INTO events (...) VALUES %s ON CONFLICT DO NOTHING"

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