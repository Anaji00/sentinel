"""
services/api-gateway/routes/events.py

This file defines the API endpoints for raw domain events.
It allows users to fetch lists of recent events filtered by domain (maritime, cyber, etc.)
and anomaly score, or dive deep into the full JSON payload of a single specific event.
"""

import logging
from fastapi import APIRouter, HTTPException, Query, Depends
from services.api_gateway.dependencies import get_db

logger = logging.getLogger("api-gateway.events")
router = APIRouter(prefix="/api/v1/events", tags=["Domain Events"])

@router.get("/{domain}")
async def get_domain_events(
    # PATH PARAMETER: FastAPI extracts `domain` from the URL path (e.g., /api/v1/events/maritime)
    domain: str, 
    # QUERY PARAMETERS: Extracted from the URL after the '?' (e.g., ?limit=100&min_anomaly=0.5)
    # FastAPI automatically rejects requests where limit > 500 or min_anomaly is outside 0.0-1.0.
    limit: int = Query(50, le=500),
    min_anomaly: float = Query(0.0, ge=0.0, le=1.0),
    # DEPENDENCY INJECTION: Grabs a warm database connection for this request.
    db = Depends(get_db)
):
    """Dynamic endpoint to fetch events for a specific domain."""
    
    # SECURITY BEST PRACTICE: The Allowlist (Strict Validation)
    valid_domains = ["maritime", "aviation", "financial", "cyber", "news"]
    domain = domain.lower()
    if domain not in valid_domains:
        raise HTTPException(status_code=400, detail=f"Invalid domain. Must be one of {valid_domains}")

    try:
        # DYNAMIC SQL BUILDING: 
        # Wait, didn't we say "Never use f-strings in SQL"? 
        # We use an f-string for the column name `{domain}_data` because standard `%s` 
        # placeholders CANNOT be used for column or table names, only for data values.
        # This is strictly safe ONLY because we mathematically guaranteed `domain` is 
        # one of our 5 hardcoded safe strings in the allowlist check above.
        
        # QUERY ANATOMY:
        # 1. SELECT: Grabs common fields + the specific JSONB column for this domain (e.g., `maritime_data`).
        # 2. WHERE type ILIKE %s: `ILIKE` is PostgreSQL's command for a Case-Insensitive match.
        # 3. ORDER & LIMIT: Sorts by newest first, cutting off at the user's requested limit.
        query = f"""
            SELECT event_id, type, occurred_at, primary_entity_name, region, anomaly_score, {domain}_data as domain_data
            FROM events 
            WHERE type ILIKE %s AND anomaly_score >= %s
            ORDER BY occurred_at DESC LIMIT %s
        """
        # SQL WILDCARD: The `%` symbol in SQL means "anything can go here".
        # So `maritime_%` matches "maritime_vessel_dark" or "maritime_position".
        like_pattern = f"{domain}_%"
        return db.query(query, (like_pattern, min_anomaly, limit))
    except Exception as e:
        logger.error(f"Failed to fetch {domain} events: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

@router.get("/detail/{event_id}")
async def get_event_detail(event_id: str, db = Depends(get_db)):
    """Fetch the complete JSON payload for a single specific event."""
    try:
        # Simple, secure parameterized query to fetch one specific record by its UUID.
        # 
        # THE PYTHON TUPLE GOTCHA:
        # Notice the comma in `(event_id,)`? In Python, `("text")` is just a string in parentheses. 
        # To create a tuple with exactly ONE item, you MUST include a trailing comma. 
        # If you forget it, the database driver will crash trying to read the string character-by-character!
        result = db.query("SELECT * FROM events WHERE event_id = %s", (event_id,))
        if not result:
            # Standard REST practice: Return a 404 Not Found if the ID doesn't exist.
            raise HTTPException(status_code=404, detail="Event not found")
        # `.query()` returns a list of dictionaries. We only want the first (and only) item.
        return result[0]
    except HTTPException:
        # If it's a 404 we raised intentionally, let it pass through to the user.
        raise
    except Exception as e:
        logger.error(f"Failed to fetch event details: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")