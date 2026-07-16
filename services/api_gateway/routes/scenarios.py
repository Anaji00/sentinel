"""
services/api-gateway/routes/scenarios.py

This file defines the API endpoints (routes) for the 'Intelligence' section of our application.
It allows front-end dashboards or external systems to retrieve data about AI-generated scenarios 
and raw correlation clusters from the TimescaleDB database.
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Depends
from services.api_gateway.dependencies import get_db

logger = logging.getLogger("api-gateway.scenarios")
router = APIRouter(prefix="/api/v1", tags=["Intelligence"])

@router.get("/scenarios")
async def get_active_scenarios(
    # INPUT VALIDATION: FastAPI automatically validates that 'limit' is an integer
    # between 1 (ge=1) and 100 (le=100). If a user asks for 500, FastAPI returns a 422 Error automatically.
    limit: int = Query(20, ge=1, le=100),
    # OPTIONAL INPUT: The user doesn't have to provide a status. Defaults to None.
    status: Optional[str] = Query(None, description="e.g., HYPOTHESIS, CONFIRMED, DENIED"),
    # DEPENDENCY INJECTION: Automatically gets a warm DB connection for this specific request.
    db = Depends(get_db)
):
    """Fetch the latest AI-generated geopolitical scenarios."""
    try:
        query = "SELECT * FROM scenarios"
        params = []
        
        # DYNAMIC SQL BUILDING: We only add the WHERE clause if the user actually provided a status.
        # Notice we STILL use `%s` and append to a `params` list to maintain security 
        # against SQL injection, rather than doing `query += f" WHERE status = '{status}'"`
        if status:
            params.append(status)
            query += f" WHERE status = ${len(params)}"
        params.append(limit)
        query += f" ORDER BY created_at DESC LIMIT ${len(params)}"
        return await db.query(query, tuple(params))
    except Exception as e:
        logger.error(f"Error fetching scenarios: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")
    
@router.get("/correlations")
async def get_correlations(
    # Input validation: cap the maximum limit to 500 to prevent database overload.
    limit: int = Query(50, le=500),
    # The min_tier helps filter out the noise (like Tier 1 WATCH events) if the user only wants critical alerts.
    min_tier: int = Query(1, description="Minimum alert tier (1=WATCH, 2=ALERT, 3=INTEL)"),
    db = Depends(get_db)
):
    """Fetch raw correlation clusters before AI scenario generation."""
    try:
        return await db.query("""
            SELECT correlation_id, rule_name, alert_tier, detected_at, description, tags 
            FROM correlations 
            WHERE alert_tier >= $1 
            ORDER BY detected_at DESC LIMIT $2
        """, min_tier, limit)
    except Exception as e:
        logger.error(f"Failed to fetch correlations: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")